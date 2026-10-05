"""Entity seed from the trimmed real backfill, ESI refresh against a fake, Parquet export."""

import bz2
import io
import json
import tarfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from evedw.domain.datasets import ENTITIES_BACKFILL
from evedw.domain.registry import ObjectStatus, RefreshEntry, RunStatus, Trigger
from evedw.domain.schemas import ATTACKERS, KILLMAILS
from evedw.jobs.entities import (
    BackfillImporter,
    ExportJob,
    RefreshJob,
    ids_from_killmails,
    seed_members,
)
from evedw.jobs.runner import JobRunner
from evedw.jobs.sync import SyncJob
from evedw.sources.archives import extract_members
from evedw.sources.esi import EsiClient, EsiStoppedError
from evedw.store.base import EntityStore
from tests.fake_esi import ESI_URL, FakeClock, FakeEsi, MemoryCache
from tests.helpers import BASE_URL, FIXTURES, TODAY, SyncEnv, md5

ARCHIVE = FIXTURES / "entities" / "eve-kill-com-karbowiak-2026-05-10.tar.bz2"
SNAPSHOT = date(2026, 5, 10)
SNAPSHOT_AT = datetime(2026, 5, 10, tzinfo=UTC)
MEMBERS = ("characters.json", "corporations.json", "alliances.json")


def fixture_records() -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    with tarfile.open(ARCHIVE, "r:bz2") as tar:
        for member in tar:
            fh = tar.extractfile(member)
            if fh is None:
                continue
            out[member.name.rsplit("/", 1)[-1]] = json.loads(fh.read())
    return out


@pytest.fixture
def members(tmp_path: Path) -> dict[str, Path]:
    found = extract_members(ARCHIVE, tmp_path / "x", MEMBERS)
    assert sorted(found) == sorted(MEMBERS)
    return found


# --- seed -------------------------------------------------------------------------------


def test_seed_counts_match_independent_json_counts(
    members: dict[str, Path], entities: EntityStore
) -> None:
    docs = fixture_records()
    counts = seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    assert counts.unknown_keys == {}
    expected = {
        "characters": len(docs["characters.json"]),
        "character_employment": sum(len(c["history"]) for c in docs["characters.json"]),
        "corporations": len(docs["corporations.json"]),
        "corporation_alliance_history": sum(len(c["history"]) for c in docs["corporations.json"]),
        "alliances": len(docs["alliances.json"]),
    }
    assert counts.rows == expected
    assert expected["characters"] == 6 and expected["character_employment"] == 75
    assert expected["corporations"] == 4 and expected["corporation_alliance_history"] == 36
    for table, count in expected.items():
        assert entities.count(table) == count


def test_seed_preserves_values(members: dict[str, Path], entities: EntityStore) -> None:
    docs = {c["character_id"]: c for c in fixture_records()["characters.json"]}
    seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    rows = {r["character_id"]: r for r in entities.lookup("characters", list(docs)).to_pylist()}
    assert rows.keys() == docs.keys()
    deleted = [cid for cid, c in docs.items() if c["deleted"]]
    assert deleted and all(rows[cid]["deleted"] for cid in deleted)
    no_birthday = next(cid for cid, c in docs.items() if c["birthday"] is None)
    assert rows[no_birthday]["birthday"] is None
    for cid, doc in docs.items():
        row = rows[cid]
        assert row["name"] == doc["name"]
        assert row["corporation_id"] == doc["corporation_id"]
        assert row["alliance_id"] == doc["alliance_id"]
        assert row["source"] == "everef_backfill:abc"
        # observed_at is the record's own update time, parsed from "+00" offsets.
        assert row["observed_at"] == datetime.fromisoformat(
            doc["updatedAt"].replace("+00", "+00:00")
        )
        if doc["birthday"]:
            assert row["birthday"] == datetime.fromisoformat(
                doc["birthday"].replace("+00", "+00:00")
            )
    # Employment events carry record ids and ISO start dates.
    rich = max(docs.values(), key=lambda c: len(c["history"]))
    events = entities.lookup("character_employment", [rich["character_id"]]).to_pylist()
    assert {e["record_id"] for e in events} == {h["record_id"] for h in rich["history"]}
    first = min(rich["history"], key=lambda h: h["record_id"])
    row = next(e for e in events if e["record_id"] == first["record_id"])
    assert row["corporation_id"] == first["corporation_id"]
    assert row["start_date"] == datetime.fromisoformat(first["start_date"].replace("Z", "+00:00"))
    # Corporation alliance history: null alliance_id means "left alliance".
    history = entities.lookup("corporation_alliance_history", [98000030]).to_pylist()
    assert len(history) == 14 and any(h["alliance_id"] is None for h in history)
    assert all(h["is_deleted"] is None for h in history)
    alliance = entities.lookup("alliances", [145667443]).to_pylist()[0]
    assert alliance["executor_corporation_id"] is None and alliance["name"]


def test_seed_reports_unknown_keys(
    members: dict[str, Path], entities: EntityStore, tmp_path: Path
) -> None:
    docs = json.loads(members["alliances.json"].read_text())
    docs[0]["new_field"] = 1
    members["alliances.json"].write_text(json.dumps(docs))
    chars = json.loads(members["characters.json"].read_text())
    chars[0]["history"][0]["extra"] = True
    members["characters.json"].write_text(json.dumps(chars))
    counts = seed_members(members, entities, source="x", snapshot=SNAPSHOT_AT)
    assert counts.unknown_keys == {"alliances": ["new_field"], "characters": ["history.extra"]}


def test_seed_is_idempotent_and_esi_wins(members: dict[str, Path], entities: EntityStore) -> None:
    seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    before = entities.lookup("characters", [90000059]).to_pylist()[0]
    newer = dict(before, name="Renamed", observed_at=TODAY_AT, source="esi")
    entities.upsert("characters", pa.Table.from_pylist([newer], schema=before_schema(entities)))
    seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    after = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert after["name"] == "Renamed" and after["source"] == "esi"
    assert entities.count("characters") == 6


TODAY_AT = datetime(2026, 10, 5, 12, tzinfo=UTC)


def before_schema(entities: EntityStore) -> pa.Schema:
    return entities.lookup("characters", [90000059]).schema


# --- seed through the registry ----------------------------------------------------------


LISTING_HTML = (
    '<html><body><table class="table table-sm"><thead><th>File</th></thead><tbody>'
    '<tr class="data-file"><td><a class="data-file-url" '
    'href="/characters-corporations-alliances/backfills/{name}">{name}</a></td>'
    '<td class="data-file-size-formatted text-right">4.31 KiB</td>'
    '<td class="data-file-size-bytes text-right">{size}</td>'
    '<td class="data-file-last-modified text-right">'
    '<time datetime="2026-05-10T11:04:43Z">2026-05-10 11:04:43 UTC</time></td></tr>'
    "</tbody></table></body></html>"
)


class FakeBackfills:
    def __init__(self, env: SyncEnv, data: bytes) -> None:
        self.data = data
        self.hits: dict[str, int] = {}
        prefix = f"{BASE_URL}/characters-corporations-alliances/backfills"
        env.router.route(method__in=["GET", "HEAD"], url__regex=rf"{prefix}.*").mock(
            side_effect=self._handle
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.hits[f"{request.method} {path}"] = self.hits.get(f"{request.method} {path}", 0) + 1
        if path.endswith("/index.json"):
            return httpx.Response(404)
        if path.endswith("/backfills/"):
            return httpx.Response(
                200, text=LISTING_HTML.format(name=ARCHIVE.name, size=f"{len(self.data):,}")
            )
        if path.endswith(ARCHIVE.name):
            headers = {
                "ETag": f'"{md5(self.data)}"',
                "Content-Length": str(len(self.data)),
                "Last-Modified": "Sun, 10 May 2026 11:04:43 GMT",
            }
            if request.method == "HEAD":
                return httpx.Response(200, headers=headers)
            return httpx.Response(200, content=self.data, headers=headers)
        return httpx.Response(404)


def test_seed_job_registers_archive_and_is_idempotent(env: SyncEnv, entities: EntityStore) -> None:
    fake = FakeBackfills(env, ARCHIVE.read_bytes())
    job = SyncJob(
        dataset=ENTITIES_BACKFILL,
        client=env.client,
        importer=BackfillImporter(entities),
        date_from=SNAPSHOT,
        date_to=SNAPSHOT,
        today=TODAY,
    )
    runner = JobRunner(env.settings, env.registry, env.lock)
    run = runner.run("entities:seed", job, trigger=Trigger.MANUAL)
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    assert run.rows_written == 6 + 4 + 3
    key = f"2026/{ARCHIVE.name}"
    obj = env.registry.get_object("entities_backfill", key)
    assert obj is not None and obj.status is ObjectStatus.IMPORTED
    assert obj.logical_date == SNAPSHOT and obj.current_revision == 1
    assert obj.upstream_etag == md5(ARCHIVE.read_bytes())  # from the HEAD, not the listing
    revision = env.registry.current_revision("entities_backfill", key)
    assert revision is not None and (env.settings.data_dir / revision.raw_path).is_file()
    assert entities.count("character_employment") == 75
    row = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert row["source"] == f"everef_backfill:{revision.sha256}"
    assert not list((env.settings.scratch_dir).iterdir())

    run = runner.run("entities:seed", job, trigger=Trigger.MANUAL)
    assert run.objects_changed == 0
    assert fake.hits[f"GET /characters-corporations-alliances/backfills/{ARCHIVE.name}"] == 1


def test_seed_job_rejects_archive_without_members(env: SyncEnv, entities: EntityStore) -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("readme.txt")
        info.size = 2
        tar.addfile(info, io.BytesIO(b"hi"))
    FakeBackfills(env, bz2.compress(buf.getvalue()))
    job = SyncJob(
        dataset=ENTITIES_BACKFILL,
        client=env.client,
        importer=BackfillImporter(entities),
        date_from=SNAPSHOT,
        date_to=SNAPSHOT,
        today=TODAY,
    )
    with pytest.raises(Exception, match="members missing"):
        JobRunner(env.settings, env.registry, env.lock).run(
            "entities:seed", job, trigger=Trigger.MANUAL
        )
    obj = env.registry.get_object("entities_backfill", f"2026/{ARCHIVE.name}")
    assert obj is not None and obj.status is ObjectStatus.FAILED


# --- refresh ----------------------------------------------------------------------------


def write_killmails(env: SyncEnv, day: date) -> None:
    killmails = pa.Table.from_pylist(
        [
            {
                "source_date": day,
                "killmail_id": 1,
                "victim_character_id": 90000059,
                "victim_corporation_id": 98000030,
                "victim_alliance_id": 99000001,
            },
            {"source_date": day, "killmail_id": 2, "victim_corporation_id": 1000001},
        ],
        schema=KILLMAILS,
    )
    attackers = pa.Table.from_pylist(
        [
            {
                "source_date": day,
                "killmail_id": 1,
                "ordinal": 0,
                "character_id": 90000089,
                "corporation_id": 98000030,
                "alliance_id": None,
            },
            {"source_date": day, "killmail_id": 1, "ordinal": 1, "corporation_id": 1000125},
        ],
        schema=ATTACKERS,
    )
    env.lake.write_partition("killmails", f"source_date={day}", killmails, metadata={})
    env.lake.write_partition("attackers", f"source_date={day}", attackers, metadata={})


def test_ids_from_killmails(env: SyncEnv) -> None:
    write_killmails(env, TODAY - timedelta(days=2))
    write_killmails(env, TODAY - timedelta(days=40))
    found = ids_from_killmails(env.lake, date_from=TODAY - timedelta(days=7))
    assert found == {
        "character": {90000059, 90000089},
        "corporation": {98000030, 1000001, 1000125},
        "alliance": {99000001},
    }


def character_body(name: str, corporation_id: int = 98000030) -> dict[str, Any]:
    return {
        "name": name,
        "corporation_id": corporation_id,
        "alliance_id": 99000001,
        "birthday": "2010-11-02T17:37:00Z",
        "security_status": -1.5,
        "bloodline_id": 1,
        "gender": "male",
        "race_id": 1,
        "achievement_score": 0,
    }


@pytest.fixture
def esi(env: SyncEnv) -> tuple[FakeEsi, FakeClock, EsiClient]:
    fake = FakeEsi(env.router)
    clock = FakeClock(now=TODAY_AT.timestamp())
    client = EsiClient(
        ESI_URL,
        compatibility_date="2026-08-18",
        contact="tests@example.test",
        cache=MemoryCache(),
        policy_path=env.settings.esi_policy_path,
        daily_budget=1000,
        client=httpx.Client(),
        sleep=clock.sleep,
        clock=clock.time,
    )
    return fake, clock, client


def serve_entities(fake: FakeEsi) -> None:
    fake.put("/characters/90000059", character_body("Victim"), etag="c1")
    fake.put(
        "/characters/90000059/corporationhistory",
        [
            {"record_id": 1, "corporation_id": 1000009, "start_date": "2010-11-02T17:28:00Z"},
            {"record_id": 2, "corporation_id": 98000030, "start_date": "2011-01-02T15:27:00Z"},
        ],
        etag="h1",
    )
    fake.put("/characters/90000089", character_body("Attacker", 1000001), etag="c2")
    fake.put("/characters/90000089/corporationhistory", [], etag="h2")
    corp: dict[str, Any] = {
        "name": "Corp",
        "ticker": "CORP",
        "alliance_id": 99000001,
        "ceo_id": 90000059,
        "member_count": 12,
        "date_founded": "2010-11-02T20:05:00Z",
        "state": "active",
        "description": "",
        "friendly_fire": "legal",
        "home_station_id": 60000001,
        "shares": 1000,
        "tax_rates": {},
        "type": "player_owned",
        "war_eligible": True,
    }
    for cid in (98000030, 1000001, 1000125):
        fake.put(f"/corporations/{cid}", dict(corp, name=f"Corp {cid}"), etag=f"k{cid}")
        fake.put(
            f"/corporations/{cid}/alliancehistory",
            [
                {"record_id": 5, "alliance_id": 99000001, "start_date": "2012-05-08T06:20:00Z"},
                {"record_id": 6, "start_date": "2012-11-26T16:14:00Z", "is_deleted": True},
            ],
            etag=f"a{cid}",
        )
    fake.put(
        "/alliances/99000001",
        {
            "name": "Alliance",
            "ticker": "ALLY",
            "executor_corporation_id": 98000030,
            "date_founded": "2010-11-02T12:01:00Z",
            "creator_corporation_id": 98000030,
            "creator_id": 90000059,
        },
        etag="al",
    )


def run_refresh(env: SyncEnv, job: RefreshJob) -> Any:
    return JobRunner(env.settings, env.registry, env.lock).run(
        "entities:refresh", job, trigger=Trigger.MANUAL
    )


def test_refresh_populates_drains_and_paces(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, clock, client = esi
    serve_entities(fake)
    write_killmails(env, TODAY - timedelta(days=1))
    job = RefreshJob(client, entities, env.lake, today=TODAY, now=lambda: TODAY_AT)
    start = clock.now
    run = run_refresh(env, job)
    assert run.status is RunStatus.SUCCEEDED
    assert run.objects_changed == 6  # 2 characters, 3 corporations, 1 alliance
    requests = sum(fake.hits.values())
    assert requests == 2 + 2 + 2 * 3 + 1 == 11
    assert clock.now - start >= requests - 1  # one second between requests

    victim = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert victim["name"] == "Victim" and victim["source"] == "esi" and victim["deleted"] is False
    assert victim["observed_at"] == TODAY_AT
    attacker = entities.lookup("characters", [90000089]).to_pylist()[0]
    assert attacker["deleted"] is True  # Doomheim
    employment = entities.lookup("character_employment", [90000059]).to_pylist()
    assert [e["record_id"] for e in employment] == [1, 2]
    assert employment[0]["start_date"] == datetime(2010, 11, 2, 17, 28, tzinfo=UTC)
    history = entities.lookup("corporation_alliance_history", [98000030]).to_pylist()
    assert history[1]["alliance_id"] is None and history[1]["is_deleted"] is True
    corp = entities.lookup("corporations", [1000125]).to_pylist()[0]
    assert corp["name"] == "Corp 1000125" and corp["member_count"] == 12
    assert entities.lookup("alliances", [99000001]).to_pylist()[0]["ticker"] == "ALLY"

    # Queue entries moved out of the due window with the entity's validator.
    due = env.registry.refresh_pop(limit=100, now=TODAY_AT)
    assert due == []
    later = env.registry.refresh_pop(limit=100, now=TODAY_AT + timedelta(days=31))
    assert len(later) == 6
    entry = next(e for e in later if e.kind == "character" and e.entity_id == 90000059)
    assert entry.priority == 2 and entry.etag == '"c1"' and entry.last_refreshed_at == TODAY_AT

    # A second run: nothing due, nothing requested.
    before = sum(fake.hits.values())
    run = run_refresh(env, job)
    assert run.objects_changed == 0 and sum(fake.hits.values()) == before


def test_refresh_respects_run_budget_and_leaves_the_rest_due(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_entities(fake)
    write_killmails(env, TODAY - timedelta(days=1))
    job = RefreshJob(client, entities, env.lake, today=TODAY, now=lambda: TODAY_AT, budget=3)
    run = run_refresh(env, job)
    assert run.status is RunStatus.SUCCEEDED
    assert sum(fake.hits.values()) == 3
    # Never-refreshed entries come first, ordered by kind: the alliance (one request) then
    # one character (two). The next character would exceed the budget, so it stays due.
    assert run.objects_changed == 2
    assert len(env.registry.refresh_pop(limit=100, now=TODAY_AT)) == 4


def test_refresh_stops_on_420_and_records_the_run_as_failed(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_entities(fake)
    fake.put("/alliances/99000001", {}, status=420, headers={"X-ESI-Error-Limit-Reset": "30"})
    env.registry.refresh_push(
        [RefreshEntry(kind="alliance", entity_id=99000001, priority=1, next_due_at=TODAY_AT)]
    )
    job = RefreshJob(client, entities, env.lake, populate=False, now=lambda: TODAY_AT)
    with pytest.raises(EsiStoppedError):
        run_refresh(env, job)
    run = env.registry.runs(limit=1)[0]
    assert run.status is RunStatus.FAILED and "420" in (run.error or "")
    assert client.policy.stopped


def test_refresh_marks_missing_entities_deleted_and_backs_off_transient(
    env: SyncEnv,
    entities: EntityStore,
    esi: tuple[FakeEsi, FakeClock, EsiClient],
    members: dict[str, Path],
) -> None:
    fake, _, client = esi
    seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    fake.put("/characters/90000059", {"error": "gone"}, status=404)
    fake.put("/alliances/99000001", {}, status=503)
    env.registry.refresh_push(
        [
            RefreshEntry(kind="character", entity_id=90000059, priority=1, next_due_at=TODAY_AT),
            RefreshEntry(kind="alliance", entity_id=99000001, priority=1, next_due_at=TODAY_AT),
        ]
    )
    job = RefreshJob(client, entities, env.lake, populate=False, now=lambda: TODAY_AT)
    run = run_refresh(env, job)
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    row = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert row["deleted"] is True and row["source"] == "esi" and row["name"]
    entries = {
        (e.kind, e.entity_id): e
        for e in env.registry.refresh_pop(limit=10, now=TODAY_AT + timedelta(days=400))
    }
    gone = entries[("character", 90000059)]
    assert gone.failures == 1 and gone.last_error == "HTTP 404"
    assert gone.next_due_at > TODAY_AT + timedelta(days=300)
    flaky = entries[("alliance", 99000001)]
    assert flaky.failures == 1 and "503" in (flaky.last_error or "")
    assert TODAY_AT < flaky.next_due_at <= TODAY_AT + timedelta(hours=2)


# --- export -----------------------------------------------------------------------------


def test_export_job_writes_readable_parquet_and_views(
    env: SyncEnv, entities: EntityStore, members: dict[str, Path]
) -> None:
    seed_members(members, entities, source="everef_backfill:abc", snapshot=SNAPSHOT_AT)
    out = env.settings.entities_dir
    run = JobRunner(env.settings, env.registry, env.lock).run(
        "entities:export", ExportJob(entities, out), trigger=Trigger.MANUAL
    )
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 5
    assert run.rows_written == 6 + 75 + 4 + 36 + 3
    assert pq.read_table(out / "character_employment.parquet").num_rows == 75
    ddl = env.lake.view_sql()
    expected = (
        "CREATE OR REPLACE VIEW characters AS SELECT * FROM "
        f"read_parquet('{out.resolve()}/characters.parquet');"
    )
    assert expected in ddl
