"""Entity seed from the trimmed real backfill, ESI refresh against a fake, Parquet export."""

import bz2
import io
import json
import tarfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from evedw.domain.datasets import ENTITIES_BACKFILL
from evedw.domain.registry import ObjectStatus, RunStatus, Trigger
from evedw.jobs.catalog import JobCatalog
from evedw.jobs.entities import (
    BackfillImporter,
    ExportJob,
    seed_members,
)
from evedw.jobs.runner import JobRunner
from evedw.jobs.sync import SyncJob
from evedw.sources.archives import extract_members
from evedw.store.base import EntityStore
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


def test_seed_skips_error_placeholders(members: dict[str, Path], entities: EntityStore) -> None:
    """eve-kill exports hold placeholders for entities it failed to fetch."""
    chars = json.loads(members["characters.json"].read_text())
    chars.append({"character_id": 2100000001, "error": "Character not found", "history": []})
    chars.append({"character_id": 2100000002, "history": [], "deleted": True})
    chars[0]["error"] = {"status": 500}
    members["characters.json"].write_text(json.dumps(chars))
    corps = json.loads(members["corporations.json"].read_text())
    corps.append({"corporation_id": 98999999, "error": "timeout"})
    members["corporations.json"].write_text(json.dumps(corps))
    counts = seed_members(members, entities, source="x", snapshot=SNAPSHOT_AT)
    assert counts.rows["characters"] == len(chars) - 3
    assert (
        entities.lookup("characters", [2100000001, 2100000002, chars[0]["character_id"]]).num_rows
        == 0
    )
    assert entities.lookup("corporations", [98999999]).num_rows == 0
    assert counts.unknown_keys["characters"] == ["error"]


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


def test_seed_all_imports_every_unseeded_archive_through_the_catalog(
    env: SyncEnv, entities: EntityStore
) -> None:
    """The scheduled seed: no snapshot named, whatever is listed and not imported."""
    fake = FakeBackfills(env, ARCHIVE.read_bytes())
    env.settings.everef_base_url = BASE_URL  # the catalog builds its own EVE Ref client
    catalog = JobCatalog(env.settings, env.registry, env.lake, entities)
    run = catalog.run("entities:seed", {"snapshot": "all"}, trigger=Trigger.SCHEDULE, lock=env.lock)
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    assert run.params == {"snapshot": "all", "force": False}
    assert entities.count("character_employment") == 75

    again = catalog.run(
        "entities:seed", {"snapshot": "all"}, trigger=Trigger.SCHEDULE, lock=env.lock
    )
    assert again.status is RunStatus.SUCCEEDED and again.objects_changed == 0
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
