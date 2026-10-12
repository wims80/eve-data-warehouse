"""zKillboard as a second killmail source and the merged layer (M8), against a fake R2Z2."""

import gzip
import json
from collections import Counter
from datetime import date
from typing import Any

import httpx
import pytest
import respx

from evedw.domain.datasets import KILLMAILS, ZKILLBOARD_KILLMAILS
from evedw.domain.registry import ObjectStatus, RunStatus, Trigger
from evedw.jobs.importers import importer_for
from evedw.jobs.runner import JobRunner
from evedw.jobs.sync import SyncJob
from evedw.jobs.verify import verify_dataset, verify_killmail_layers
from evedw.sources.zkillboard import ZKillboardClient, ZKillboardRefused, load_policy
from tests.fake_esi import MemoryCache
from tests.helpers import TODAY, FakeEveRef, SyncEnv, killmail_name, md5
from tests.jobs.test_killmails import KILLMAIL_FIXTURE, fixture_documents

ZKILL_URL = "https://zkill.test"
DAY = date(2026, 10, 1)


class FakeR2Z2:
    """``history/totals.json``, ``history/raw/YYYYMMDD.json`` and ``history/YYYYMMDD.json``
    as R2Z2 serves them: compressed bodies with weak ETags, a 304 for the strong form."""

    def __init__(self, router: respx.Router) -> None:
        self.days: dict[date, list[dict[str, Any]]] = {}
        self.hits: Counter[str] = Counter()
        self.refuse: int | None = None
        """Answer every request with this status, e.g. 429."""
        self.stale: dict[date, list[dict[str, Any]]] = {}
        """Days whose GETs serve this older copy while HEAD and totals announce the
        current one, as zKillboard's CDN did on 2026-10-12."""
        router.route(url__regex=rf"{ZKILL_URL}/.*").mock(side_effect=self._handle)

    def put(self, day: date, docs: list[dict[str, Any]]) -> None:
        self.days[day] = docs

    def _respond(self, request: httpx.Request, body: bytes) -> httpx.Response:
        etag = f'"{md5(body)}"'
        if request.headers.get("If-None-Match") == etag:
            return httpx.Response(304, headers={"ETag": etag})
        headers = {"ETag": f"W/{etag}", "Last-Modified": "Mon, 05 Oct 2026 12:00:00 GMT"}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        return httpx.Response(200, content=body, headers=headers)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.hits[f"{request.method} {path}"] += 1
        if self.refuse is not None:
            return httpx.Response(self.refuse)
        if path == "/history/totals.json":
            totals = {d.strftime("%Y%m%d"): len(docs) for d, docs in self.days.items()}
            return self._respond(request, json.dumps(totals).encode())
        name = path.rsplit("/", 1)[-1].removesuffix(".json")
        day = next((d for d in self.days if d.strftime("%Y%m%d") == name), None)
        if day is None:
            return httpx.Response(404)
        docs = self.days[day]
        if request.method == "GET" and day in self.stale:
            docs = self.stale[day]
        if path.startswith("/history/raw/"):
            body = {
                str(d["killmail_id"]): {k: v for k, v in d.items() if k != "killmail_hash"}
                for d in docs
            }
        else:
            body = {str(d["killmail_id"]): d.get("killmail_hash") for d in docs}
        return self._respond(request, json.dumps(body).encode())


@pytest.fixture
def r2z2(env: SyncEnv) -> FakeR2Z2:
    return FakeR2Z2(env.router)


def zkill_client(env: SyncEnv) -> ZKillboardClient:
    return ZKillboardClient(
        ZKILL_URL,
        policy_path=env.settings.zkillboard_policy_path,
        client=httpx.Client(),
        cache=MemoryCache(),
        spacing=0.0,
        download_spacing=0.0,
        backoff_seconds=0.0,
        sleep=lambda _: None,
    )


def sync_zkill(env: SyncEnv, client: ZKillboardClient, **kwargs: Any) -> Any:
    job = SyncJob(
        dataset=ZKILLBOARD_KILLMAILS,
        client=client,
        importer=importer_for(ZKILLBOARD_KILLMAILS, env.lake),
        today=TODAY,
        **kwargs,
    )
    return JobRunner(env.settings, env.registry, env.lock).run(
        "sync:zkillboard_killmails", job, trigger=Trigger.MANUAL
    )


def everef_day(env: SyncEnv) -> FakeEveRef:
    fake = env.fake_for(KILLMAILS, killmail_name)
    fake.put(DAY, KILLMAIL_FIXTURE.read_bytes(), expected=50)
    return fake


def test_zkillboard_history_lands_per_source_and_merged(env: SyncEnv, r2z2: FakeR2Z2) -> None:
    docs = fixture_documents()[:5]
    r2z2.put(DAY, docs)
    client = zkill_client(env)
    run = sync_zkill(env, client)
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1

    source = env.lake.read("zkillboard_killmails")
    assert source.num_rows == 5
    # zKillboard's raw history has no hashes; the day's index supplies them.
    by_id = {d["killmail_id"]: d["killmail_hash"] for d in docs}
    for row in source.to_pylist():
        assert row["killmail_hash"] == by_id[row["killmail_id"]]
    raw = sorted(p.name for p in (env.settings.raw_dir / "zkillboard_killmails").rglob("*.gz"))
    assert len(raw) == 2 and raw[0].endswith(".index.json.gz") and raw[1].endswith(".json.gz")
    day_file = next((env.settings.raw_dir / "zkillboard_killmails").rglob("*[0-9a-f].json.gz"))
    with gzip.open(day_file, "rt") as fh:
        assert len(json.load(fh)) == 5

    merged = env.lake.read("killmails")
    assert merged.num_rows == 5
    assert {tuple(s or ()) for s in merged.column("sources").to_pylist()} == {("zkillboard",)}
    assert client.traffic.downloads == 2  # the day file and its index
    client.close()


def test_merge_keeps_each_killmail_once_with_every_source(env: SyncEnv, r2z2: FakeR2Z2) -> None:
    everef_day(env)
    assert env.sync(KILLMAILS).status is RunStatus.SUCCEEDED
    docs = fixture_documents()
    extra = dict(docs[0], killmail_id=999_999_999, killmail_hash="f" * 40)
    r2z2.put(DAY, [*docs[:10], extra])  # ten shared, one only zKillboard has
    client = zkill_client(env)
    assert sync_zkill(env, client).status is RunStatus.SUCCEEDED
    client.close()

    merged = env.lake.read("killmails")
    assert merged.num_rows == 51
    sources = Counter(tuple(s or ()) for s in merged.column("sources").to_pylist())
    assert sources == {("everef", "zkillboard"): 10, ("everef",): 40, ("zkillboard",): 1}
    shared = docs[0]["killmail_id"]
    (row,) = [r for r in merged.to_pylist() if r["killmail_id"] == shared]
    assert row["killmail_hash"] == docs[0]["killmail_hash"]
    assert row["http_last_modified"] is not None  # EVE Ref's row was kept
    attackers = env.lake.read("attackers")
    assert set(attackers.column("killmail_id").to_pylist()) == set(
        merged.column("killmail_id").to_pylist()
    )
    report = verify_killmail_layers(env.lake)
    assert report.ok and not report.notes


def test_unchanged_counts_need_no_head_and_a_changed_day_is_fetched_again(
    env: SyncEnv, r2z2: FakeR2Z2
) -> None:
    docs = fixture_documents()
    old = date(2026, 6, 1)
    r2z2.put(old, docs[:3])
    r2z2.put(DAY, docs[3:6])
    client = zkill_client(env)
    sync_zkill(env, client)
    head_old = f"HEAD /history/raw/{old:%Y%m%d}.json"
    assert r2z2.hits[head_old] == 1

    sync_zkill(env, client)  # totals.json answered 304: no HEAD for the old day
    assert r2z2.hits[head_old] == 1

    r2z2.put(old, docs[:4])  # the old day gains a killmail
    run = sync_zkill(env, client)
    assert r2z2.hits[head_old] == 2
    assert run.objects_changed == 1
    assert env.lake.read("zkillboard_killmails", date_from=old, date_to=old).num_rows == 4
    client.close()


def test_a_refusal_pauses_zkillboard_and_the_third_in_a_day_stops_it(
    env: SyncEnv, r2z2: FakeR2Z2
) -> None:
    r2z2.put(DAY, fixture_documents()[:2])
    r2z2.refuse = 429
    client = zkill_client(env)
    with pytest.raises(ZKillboardRefused, match="HTTP 429"):
        client.totals(ZKILLBOARD_KILLMAILS)
    assert sum(r2z2.hits.values()) == 1  # never retried
    with pytest.raises(ZKillboardRefused, match="paused"):
        client.totals(ZKILLBOARD_KILLMAILS)
    assert sum(r2z2.hits.values()) == 1  # nothing sent while paused

    path = env.settings.zkillboard_policy_path
    for _ in range(2):  # two more refusals today, each after the pause has passed
        policy = load_policy(path)
        policy.blocked_until = 0
        path.write_text(json.dumps(policy.to_dict()))
        with pytest.raises(ZKillboardRefused):
            client.totals(ZKILLBOARD_KILLMAILS)
    assert load_policy(path).stopped
    with pytest.raises(ZKillboardRefused, match="stopped"):
        client.totals(ZKILLBOARD_KILLMAILS)
    client.close()


def test_verify_notes_a_day_one_source_lacks(env: SyncEnv) -> None:
    everef_day(env)
    env.sync(KILLMAILS)
    report = verify_killmail_layers(env.lake)
    assert report.ok
    assert [n.kind for n in report.notes] == ["source_lacks_day"]
    assert "zkillboard lacks 2026-10-01" in report.notes[0].detail


def test_a_capped_backfill_heads_its_pending_days_once(env: SyncEnv, r2z2: FakeR2Z2) -> None:
    docs = fixture_documents()
    days = [date(2026, 8, d) for d in range(1, 6)]  # outside the 30-day header window
    for i, day in enumerate(days):
        r2z2.put(day, docs[i * 2 : i * 2 + 2])
    client = zkill_client(env)
    sync_zkill(env, client, max_objects=2)
    heads = sum(n for k, n in r2z2.hits.items() if k.startswith("HEAD "))
    assert heads == 5  # every new day once
    sync_zkill(env, client, max_objects=2)  # three still pending, listing unchanged
    assert sum(n for k, n in r2z2.hits.items() if k.startswith("HEAD ")) == heads
    assert env.lake.read("zkillboard_killmails").num_rows == 8
    client.close()


def test_an_older_copy_served_three_syncs_running_is_imported_as_provisional(
    env: SyncEnv, r2z2: FakeR2Z2
) -> None:
    docs = fixture_documents()
    key = f"{DAY.year}/{DAY:%Y%m%d}.json"
    r2z2.put(DAY, docs[:4])
    r2z2.stale[DAY] = docs[:3]
    client = zkill_client(env)

    for _ in range(2):  # could be a rewrite during the sync: deferred, nothing imported
        run = sync_zkill(env, client)
        assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 0
    obj = env.registry.get_object(ZKILLBOARD_KILLMAILS.name, key)
    assert obj is not None and obj.status is ObjectStatus.CHANGED and obj.stale_runs == 2

    run = sync_zkill(env, client)  # the third: the copy served is taken
    assert run.objects_changed == 1
    obj = env.registry.get_object(ZKILLBOARD_KILLMAILS.name, key)
    assert obj is not None and obj.status is ObjectStatus.PROVISIONAL
    assert env.lake.read("killmails", date_from=DAY, date_to=DAY).num_rows == 3
    report = verify_dataset(
        env.settings, env.registry, env.lake, ZKILLBOARD_KILLMAILS, totals={key[5:13]: 4}
    )
    assert report.ok, report.issues
    assert [n.kind for n in report.notes] == ["provisional"]

    raw_gets = f"GET /history/raw/{DAY:%Y%m%d}.json"
    before = r2z2.hits[raw_gets]
    run = sync_zkill(env, client)  # still the older copy: checked, nothing new
    assert run.objects_changed == 0 and r2z2.hits[raw_gets] == before + 1
    assert len(env.registry.revisions(ZKILLBOARD_KILLMAILS.name, key)) == 1
    obj = env.registry.get_object(ZKILLBOARD_KILLMAILS.name, key)
    assert obj is not None and obj.status is ObjectStatus.PROVISIONAL

    del r2z2.stale[DAY]  # the CDN catches up
    run = sync_zkill(env, client)
    assert run.objects_changed == 1
    obj = env.registry.get_object(ZKILLBOARD_KILLMAILS.name, key)
    assert obj is not None and obj.status is ObjectStatus.IMPORTED and obj.stale_etag is None
    assert env.lake.read("killmails", date_from=DAY, date_to=DAY).num_rows == 4
    client.close()
