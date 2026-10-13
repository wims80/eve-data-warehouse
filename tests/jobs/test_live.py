"""zKillboard's live feed (M9) against a fake R2Z2: raw hourly files, the flush into the
``live`` source and ``killmail_zkb``, the merge with daily files, and the follower."""

import gzip
import json
import threading
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from evedw.domain.datasets import KILLMAILS
from evedw.jobs.follower import AFTER_NOT_FOUND, SEQUENCE_STATE, LiveFollower
from evedw.jobs.live_flush import LiveFlush
from evedw.jobs.live_raw import LiveRaw
from evedw.jobs.verify import verify_killmail_layers
from evedw.sources.zkillboard import ZKillboardClient, ZKillboardGate
from tests.helpers import SyncEnv, killmail_name
from tests.jobs.test_killmails import KILLMAIL_FIXTURE, fixture_documents

ZKILL_URL = "https://zkill.test"
DAY = date(2026, 10, 1)
FIXTURE = Path(__file__).parent.parent / "fixtures" / "r2z2_live_object.json"
T0 = datetime(2026, 10, 2, 9, 30, tzinfo=UTC)


def live_object(doc: dict[str, Any], sequence: int, *, total: float = 1000.0) -> dict[str, Any]:
    """A live object as R2Z2 serves it (shape from ``r2z2_live_object.json``)."""
    template = json.loads(FIXTURE.read_text())
    esi = {k: v for k, v in doc.items() if k != "killmail_hash"}
    zkb = dict(template["zkb"], hash=doc["killmail_hash"], totalValue=total)
    return {
        "esi": esi,
        "hash": doc["killmail_hash"],
        "killmail_id": doc["killmail_id"],
        "sequence_id": sequence,
        "uploaded_at": 1791836876,
        "zkb": zkb,
    }


class FakeFeed:
    """``ephemeral/sequence.json`` and ``ephemeral/{n}.json``."""

    def __init__(self, router: respx.Router) -> None:
        self.objects: dict[int, dict[str, Any]] = {}
        self.pointer = 0
        self.hits: Counter[str] = Counter()
        self.refuse: int | None = None
        router.route(url__regex=rf"{ZKILL_URL}/ephemeral/.*").mock(side_effect=self._handle)

    def publish(self, obj: dict[str, Any]) -> None:
        self.objects[obj["sequence_id"]] = obj

    def _handle(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        self.hits[name] += 1
        if self.refuse is not None:
            return httpx.Response(self.refuse)
        if name == "sequence.json":
            return httpx.Response(200, json={"sequence": self.pointer})
        obj = self.objects.get(int(name.removesuffix(".json")))
        return httpx.Response(404) if obj is None else httpx.Response(200, json=obj)


@pytest.fixture
def feed(env: SyncEnv) -> FakeFeed:
    return FakeFeed(env.router)


class Clock:
    def __init__(self) -> None:
        self.at = T0

    def __call__(self) -> datetime:
        return self.at


def client(env: SyncEnv) -> ZKillboardClient:
    return ZKillboardClient(
        ZKILL_URL,
        policy_path=env.settings.zkillboard_policy_path,
        client=httpx.Client(),
        spacing=0.0,
        backoff_seconds=0.0,
        sleep=lambda _: None,
    )


def follower(
    env: SyncEnv, clock: Clock, waits: list[float], *, stop_after_404s: int = 1
) -> tuple[LiveFollower, LiveRaw]:
    """A follower whose waits are recorded, not slept; it stops after a number of 404s."""
    raw = LiveRaw(env.settings.live_raw_dir)
    not_found = [0]

    def wait(stop: threading.Event, seconds: float) -> bool:
        waits.append(seconds)
        clock.at += timedelta(seconds=seconds)
        if seconds == AFTER_NOT_FOUND:
            not_found[0] += 1
            if not_found[0] >= stop_after_404s:
                stop.set()
        return stop.is_set()

    return (
        LiveFollower(
            client=client(env),
            registry=env.registry,
            raw=raw,
            flush=LiveFlush(env.lake, raw, env.settings.scratch_dir / "live", now=clock),
            flush_interval=timedelta(seconds=30),
            now=clock,
            wait=wait,
        ),
        raw,
    )


def run(f: LiveFollower) -> None:
    f.run(threading.Event())


# -- raw files -----------------------------------------------------------------------------


def test_raw_hours_are_compressed_when_closed_and_read_after_a_checkpoint(tmp_path: Path) -> None:
    raw = LiveRaw(tmp_path / "live")
    docs = fixture_documents()
    raw.append(live_object(docs[0], 1), now=T0)
    raw.append(live_object(docs[1], 2), now=T0 + timedelta(minutes=5))
    raw.append(live_object(docs[2], 3), now=T0 + timedelta(hours=1))  # closes 09:00
    names = [p.name for p in raw.files()]
    assert names == ["09.ndjson.gz", "10.ndjson"]
    with gzip.open(raw.files()[0], "rt") as fh:
        assert [json.loads(line)["sequence_id"] for line in fh] == [1, 2]
    assert [o["sequence_id"] for o in raw.read_since(None)] == [1, 2, 3]
    assert [o["sequence_id"] for o in raw.read_since(2)] == [3]

    with raw.files()[1].open("a") as fh:
        fh.write('{"sequence_id": 4, "esi"')  # cut short by a crash
    assert [o["sequence_id"] for o in raw.read_since(2)] == [3]
    raw.close(now=T0 + timedelta(hours=2))
    assert all(p.name.endswith(".ndjson.gz") for p in raw.files())


# -- flush and merge -----------------------------------------------------------------------


def test_flush_writes_live_rows_and_zkb_and_a_reprocessed_killmail_replaces_its_copy(
    env: SyncEnv,
) -> None:
    raw = LiveRaw(env.settings.live_raw_dir)
    flush = LiveFlush(env.lake, raw, env.settings.scratch_dir / "live")
    docs = fixture_documents()[:3]
    for seq, doc in enumerate(docs, start=100):
        raw.append(live_object(doc, seq), now=T0)
    report = flush.flush(None)
    assert (report.objects, report.killmails, report.days) == (3, 3, [DAY])
    assert report.last_sequence == 102 and not report.unknown

    merged = env.lake.read("killmails")
    assert merged.num_rows == 3
    assert {tuple(s or ()) for s in merged.column("sources").to_pylist()} == {("live",)}
    zkb = env.lake.read("killmail_zkb").to_pylist()
    assert {r["killmail_id"] for r in zkb} == {d["killmail_id"] for d in docs}
    first = next(r for r in zkb if r["killmail_id"] == docs[0]["killmail_id"])
    assert first["sequence_id"] == 100 and first["total_value"] == 1000.0
    assert first["labels"] and first["location_id"] == 50014206 and first["npc"] is False

    raw.append(live_object(docs[0], 103, total=2500.0), now=T0)  # reprocessed
    report = flush.flush(102)
    assert (report.objects, report.killmails) == (1, 1)
    zkb = env.lake.read("killmail_zkb").to_pylist()
    assert len(zkb) == 3
    again = next(r for r in zkb if r["killmail_id"] == docs[0]["killmail_id"])
    assert again["sequence_id"] == 103 and again["total_value"] == 2500.0
    assert env.lake.read("live_killmails").num_rows == 3
    attackers = env.lake.read("live_attackers")
    expected = sum(len(d["attackers"]) for d in docs)
    assert attackers.num_rows == expected  # replaced, not appended


def test_live_rows_give_way_to_a_daily_file(env: SyncEnv) -> None:
    raw = LiveRaw(env.settings.live_raw_dir)
    flush = LiveFlush(env.lake, raw, env.settings.scratch_dir / "live")
    docs = fixture_documents()
    for seq, doc in enumerate(docs[:5], start=1):
        raw.append(live_object(doc, seq), now=T0)
    extra = dict(docs[0], killmail_id=999_000_001)  # a killmail EVE Ref will not have
    raw.append(live_object(extra, 6), now=T0)
    flush.flush(None)

    fake = env.fake_for(KILLMAILS, killmail_name)
    fake.put(DAY, KILLMAIL_FIXTURE.read_bytes(), expected=50)
    env.sync(KILLMAILS)

    merged = env.lake.read("killmails")
    sources = Counter(tuple(s or ()) for s in merged.column("sources").to_pylist())
    assert sources == {("everef", "live"): 5, ("everef",): 45, ("live",): 1}
    row = next(r for r in merged.to_pylist() if r["killmail_id"] == docs[0]["killmail_id"])
    assert row["http_last_modified"] is not None  # EVE Ref's row was kept
    report = verify_killmail_layers(env.lake)
    assert report.ok  # the live feed lacking a day is no note, only a daily source
    assert [n.detail.split()[0] for n in report.notes] == ["zkillboard"]


# -- follower ------------------------------------------------------------------------------


def test_follower_starts_at_the_pointer_and_resumes_with_no_gap_and_no_duplicate(
    env: SyncEnv, feed: FakeFeed
) -> None:
    docs = fixture_documents()
    feed.pointer = 500
    for seq, doc in enumerate(docs[:4], start=500):
        feed.publish(live_object(doc, seq))
    clock, waits = Clock(), list[float]()
    f, _ = follower(env, clock, waits)
    run(f)
    assert feed.hits["sequence.json"] == 1
    assert [feed.hits[f"{n}.json"] for n in range(500, 505)] == [1, 1, 1, 1, 1]
    assert AFTER_NOT_FOUND in waits
    stored = env.registry.state_get(SEQUENCE_STATE)
    assert stored is not None and json.loads(stored)["next"] == 504
    assert env.lake.read("killmails").num_rows == 4
    assert f.status.state == "stopped" and f.status.objects_today == 4

    for seq, doc in enumerate(docs[4:7], start=504):
        feed.publish(live_object(doc, seq))
    clock.at += timedelta(hours=3)
    f, _ = follower(env, clock, waits)
    run(f)
    assert f.status.objects_today == 7  # counted from today's raw files, not from zero
    # Resumed from the stored sequence; the one read is the gap check at the feed's head.
    assert feed.hits["sequence.json"] == 2
    assert [feed.hits[f"{n}.json"] for n in range(500, 508)] == [1, 1, 1, 1, 2, 1, 1, 1]
    merged = env.lake.read("killmails")
    assert merged.num_rows == 7
    assert len(set(merged.column("killmail_id").to_pylist())) == 7


def test_follower_skips_a_sequence_below_the_pointer_and_starts_over_after_a_day(
    env: SyncEnv, feed: FakeFeed
) -> None:
    docs = fixture_documents()
    feed.pointer = 10
    feed.publish(live_object(docs[0], 10))
    feed.publish(live_object(docs[1], 12))  # 11 never published
    feed.pointer = 12
    clock, waits = Clock(), list[float]()
    env.registry.state_put(SEQUENCE_STATE, json.dumps({"next": 10, "at": T0.isoformat()}), now=T0)
    f, _ = follower(env, clock, waits)
    run(f)
    assert f.status.skipped_sequences == 1
    assert env.lake.read("killmails").num_rows == 2

    feed.pointer = 40
    feed.publish(live_object(docs[2], 40))
    clock.at += timedelta(days=2)
    f, _ = follower(env, clock, waits)
    run(f)
    assert feed.hits["14.json"] == 0  # started over at the pointer, not at 13
    assert env.lake.read("killmails").num_rows == 3


def test_a_refusal_pauses_the_follower_without_further_requests(
    env: SyncEnv, feed: FakeFeed
) -> None:
    feed.pointer = 1
    feed.refuse = 429
    clock, waits = Clock(), list[float]()
    f, _ = follower(env, clock, waits)
    stop = threading.Event()

    def wait(event: threading.Event, seconds: float) -> bool:
        waits.append(seconds)
        if len(waits) >= 3:
            event.set()
        return event.is_set()

    f._wait = wait  # pyright: ignore[reportPrivateUsage]
    f.run(stop)
    assert sum(feed.hits.values()) == 1  # the 429; nothing sent while paused
    assert f.status.last_error is not None and "paused" in f.status.last_error


def test_clients_sharing_a_gate_keep_one_spacing_between_them(env: SyncEnv, feed: FakeFeed) -> None:
    feed.pointer = 7
    now = [0.0]
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    gate = ZKillboardGate()
    clients = [
        ZKillboardClient(
            ZKILL_URL,
            policy_path=env.settings.zkillboard_policy_path,
            client=httpx.Client(),
            spacing=0.2,
            sleep=sleep,
            clock=lambda: now[0],
            gate=gate,
        )
        for _ in range(2)
    ]
    assert [c.live_sequence() for c in (*clients, *clients)] == [7, 7, 7, 7]
    assert slept == pytest.approx([0.2, 0.2, 0.2])  # one spacing across both clients
    for c in clients:
        c.close()
