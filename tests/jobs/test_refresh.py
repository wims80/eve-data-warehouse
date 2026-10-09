"""Entity refresh against a fake ESI: sweeps, change queue, active refresh, crawl."""

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa
import pytest

import evedw.jobs.refresh as refresh_module
from evedw.domain.registry import RefreshClass, RefreshEntry, RunStatus, Trigger
from evedw.domain.schemas import (
    ALLIANCES,
    ATTACKERS,
    CHARACTERS,
    CORPORATIONS,
    KILLMAILS,
)
from evedw.jobs.focus import FocusJob
from evedw.jobs.refresh import CRAWL_STATE, CYCLE_STATE, RefreshJob, ids_from_killmails
from evedw.jobs.runner import JobRunner
from evedw.sources.esi import EsiClient, EsiStoppedError
from evedw.store.base import EntityStore, Registry
from tests.fake_esi import ESI_URL, FakeClock, FakeEsi, MemoryCache
from tests.helpers import TODAY, SyncEnv

TODAY_AT = datetime(2026, 10, 5, 12, tzinfo=UTC)
SEEDED_AT = datetime(2026, 5, 10, tzinfo=UTC)
FOCUS, CHANGE, ACTIVE, IDLE, CRAWL, DEFERRED = (int(c) for c in RefreshClass)


# --- fixtures and helpers -----------------------------------------------------------------


@pytest.fixture
def cache() -> MemoryCache:
    return MemoryCache()


@pytest.fixture
def esi(env: SyncEnv, cache: MemoryCache) -> tuple[FakeEsi, FakeClock, EsiClient]:
    fake = FakeEsi(env.router)
    clock = FakeClock(now=TODAY_AT.timestamp())
    client = EsiClient(
        ESI_URL,
        compatibility_date="2026-08-18",
        contact="tests@example.test",
        cache=cache,
        policy_path=env.settings.esi_policy_path,
        daily_budget=1000,
        client=httpx.Client(),
        sleep=clock.sleep,
        clock=clock.time,
    )
    return fake, clock, client


def job(client: EsiClient, entities: EntityStore, env: SyncEnv, **kwargs: Any) -> RefreshJob:
    return RefreshJob(client, entities, env.lake, today=TODAY, now=lambda: TODAY_AT, **kwargs)


def run(env: SyncEnv, refresh: RefreshJob) -> Any:
    return JobRunner(env.settings, env.registry, env.lock).run(
        "entities:refresh", refresh, trigger=Trigger.MANUAL
    )


def requests(fake: FakeEsi) -> int:
    return sum(fake.hits.values())


def seed_characters(entities: EntityStore, *rows: dict[str, Any]) -> None:
    base: dict[str, Any] = {
        "name": "C",
        "corporation_id": 98000001,
        "alliance_id": None,
        "faction_id": None,
        "birthday": SEEDED_AT,
        "security_status": 0.0,
        "deleted": False,
        "observed_at": SEEDED_AT,
        "source": "everef_backfill:abc",
    }
    entities.upsert(
        "characters", pa.Table.from_pylist([{**base, **r} for r in rows], schema=CHARACTERS)
    )


def seed_corporations(entities: EntityStore, *rows: dict[str, Any]) -> None:
    base: dict[str, Any] = {
        "name": "K",
        "ticker": "K",
        "alliance_id": None,
        "ceo_id": None,
        "member_count": 1,
        "date_founded": SEEDED_AT,
        "deleted": False,
        "observed_at": SEEDED_AT,
        "source": "everef_backfill:abc",
    }
    entities.upsert(
        "corporations", pa.Table.from_pylist([{**base, **r} for r in rows], schema=CORPORATIONS)
    )


def seed_alliances(entities: EntityStore, *ids: int) -> None:
    rows = [
        {
            "alliance_id": i,
            "name": f"A{i}",
            "ticker": "A",
            "executor_corporation_id": None,
            "date_founded": SEEDED_AT,
            "deleted": False,
            "observed_at": SEEDED_AT,
            "source": "everef_backfill:abc",
        }
        for i in ids
    ]
    entities.upsert("alliances", pa.Table.from_pylist(rows, schema=ALLIANCES))


def queue(registry: Registry) -> dict[tuple[str, int], int]:
    """(kind, id) -> priority of every entry due within ten years."""
    far = TODAY_AT + timedelta(days=4000)
    return {(e.kind, e.entity_id): e.priority for e in registry.refresh_pop(limit=10_000, now=far)}


def write_killmails(
    env: SyncEnv, day: date, *, victim: int = 90000059, attacker: int = 90000089
) -> None:
    killmails = pa.Table.from_pylist(
        [
            {
                "source_date": day,
                "killmail_id": 1,
                "victim_character_id": victim,
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
                "character_id": attacker,
                "corporation_id": 98000030,
                "alliance_id": None,
            },
            {"source_date": day, "killmail_id": 1, "ordinal": 1, "corporation_id": 1000125},
        ],
        schema=ATTACKERS,
    )
    env.lake.write_partition("killmails", f"source_date={day}", killmails, metadata={})
    env.lake.write_partition("attackers", f"source_date={day}", attackers, metadata={})


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
    }


def serve_entities(fake: FakeEsi) -> None:
    fake.put("/characters/90000059", character_body("Victim"))
    fake.put(
        "/characters/90000059/corporationhistory",
        [
            {"record_id": 1, "corporation_id": 1000009, "start_date": "2010-11-02T17:28:00Z"},
            {"record_id": 2, "corporation_id": 98000030, "start_date": "2011-01-02T15:27:00Z"},
        ],
    )
    fake.put("/characters/90000089", character_body("Attacker", 1000001))
    fake.put("/characters/90000089/corporationhistory", [])
    corp: dict[str, Any] = {
        "name": "Corp",
        "ticker": "CORP",
        "alliance_id": 99000001,
        "ceo_id": 90000059,
        "member_count": 12,
        "date_founded": "2010-11-02T20:05:00Z",
        "state": "active",
    }
    for cid in (98000030, 1000001, 1000125):
        fake.put(f"/corporations/{cid}", dict(corp, name=f"Corp {cid}"))
        fake.put(
            f"/corporations/{cid}/alliancehistory",
            [
                {"record_id": 5, "alliance_id": 99000001, "start_date": "2012-05-08T06:20:00Z"},
                {"record_id": 6, "start_date": "2012-11-26T16:14:00Z", "is_deleted": True},
            ],
        )
    fake.put(
        "/alliances/99000001",
        {
            "name": "Alliance",
            "ticker": "ALLY",
            "executor_corporation_id": 98000030,
            "date_founded": "2010-11-02T12:01:00Z",
        },
    )


def affiliation_server(
    fake: FakeEsi, known: dict[int, tuple[int, int | None]], invalid: set[int]
) -> list[list[int]]:
    """``POST /characters/affiliation`` answering from ``known``; any id in ``invalid``
    fails the whole batch with 400, like ESI. Returns the batches it saw."""
    seen: list[list[int]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        ids: list[int] = json.loads(request.content)
        seen.append(ids)
        if invalid & set(ids):
            return httpx.Response(400, json={"error": "Invalid character ID"})
        body = [
            {"character_id": i, "corporation_id": known[i][0]}
            | ({"alliance_id": known[i][1]} if known[i][1] else {})
            for i in ids
        ]
        return httpx.Response(200, json=body)

    fake.handlers["/characters/affiliation"] = handle
    return seen


# --- killmail ids -------------------------------------------------------------------------


def test_ids_from_killmails(env: SyncEnv) -> None:
    write_killmails(env, TODAY - timedelta(days=2))
    write_killmails(env, TODAY - timedelta(days=40), victim=1, attacker=2)
    found = ids_from_killmails(env.lake, date_from=TODAY - timedelta(days=7))
    assert found == {
        "character": {90000059, 90000089},
        "corporation": {98000030, 1000001, 1000125},
        "alliance": {99000001},
    }


# --- drain --------------------------------------------------------------------------------


def test_change_entries_get_details_and_history_and_settle(
    env: SyncEnv,
    entities: EntityStore,
    esi: tuple[FakeEsi, FakeClock, EsiClient],
    cache: MemoryCache,
) -> None:
    fake, clock, client = esi
    serve_entities(fake)
    env.registry.refresh_push(
        [
            RefreshEntry("character", 90000059, priority=CHANGE, next_due_at=TODAY_AT),
            RefreshEntry("character", 90000089, priority=CHANGE, next_due_at=TODAY_AT),
            RefreshEntry("corporation", 98000030, priority=CHANGE, next_due_at=TODAY_AT),
            RefreshEntry("alliance", 99000001, priority=CHANGE, next_due_at=TODAY_AT),
        ]
    )
    start = clock.now
    result = run(env, job(client, entities, env, populate=False))
    assert result.status is RunStatus.SUCCEEDED and result.objects_changed == 4
    assert requests(fake) == 2 + 2 + 2 + 1
    assert clock.now - start >= requests(fake) - 1  # one second between requests
    assert not cache.entries  # refresh requests keep no response bodies

    victim = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert victim["name"] == "Victim" and victim["source"] == "esi"
    assert victim["observed_at"] == TODAY_AT and victim["deleted"] is False
    assert entities.lookup("characters", [90000089]).to_pylist()[0]["deleted"] is True
    employment = entities.lookup("character_employment", [90000059]).to_pylist()
    assert [e["record_id"] for e in employment] == [1, 2]
    history = entities.lookup("corporation_alliance_history", [98000030]).to_pylist()
    assert history[1]["alliance_id"] is None and history[1]["is_deleted"] is True
    assert entities.lookup("alliances", [99000001]).to_pylist()[0]["ticker"] == "ALLY"

    # Settled: idle and not due for years, so a second run requests nothing.
    assert env.registry.refresh_pop(limit=10, now=TODAY_AT + timedelta(days=365)) == []
    assert set(queue(env.registry).values()) == {IDLE}
    before = requests(fake)
    assert run(env, job(client, entities, env, populate=False)).objects_changed == 0
    assert requests(fake) == before


def test_crawl_fetches_history_only_unless_the_entity_is_unknown(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_entities(fake)
    seed_characters(entities, {"character_id": 90000059})
    env.registry.refresh_push(
        [
            RefreshEntry("character", 90000059, priority=CRAWL, next_due_at=TODAY_AT),
            RefreshEntry("character", 90000089, priority=CRAWL, next_due_at=TODAY_AT),
        ]
    )
    result = run(env, job(client, entities, env, populate=False))
    assert result.objects_changed == 2
    assert fake.hits["GET /characters/90000059"] == 0  # known: history only
    assert fake.hits["GET /characters/90000059/corporationhistory"] == 1
    assert fake.hits["GET /characters/90000089"] == 1  # unknown: details too
    assert entities.lookup("characters", [90000059]).to_pylist()[0]["name"] == "C"
    assert len(entities.lookup("character_employment", [90000059])) == 2


def test_budget_stops_before_an_entity_that_would_exceed_it_and_kinds_take_turns(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_entities(fake)
    env.registry.refresh_push(
        [
            RefreshEntry("character", 90000059, priority=ACTIVE, next_due_at=TODAY_AT),
            RefreshEntry("character", 90000089, priority=ACTIVE, next_due_at=TODAY_AT),
            RefreshEntry("corporation", 98000030, priority=ACTIVE, next_due_at=TODAY_AT),
        ]
    )
    result = run(env, job(client, entities, env, populate=False, budget=5))
    assert result.status is RunStatus.SUCCEEDED
    # One character, then the corporation (kinds take turns); the second character would
    # need two more requests than the one left.
    assert requests(fake) == 4 and result.objects_changed == 2
    assert fake.hits["GET /corporations/98000030"] == 1
    assert queue(env.registry)[("character", 90000089)] == ACTIVE


def test_420_stops_everything_and_fails_the_run(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    fake.put("/alliances/99000001", {}, status=420, headers={"X-ESI-Error-Limit-Reset": "30"})
    env.registry.refresh_push(
        [RefreshEntry("alliance", 99000001, priority=ACTIVE, next_due_at=TODAY_AT)]
    )
    with pytest.raises(EsiStoppedError):
        run(env, job(client, entities, env, populate=False))
    last = env.registry.runs(limit=1)[0]
    assert last.status is RunStatus.FAILED and "420" in (last.error or "")
    assert client.policy.stopped


def test_missing_entities_are_marked_deleted_and_transient_failures_back_off(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_characters(entities, {"character_id": 90000059, "name": "Gone"})
    fake.put("/characters/90000059", {"error": "gone"}, status=404)
    fake.put("/alliances/99000001", {}, status=503)
    env.registry.refresh_push(
        [
            RefreshEntry("character", 90000059, priority=CHANGE, next_due_at=TODAY_AT),
            RefreshEntry("alliance", 99000001, priority=CHANGE, next_due_at=TODAY_AT),
        ]
    )
    result = run(env, job(client, entities, env, populate=False))
    assert result.status is RunStatus.SUCCEEDED and result.objects_changed == 1
    row = entities.lookup("characters", [90000059]).to_pylist()[0]
    assert row["deleted"] is True and row["source"] == "esi" and row["name"] == "Gone"
    entries = {
        (e.kind, e.entity_id): e
        for e in env.registry.refresh_pop(limit=10, now=TODAY_AT + timedelta(days=400))
    }
    gone = entries[("character", 90000059)]
    assert gone.failures == 1 and gone.last_error == "HTTP 404" and gone.priority == IDLE
    flaky = entries[("alliance", 99000001)]
    assert flaky.failures == 1 and "503" in (flaky.last_error or "")
    assert TODAY_AT < flaky.next_due_at <= TODAY_AT + timedelta(hours=2)
    assert flaky.priority == CHANGE


# --- alliance sweep -----------------------------------------------------------------------


def serve_alliances(fake: FakeEsi) -> None:
    fake.put("/alliances", [99000001, 99000002])
    fake.put("/alliances/99000001/corporations", [98000001])
    fake.put("/alliances/99000002/corporations", [98000003, 98000004])


def seed_alliance_world(entities: EntityStore) -> None:
    seed_alliances(entities, 99000001, 99000003)
    seed_corporations(
        entities,
        {"corporation_id": 98000001, "alliance_id": 99000001},  # stays
        {"corporation_id": 98000002, "alliance_id": 99000001},  # left
        {"corporation_id": 98000003},  # joins 99000002
    )


def test_alliance_sweep_detects_joins_leaves_closures_and_new_entities(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_alliances(fake)
    seed_alliance_world(entities)
    refresh = job(client, entities, env)
    refresh.sweep_alliances(env.registry, cancel=lambda: None)
    assert requests(fake) == 3

    joined = entities.lookup("corporations", [98000003]).to_pylist()[0]
    assert joined["alliance_id"] == 99000002 and joined["source"] == "esi"
    left = entities.lookup("corporations", [98000002]).to_pylist()[0]
    assert left["alliance_id"] == 99000001  # not written: the full refresh settles it
    assert entities.lookup("alliances", [99000003]).to_pylist()[0]["deleted"] is True
    assert queue(env.registry) == {
        ("alliance", 99000002): CHANGE,  # unknown alliance
        ("corporation", 98000002): CHANGE,  # left
        ("corporation", 98000003): CHANGE,  # joined
        ("corporation", 98000004): CHANGE,  # unknown corporation
        ("alliance", 99000001): ACTIVE,  # details never fetched
        ("corporation", 98000001): ACTIVE,
    }

    # Within the interval the sweep does not run again.
    refresh.sweep_alliances(env.registry, cancel=lambda: None)
    assert requests(fake) == 3


def test_alliance_sweep_resumes_after_running_out_of_budget(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_alliances(fake)
    seed_alliance_world(entities)
    serve_entities(fake)
    first = run(env, job(client, entities, env, budget=2))
    assert first.status is RunStatus.SUCCEEDED
    assert fake.hits["GET /alliances/99000001/corporations"] == 1
    assert fake.hits["GET /alliances/99000002/corporations"] == 0
    run(env, job(client, entities, env, budget=1))
    assert fake.hits["GET /alliances"] == 1  # the sweep resumed, it did not restart
    assert fake.hits["GET /alliances/99000002/corporations"] == 1


# --- affiliation --------------------------------------------------------------------------


def test_affiliation_cycle_writes_changes_splits_bad_batches_and_finishes(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_corporations(entities, {"corporation_id": 98000001}, {"corporation_id": 98000002})
    seed_characters(
        entities,
        {"character_id": 1},  # unchanged
        {"character_id": 2},  # moves to 98000002
        {"character_id": 3},  # biomassed
        {"character_id": 4},  # its corporation joined an alliance
        {"character_id": 5},  # ESI calls the id invalid
        {"character_id": 6, "deleted": True},  # not swept
        {"character_id": 7},  # moves to a corporation we do not know
    )
    seen = affiliation_server(
        fake,
        {
            1: (98000001, None),
            2: (98000002, None),
            3: (1000001, None),
            4: (98000001, 99000001),
            7: (98000099, None),
        },
        invalid={5},
    )
    refresh = job(client, entities, env)
    refresh.affiliation_cycle_step(env.registry, cancel=lambda: None)
    # One bad id fails its batch; tenths are retried until it stands alone.
    assert seen == [[1, 2, 3, 4, 5, 7], [1], [2], [3], [4], [5], [7]]

    rows = {r["character_id"]: r for r in entities.lookup("characters", range(1, 8)).to_pylist()}
    assert rows[1]["source"] == "everef_backfill:abc"  # nothing changed, nothing written
    assert rows[2]["corporation_id"] == 98000002 and rows[2]["source"] == "esi"
    assert rows[3]["deleted"] is True and rows[3]["corporation_id"] == 1000001
    assert rows[4]["alliance_id"] == 99000001 and rows[4]["corporation_id"] == 98000001
    assert rows[5]["deleted"] is True
    assert rows[7]["corporation_id"] == 98000099
    assert queue(env.registry) == {
        ("character", 2): CHANGE,
        ("character", 7): CHANGE,
        ("corporation", 98000099): CHANGE,
        ("alliance", 99000001): CHANGE,
    }
    state = json.loads(env.registry.state_get(CYCLE_STATE) or "{}")
    assert state["finished_at"] and state["checked"] == 6 and state["changed"] == 4

    # The cycle is done until it is due again.
    batches = len(seen)
    refresh.affiliation_cycle_step(env.registry, cancel=lambda: None)
    assert len(seen) == batches
    later = job(client, entities, env)
    later.now = lambda: TODAY_AT + timedelta(days=8)
    later.affiliation_cycle_step(env.registry, cancel=lambda: None)
    assert len(seen) > batches


def test_affiliation_of_unknown_and_revived_characters(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_corporations(entities, {"corporation_id": 98000001})
    seed_characters(entities, {"character_id": 1, "corporation_id": 1000001, "deleted": True})
    affiliation_server(fake, {1: (98000001, None), 2: (98000001, None)}, invalid=set())
    refresh = job(client, entities, env)
    refresh.affiliate_recent(env.registry, [1, 2])
    revived = entities.lookup("characters", [1]).to_pylist()[0]
    assert revived["deleted"] is False and revived["corporation_id"] == 98000001
    assert queue(env.registry) == {("character", 1): CHANGE, ("character", 2): CHANGE}
    refresh.affiliate_recent(env.registry, [1, 2])  # once a day
    assert fake.hits["POST /characters/affiliation"] == 1


# --- active and crawl ---------------------------------------------------------------------


def test_active_queues_recent_entities_whose_details_are_stale(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    _, _, client = esi
    for entity_id, refreshed in (
        (1, TODAY_AT - timedelta(days=2)),
        (2, TODAY_AT - timedelta(days=40)),
    ):
        env.registry.refresh_push(
            [RefreshEntry("character", entity_id, priority=IDLE, next_due_at=TODAY_AT)]
        )
        env.registry.refresh_update(
            RefreshEntry(
                "character",
                entity_id,
                priority=IDLE,
                next_due_at=TODAY_AT + timedelta(days=3000),
                last_refreshed_at=refreshed,
            )
        )
    refresh = job(client, entities, env)
    refresh.queue_active(env.registry, {"character": {1, 2, 3}, "corporation": {9}})
    found = queue(env.registry)
    assert found[("character", 1)] == IDLE
    assert found[("character", 2)] == ACTIVE
    assert found[("character", 3)] == ACTIVE and found[("corporation", 9)] == ACTIVE


def test_crawl_feeds_killmail_entities_first_then_everyone_by_id(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    _, _, client = esi
    write_killmails(env, date(2009, 3, 1), victim=11, attacker=12)
    seed_characters(
        entities,
        {"character_id": 11},
        {"character_id": 20},
        {"character_id": 21},
        {"character_id": 22, "deleted": True},
    )
    seed_corporations(entities, {"corporation_id": 98000030}, {"corporation_id": 98000077})
    env.registry.refresh_push(
        [RefreshEntry("character", 21, priority=IDLE, next_due_at=TODAY_AT + timedelta(days=9))]
    )
    refresh = job(client, entities, env, crawl_chunk=1)
    refresh.feed_crawl(env.registry)
    assert queue(env.registry) == {
        ("character", 11): CRAWL,
        ("character", 12): CRAWL,  # on a killmail, even though we have no row for it
        ("corporation", 98000030): CRAWL,
        ("corporation", 1000125): CRAWL,
        ("corporation", 1000001): CRAWL,
        ("character", 21): IDLE,
    }
    assert json.loads(env.registry.state_get(CRAWL_STATE) or "{}")["phase"] == "all"

    refresh.feed_crawl(env.registry)  # chunk of one per kind, highest id first
    found = queue(env.registry)
    assert found[("character", 20)] == CRAWL and found[("corporation", 98000077)] == CRAWL
    for _ in range(4):
        refresh.feed_crawl(env.registry)
    assert ("character", 22) not in queue(env.registry)  # deleted characters are not crawled
    assert json.loads(env.registry.state_get(CRAWL_STATE) or "{}")["phase"] == "done"


def test_crawl_is_not_fed_while_a_day_of_it_is_due(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    _, _, client = esi
    seed_characters(entities, {"character_id": 1})
    env.registry.refresh_push([RefreshEntry("character", 9, priority=CRAWL, next_due_at=TODAY_AT)])
    env.registry.state_put(CRAWL_STATE, json.dumps({"phase": "all", "cursor": {}}), now=TODAY_AT)
    refresh = job(client, entities, env, crawl_low_water=1)
    refresh.feed_crawl(env.registry)
    assert ("character", 1) not in queue(env.registry)


def test_tenths_isolate_an_invalid_id_with_few_errors(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_corporations(entities, {"corporation_id": 98000001})
    seed_characters(entities, *({"character_id": i} for i in range(1, 251)))
    seen = affiliation_server(
        fake, {i: (98000001, None) for i in range(1, 251) if i != 137}, invalid={137}
    )
    refresh = job(client, entities, env)
    refresh.affiliation_cycle_step(env.registry, cancel=lambda: None)
    rejected = [b for b in seen if 137 in b]
    assert [len(b) for b in rejected] == [250, 25, 3, 1]  # four errors, not eight
    assert len(seen) == 1 + 10 + 9 + 3
    assert entities.lookup("characters", [137]).to_pylist()[0]["deleted"] is True


def test_error_cap_stops_the_slice_after_saving_progress(
    env: SyncEnv,
    entities: EntityStore,
    esi: tuple[FakeEsi, FakeClock, EsiClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_module, "SLICE_ERROR_CAP", 3)
    monkeypatch.setattr(refresh_module, "AFFILIATION_BATCH", 10)
    fake, _, client = esi
    seed_corporations(entities, {"corporation_id": 98000001})
    seed_characters(entities, *({"character_id": i} for i in range(1, 31)))
    fake.put("/alliances", [])
    seen = affiliation_server(
        fake, {i: (98000001, None) for i in range(1, 31)}, invalid={4, 15, 26}
    )
    result = run(env, job(client, entities, env, populate=True))
    assert result.status is RunStatus.SUCCEEDED
    # Batch 1 (two errors) and batch 2 (two more) complete, then the cap ends the slice:
    # the cursor is saved past batch 2, so nothing that failed is sent again.
    state = json.loads(env.registry.state_get(CYCLE_STATE) or "{}")
    assert state["cursor"] == 20 and "finished_at" not in state
    sent = len(seen)
    run(env, job(client, entities, env, populate=True))
    assert seen[sent][0] == 21  # resumed with batch 3
    assert not any(4 in b or 15 in b for b in seen[sent:])


# --- daily downtime -----------------------------------------------------------------------


def test_downtime_ends_the_slice_and_leaves_the_entity_due(
    env: SyncEnv, entities: EntityStore, cache: MemoryCache
) -> None:
    at = datetime(2026, 10, 5, 10, 50, tzinfo=UTC)
    fake = FakeEsi(env.router)
    clock = FakeClock(now=at.timestamp())
    client = EsiClient(
        ESI_URL,
        compatibility_date="2026-08-18",
        contact="tests@example.test",
        cache=cache,
        policy_path=env.settings.esi_policy_path,
        daily_budget=1000,
        client=httpx.Client(),
        sleep=clock.sleep,
        clock=clock.time,
    )
    serve_entities(fake)
    fake.queue("/characters/90000059", 502)
    env.registry.refresh_push(
        [RefreshEntry("character", 90000059, priority=CHANGE, next_due_at=at)]
    )

    def slice_() -> Any:
        refresh = RefreshJob(
            client, entities, env.lake, today=TODAY, now=lambda: at, populate=False
        )
        return run(env, refresh)

    result = slice_()
    assert result.status is RunStatus.SUCCEEDED and result.objects_changed == 0
    assert requests(fake) == 1
    (entry,) = env.registry.refresh_pop(limit=10, now=at)
    assert entry.failures == 0 and entry.last_error is None and entry.priority == CHANGE

    # Within the minute the next slice sends nothing; after it, /status says the server is
    # back and the same entity is refreshed.
    assert slice_().objects_changed == 0 and requests(fake) == 1
    fake.put("/status", {"players": 1, "server_version": "1", "start_time": "2026-10-05T10:56:00Z"})
    clock.now += 61
    assert slice_().objects_changed == 1
    assert queue(env.registry) == {("character", 90000059): IDLE}


# --- operator focus (entities add) --------------------------------------------------------


def test_focus_on_an_alliance_queues_its_members_ahead_of_everything(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    serve_entities(fake)
    # ESI lists 98000030 under the alliance now; 98000031 is stored with it but has left.
    fake.put("/alliances/99000001/corporations", [98000030])
    seed_alliances(entities, 99000001)
    seed_corporations(
        entities,
        {"corporation_id": 98000030, "alliance_id": None},
        {"corporation_id": 98000031, "alliance_id": 99000001},
        {"corporation_id": 98000099, "alliance_id": 99000002},
    )
    seed_characters(
        entities,
        {"character_id": 90000059, "corporation_id": 98000030},  # member corporation
        {"character_id": 90000060, "corporation_id": 98000099, "alliance_id": 99000001},
        {"character_id": 90000061, "corporation_id": 98000099},  # elsewhere
        {"character_id": 90000062, "corporation_id": 98000030, "deleted": True},
    )
    env.registry.refresh_push(
        [RefreshEntry("character", 90000089, priority=CHANGE, next_due_at=TODAY_AT)]
    )

    focus = FocusJob(client, entities, "alliance", [99000001], members=True, now=lambda: TODAY_AT)
    result = JobRunner(env.settings, env.registry, env.lock).run(
        "entities:add", focus, trigger=Trigger.MANUAL
    )
    assert result.status is RunStatus.SUCCEEDED and result.objects_changed == 5
    assert fake.hits == {"GET /alliances/99000001/corporations": 1}
    assert queue(env.registry) == {
        ("alliance", 99000001): FOCUS,
        ("corporation", 98000030): FOCUS,
        ("corporation", 98000031): FOCUS,
        ("character", 90000059): FOCUS,
        ("character", 90000060): FOCUS,
        ("character", 90000089): CHANGE,
    }

    # The drain serves the focus entries, in full, before the change queue.
    budget = 2 + 2 + 2 + 2 + 1  # both characters and corporations in full, the alliance
    fake.put("/characters/90000060", character_body("Moved"))
    fake.put("/characters/90000060/corporationhistory", [])
    fake.put("/corporations/98000031", {"name": "Left", "ticker": "L", "member_count": 1})
    fake.put("/corporations/98000031/alliancehistory", [])
    run(env, job(client, entities, env, populate=False, budget=budget))
    assert queue(env.registry)[("character", 90000089)] == CHANGE
    assert {v for k, v in queue(env.registry).items() if k != ("character", 90000089)} == {IDLE}


def test_focus_on_a_corporation_queues_its_known_characters_without_requests(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_characters(
        entities,
        {"character_id": 90000059, "corporation_id": 98000030},
        {"character_id": 90000061, "corporation_id": 98000099},
    )
    env.registry.refresh_push(
        [RefreshEntry("character", 90000059, priority=IDLE, next_due_at=TODAY_AT)]
    )
    focus = FocusJob(
        client, entities, "corporation", [98000030], members=True, now=lambda: TODAY_AT
    )
    JobRunner(env.settings, env.registry, env.lock).run(
        "entities:add", focus, trigger=Trigger.MANUAL
    )
    assert not fake.hits
    assert queue(env.registry) == {
        ("corporation", 98000030): FOCUS,
        ("character", 90000059): FOCUS,  # a settled entry is pulled forward
    }


# --- deleted characters wait behind the crawl ---------------------------------------------


def test_deleted_and_unknown_characters_are_crawled_last_and_still_requested(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    seed_characters(
        entities,
        {"character_id": 90000001, "deleted": True},
        {"character_id": 90000059},
    )
    fake.put("/characters/90000001/corporationhistory", {"error": "deleted"}, status=404)
    fake.put("/characters/90000002", {"error": "deleted"}, status=404)  # never stored
    fake.put("/characters/90000059/corporationhistory", [])
    env.registry.refresh_push(
        [
            RefreshEntry("character", i, priority=CRAWL, next_due_at=TODAY_AT)
            for i in (90000001, 90000002, 90000059)
        ]
    )

    # With room for one request: the deleted and the unknown one move back without a
    # request and the live one is crawled.
    run(env, job(client, entities, env, populate=False, budget=1))
    assert fake.hits == {"GET /characters/90000059/corporationhistory": 1}
    assert queue(env.registry) == {
        ("character", 90000001): DEFERRED,
        ("character", 90000002): DEFERRED,
        ("character", 90000059): IDLE,
    }

    # Once the crawl is empty the deferred entries are requested: history only for the
    # stored one, in full (details first) for the unknown one.
    run(env, job(client, entities, env, populate=False))
    assert fake.hits["GET /characters/90000001/corporationhistory"] == 1
    assert "GET /characters/90000001" not in fake.hits
    assert fake.hits["GET /characters/90000002"] == 1
    parked = env.registry.refresh_pop(limit=10, now=TODAY_AT + timedelta(days=400))
    assert {(e.entity_id, e.last_error) for e in parked} == {
        (90000001, "HTTP 404"),
        (90000002, "HTTP 404"),
    }


def test_a_deleted_entity_on_a_recent_killmail_is_not_asked_again_every_slice(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, client = esi
    fake.put("/characters/90000001", {"error": "deleted"}, status=404)
    recent = {"character": {90000001}}
    refresh = job(client, entities, env, populate=False)
    refresh.queue_active(env.registry, recent)
    run(env, refresh)
    assert fake.hits == {"GET /characters/90000001": 1}

    # The next slice sees the same killmail: the 404 counts as a refresh, so the parked
    # entry is not queued as active and asked again.
    refresh = job(client, entities, env, populate=False)
    refresh.queue_active(env.registry, recent)
    assert env.registry.refresh_pop(limit=10, now=TODAY_AT) == []
    run(env, refresh)
    assert fake.hits == {"GET /characters/90000001": 1}
