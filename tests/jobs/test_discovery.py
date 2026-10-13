"""Entity discovery and manual add (M10) against a fake eve-kill and a fake ESI."""

import json
from collections import Counter
from typing import Any

import httpx
import pytest
import respx

from evedw.domain.registry import RunStatus, Trigger
from evedw.jobs.discovery import DiscoveryJob, load_state
from evedw.jobs.focus import FocusJob
from evedw.jobs.runner import JobRunner
from evedw.sources.esi import EsiClient
from evedw.sources.evekill import EveKillClient, EveKillRefused
from evedw.store.base import EntityStore
from tests.fake_esi import FakeClock, FakeEsi
from tests.helpers import SyncEnv
from tests.jobs.test_refresh import (
    CHANGE,
    TODAY_AT,
    character_body,
    queue,
    seed_characters,
    seed_corporations,
)

EVEKILL_URL = "https://evekill.test/api"


def person(
    character_id: int, corporation_id: int = 98000001, alliance_id: int | None = None
) -> dict[str, Any]:
    return {
        "character_id": character_id,
        "name": f"C{character_id}",
        "corporation_id": corporation_id,
        "alliance_id": alliance_id,
        "faction_id": None,
        "security_status": 0,
        "last_active": None,
    }


class FakeEveKill:
    """``/characters`` and ``/{kind}s/{id}/members`` paged by ascending id, ``after`` and
    ``limit`` as eve-kill serves them."""

    def __init__(self, router: respx.Router, page: int = 2) -> None:
        self.characters: list[dict[str, Any]] = []
        self.members: dict[str, list[int]] = {}
        self.page = page
        self.hits: Counter[str] = Counter()
        self.answers: list[httpx.Response] = []
        """One-shot answers served before anything else, e.g. a 429."""
        router.route(url__regex=rf"{EVEKILL_URL}/.*").mock(side_effect=self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api")
        self.hits[path] += 1
        if self.answers:
            return self.answers.pop(0)
        after = int(request.url.params.get("after", "0"))
        if path == "/characters":
            rows = sorted(self.characters, key=lambda r: r["character_id"])
        elif path in self.members:
            rows = [person(i) for i in sorted(self.members[path])]
        else:
            return httpx.Response(404)
        rows = [r for r in rows if r["character_id"] > after]
        chunk = rows[: self.page]
        more = len(rows) > self.page
        cursor = chunk[-1]["character_id"] if chunk else None
        return httpx.Response(
            200, json={"data": chunk, "pagination": {"cursor": cursor, "hasMore": more}}
        )


@pytest.fixture
def evekill(env: SyncEnv) -> FakeEveKill:
    return FakeEveKill(env.router)


def client(slept: list[float] | None = None) -> EveKillClient:
    return EveKillClient(
        EVEKILL_URL,
        client=httpx.Client(),
        spacing=0.0,
        backoff_seconds=0.0,
        sleep=(slept.append if slept is not None else lambda _: None),
    )


def discover(env: SyncEnv, entities: EntityStore, **kwargs: Any) -> Any:
    job = DiscoveryJob(client(), entities, now=lambda: TODAY_AT, **kwargs)
    return JobRunner(env.settings, env.registry, env.lock).run(
        "entities:discover", job, trigger=Trigger.MANUAL
    )


def test_discovery_queues_new_characters_past_the_highest_id_and_what_they_name(
    env: SyncEnv, entities: EntityStore, evekill: FakeEveKill
) -> None:
    seed_characters(entities, {"character_id": 90000010}, {"character_id": 90000020})
    seed_corporations(entities, {"corporation_id": 98000001})
    evekill.characters = [
        person(90000005),  # below our highest id: only the full walk sees it
        person(90000010),
        person(90000020),
        person(90000021),
        person(90000022, corporation_id=98000002, alliance_id=99000002),
        person(90000023),
    ]
    run = discover(env, entities, full_pages=0)
    assert run.status is RunStatus.SUCCEEDED
    assert queue(env.registry) == {
        ("character", 90000021): CHANGE,
        ("character", 90000022): CHANGE,
        ("character", 90000023): CHANGE,
        ("corporation", 98000002): CHANGE,
        ("alliance", 99000002): CHANGE,
    }
    state = load_state(env.registry)
    assert state["after"] == 90000023 and state["new_found"] == 3
    assert evekill.hits["/characters"] == 2  # two pages past 90000020

    evekill.characters.append(person(90000030))  # created since
    discover(env, entities, full_pages=0)
    assert ("character", 90000030) in queue(env.registry)
    assert load_state(env.registry)["new_found"] == 4
    assert evekill.hits["/characters"] == 3  # resumed past 90000023: one page


def test_the_full_walk_runs_in_slices_resumes_and_finishes_once(
    env: SyncEnv, entities: EntityStore, evekill: FakeEveKill
) -> None:
    seed_characters(entities, {"character_id": 90000010})
    seed_corporations(entities, {"corporation_id": 98000001})
    evekill.characters = [person(i) for i in (90000001, 90000002, 90000003, 90000004, 90000010)]
    discover(env, entities, full_pages=1)
    state = load_state(env.registry)
    assert state["full_after"] == 90000002 and state["full_found"] == 2
    assert "full_done_at" not in state

    discover(env, entities, full_pages=1)
    discover(env, entities, full_pages=1)
    state = load_state(env.registry)
    assert state["full_done_at"] is not None and state["full_found"] == 4
    assert state["full_pages"] == 3
    walked = evekill.hits["/characters"]
    discover(env, entities, full_pages=1)  # done: only the walk past the highest id
    assert evekill.hits["/characters"] == walked + 1
    assert set(queue(env.registry)) == {
        ("character", i) for i in (90000001, 90000002, 90000003, 90000004)
    }


def test_a_429_waits_for_retry_after_and_a_403_stops(evekill: FakeEveKill) -> None:
    evekill.characters = [person(90000001)]
    evekill.answers = [httpx.Response(429, headers={"Retry-After": "45"})]
    slept: list[float] = []
    c = client(slept)
    assert [r["character_id"] for r in c.characters(after=None).rows] == [90000001]
    assert slept == [45.0] and c.requests == 2
    evekill.answers = [httpx.Response(403)]
    with pytest.raises(EveKillRefused):
        c.characters(after=None)
    assert c.requests == 3  # never retried
    c.close()


# -- entities add ----------------------------------------------------------------------------


def add(env: SyncEnv, focus: FocusJob) -> Any:
    return JobRunner(env.settings, env.registry, env.lock).run(
        "entities:add", focus, trigger=Trigger.MANUAL
    )


def test_adding_a_character_by_name_stores_details_and_history_in_one_run(
    env: SyncEnv, entities: EntityStore, esi: tuple[FakeEsi, FakeClock, EsiClient]
) -> None:
    fake, _, esi_client = esi

    def resolve(request: httpx.Request) -> httpx.Response:
        names = json.loads(request.content)
        assert names == ["Dame Hel", "Nobody Here"]
        return httpx.Response(200, json={"characters": [{"id": 90000077, "name": "Dame Hel"}]})

    fake.handlers["/universe/ids"] = resolve
    fake.put("/characters/90000077", character_body("Dame Hel"))
    fake.put(
        "/characters/90000077/corporationhistory",
        [{"record_id": 1, "corporation_id": 98000030, "start_date": "2010-11-02T17:28:00Z"}],
    )
    focus = FocusJob(
        esi_client,
        entities,
        "character",
        [],
        names=["Dame Hel", "Nobody Here"],
        lake=env.lake,
        refresh=True,
        now=lambda: TODAY_AT,
    )
    result = add(env, focus)
    assert result.status is RunStatus.SUCCEEDED, result.error
    (row,) = entities.lookup("characters", [90000077]).to_pylist()
    assert row["name"] == "Dame Hel" and row["source"] == "esi"
    assert entities.lookup("character_employment", [90000077]).num_rows == 1
    assert fake.hits["GET /characters/90000077"] == 1


def test_adding_a_corporation_with_members_stores_the_members_evekill_lists(
    env: SyncEnv,
    entities: EntityStore,
    esi: tuple[FakeEsi, FakeClock, EsiClient],
    evekill: FakeEveKill,
) -> None:
    fake, _, esi_client = esi
    seed_characters(entities, {"character_id": 90000040, "corporation_id": 98000030})
    evekill.members["/corporations/98000030/members"] = [90000040, 90000041, 90000042]
    fake.put("/corporations/98000030", {"name": "Corp", "ticker": "C", "member_count": 3})
    fake.put("/corporations/98000030/alliancehistory", [])
    for character_id in (90000040, 90000041, 90000042):
        fake.put(f"/characters/{character_id}", character_body(f"M{character_id}"))
        fake.put(f"/characters/{character_id}/corporationhistory", [])
    focus = FocusJob(
        esi_client,
        entities,
        "corporation",
        [98000030],
        members=True,
        evekill=client(),
        lake=env.lake,
        refresh=True,
        now=lambda: TODAY_AT,
    )
    result = add(env, focus)
    assert result.status is RunStatus.SUCCEEDED, result.error
    stored = {
        r["character_id"] for r in entities.lookup("characters", [90000041, 90000042]).to_pylist()
    }
    assert stored == {90000041, 90000042}
    assert entities.lookup("corporations", [98000030]).to_pylist()[0]["name"] == "Corp"
    assert evekill.hits["/corporations/98000030/members"] == 2  # two pages of members
