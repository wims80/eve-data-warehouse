"""The ESI client against a fake server: caching, conditional requests, pacing, stops."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from evedw.sources.esi import (
    EsiBudgetError,
    EsiClient,
    EsiPermanentError,
    EsiStoppedError,
    EsiTransientError,
    load_policy,
)
from evedw.sources.esi.client import expiry
from tests.fake_esi import ESI_URL, FakeClock, FakeEsi, MemoryCache


@pytest.fixture
def router() -> Iterator[respx.Router]:
    with respx.mock(assert_all_called=False) as mock:
        yield mock


@pytest.fixture
def fake(router: respx.Router) -> FakeEsi:
    return FakeEsi(router)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def cache() -> MemoryCache:
    return MemoryCache()


@pytest.fixture
def policy_path(tmp_path: Path) -> Path:
    return tmp_path / "esi" / "policy.json"


def make_client(
    cache: MemoryCache, clock: FakeClock, policy_path: Path, *, budget: int = 1000
) -> EsiClient:
    return EsiClient(
        ESI_URL,
        compatibility_date="2026-08-18",
        contact="tests@example.test",
        cache=cache,
        policy_path=policy_path,
        daily_budget=budget,
        client=httpx.Client(),
        sleep=clock.sleep,
        clock=clock.time,
    )


@pytest.fixture
def client(cache: MemoryCache, clock: FakeClock, policy_path: Path) -> Iterator[EsiClient]:
    c = make_client(cache, clock, policy_path)
    yield c
    c.close()


def test_headers_and_user_agent(fake: FakeEsi, client: EsiClient) -> None:
    fake.put("/alliances/1", {"name": "A"})
    client.get("/alliances/1")
    request = fake.requests[-1]
    assert request.headers["X-Compatibility-Date"] == "2026-08-18"
    assert request.headers["User-Agent"].startswith("evedw/")
    assert "tests@example.test" in request.headers["User-Agent"]
    assert request.headers["Accept"] == "application/json"


def test_fresh_cache_serves_without_a_request(
    fake: FakeEsi, client: EsiClient, clock: FakeClock
) -> None:
    fake.put("/alliances/1", {"name": "A"}, max_age=600)
    first = client.get("/alliances/1")
    assert not first.from_cache and first.body == {"name": "A"}
    clock.now += 100
    second = client.get("/alliances/1")
    assert second.from_cache and second.body == {"name": "A"}
    assert fake.hits["GET /alliances/1"] == 1
    assert client.policy.cache_hits == 1


def test_expired_cache_sends_conditional_request_and_304_keeps_body(
    fake: FakeEsi, client: EsiClient, clock: FakeClock
) -> None:
    fake.put("/alliances/1", {"name": "A"}, etag="one", max_age=0)
    first = client.get("/alliances/1")
    assert first.etag == '"one"'
    clock.now += 5
    fake.put("/alliances/1", {"name": "A"}, etag="one", max_age=3600)
    second = client.get("/alliances/1")
    assert second.not_modified and second.body == {"name": "A"} and second.status == 200
    assert fake.requests[-1].headers["If-None-Match"] == '"one"'
    assert second.expires_at > datetime.fromtimestamp(clock.now, tz=UTC)
    # Now fresh again: no request.
    assert client.get("/alliances/1").from_cache
    assert fake.hits["GET /alliances/1"] == 2


def test_requests_are_spaced_one_second_apart(
    fake: FakeEsi, client: EsiClient, clock: FakeClock
) -> None:
    fake.put("/alliances/1", {"name": "A"}, max_age=0)
    fake.put("/alliances/2", {"name": "B"}, max_age=0)
    start = clock.now
    client.get("/alliances/1")
    client.get("/alliances/2")
    assert clock.now - start >= 1
    assert client.policy.pauses == 1


def test_permanent_error_is_raised_and_not_cached(
    fake: FakeEsi, client: EsiClient, cache: MemoryCache
) -> None:
    fake.put("/characters/1", {"error": "not found"}, status=404)
    with pytest.raises(EsiPermanentError) as info:
        client.get("/characters/1")
    assert info.value.status == 404
    assert not cache.entries


def test_retry_after_is_honoured_then_request_succeeds(
    fake: FakeEsi, client: EsiClient, clock: FakeClock
) -> None:
    fake.put("/alliances/1", {"name": "A"})
    fake.queue("/alliances/1", 429, {}, headers={"Retry-After": "7"})
    start = clock.now
    response = client.get("/alliances/1")
    assert response.status == 200
    assert clock.now - start >= 7
    assert fake.hits["GET /alliances/1"] == 2
    assert client.policy.retries == 1


def test_transient_failures_stop_after_five_attempts_and_persist_cooldown(
    fake: FakeEsi, client: EsiClient, clock: FakeClock, policy_path: Path
) -> None:
    fake.put("/alliances/1", {}, status=503)
    with pytest.raises(EsiTransientError):
        client.get("/alliances/1")
    assert fake.hits["GET /alliances/1"] == 5
    persisted = load_policy(policy_path)
    assert persisted.blocked_until > clock.now


def test_forbidden_stops_everything_until_resumed(
    fake: FakeEsi, client: EsiClient, cache: MemoryCache, clock: FakeClock, policy_path: Path
) -> None:
    fake.put("/alliances/1", "banned", status=403)
    fake.put("/alliances/2", {"name": "B"})
    with pytest.raises(EsiStoppedError):
        client.get("/alliances/1")
    with pytest.raises(EsiStoppedError):
        client.get("/alliances/2")
    assert fake.hits["GET /alliances/2"] == 0
    # The stop survives a restart.
    restarted = make_client(cache, clock, policy_path)
    with pytest.raises(EsiStoppedError):
        restarted.get("/alliances/2")
    restarted.resume()
    assert restarted.get("/alliances/2").body == {"name": "B"}


def test_420_stops_too(fake: FakeEsi, client: EsiClient) -> None:
    fake.put("/alliances/1", {}, status=420, headers={"X-ESI-Error-Limit-Reset": "30"})
    with pytest.raises(EsiStoppedError):
        client.get("/alliances/1")
    assert client.policy.stopped is not None
    assert fake.hits["GET /alliances/1"] == 1


def test_no_store_is_not_retained(fake: FakeEsi, client: EsiClient, cache: MemoryCache) -> None:
    fake.put("/alliances/1", {"name": "A"}, max_age=None, headers={"Cache-Control": "no-store"})
    client.get("/alliances/1")
    client.get("/alliances/1")
    assert fake.hits["GET /alliances/1"] == 2
    assert not cache.entries


def test_daily_budget_counts_requests_not_cache_hits(
    fake: FakeEsi, cache: MemoryCache, clock: FakeClock, policy_path: Path
) -> None:
    client = make_client(cache, clock, policy_path, budget=2)
    fake.put("/alliances/1", {"name": "A"})
    fake.put("/alliances/2", {"name": "B"})
    fake.put("/alliances/3", {"name": "C"})
    client.get("/alliances/1")
    client.get("/alliances/1")  # cache hit, free
    client.get("/alliances/2")
    assert client.budget_remaining() == 0
    with pytest.raises(EsiBudgetError):
        client.get("/alliances/3")
    clock.now += 86400
    assert client.budget_remaining() == 2
    assert client.get("/alliances/3").body == {"name": "C"}


def test_rate_limit_bucket_pauses_near_exhaustion(
    fake: FakeEsi, client: EsiClient, clock: FakeClock
) -> None:
    fake.put(
        "/characters/1",
        {"name": "X"},
        max_age=0,
        headers={
            "X-RateLimit-Group": "character",
            "X-RateLimit-Limit": "100/1m",
            "X-RateLimit-Remaining": "3",
        },
    )
    fake.put("/characters/2", {"name": "Y"}, max_age=0)
    client.get("/characters/1")
    start = clock.now
    client.get("/characters/2")
    assert clock.now - start >= 60
    assert client.policy.routes["/characters/{id}"] == "character"


def test_post_ids_batches_and_validates(fake: FakeEsi, client: EsiClient) -> None:
    fake.put("/universe/names", [{"id": 1, "name": "A", "category": "character"}])
    response = client.post_ids("/universe/names", [2, 1, 2])
    assert response.body[0]["name"] == "A"
    assert json.loads(fake.requests[-1].content) == [1, 2]
    with pytest.raises(ValueError):
        client.post_ids("/universe/names", [])
    with pytest.raises(ValueError):
        client.post_ids("/universe/names", range(1, 1002))
    with pytest.raises(ValueError):
        client.post_ids("/universe/names", [0])


def test_invalid_route_rejected(client: EsiClient) -> None:
    with pytest.raises(ValueError):
        client.get("alliances/1")
    with pytest.raises(ValueError):
        client.get("/alliances/1?x=1")


def test_expiry_honours_age_expires_and_no_store() -> None:
    assert expiry({"cache-control": "max-age=3600", "age": "600"}, 1000, 10) == 4000
    assert expiry({"cache-control": "no-store"}, 1000, 10) == 1000
    assert expiry({}, 1000, 3600) == 4600
    assert expiry({"expires": "Tue, 08 Sep 2026 00:00:00 GMT"}, 1000, 3600) == 1000 + (
        int(datetime(2026, 9, 8, tzinfo=UTC).timestamp()) - 1000
    )
    headers = {
        "date": "Tue, 08 Sep 2026 00:00:00 GMT",
        "expires": "Tue, 08 Sep 2026 01:00:00 GMT",
        "age": "1800",
    }
    now = int(datetime(2026, 9, 8, 0, 10, tzinfo=UTC).timestamp())
    assert expiry(headers, now, 3600) == now + 1800
