"""EVE Ref traffic (M7): conditional listing GETs, the header window, the traffic line."""

import dataclasses
import logging
from datetime import timedelta

import httpx
import pytest

from evedw.domain.datasets import MARKET_HISTORY
from evedw.sources.everef import EveRefClient, listing_key
from tests.fake_esi import MemoryCache
from tests.helpers import (
    BASE_URL,
    D1,
    TODAY,
    SyncEnv,
    market_csv_bz2,
    market_name,
    publish_three_days,
)


@pytest.fixture
def cached(env: SyncEnv) -> tuple[SyncEnv, MemoryCache]:
    cache = MemoryCache()
    client = EveRefClient(
        BASE_URL,
        client=httpx.Client(),
        backoff_seconds=0.0,
        sleep=lambda _: None,
        cache=cache,
        spacing=0.0,
    )
    return dataclasses.replace(env, client=client), cache


def listings(env: SyncEnv) -> int:
    return env.fake.requests("GET", suffix="index.json") + env.fake.requests(
        "GET", suffix="totals.json"
    )


def test_an_unchanged_sync_revalidates_every_listing_and_downloads_none(
    cached: tuple[SyncEnv, MemoryCache], caplog: pytest.LogCaptureFixture
) -> None:
    env, cache = cached
    publish_three_days(env.fake)
    env.sync()
    years = len(list(MARKET_HISTORY.years(TODAY)))
    assert listings(env) == years + 1  # every year's index and the totals
    assert not env.fake.not_modified
    assert cache.get(listing_key(f"{BASE_URL}/market-history/totals.json")) is not None

    before_listings = listings(env)
    before_files = env.fake.requests("GET", suffix=".csv.bz2")
    with caplog.at_level(logging.INFO, logger="evedw.jobs.sync"):
        run = env.sync()
    assert run.objects_changed == 0
    # Discovery still reads every year's index, as the invariant requires; each is a 304.
    assert listings(env) - before_listings == years + 1
    assert sum(env.fake.not_modified.values()) == years + 1
    assert env.fake.requests("GET", suffix=".csv.bz2") == before_files
    line = next(r.getMessage() for r in caplog.records if "EVE Ref traffic" in r.getMessage())
    assert line == (
        f"EVE Ref traffic: {years + 1} listing requests ({years + 1} answered 304), "
        "3 HEADs, 0 downloads"
    )


def test_a_changed_year_is_downloaded_again_and_the_rest_answer_304(
    cached: tuple[SyncEnv, MemoryCache],
) -> None:
    env, _ = cached
    publish_three_days(env.fake)
    env.sync()
    env.fake.put(D1, market_csv_bz2(D1, rows=250), expected=250)  # rewrite in 2026
    env.fake.not_modified.clear()
    run = env.sync()
    assert run.objects_changed == 1
    years = len(list(MARKET_HISTORY.years(TODAY)))
    # The 2026 index and the totals changed; every other year answered 304.
    assert sum(env.fake.not_modified.values()) == years - 1
    assert "GET /market-history/2026/index.json" not in env.fake.not_modified
    assert env.obj(D1).current_revision == 2


def test_a_cached_body_whose_year_disappears_is_dropped(
    cached: tuple[SyncEnv, MemoryCache],
) -> None:
    env, cache = cached
    publish_three_days(env.fake)
    env.sync()
    key = listing_key(f"{BASE_URL}/market-history/2026/index.json")
    assert cache.get(key) is not None
    env.fake.missing_years.add(2026)
    env.sync()
    assert cache.get(key) is None


def test_an_index_that_lags_its_file_is_checked_once_per_change_of_the_listing(
    cached: tuple[SyncEnv, MemoryCache],
) -> None:
    env, _ = cached
    old = TODAY - timedelta(days=400)  # outside the header window
    env.fake.put(old, market_csv_bz2(old, rows=100), expected=100)
    env.fake.stale_index[old] = market_csv_bz2(old, rows=90)  # the index lags the file
    head = f"HEAD /market-history/{old.year}/{market_name(old)}"
    env.sync()
    first = env.fake.hits[head]
    assert first == 1  # new object

    env.sync()  # the index still differs from the registry, but the listing is a 304
    assert env.fake.hits[head] == first

    later = old + timedelta(days=1)  # the year's listing changes
    env.fake.put(later, market_csv_bz2(later, rows=50), expected=50)
    env.sync()
    assert env.fake.hits[head] == first + 1
