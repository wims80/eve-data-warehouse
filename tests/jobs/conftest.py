from collections.abc import Iterator
from datetime import UTC, datetime

import httpx
import pytest
import respx

from evedw.config import Settings
from evedw.domain.datasets import MARKET_HISTORY
from evedw.jobs.runner import WriterLock
from evedw.sources.esi import EsiClient
from evedw.sources.everef import EveRefClient
from evedw.store import open_lake
from evedw.store.base import Registry
from tests.fake_esi import ESI_URL, FakeClock, FakeEsi, MemoryCache
from tests.helpers import BASE_URL, FakeEveRef, SyncEnv

TODAY_AT = datetime(2026, 10, 5, 12, tzinfo=UTC)
"""The fake ESI clock's start, shared with the refresh and discovery tests."""


@pytest.fixture
def env(settings: Settings, registry: Registry) -> Iterator[SyncEnv]:
    """A migrated registry, a lake, the writer lock and a fake EVE Ref for market history."""
    with respx.mock(assert_all_called=False) as router:
        fake = FakeEveRef(router, MARKET_HISTORY)
        client = EveRefClient(
            BASE_URL, client=httpx.Client(), backoff_seconds=0.0, sleep=lambda _: None, spacing=0.0
        )
        with WriterLock(settings.lock_path) as lock:
            yield SyncEnv(settings, registry, open_lake(settings), lock, client, fake, router)
        client.close()


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
