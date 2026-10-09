from collections.abc import Iterator

import httpx
import pytest
import respx

from evedw.config import Settings
from evedw.domain.datasets import MARKET_HISTORY
from evedw.jobs.runner import WriterLock
from evedw.sources.everef import EveRefClient
from evedw.store import open_lake
from evedw.store.base import Registry
from tests.helpers import BASE_URL, FakeEveRef, SyncEnv


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
