import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from evedw.config import Settings
from evedw.store import open_entity_store, open_registry, open_response_cache
from evedw.store.base import EntityStore, Registry, ResponseCache
from tests.store.memory import MemoryEntityStore, MemoryRegistry, MemoryResponseCache

BACKENDS = ["duckdb", "memory"]
"""Every test on the ``registry`` fixture runs once per backend. ``memory`` is the test
stub in ``tests/store/memory.py`` that proves the Protocols are complete."""


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """``@pytest.mark.duckdb_only`` keeps a test that needs the real backend off the stub."""
    keep: list[pytest.Item] = []
    dropped: list[pytest.Item] = []
    for item in items:
        callspec = getattr(item, "callspec", None)
        on_stub = callspec is not None and callspec.params.get("registry") == "memory"
        (dropped if on_stub and item.get_closest_marker("duckdb_only") else keep).append(item)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = keep


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    # Keep the real .env and environment out of tests.
    for key in [k for k in os.environ if k.startswith("EVEDW_")]:
        monkeypatch.delenv(key)
    return Settings(data_dir=tmp_path / "data", _env_file=None)  # type: ignore[call-arg]


@pytest.fixture(params=BACKENDS)
def registry(request: pytest.FixtureRequest, settings: Settings) -> Iterator[Registry]:
    reg: Registry = MemoryRegistry() if request.param == "memory" else open_registry(settings)
    reg.migrate()
    try:
        yield reg
    finally:
        reg.close()


@pytest.fixture
def entities(registry: Registry, settings: Settings) -> Iterator[EntityStore]:
    """Entity store of the same backend as ``registry``, already migrated."""
    store: EntityStore = (
        MemoryEntityStore() if isinstance(registry, MemoryRegistry) else open_entity_store(settings)
    )
    try:
        yield store
    finally:
        store.close()


@pytest.fixture
def response_cache(registry: Registry, settings: Settings) -> Iterator[ResponseCache]:
    cache: ResponseCache = (
        MemoryResponseCache()
        if isinstance(registry, MemoryRegistry)
        else open_response_cache(settings)
    )
    try:
        yield cache
    finally:
        cache.close()
