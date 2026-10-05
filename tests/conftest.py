import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from evedw.config import Settings
from evedw.store import open_entity_store, open_registry, open_response_cache
from evedw.store.base import EntityStore, Registry, ResponseCache


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    # Keep the real .env and environment out of tests.
    for key in [k for k in os.environ if k.startswith("EVEDW_")]:
        monkeypatch.delenv(key)
    return Settings(data_dir=tmp_path / "data", _env_file=None)  # type: ignore[call-arg]


@pytest.fixture(params=["duckdb"])
def registry(request: pytest.FixtureRequest, settings: Settings) -> Iterator[Registry]:
    settings.store_backend = request.param
    reg = open_registry(settings)
    reg.migrate()
    try:
        yield reg
    finally:
        reg.close()


@pytest.fixture
def entities(registry: Registry, settings: Settings) -> Iterator[EntityStore]:
    """Entity store of the same backend as ``registry``, already migrated."""
    store = open_entity_store(settings)
    try:
        yield store
    finally:
        store.close()


@pytest.fixture
def response_cache(registry: Registry, settings: Settings) -> Iterator[ResponseCache]:
    cache = open_response_cache(settings)
    try:
        yield cache
    finally:
        cache.close()
