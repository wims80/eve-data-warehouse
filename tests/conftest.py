import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from evedw.config import Settings
from evedw.store import open_registry
from evedw.store.base import Registry


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
