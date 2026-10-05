from datetime import timedelta
from pathlib import Path

import pytest

from evedw.config import Settings


def test_defaults(settings: Settings) -> None:
    assert settings.bind == "127.0.0.1:8470"
    assert settings.min_free_gb == 20.0
    assert settings.sync_interval_killmails == timedelta(hours=6)
    assert settings.warehouse_path == settings.data_dir / "warehouse.duckdb"
    assert settings.lock_path.parent == settings.data_dir


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("EVEDW_DATA_DIR", str(tmp_path / "elsewhere"))
    monkeypatch.setenv("EVEDW_SYNC_INTERVAL_KILLMAILS", "3600")
    monkeypatch.setenv("EVEDW_ESI_CONTACT", "someone@example.test")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.data_dir == tmp_path / "elsewhere"
    assert settings.sync_interval_killmails == timedelta(hours=1)
    assert settings.esi_contact == "someone@example.test"


def test_ensure_dirs_creates_layout(settings: Settings) -> None:
    settings.ensure_dirs()
    assert settings.raw_dir.is_dir()
    assert settings.lake_dir.is_dir()
    assert settings.scratch_dir.is_dir()
