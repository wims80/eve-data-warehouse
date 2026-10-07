import os
from datetime import timedelta
from pathlib import Path

import pytest

from evedw.config import Settings, project_home


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


@pytest.mark.parametrize("blank", ["", "  "])
def test_blank_esi_contact_is_unset(monkeypatch: pytest.MonkeyPatch, blank: str) -> None:
    monkeypatch.setenv("EVEDW_ESI_CONTACT", blank)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.esi_contact is None


def test_home_from_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("EVEDW_HOME", str(tmp_path))
    assert project_home() == tmp_path.resolve()


def test_home_defaults_to_the_source_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EVEDW_HOME")
    assert (project_home() / "src" / "evedw" / "config.py").is_file()


def test_load_reads_the_home_env_from_anywhere(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for key in [k for k in os.environ if k.startswith("EVEDW_") and k != "EVEDW_HOME"]:
        monkeypatch.delenv(key)
    (tmp_path / ".env").write_text("EVEDW_DATA_DIR=store\nEVEDW_ESI_CONTACT=me@example.test\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    settings = Settings.load()
    assert settings.data_dir == tmp_path / "store"
    assert settings.esi_contact == "me@example.test"
    assert Settings.load(data_dir=Path("/srv/evedw")).data_dir == Path("/srv/evedw")
