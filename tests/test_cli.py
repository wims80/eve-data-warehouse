from pathlib import Path

import pytest
from typer.testing import CliRunner

from evedw.cli import app
from evedw.jobs.runner import WriterLock

runner = CliRunner()


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv("EVEDW_DATA_DIR", raising=False)
    monkeypatch.chdir(tmp_path)  # no project .env in reach
    return tmp_path / "data"


def test_status_before_migrate(data_dir: Path) -> None:
    result = runner.invoke(app, ["--data-dir", str(data_dir), "status"])
    assert result.exit_code == 1
    assert "run `evedw migrate` first" in result.output


def test_migrate_then_status(data_dir: Path) -> None:
    result = runner.invoke(app, ["--data-dir", str(data_dir), "migrate"])
    assert result.exit_code == 0, result.output
    assert "upgraded from version 0 to 2" in result.output
    assert (data_dir / "warehouse.duckdb").exists()

    result = runner.invoke(app, ["--data-dir", str(data_dir), "migrate"])
    assert result.exit_code == 0
    assert "current at version 2" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "status"])
    assert result.exit_code == 0, result.output
    assert "schema        version 2" in result.output
    assert "killmails" in result.output
    assert "market_history" in result.output
    assert "no runs recorded" in result.output


def test_migrate_fails_fast_when_lock_held(data_dir: Path) -> None:
    data_dir.mkdir()
    with WriterLock(data_dir / "writer.lock"):
        result = runner.invoke(app, ["--data-dir", str(data_dir), "migrate"])
    assert result.exit_code == 1
    assert "writer lock" in result.output and "held by pid" in result.output


def test_entities_and_esi_status(data_dir: Path) -> None:
    assert runner.invoke(app, ["--data-dir", str(data_dir), "migrate"]).exit_code == 0
    result = runner.invoke(app, ["--data-dir", str(data_dir), "entities", "status"])
    assert result.exit_code == 0, result.output
    assert "characters" in result.output and "corporation_alliance_history" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "esi", "status"])
    assert result.exit_code == 0, result.output
    assert "stopped        no" in result.output
    assert "0 of 20000 used" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "esi", "resume"])
    assert result.exit_code == 0 and "not stopped" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "entities", "seed", "not-a-date"])
    assert result.exit_code == 2 and "not a date" in result.output


def test_entities_export_before_seed_writes_empty_files(data_dir: Path) -> None:
    assert runner.invoke(app, ["--data-dir", str(data_dir), "migrate"]).exit_code == 0
    result = runner.invoke(app, ["--data-dir", str(data_dir), "entities", "export"])
    assert result.exit_code == 0, result.output
    assert "5 files written, 0 rows written" in result.output
    assert (data_dir / "lake" / "entities" / "characters.parquet").is_file()
