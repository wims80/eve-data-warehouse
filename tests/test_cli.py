import json
import re
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import respx
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
    assert "upgraded from version 0 to 3" in result.output
    assert (data_dir / "warehouse.duckdb").exists()

    result = runner.invoke(app, ["--data-dir", str(data_dir), "migrate"])
    assert result.exit_code == 0
    assert "current at version 3" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "status"])
    assert result.exit_code == 0, result.output
    assert "schema        version 3" in result.output
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
    assert "change" in result.output and "crawl" in result.output
    assert "affiliation_cycle      not started" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "esi", "status"])
    assert result.exit_code == 0, result.output
    assert "stopped        no" in result.output
    assert "0 of 800000 used" in result.output
    assert "pace           1.00s between requests (calm 0.05s)" in result.output

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


# --- with a service running -----------------------------------------------------------------


SERVICE = "http://127.0.0.1:8470"


@pytest.fixture
def service(data_dir: Path) -> Iterator[respx.Router]:
    """The writer lock held by a 'service' whose API is a respx mock."""
    data_dir.mkdir()
    lock = WriterLock(data_dir / "writer.lock")
    lock.acquire(service_url=SERVICE)
    run = {
        "run_id": "r-1",
        "job": "sync:market_history",
        "trigger": "manual",
        "status": "queued",
        "started_at": "2026-10-05T12:00:00Z",
        "finished_at": None,
        "params": {"from": "2026-10-01", "to": None, "force": False, "sweep": False},
        "objects_changed": 0,
        "rows_written": 0,
        "error": None,
    }
    finished = {**run, "status": "succeeded", "objects_changed": 3, "rows_written": 600}
    try:
        with respx.mock(assert_all_called=False) as router:
            router.post(f"{SERVICE}/jobs/sync:market_history").mock(
                return_value=httpx.Response(
                    202, json={k: run[k] for k in ("run_id", "job", "status", "params")}
                )
            )
            router.get(f"{SERVICE}/runs/r-1").mock(
                side_effect=[httpx.Response(200, json=run), httpx.Response(200, json=finished)]
            )
            router.post(f"{SERVICE}/jobs/verify").mock(
                return_value=httpx.Response(
                    202, json={"run_id": "r-2", "job": "verify", "status": "queued", "params": {}}
                )
            )
            router.get(f"{SERVICE}/runs/r-2").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        **finished,
                        "run_id": "r-2",
                        "job": "verify",
                        "status": "failed",
                        "error": "VerifyError: 2 issues (missing_partition)",
                    },
                )
            )
            router.post(f"{SERVICE}/jobs/sync:killmails").mock(
                return_value=httpx.Response(422, json={"detail": "from: not a date"})
            )
            router.get(f"{SERVICE}/datasets").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "datasets": [
                            {
                                "name": "market_history",
                                "parser_version": 1,
                                "objects_by_status": {"imported": 34, "new": 2},
                                "oldest_imported": "2026-09-01",
                                "newest_imported": "2026-10-04",
                            }
                        ],
                        "entities": {"characters": 7, "corporations": 1},
                    },
                )
            )
            router.get(f"{SERVICE}/runs").mock(
                return_value=httpx.Response(200, json={"runs": [finished]})
            )
            yield router
    finally:
        lock.release()


def test_sync_is_sent_to_the_running_service(
    data_dir: Path, service: respx.Router, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("evedw.service.client.time.sleep", lambda _s: None)  # pyright: ignore[reportUnknownLambdaType]
    result = runner.invoke(
        app, ["--data-dir", str(data_dir), "sync", "market_history", "--from", "2026-10-01"]
    )
    assert result.exit_code == 0, result.output
    assert f"queued run r-1 for sync:market_history on {SERVICE}" in result.output
    assert "run r-1 succeeded: 3 objects imported, 600 rows written" in result.output
    sent = service.routes[0].calls.last.request
    assert json.loads(sent.content) == {
        "from": "2026-10-01",
        "to": None,
        "force": False,
        "sweep": False,
        "offline": False,
    }
    assert not (data_dir / "warehouse.duckdb").exists()

    result = runner.invoke(
        app, ["--data-dir", str(data_dir), "sync", "market_history", "--no-wait"]
    )
    assert (
        result.exit_code == 0
        and "queued run r-1" in result.output
        and "succeeded" not in result.output
    )

    result = runner.invoke(app, ["--data-dir", str(data_dir), "sync", "killmails"])
    assert result.exit_code == 1 and "HTTP 422: from: not a date" in result.output


def test_status_and_verify_use_the_service(data_dir: Path, service: respx.Router) -> None:
    result = runner.invoke(app, ["--data-dir", str(data_dir), "status"])
    assert result.exit_code == 0, result.output
    assert f"service       {SERVICE}" in result.output
    assert re.search(r"market_history\s+1\s+34\s+2\s+0\s+2026-09-01 \.\. 2026-10-04", result.output)
    assert "sync:market_history      manual   succeeded  objects=3 rows=600" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "entities", "status"])
    assert result.exit_code == 0 and "characters                                7" in result.output

    result = runner.invoke(app, ["--data-dir", str(data_dir), "verify", "--no-hash"])
    assert result.exit_code == 1
    assert "run r-2 failed: VerifyError: 2 issues" in result.output
    sent = json.loads(service.routes[2].calls.last.request.content)
    assert sent == {"dataset": None, "from": None, "to": None, "hash": False, "offline": False}

    result = runner.invoke(app, ["--data-dir", str(data_dir), "migrate"])
    assert result.exit_code == 1 and f"service at {SERVICE}" in result.output


def test_unreachable_service_is_an_error(data_dir: Path) -> None:
    data_dir.mkdir()
    lock = WriterLock(data_dir / "writer.lock")
    lock.acquire(service_url="http://127.0.0.1:1")
    try:
        with respx.mock(assert_all_mocked=False) as router:
            router.get("http://127.0.0.1:1/datasets").mock(
                side_effect=httpx.ConnectError("refused")
            )
            result = runner.invoke(app, ["--data-dir", str(data_dir), "status"])
    finally:
        lock.release()
    assert result.exit_code == 1 and "not answering" in result.output


def test_serve_fails_fast_when_lock_held(data_dir: Path) -> None:
    data_dir.mkdir()
    with WriterLock(data_dir / "writer.lock"):
        result = runner.invoke(app, ["--data-dir", str(data_dir), "serve", "--bind", "127.0.0.1:0"])
    assert result.exit_code == 1 and "writer lock" in result.output


def test_speed_without_a_service(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert runner.invoke(app, ["--data-dir", str(data_dir), "migrate"]).exit_code == 0
    policy_path = data_dir / "esi" / "policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(json.dumps({"requests": 10}))

    def more_requests(_seconds: float) -> None:
        policy_path.write_text(json.dumps({"requests": 20}))

    monkeypatch.setattr("evedw.cli.time.sleep", more_requests)
    result = runner.invoke(app, ["--data-dir", str(data_dir), "speed", "--seconds", "1"])
    assert result.exit_code == 0, result.output
    assert re.search(r"esi\s+[\d,]+ requests/min", result.output)
    assert "affiliation    cycle not started" in result.output
    assert "queue          change +0" in result.output


def test_esi_resume_edits_only_the_policy_file(data_dir: Path) -> None:
    policy_path = data_dir / "esi" / "policy.json"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(json.dumps({"stopped": "HTTP 420 on /characters/1"}))
    result = runner.invoke(app, ["--data-dir", str(data_dir), "esi", "resume"])
    assert result.exit_code == 0, result.output
    assert "cleared stop: HTTP 420" in result.output
    assert json.loads(policy_path.read_text())["stopped"] is None
    assert not (data_dir / "warehouse.duckdb").exists()
