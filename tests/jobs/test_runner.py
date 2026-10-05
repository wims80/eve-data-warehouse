import logging

import pytest

from evedw.config import Settings
from evedw.domain.registry import RunStatus, Trigger
from evedw.jobs.runner import JobOutcome, JobRunner, LockHeldError, RunContext, WriterLock
from evedw.logs import current_context
from evedw.store.base import Registry


def test_writer_lock_is_exclusive(settings: Settings) -> None:
    first = WriterLock(settings.lock_path)
    second = WriterLock(settings.lock_path)
    with first:
        assert first.held
        with pytest.raises(LockHeldError, match="held by pid"):
            second.acquire()
        assert not second.held
    with second:
        assert second.held


def test_runner_requires_lock(settings: Settings, registry: Registry) -> None:
    runner = JobRunner(settings, registry, WriterLock(settings.lock_path))
    with pytest.raises(RuntimeError, match="writer lock"):
        runner.run("noop", lambda _ctx: JobOutcome(), trigger=Trigger.CLI)


def test_runner_records_success(settings: Settings, registry: Registry) -> None:
    seen: dict[str, object] = {}

    def job(ctx: RunContext) -> JobOutcome:
        seen["context"] = current_context()
        seen["params"] = dict(ctx.params)
        return JobOutcome(objects_changed=2, rows_written=10)

    with WriterLock(settings.lock_path) as lock:
        run = JobRunner(settings, registry, lock).run(
            "sync:test", job, trigger=Trigger.MANUAL, params={"force": True}
        )
    assert run.status is RunStatus.SUCCEEDED
    assert run.objects_changed == 2 and run.rows_written == 10
    assert seen["params"] == {"force": True}
    assert seen["context"] == {"run_id": run.run_id, "job": "sync:test"}
    assert current_context() == {}


def test_runner_records_failure_and_reraises(
    settings: Settings, registry: Registry, caplog: pytest.LogCaptureFixture
) -> None:
    def job(_ctx: RunContext) -> JobOutcome:
        raise ValueError("boom")

    with WriterLock(settings.lock_path) as lock, caplog.at_level(logging.ERROR):
        runner = JobRunner(settings, registry, lock)
        with pytest.raises(ValueError, match="boom"):
            runner.run("sync:test", job, trigger=Trigger.SCHEDULE)
    (run,) = registry.runs()
    assert run.status is RunStatus.FAILED
    assert run.error == "ValueError: boom"
    assert any("run failed" in r.getMessage() for r in caplog.records)
