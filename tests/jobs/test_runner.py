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


def test_lock_holder_reports_service_url(settings: Settings) -> None:
    lock = WriterLock(settings.lock_path)
    assert lock.holder() is None
    lock.acquire(service_url="http://127.0.0.1:8470")
    try:
        other = WriterLock(settings.lock_path)
        holder = other.holder()
        assert holder is not None
        assert holder.service_url == "http://127.0.0.1:8470"
        with pytest.raises(LockHeldError, match=r"service at http://127.0.0.1:8470") as info:
            other.acquire()
        assert info.value.holder.pid is not None
    finally:
        lock.release()
    assert WriterLock(settings.lock_path).holder() is None
    with WriterLock(settings.lock_path) as plain:
        assert plain.holder() is not None and plain.holder().service_url is None  # type: ignore[union-attr]
        held = WriterLock(settings.lock_path).holder()
        assert held is not None and held.service_url is None


def test_runner_continues_a_queued_run_and_records_cancellation(
    settings: Settings, registry: Registry
) -> None:
    from datetime import UTC, datetime
    from threading import Event

    from evedw.jobs.runner import JobCancelled

    queued = registry.queue_run(
        "sync:test", Trigger.MANUAL, {"force": False}, now=datetime.now(UTC)
    )
    cancel = Event()

    def job(ctx: RunContext) -> JobOutcome:
        assert ctx.run_id == queued.run_id
        ctx.check_cancelled()
        cancel.set()
        ctx.check_cancelled()
        raise AssertionError("unreachable")

    with WriterLock(settings.lock_path) as lock:
        runner = JobRunner(settings, registry, lock)
        with pytest.raises(JobCancelled):
            runner.run(
                "sync:test", job, trigger=Trigger.MANUAL, run_id=queued.run_id, cancel=cancel
            )
    (run,) = registry.runs()
    assert run.run_id == queued.run_id
    assert run.status is RunStatus.CANCELLED and run.error == "cancelled"
    assert run.params == {"force": False}
