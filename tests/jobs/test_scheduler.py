"""Interval scheduler: seeding from the run log, due ordering, rescheduling after runs."""

from datetime import UTC, datetime, timedelta

import pytest

from evedw.config import Settings
from evedw.domain.registry import ImportRun, RunStatus, Trigger
from evedw.jobs.scheduler import ScheduledJob, Scheduler, default_jobs
from evedw.store.base import Registry

T0 = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


class Clock:
    def __init__(self, now: datetime = T0) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def test_default_jobs_skip_refresh_without_contact(settings: Settings) -> None:
    names = [j.label for j in default_jobs(settings)]
    assert names == [
        "sync:killmails",
        "sync:market_history",
        "sync:killmails sweep",
        "sync:market_history sweep",
        "entities:seed",
        "entities:export",
        "verify hash",
    ]
    settings.esi_contact = "ops@example.test"
    with_refresh = {j.name: j for j in default_jobs(settings)}
    assert with_refresh["entities:refresh"].params == {"budget": settings.refresh_slice}
    assert with_refresh["entities:refresh"].interval == settings.refresh_interval
    assert with_refresh["entities:seed"].params == {"snapshot": "all"}
    assert with_refresh["entities:seed"].interval == settings.seed_interval


def test_seed_uses_last_matching_run(registry: Registry) -> None:
    plain = registry.start_run(
        "sync:killmails", Trigger.CLI, {"sweep": False, "from": None}, now=T0 - timedelta(hours=1)
    )
    registry.finish_run(plain.run_id, RunStatus.SUCCEEDED, now=T0 - timedelta(minutes=50))
    sweep = registry.start_run(
        "sync:killmails", Trigger.SCHEDULE, {"sweep": True}, now=T0 - timedelta(days=8)
    )
    registry.finish_run(sweep.run_id, RunStatus.FAILED, now=T0 - timedelta(days=8), error="x")
    queued = registry.queue_run("verify", Trigger.MANUAL, {"hash": True}, now=T0)
    old_style = registry.start_run(
        "sync:market_history", Trigger.CLI, {}, now=T0 - timedelta(hours=2)
    )
    registry.finish_run(old_style.run_id, RunStatus.SUCCEEDED, now=T0 - timedelta(hours=2))

    jobs = [
        ScheduledJob("sync:killmails", timedelta(hours=6), {"sweep": False}),
        ScheduledJob("sync:killmails", timedelta(days=7), {"sweep": True}, defer_first=True),
        ScheduledJob("verify", timedelta(days=7), {"hash": True}, defer_first=True),
        ScheduledJob("entities:export", timedelta(days=1)),
        ScheduledJob("sync:market_history", timedelta(hours=6), {"sweep": False}),
    ]
    clock = Clock()
    scheduler = Scheduler(jobs, lambda _job: _never(), now=clock)
    scheduler.seed(registry)
    by_label = {j.label: j for j in jobs}
    # Cadence measured from the end of the last run, whatever its trigger.
    assert by_label["sync:killmails"].next_due == T0 - timedelta(minutes=50) + timedelta(hours=6)
    # A failed run still counts; it is already overdue, so it is due now.
    assert by_label["sync:killmails sweep"].next_due == T0
    assert by_label["sync:killmails sweep"].last_run is not None
    # Queued runs do not count; never run and deferred -> one interval out.
    assert by_label["verify hash"].next_due == T0 + timedelta(days=7)
    assert queued.status is RunStatus.QUEUED
    # Never run and not deferred -> now.
    assert by_label["entities:export"].next_due == T0
    # A run recorded before the sweep flag existed matches the non-sweep entry.
    assert by_label["sync:market_history"].next_due == T0 + timedelta(hours=4)
    assert [j.label for j in scheduler.due()] == ["sync:killmails sweep", "entities:export"]


async def _never() -> ImportRun | None:
    raise AssertionError("submit must not be called")


def test_cancelled_runs_do_not_push_the_schedule_back(registry: Registry) -> None:
    done = registry.start_run(
        "sync:killmails", Trigger.SCHEDULE, {"sweep": False}, now=T0 - timedelta(hours=8)
    )
    registry.finish_run(done.run_id, RunStatus.SUCCEEDED, now=T0 - timedelta(hours=7))
    stopped = registry.start_run(
        "sync:killmails", Trigger.MANUAL, {"sweep": False}, now=T0 - timedelta(minutes=5)
    )
    registry.finish_run(stopped.run_id, RunStatus.CANCELLED, now=T0 - timedelta(minutes=4))
    job = ScheduledJob("sync:killmails", timedelta(hours=6), {"sweep": False})
    Scheduler([job], lambda _job: _never(), now=Clock()).seed(registry)
    assert job.last_run is not None and job.last_run.run_id == done.run_id
    assert job.next_due == T0


async def test_run_pending_runs_due_jobs_in_order_and_reschedules() -> None:
    clock = Clock()
    calls: list[str] = []

    async def submit(job: ScheduledJob) -> ImportRun | None:
        calls.append(job.label)
        clock.now += timedelta(minutes=10)
        if job.name == "verify":
            raise RuntimeError("worker gone")
        return ImportRun(
            run_id="r",  # type: ignore[arg-type]
            job=job.name,
            trigger=Trigger.SCHEDULE,
            started_at=clock.now,
            finished_at=clock.now,
            status=RunStatus.SUCCEEDED,
        )

    jobs = [
        ScheduledJob("verify", timedelta(days=7), next_due=T0 - timedelta(minutes=1)),
        ScheduledJob("sync:killmails", timedelta(hours=6), next_due=T0 - timedelta(minutes=2)),
        ScheduledJob("entities:export", timedelta(days=1), next_due=T0 + timedelta(hours=1)),
    ]
    scheduler = Scheduler(jobs, submit, now=clock)
    assert await scheduler.run_pending() == 2
    assert calls == ["sync:killmails", "verify"]
    by_name = {j.name: j for j in jobs}
    assert by_name["sync:killmails"].next_due == T0 + timedelta(minutes=10, hours=6)
    assert by_name["sync:killmails"].last_run is not None
    # A submit failure is logged and the entry rescheduled at its normal interval.
    assert by_name["verify"].next_due == T0 + timedelta(minutes=20, days=7)
    assert by_name["entities:export"].next_due == T0 + timedelta(hours=1)
    assert await scheduler.run_pending() == 0
    assert scheduler.next_due() == T0 + timedelta(hours=1)
    snapshot = scheduler.snapshot()
    assert snapshot[1]["name"] == "sync:killmails" and snapshot[1]["interval_seconds"] == 21600


async def test_run_forever_sleeps_until_next_due(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Clock()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise StopAsyncIteration

    async def submit(_job: ScheduledJob) -> ImportRun | None:
        return None

    monkeypatch.setattr("evedw.jobs.scheduler.asyncio.sleep", fake_sleep)
    jobs = [ScheduledJob("entities:export", timedelta(days=1), next_due=T0 + timedelta(seconds=30))]
    scheduler = Scheduler(jobs, submit, now=clock, max_sleep=timedelta(seconds=60))
    with pytest.raises(StopAsyncIteration):
        await scheduler.run_forever()
    assert sleeps == [30.0, 30.0]
