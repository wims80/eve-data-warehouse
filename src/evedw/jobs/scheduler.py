"""Interval scheduler for the service (design §8).

Each entry has a job name, parameters, an interval and a next-due time. The loop runs the
earliest due entry through ``submit`` and waits for it to finish before computing its next
due time, so a job's cadence is measured from the end of its previous run and a slow run
can never queue itself twice. Entries never overlap because ``submit`` goes through the
service's single job worker. Next-due times are seeded from the run log, so a restart does
not reset a weekly job.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from evedw.config import Settings
from evedw.domain.registry import ImportRun, RunStatus
from evedw.store.base import Registry

log = logging.getLogger(__name__)

Submit = Callable[["ScheduledJob"], Awaitable[ImportRun | None]]
"""Runs the job and returns once it has finished, with the recorded run when there is one."""


@dataclass(slots=True)
class ScheduledJob:
    name: str
    interval: timedelta
    params: dict[str, Any] = field(default_factory=lambda: {})
    next_due: datetime = field(default_factory=lambda: datetime.now(UTC))
    defer_first: bool = False
    """When the run log has no matching run, wait one interval instead of running at
    startup. Set on the expensive weekly jobs."""
    last_run: ImportRun | None = None

    @property
    def label(self) -> str:
        flags = [k for k, v in self.params.items() if v is True]
        return f"{self.name} {' '.join(flags)}".strip()

    def matches(self, run: ImportRun) -> bool:
        """A run of this job whose parameters agree with this entry's. Any trigger counts:
        a manual sync pushes the next scheduled one back by a full interval."""
        if run.job != self.name or run.status is RunStatus.QUEUED:
            return False
        return all(_json_equal(run.params.get(k), v) for k, v in self.params.items())


def _json_equal(stored: Any, wanted: Any) -> bool:
    """``ImportRun.params`` come back through JSON; compare dates and ints as strings. A
    key a run never recorded (older CLI runs predate ``sweep``) counts as switched off."""
    if isinstance(wanted, bool) or wanted is None:
        return stored == wanted or (stored is None and not wanted)
    return stored == wanted or str(stored) == str(wanted)


def default_jobs(settings: Settings) -> list[ScheduledJob]:
    jobs = [
        ScheduledJob("sync:killmails", settings.sync_interval_killmails, {"sweep": False}),
        ScheduledJob("sync:market_history", settings.sync_interval_market, {"sweep": False}),
        ScheduledJob("sync:killmails", settings.sweep_interval, {"sweep": True}, defer_first=True),
        ScheduledJob(
            "sync:market_history", settings.sweep_interval, {"sweep": True}, defer_first=True
        ),
        ScheduledJob(
            "entities:refresh", settings.refresh_interval, {"budget": settings.refresh_slice}
        ),
        ScheduledJob("entities:export", settings.export_interval),
        ScheduledJob("verify", settings.verify_interval, {"hash": True}, defer_first=True),
    ]
    if settings.esi_contact is None:
        log.warning(
            "EVEDW_ESI_CONTACT is not set; entities:refresh is not scheduled "
            "(ESI asks for contact details in the User-Agent)"
        )
        jobs = [j for j in jobs if j.name != "entities:refresh"]
    return jobs


class Scheduler:
    def __init__(
        self,
        jobs: list[ScheduledJob],
        submit: Submit,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_sleep: timedelta = timedelta(seconds=60),
    ) -> None:
        self.jobs = jobs
        self._submit = submit
        self._now = now
        self._max_sleep = max_sleep

    def seed(self, registry: Registry) -> None:
        """Set every entry's next-due time from its last run in the registry."""
        now = self._now()
        for job in self.jobs:
            last = next((r for r in registry.runs(job=job.name, limit=50) if job.matches(r)), None)
            job.last_run = last
            if last is not None:
                anchor = last.finished_at or last.started_at
                job.next_due = max(now, anchor + job.interval)
            elif job.defer_first:
                job.next_due = now + job.interval
            else:
                job.next_due = now
            log.info("scheduled %s every %s, next at %s", job.label, job.interval, job.next_due)

    def due(self, now: datetime | None = None) -> list[ScheduledJob]:
        now = now or self._now()
        return sorted((j for j in self.jobs if j.next_due <= now), key=lambda j: j.next_due)

    def next_due(self) -> datetime | None:
        return min((j.next_due for j in self.jobs), default=None)

    async def run_pending(self) -> int:
        """Run every entry that is due, one after another. Returns how many ran."""
        ran = 0
        while due := self.due():
            job = due[0]
            # Reschedule before running so a crash in submit cannot spin the loop.
            job.next_due = self._now() + job.interval
            try:
                job.last_run = await self._submit(job)
            except Exception:
                log.exception("scheduled %s could not be submitted", job.label)
            job.next_due = self._now() + job.interval
            ran += 1
        return ran

    async def run_forever(self) -> None:
        while True:
            await self.run_pending()
            wait = self._max_sleep
            upcoming = self.next_due()
            if upcoming is not None:
                wait = min(wait, max(upcoming - self._now(), timedelta(0)))
            await asyncio.sleep(wait.total_seconds())

    def snapshot(self) -> list[Mapping[str, Any]]:
        return [
            {
                "name": job.name,
                "params": dict(job.params),
                "interval_seconds": int(job.interval.total_seconds()),
                "next_due": job.next_due,
                "last_run": job.last_run,
            }
            for job in self.jobs
        ]
