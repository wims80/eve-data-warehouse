"""The service's single job worker.

Every trigger, whether from the scheduler or from ``POST /jobs``, becomes a queued run and
is executed in a worker thread one at a time. A trigger therefore returns a run id at
once, and the job runs when the writer is free. Shutdown sets the cancel flag the jobs
check between units of work, waits for the current job, and marks queued runs cancelled.

Queued runs start in order, except that a scheduled ``entities:refresh`` slice yields to
any other queued run: it is filler work that runs between syncs, and a backfill should not
wait behind it.
"""

import asyncio
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from evedw.domain.ids import RunId
from evedw.domain.registry import ImportRun, RunStatus, Trigger
from evedw.jobs.catalog import JobCatalog, normalise_params
from evedw.jobs.runner import WriterLock
from evedw.jobs.scheduler import ScheduledJob
from evedw.store.base import Registry

log = logging.getLogger(__name__)


@dataclass(slots=True)
class _Item:
    run: ImportRun
    done: asyncio.Future[ImportRun]

    @property
    def yields(self) -> bool:
        return self.run.job == "entities:refresh" and self.run.trigger is Trigger.SCHEDULE


@dataclass(slots=True)
class JobWorker:
    catalog: JobCatalog
    registry: Registry
    lock: WriterLock
    _queue: asyncio.Queue[_Item | None] = field(
        default_factory=lambda: asyncio.Queue[_Item | None]()
    )
    """One entry per submit, used only to wake the loop; ``_next`` picks the run."""
    _pending: list[_Item] = field(default_factory=lambda: [])
    _current: ImportRun | None = None
    _cancel: threading.Event = field(default_factory=threading.Event)
    _task: asyncio.Task[None] | None = None

    @property
    def current(self) -> ImportRun | None:
        return self._current

    @property
    def queued(self) -> list[ImportRun]:
        return [item.run for item in self._pending]

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="evedw-job-worker")

    async def stop(self) -> None:
        """Cancel the running job cooperatively, drop the queue and wait for the worker.

        The job runs in a thread that cannot be interrupted, so the worker task is not
        cancelled; it exits by itself once the current job has noticed the cancel flag and
        the stop sentinel reaches the front of the queue.
        """
        self._cancel.set()
        for item in self._pending:
            self.registry.finish_run(
                item.run.run_id,
                RunStatus.CANCELLED,
                now=datetime.now(UTC),
                error="service stopped before the run started",
            )
            if not item.done.done():
                item.done.cancel()
        self._pending.clear()
        if self._task is not None:
            self._queue.put_nowait(None)
            await self._task
            self._task = None

    def submit(self, name: str, params: Mapping[str, Any], *, trigger: Trigger) -> ImportRun:
        """Validate, record a queued run and enqueue it. Raises ``UnknownJobError`` or
        ``ValueError`` before anything is recorded. Must be called on the event loop."""
        normalised = normalise_params(name, params)
        run = self.registry.queue_run(name, trigger, normalised, now=datetime.now(UTC))
        item = _Item(run=run, done=asyncio.get_running_loop().create_future())
        self._pending.append(item)
        self._queue.put_nowait(item)
        log.info(
            "queued %s run %s trigger=%s params=%s", name, run.run_id, trigger.value, normalised
        )
        return run

    async def submit_and_wait(
        self, name: str, params: Mapping[str, Any], *, trigger: Trigger
    ) -> ImportRun:
        run = self.submit(name, params, trigger=trigger)
        item = next(i for i in self._pending if i.run.run_id == run.run_id)
        return await item.done

    async def run_scheduled(self, job: ScheduledJob) -> ImportRun | None:
        return await self.submit_and_wait(job.name, job.params, trigger=Trigger.SCHEDULE)

    def lookup(self, run_id: RunId) -> ImportRun | None:
        return self.registry.get_run(run_id)

    def _next(self) -> _Item:
        return next((i for i in self._pending if not i.yields), self._pending[0])

    async def _loop(self) -> None:
        while True:
            if await self._queue.get() is None:
                return
            if not self._pending:
                continue
            item = self._next()
            self._pending.remove(item)
            self._current = item.run
            try:
                finished = await asyncio.to_thread(self._execute, item.run)
            finally:
                self._current = None
            if not item.done.done():
                item.done.set_result(finished)

    def _execute(self, run: ImportRun) -> ImportRun:
        try:
            return self.catalog.run(
                run.job,
                run.params,
                trigger=run.trigger,
                lock=self.lock,
                run_id=run.run_id,
                cancel=self._cancel,
            )
        except Exception:
            # Already recorded and logged by the runner; the outcome is in the registry.
            pass
        finished = self.registry.get_run(run.run_id)
        if finished is None:
            raise RuntimeError(f"run {run.run_id} vanished from the registry")
        return finished
