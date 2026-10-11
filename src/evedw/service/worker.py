"""The service's job worker: two lanes, each running one job at a time (design §8).

Every trigger, whether from the scheduler or from ``POST /jobs``, becomes a queued run in its
job's lane and is executed in a worker thread. The ``esi`` lane runs every job that talks to
ESI (``entities:refresh``, ``entities:add``), so ESI keeps one job and one request at a time;
the ``archive`` lane runs everything else (syncs, merge, seed, export, verify), so a long
backfill no longer stops the entity refresh. A trigger returns a run id at once, and the job
runs when its lane is free. Shutdown sets the cancel flag the jobs check between units of
work, waits for both lanes, and marks queued runs cancelled.

Within a lane queued runs start in order, except that a scheduled ``entities:refresh`` slice
yields to any other queued run: it is filler work, and a manual job should not wait behind it.
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


ESI_JOBS = frozenset({"entities:refresh", "entities:add"})
"""Every job that sends ESI requests. They share the ``esi`` lane, so ESI's one request at a
time holds across jobs; the ESI client also serialises its requests process-wide."""
LANES = ("archive", "esi")


def lane_of(job: str) -> str:
    return "esi" if job in ESI_JOBS else "archive"


@dataclass(slots=True)
class _Lane:
    name: str
    queue: asyncio.Queue[_Item | None] = field(
        default_factory=lambda: asyncio.Queue[_Item | None]()
    )
    """One entry per submit, used only to wake the loop; ``next`` picks the run."""
    pending: list[_Item] = field(default_factory=lambda: [])
    current: ImportRun | None = None
    task: asyncio.Task[None] | None = None

    def next(self) -> _Item:
        return next((i for i in self.pending if not i.yields), self.pending[0])


@dataclass(slots=True)
class JobWorker:
    catalog: JobCatalog
    registry: Registry
    lock: WriterLock
    _lanes: dict[str, _Lane] = field(default_factory=lambda: {n: _Lane(n) for n in LANES})
    _cancel: threading.Event = field(default_factory=threading.Event)

    @property
    def current(self) -> ImportRun | None:
        """The archive lane's run if any, else the ESI lane's."""
        return next((r for r in self.currents), None)

    @property
    def currents(self) -> list[ImportRun]:
        return [lane.current for lane in self._lanes.values() if lane.current is not None]

    @property
    def queued(self) -> list[ImportRun]:
        return [item.run for lane in self._lanes.values() for item in lane.pending]

    def start(self) -> None:
        for lane in self._lanes.values():
            lane.task = asyncio.create_task(self._loop(lane), name=f"evedw-worker-{lane.name}")

    async def stop(self) -> None:
        """Cancel the running job cooperatively, drop the queue and wait for the worker.

        The job runs in a thread that cannot be interrupted, so the worker task is not
        cancelled; it exits by itself once the current job has noticed the cancel flag and
        the stop sentinel reaches the front of the queue.
        """
        self._cancel.set()
        for lane in self._lanes.values():
            for item in lane.pending:
                self.registry.finish_run(
                    item.run.run_id,
                    RunStatus.CANCELLED,
                    now=datetime.now(UTC),
                    error="service stopped before the run started",
                )
                if not item.done.done():
                    item.done.cancel()
            lane.pending.clear()
        for lane in self._lanes.values():
            if lane.task is not None:
                lane.queue.put_nowait(None)
                await lane.task
                lane.task = None

    def submit(self, name: str, params: Mapping[str, Any], *, trigger: Trigger) -> ImportRun:
        """Validate, record a queued run and enqueue it. Raises ``UnknownJobError`` or
        ``ValueError`` before anything is recorded. Must be called on the event loop."""
        normalised = normalise_params(name, params)
        run = self.registry.queue_run(name, trigger, normalised, now=datetime.now(UTC))
        item = _Item(run=run, done=asyncio.get_running_loop().create_future())
        lane = self._lanes[lane_of(name)]
        lane.pending.append(item)
        lane.queue.put_nowait(item)
        log.info(
            "queued %s run %s in the %s lane trigger=%s params=%s",
            name,
            run.run_id,
            lane.name,
            trigger.value,
            normalised,
        )
        return run

    async def submit_and_wait(
        self, name: str, params: Mapping[str, Any], *, trigger: Trigger
    ) -> ImportRun:
        run = self.submit(name, params, trigger=trigger)
        lane = self._lanes[lane_of(name)]
        item = next(i for i in lane.pending if i.run.run_id == run.run_id)
        return await item.done

    async def run_scheduled(self, job: ScheduledJob) -> ImportRun | None:
        return await self.submit_and_wait(job.name, job.params, trigger=Trigger.SCHEDULE)

    def lookup(self, run_id: RunId) -> ImportRun | None:
        return self.registry.get_run(run_id)

    async def _loop(self, lane: _Lane) -> None:
        while True:
            if await lane.queue.get() is None:
                return
            if not lane.pending:
                continue
            item = lane.next()
            lane.pending.remove(item)
            lane.current = item.run
            try:
                finished = await asyncio.to_thread(self._execute, item.run)
            finally:
                lane.current = None
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
