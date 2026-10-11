"""The job worker's two lanes (design §8): archive and ESI work run at once, ESI jobs never
overlap, and a scheduled refresh slice yields to other ESI work."""

import asyncio
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from evedw.domain.ids import RunId
from evedw.domain.registry import ImportRun, RunStatus, Trigger
from evedw.jobs.catalog import JobCatalog
from evedw.jobs.runner import WriterLock
from evedw.service.worker import JobWorker, lane_of
from evedw.store.base import Registry


class StubCatalog:
    """Runs a job by waiting on its gate; records starts and how many ESI jobs overlap."""

    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self.gates: dict[str, threading.Event] = {}
        self.started: list[str] = []
        self.esi_running = 0
        self.esi_peak = 0
        self._lock = threading.Lock()

    def run(
        self,
        name: str,
        params: Mapping[str, Any],
        *,
        trigger: Trigger,
        lock: WriterLock,
        run_id: RunId | None = None,
        cancel: threading.Event | None = None,
    ) -> ImportRun:
        del params, trigger, lock, cancel
        assert run_id is not None
        self.registry.begin_run(run_id, now=datetime.now(UTC))
        esi = lane_of(name) == "esi"
        with self._lock:
            self.started.append(name)
            if esi:
                self.esi_running += 1
                self.esi_peak = max(self.esi_peak, self.esi_running)
        gate = self.gates.get(name)
        if gate is not None:
            gate.wait(5)
        else:
            time.sleep(0.05)
        with self._lock:
            if esi:
                self.esi_running -= 1
        self.registry.finish_run(run_id, RunStatus.SUCCEEDED, now=datetime.now(UTC))
        run = self.registry.get_run(run_id)
        assert run is not None
        return run


def worker_for(registry: Registry, tmp_path: Path) -> tuple[JobWorker, StubCatalog]:
    registry.migrate()
    catalog = StubCatalog(registry)
    worker = JobWorker(cast(JobCatalog, catalog), registry, WriterLock(tmp_path / "lock"))
    worker.start()
    return worker, catalog


ADD = {"kind": "character", "ids": [90000059]}


def params(name: str) -> dict[str, Any]:
    return ADD if name == "entities:add" else {}


async def until(condition: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.01)


async def test_archive_and_esi_lanes_run_at_once(registry: Registry, tmp_path: Path) -> None:
    worker, catalog = worker_for(registry, tmp_path)
    gate = catalog.gates["sync:zkillboard_killmails"] = threading.Event()
    sync = worker.submit("sync:zkillboard_killmails", {}, trigger=Trigger.SCHEDULE)
    await until(lambda: "sync:zkillboard_killmails" in catalog.started)
    refresh = worker.submit("entities:refresh", {}, trigger=Trigger.SCHEDULE)
    await until(lambda: (r := registry.get_run(refresh.run_id)) is not None and r.status.finished)
    # The ESI job finished while the archive job is still running.
    await until(lambda: {r.job for r in worker.currents} == {"sync:zkillboard_killmails"})
    gate.set()
    await until(lambda: (r := registry.get_run(sync.run_id)) is not None and r.status.finished)
    await worker.stop()


async def test_esi_jobs_never_overlap(registry: Registry, tmp_path: Path) -> None:
    worker, catalog = worker_for(registry, tmp_path)
    runs = [
        worker.submit(name, params(name), trigger=Trigger.MANUAL)
        for name in ("entities:refresh", "entities:add", "entities:refresh", "entities:add")
    ]
    await until(
        lambda: all(
            (r := registry.get_run(x.run_id)) is not None and r.status.finished for x in runs
        )
    )
    assert catalog.esi_peak == 1
    await worker.stop()


async def test_a_scheduled_refresh_yields_to_other_esi_work(
    registry: Registry, tmp_path: Path
) -> None:
    worker, catalog = worker_for(registry, tmp_path)
    gate = catalog.gates["entities:add"] = threading.Event()
    worker.submit("entities:add", ADD, trigger=Trigger.MANUAL)  # occupies the ESI lane
    await until(lambda: catalog.started == ["entities:add"])
    worker.submit("entities:refresh", {}, trigger=Trigger.SCHEDULE)
    worker.submit("entities:export", {}, trigger=Trigger.MANUAL)  # archive lane: runs now
    del catalog.gates["entities:add"]
    later = worker.submit("entities:add", ADD, trigger=Trigger.MANUAL)
    gate.set()
    await until(lambda: (r := registry.get_run(later.run_id)) is not None and r.status.finished)
    await until(lambda: len(catalog.started) == 4)
    esi_order = [n for n in catalog.started if lane_of(n) == "esi"]
    assert esi_order == ["entities:add", "entities:add", "entities:refresh"]
    assert catalog.started.index("entities:export") < catalog.started.index("entities:refresh")
    await worker.stop()
