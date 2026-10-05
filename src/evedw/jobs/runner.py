"""Writer lock and job runner.

Exactly one process writes to the warehouse at a time. The service holds the writer lock
for its lifetime; a CLI invocation with no service running takes it for the duration of
its job. ``JobRunner`` records every execution in the run log and binds the logging
context so each line carries the run id.
"""

import fcntl
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Self

from evedw.config import Settings
from evedw.domain.ids import RunId
from evedw.domain.registry import ImportRun, RunStatus, Trigger
from evedw.logs import log_context
from evedw.store.base import Registry

log = logging.getLogger(__name__)


class LockHeldError(RuntimeError):
    """Another process holds the writer lock."""


class WriterLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: IO[str] | None = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self) -> None:
        if self._fh is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.path.open("a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.seek(0)
            owner = fh.read().strip() or "unknown pid"
            fh.close()
            raise LockHeldError(
                f"writer lock {self.path} is held by {owner}; "
                "stop the other evedw process or trigger the job through the service"
            ) from None
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid {os.getpid()}\n")
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        if self._fh is None:
            return
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


@dataclass(slots=True)
class JobOutcome:
    objects_changed: int = 0
    rows_written: int = 0


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: RunId
    job: str
    settings: Settings
    registry: Registry
    params: Mapping[str, Any] = field(default_factory=lambda: {})


JobFn = Callable[[RunContext], JobOutcome]


class JobRunner:
    def __init__(self, settings: Settings, registry: Registry, lock: WriterLock) -> None:
        self._settings = settings
        self._registry = registry
        self._lock = lock

    def run(
        self,
        job: str,
        fn: JobFn,
        *,
        trigger: Trigger,
        params: Mapping[str, Any] | None = None,
    ) -> ImportRun:
        if not self._lock.held:
            raise RuntimeError("JobRunner.run called without the writer lock held")
        params = dict(params or {})
        started = self._registry.start_run(job, trigger, params, now=datetime.now(UTC))
        ctx = RunContext(
            run_id=started.run_id,
            job=job,
            settings=self._settings,
            registry=self._registry,
            params=params,
        )
        with log_context(run_id=started.run_id, job=job):
            log.info("run started trigger=%s params=%s", trigger.value, params)
            try:
                outcome = fn(ctx)
            except KeyboardInterrupt:
                self._registry.finish_run(
                    started.run_id, RunStatus.CANCELLED, now=datetime.now(UTC), error="interrupted"
                )
                log.warning("run cancelled")
                raise
            except Exception as exc:
                self._registry.finish_run(
                    started.run_id,
                    RunStatus.FAILED,
                    now=datetime.now(UTC),
                    error=f"{type(exc).__name__}: {exc}",
                )
                log.exception("run failed")
                raise
            self._registry.finish_run(
                started.run_id,
                RunStatus.SUCCEEDED,
                now=datetime.now(UTC),
                objects_changed=outcome.objects_changed,
                rows_written=outcome.rows_written,
            )
            log.info(
                "run finished objects_changed=%d rows_written=%d",
                outcome.objects_changed,
                outcome.rows_written,
            )
        finished = self._registry.get_run(started.run_id)
        if finished is None:
            raise RuntimeError("run vanished from the registry")
        return finished
