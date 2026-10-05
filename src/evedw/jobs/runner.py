"""Writer lock and job runner.

Exactly one process writes to the warehouse at a time. The service holds the writer lock
for its lifetime; a CLI invocation with no service running takes it for the duration of
its job. ``JobRunner`` records every execution in the run log and binds the logging
context so each line carries the run id.
"""

import fcntl
import logging
import os
import threading
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

SERVICE_NOTE = "service"
"""Lock-file line prefix the service writes so a CLI can find it: ``service <url>``."""


class LockHeldError(RuntimeError):
    """Another process holds the writer lock."""

    def __init__(self, path: Path, holder: "LockHolder") -> None:
        self.path = path
        self.holder = holder
        where = f" (service at {holder.service_url})" if holder.service_url else ""
        super().__init__(
            f"writer lock {path} is held by {holder.describe()}{where}; "
            "stop the other evedw process or trigger the job through the service"
        )


@dataclass(frozen=True, slots=True)
class LockHolder:
    """What the lock file says about the process holding it."""

    pid: int | None
    service_url: str | None

    @classmethod
    def parse(cls, text: str) -> "LockHolder":
        pid: int | None = None
        service_url: str | None = None
        for line in text.splitlines():
            key, _, value = line.strip().partition(" ")
            if key == "pid" and value.isdigit():
                pid = int(value)
            elif key == SERVICE_NOTE and value:
                service_url = value
        return cls(pid=pid, service_url=service_url)

    def describe(self) -> str:
        return f"pid {self.pid}" if self.pid is not None else "unknown pid"


class WriterLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: IO[str] | None = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self, *, service_url: str | None = None) -> None:
        """Take the lock or raise ``LockHeldError``. A service passes its URL so that a
        CLI finding the lock held knows where to send its request instead."""
        if self._fh is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.path.open("a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.seek(0)
            holder = LockHolder.parse(fh.read())
            fh.close()
            raise LockHeldError(self.path, holder) from None
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid {os.getpid()}\n")
        if service_url is not None:
            fh.write(f"{SERVICE_NOTE} {service_url}\n")
        fh.flush()
        self._fh = fh

    def holder(self) -> LockHolder | None:
        """Who holds the lock right now, without taking it. ``None`` when it is free."""
        if self._fh is not None:
            return LockHolder(pid=os.getpid(), service_url=None)
        if not self.path.exists():
            return None
        with self.path.open("r+") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fh.seek(0)
                return LockHolder.parse(fh.read())
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        return None

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


class JobCancelled(Exception):
    """Raised by a job that noticed ``RunContext.cancel`` was set."""


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
    cancel: threading.Event = field(default_factory=threading.Event)
    """Set by the service on shutdown. Jobs check it between units of work and raise
    ``JobCancelled``; the run is then recorded as cancelled."""

    def check_cancelled(self) -> None:
        if self.cancel.is_set():
            raise JobCancelled(f"run {self.run_id} cancelled")


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
        run_id: RunId | None = None,
        cancel: threading.Event | None = None,
    ) -> ImportRun:
        """Execute ``fn`` under the run log. ``run_id`` continues a run the registry already
        holds in status ``queued``; otherwise a new running row is inserted."""
        if not self._lock.held:
            raise RuntimeError("JobRunner.run called without the writer lock held")
        params = dict(params or {})
        now = datetime.now(UTC)
        if run_id is None:
            run_id = self._registry.start_run(job, trigger, params, now=now).run_id
        else:
            self._registry.begin_run(run_id, now=now)
        ctx = RunContext(
            run_id=run_id,
            job=job,
            settings=self._settings,
            registry=self._registry,
            params=params,
            cancel=cancel or threading.Event(),
        )
        with log_context(run_id=run_id, job=job):
            log.info("run started trigger=%s params=%s", trigger.value, params)
            try:
                outcome = fn(ctx)
            except (KeyboardInterrupt, JobCancelled) as exc:
                reason = "interrupted" if isinstance(exc, KeyboardInterrupt) else "cancelled"
                self._registry.finish_run(
                    run_id, RunStatus.CANCELLED, now=datetime.now(UTC), error=reason
                )
                log.warning("run %s", reason)
                raise
            except Exception as exc:
                self._registry.finish_run(
                    run_id,
                    RunStatus.FAILED,
                    now=datetime.now(UTC),
                    error=f"{type(exc).__name__}: {exc}",
                )
                log.exception("run failed")
                raise
            self._registry.finish_run(
                run_id,
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
        finished = self._registry.get_run(run_id)
        if finished is None:
            raise RuntimeError("run vanished from the registry")
        return finished
