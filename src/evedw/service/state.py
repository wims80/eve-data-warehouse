"""What the service owns for its lifetime: the writer lock, the store handles, the job
worker and the scheduler. Routes reach it through ``state_of(request)``."""

from dataclasses import dataclass

from fastapi import Request

from evedw.config import Settings
from evedw.jobs.catalog import JobCatalog
from evedw.jobs.runner import WriterLock
from evedw.jobs.scheduler import Scheduler
from evedw.service.worker import JobWorker
from evedw.store.base import EntityStore, Lake, Queries, Registry


@dataclass(slots=True)
class ServiceState:
    settings: Settings
    lock: WriterLock
    registry: Registry
    lake: Lake
    entities: EntityStore
    queries: Queries
    catalog: JobCatalog
    worker: JobWorker
    scheduler: Scheduler


def state_of(request: Request) -> ServiceState:
    state = getattr(request.app.state, "warehouse", None)
    if not isinstance(state, ServiceState):
        raise RuntimeError("service state is not initialised; the lifespan did not run")
    return state
