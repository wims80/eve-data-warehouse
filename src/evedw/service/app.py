"""FastAPI application factory (design §9).

The lifespan takes the writer lock with the service URL written into the lock file, opens
the store handles, starts the job worker and the scheduler, and releases everything on
shutdown. Routes live in ``evedw.service.routes``.
"""

import asyncio
import contextlib
import logging
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from evedw import __version__
from evedw.config import Settings
from evedw.jobs.catalog import JobCatalog, UnknownJobError
from evedw.jobs.follower import LiveFollower
from evedw.jobs.live_flush import LiveFlush
from evedw.jobs.live_raw import LiveRaw
from evedw.jobs.runner import WriterLock
from evedw.jobs.scheduler import Scheduler, default_jobs
from evedw.service.routes import datasets, health, jobs, lake, live, query, runs
from evedw.service.state import ServiceState
from evedw.service.worker import JobWorker
from evedw.store import open_entity_store, open_lake, open_queries, open_registry

log = logging.getLogger(__name__)


class NotMigratedError(RuntimeError):
    pass


def create_app(settings: Settings, *, schedule: bool = True, follow: bool | None = None) -> FastAPI:
    """``schedule=False`` leaves the scheduler idle; tests trigger jobs explicitly. The live
    feed is followed when ``EVEDW_ZKILLBOARD_LIVE`` is on and ``follow`` (by default
    ``schedule``) allows it."""
    follow_live = (schedule if follow is None else follow) and settings.zkillboard_live

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        settings.ensure_dirs()
        lock = WriterLock(settings.lock_path)
        lock.acquire(service_url=settings.service_url)
        registry = open_registry(settings)
        try:
            if registry.schema_version() == 0:
                raise NotMigratedError("registry is not migrated; run `evedw migrate` first")
            orphans = registry.cancel_unfinished_runs(
                now=datetime.now(UTC), error="interrupted: the service stopped before it finished"
            )
            if orphans:
                log.warning(
                    "recorded %d unfinished runs of an earlier process as cancelled", orphans
                )
            lake_store = open_lake(settings)
            if lake_store.layout_version() < 2:
                raise NotMigratedError("the lake has an old layout; run `evedw migrate` first")
            entities = open_entity_store(settings)
            queries = open_queries(settings, lake_store)
            catalog = JobCatalog(settings, registry, lake_store, entities)
            worker = JobWorker(catalog, registry, lock)
            scheduler = Scheduler(default_jobs(settings) if schedule else [], worker.run_scheduled)
            scheduler.seed(registry)
            follower: LiveFollower | None = None
            if follow_live:
                raw = LiveRaw(settings.live_raw_dir)
                follower = LiveFollower(
                    client=catalog.zkillboard_client(),
                    registry=registry,
                    raw=raw,
                    flush=LiveFlush(lake_store, raw, settings.scratch_dir / "live"),
                    flush_interval=settings.live_flush_interval,
                )
            app.state.warehouse = ServiceState(
                settings=settings,
                lock=lock,
                registry=registry,
                lake=lake_store,
                entities=entities,
                queries=queries,
                catalog=catalog,
                worker=worker,
                scheduler=scheduler,
                live=follower,
            )
            worker.start()
            ticker = asyncio.create_task(scheduler.run_forever(), name="evedw-scheduler")
            stop_live = threading.Event()
            following = (
                asyncio.create_task(asyncio.to_thread(follower.run, stop_live), name="evedw-live")
                if follower is not None
                else None
            )
            log.info("service ready at %s, data dir %s", settings.service_url, settings.data_dir)
            try:
                yield
            finally:
                log.info("service stopping")
                stop_live.set()
                if following is not None:
                    await following
                ticker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ticker
                await worker.stop()
                queries.close()
                entities.close()
        finally:
            registry.close()
            lock.release()

    app = FastAPI(
        title="EVE data warehouse",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    app.include_router(health.router)
    app.include_router(live.router)
    app.include_router(datasets.router)
    app.include_router(jobs.router)
    app.include_router(runs.router)
    app.include_router(lake.router)
    app.include_router(query.router)

    @app.exception_handler(UnknownJobError)
    async def unknown_job(_request: Request, exc: UnknownJobError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc.args[0])})

    @app.exception_handler(ValueError)
    async def bad_value(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(KeyError)
    async def missing(_request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc.args[0])})

    return app
