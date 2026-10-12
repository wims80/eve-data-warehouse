"""``GET /health``: lock state, free space, current and queued runs, schedule."""

import shutil
from datetime import UTC, datetime

from fastapi import APIRouter, Request

from evedw import __version__
from evedw.jobs.catalog import JOB_NAMES
from evedw.service.models import HealthOut, LiveOut, RunOut, ScheduleOut
from evedw.service.state import state_of

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthOut)
def health(request: Request) -> HealthOut:
    state = state_of(request)
    settings = state.settings
    free_gb = shutil.disk_usage(settings.data_dir).free / 1e9
    last_runs: dict[str, RunOut] = {}
    for job in JOB_NAMES:
        recent = state.registry.runs(limit=1, job=job)
        if recent:
            last_runs[job] = RunOut.from_domain(recent[0])
    # The worker holds runs as queued; the registry has their live status.
    currents = [state.registry.get_run(r.run_id) or r for r in state.worker.currents]
    current = currents[0] if currents else None
    return HealthOut(
        ok=state.lock.held and free_gb >= settings.min_free_gb,
        version=__version__,
        service_url=settings.service_url,
        lock_held=state.lock.held,
        data_dir=str(settings.data_dir.resolve()),
        free_gb=round(free_gb, 2),
        min_free_gb=settings.min_free_gb,
        current_run=RunOut.from_domain(current) if current else None,
        current_runs=[RunOut.from_domain(r) for r in currents],
        queued=[RunOut.from_domain(r) for r in state.worker.queued],
        schedule=[
            ScheduleOut(
                name=str(entry["name"]),
                params=dict(entry["params"]),
                interval_seconds=int(entry["interval_seconds"]),
                next_due=entry["next_due"],
                last_run=RunOut.from_domain(entry["last_run"]) if entry["last_run"] else None,
            )
            for entry in state.scheduler.snapshot()
        ],
        last_runs=last_runs,
        live=LiveOut.from_status(
            state.live.status if state.live is not None else None, now=datetime.now(UTC)
        ),
    )
