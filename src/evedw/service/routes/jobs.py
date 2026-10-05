"""``POST /jobs/{name}``: queue a job and return its run id."""

from typing import Any

from fastapi import APIRouter, Request

from evedw.domain.registry import Trigger
from evedw.jobs.catalog import JOB_NAMES
from evedw.service.models import TriggerOut
from evedw.service.state import state_of

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("")
def list_jobs() -> dict[str, list[str]]:
    return {"jobs": list(JOB_NAMES)}


@router.post("/{name}", response_model=TriggerOut, status_code=202)
async def trigger(request: Request, name: str, params: dict[str, Any] | None = None) -> TriggerOut:
    """Body: the job's parameters as a JSON object, e.g. ``{"from": "2026-10-01",
    "force": true}``. Unknown names are 404, bad parameters 422. The run is queued behind
    whatever the worker is doing and starts when the writer is free. Async on purpose:
    the worker's queue belongs to the event loop."""
    state = state_of(request)
    run = state.worker.submit(name, params or {}, trigger=Trigger.MANUAL)
    return TriggerOut(run_id=run.run_id, job=run.job, status=run.status.value, params=run.params)
