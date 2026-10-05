"""``/runs``: the run log."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from evedw.domain.ids import RunId
from evedw.service.models import RunOut, RunsOut
from evedw.service.state import state_of

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_model=RunsOut)
def list_runs(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=1000)] = 20,
    job: Annotated[str | None, Query(description="Only runs of this job name.")] = None,
) -> RunsOut:
    state = state_of(request)
    return RunsOut(runs=[RunOut.from_domain(r) for r in state.registry.runs(limit=limit, job=job)])


@router.get("/{run_id}", response_model=RunOut)
def get_run(request: Request, run_id: str) -> RunOut:
    state = state_of(request)
    run = state.registry.get_run(RunId(run_id))
    if run is None:
        raise HTTPException(status_code=404, detail=f"no run {run_id}")
    return RunOut.from_domain(run)
