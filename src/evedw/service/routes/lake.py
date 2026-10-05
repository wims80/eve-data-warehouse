"""``GET /lake``: the partition manifest."""

from typing import Annotated

from fastapi import APIRouter, Query, Request

from evedw.domain.schemas import LAKE_TABLES
from evedw.service.models import LakeOut, PartitionOut
from evedw.service.state import state_of

router = APIRouter(prefix="/lake", tags=["lake"])


@router.get("", response_model=LakeOut)
def manifest(
    request: Request,
    table: Annotated[str | None, Query(description="Only this lake table.")] = None,
) -> LakeOut:
    state = state_of(request)
    tables = [table] if table else list(LAKE_TABLES)
    for name in tables:
        if name not in LAKE_TABLES:
            raise KeyError(f"unknown lake table {name!r}; known: {', '.join(LAKE_TABLES)}")
    return LakeOut(
        lake_dir=str(state.settings.lake_dir.resolve()),
        partitions=[
            PartitionOut.from_domain(p) for name in tables for p in state.lake.partitions(name)
        ],
    )
