"""``/datasets``: summaries, the incremental-pull object listing, revision chains."""

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from evedw.domain.datasets import DATASETS, get_dataset
from evedw.domain.schemas import ENTITY_TABLES
from evedw.jobs.refresh import refresh_status
from evedw.service.models import (
    DatasetOut,
    DatasetsOut,
    ObjectDetailOut,
    ObjectOut,
    ObjectsOut,
    RevisionOut,
)
from evedw.service.state import state_of

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.get("", response_model=DatasetsOut)
def list_datasets(request: Request) -> DatasetsOut:
    state = state_of(request)
    summaries = {s.dataset: s for s in state.registry.dataset_summaries()}
    queue, sweeps = refresh_status(state.registry, now=datetime.now(UTC))
    return DatasetsOut(
        datasets=[
            DatasetOut.from_domain(name, ds.parser_version, summaries.get(name))
            for name, ds in sorted(DATASETS.items())
        ],
        entities={table: state.entities.count(table) for table in ENTITY_TABLES},
        refresh_queue=queue,
        refresh_state=sweeps,
    )


@router.get("/{name}/objects", response_model=ObjectsOut)
def list_objects(
    request: Request,
    name: str,
    changed_since: Annotated[
        datetime | None,
        Query(description="Only objects whose current revision was imported after this instant."),
    ] = None,
) -> ObjectsOut:
    state = state_of(request)
    dataset = get_dataset(name)
    if changed_since is not None:
        if changed_since.tzinfo is None:
            raise ValueError("changed_since needs a timezone, e.g. 2026-10-05T00:00:00Z")
        objects = state.registry.objects_changed_since(dataset.name, changed_since)
    else:
        objects = state.registry.objects(dataset=dataset.name, newest_first=False)
    return ObjectsOut(
        dataset=dataset.name,
        changed_since=changed_since,
        objects=[ObjectOut.from_domain(o) for o in objects],
    )


@router.get("/{name}/objects/{key:path}", response_model=ObjectDetailOut)
def object_detail(request: Request, name: str, key: str) -> ObjectDetailOut:
    state = state_of(request)
    dataset = get_dataset(name)
    obj = state.registry.get_object(dataset.name, key)
    if obj is None:
        raise HTTPException(status_code=404, detail=f"no object {key!r} in {dataset.name}")
    return ObjectDetailOut(
        object=ObjectOut.from_domain(obj),
        revisions=[RevisionOut.from_domain(r) for r in state.registry.revisions(dataset.name, key)],
    )
