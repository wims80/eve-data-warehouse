"""Response bodies of the service API (design §9), built from domain records. The CLI's
service client parses the same models, so both sides of the wire share one definition."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel

from evedw.domain.ids import RunId
from evedw.domain.registry import (
    DatasetSummary,
    ImportRun,
    RunStatus,
    SourceObject,
    SourceRevision,
    Trigger,
)
from evedw.store.base import PartitionInfo, QueryInfo


class RunOut(BaseModel):
    run_id: str
    job: str
    trigger: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    params: dict[str, Any]
    objects_changed: int
    rows_written: int
    error: str | None

    @classmethod
    def from_domain(cls, run: ImportRun) -> "RunOut":
        return cls(
            run_id=run.run_id,
            job=run.job,
            trigger=run.trigger.value,
            status=run.status.value,
            started_at=run.started_at,
            finished_at=run.finished_at,
            params=run.params,
            objects_changed=run.objects_changed,
            rows_written=run.rows_written,
            error=run.error,
        )

    def to_domain(self) -> ImportRun:
        return ImportRun(
            run_id=RunId(self.run_id),
            job=self.job,
            trigger=Trigger(self.trigger),
            started_at=self.started_at,
            finished_at=self.finished_at,
            status=RunStatus(self.status),
            params=self.params,
            objects_changed=self.objects_changed,
            rows_written=self.rows_written,
            error=self.error,
        )


class RunsOut(BaseModel):
    runs: list[RunOut]


class TriggerOut(BaseModel):
    run_id: str
    job: str
    status: str
    params: dict[str, Any]


class DatasetOut(BaseModel):
    name: str
    parser_version: int
    objects_by_status: dict[str, int]
    oldest_imported: date | None
    newest_imported: date | None

    @classmethod
    def from_domain(
        cls, name: str, parser_version: int, summary: DatasetSummary | None
    ) -> "DatasetOut":
        return cls(
            name=name,
            parser_version=parser_version,
            objects_by_status=summary.objects_by_status if summary else {},
            oldest_imported=summary.oldest_imported if summary else None,
            newest_imported=summary.newest_imported if summary else None,
        )

    def to_domain(self) -> DatasetSummary:
        return DatasetSummary(
            dataset=self.name,
            objects_by_status=self.objects_by_status,
            oldest_imported=self.oldest_imported,
            newest_imported=self.newest_imported,
        )


class DatasetsOut(BaseModel):
    datasets: list[DatasetOut]
    entities: dict[str, int]
    """Row count per entity table."""


class ObjectOut(BaseModel):
    dataset: str
    object_key: str
    url: str
    logical_date: date | None
    upstream_etag: str | None
    upstream_size: int | None
    upstream_last_modified: datetime | None
    expected_count: int | None
    discovered_at: datetime
    last_seen_at: datetime
    current_revision: int | None
    status: str
    last_error: str | None

    @classmethod
    def from_domain(cls, obj: SourceObject) -> "ObjectOut":
        return cls(
            dataset=obj.dataset,
            object_key=obj.object_key,
            url=obj.url,
            logical_date=obj.logical_date,
            upstream_etag=obj.upstream_etag,
            upstream_size=obj.upstream_size,
            upstream_last_modified=obj.upstream_last_modified,
            expected_count=obj.expected_count,
            discovered_at=obj.discovered_at,
            last_seen_at=obj.last_seen_at,
            current_revision=obj.current_revision,
            status=obj.status.value,
            last_error=obj.last_error,
        )


class ObjectsOut(BaseModel):
    dataset: str
    changed_since: datetime | None
    objects: list[ObjectOut]


class RevisionOut(BaseModel):
    revision: int
    sha256: str
    size: int
    raw_path: str
    upstream_etag: str | None
    upstream_last_modified: datetime | None
    fetched_at: datetime
    imported_at: datetime | None
    parser_version: int
    observed_count: int | None
    verified: bool | None
    status: str

    @classmethod
    def from_domain(cls, rev: SourceRevision) -> "RevisionOut":
        return cls(
            revision=rev.revision,
            sha256=rev.sha256,
            size=rev.size,
            raw_path=rev.raw_path,
            upstream_etag=rev.upstream_etag,
            upstream_last_modified=rev.upstream_last_modified,
            fetched_at=rev.fetched_at,
            imported_at=rev.imported_at,
            parser_version=rev.parser_version,
            observed_count=rev.observed_count,
            verified=rev.verified,
            status=rev.status.value,
        )


class ObjectDetailOut(BaseModel):
    object: ObjectOut
    revisions: list[RevisionOut]


class PartitionOut(BaseModel):
    table: str
    partition: str
    path: str
    row_count: int | None
    revision: int | None
    sha256: str | None
    parser_version: int | None
    written_at: datetime | None

    @classmethod
    def from_domain(cls, info: PartitionInfo) -> "PartitionOut":
        meta = info.metadata
        revision = meta.get("evedw.revision")
        parser = meta.get("evedw.parser_version")
        written = meta.get("evedw.written_at")
        return cls(
            table=info.table,
            partition=info.partition,
            path=str(info.path),
            row_count=info.row_count,
            revision=int(revision) if revision and revision.isdigit() else None,
            sha256=meta.get("evedw.source_sha256"),
            parser_version=int(parser) if parser and parser.isdigit() else None,
            written_at=datetime.fromisoformat(written) if written else None,
        )


class LakeOut(BaseModel):
    lake_dir: str
    partitions: list[PartitionOut]


class ScheduleOut(BaseModel):
    name: str
    params: dict[str, Any]
    interval_seconds: int
    next_due: datetime
    last_run: RunOut | None


class HealthOut(BaseModel):
    ok: bool
    version: str
    service_url: str
    lock_held: bool
    data_dir: str
    free_gb: float
    min_free_gb: float
    current_run: RunOut | None
    queued: list[RunOut]
    schedule: list[ScheduleOut]
    last_runs: dict[str, RunOut]
    """Most recent run per job name."""


class QueryParamOut(BaseModel):
    name: str
    kind: str
    required: bool


class QueryOut(BaseModel):
    name: str
    description: str
    params: list[QueryParamOut]

    @classmethod
    def from_domain(cls, info: QueryInfo) -> "QueryOut":
        return cls(
            name=info.name,
            description=info.description,
            params=[
                QueryParamOut(name=p.name, kind=p.kind, required=p.required) for p in info.params
            ],
        )


class QueriesOut(BaseModel):
    queries: list[QueryOut]
    arrow_media_type: str


class ErrorOut(BaseModel):
    detail: str
