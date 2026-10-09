"""Registry record types and their status vocabularies. See docs/design.md section 5."""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import IntEnum, StrEnum
from typing import Any

from evedw.domain.ids import RunId


class ObjectStatus(StrEnum):
    NEW = "new"
    CHANGED = "changed"
    FETCHING = "fetching"
    FETCHED = "fetched"
    IMPORTING = "importing"
    IMPORTED = "imported"
    FAILED = "failed"
    GONE = "gone"
    SKIPPED = "skipped"
    """Listed upstream, deliberately not imported: the dataset marks it unsupported."""


PENDING_STATUSES: frozenset[ObjectStatus] = frozenset(
    {ObjectStatus.NEW, ObjectStatus.CHANGED, ObjectStatus.FETCHED, ObjectStatus.FAILED}
)
"""Statuses a sync run should pick up and work on."""


class RevisionStatus(StrEnum):
    FETCHED = "fetched"
    IMPORTED = "imported"
    SUPERSEDED = "superseded"
    FAILED = "failed"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def finished(self) -> bool:
        return self in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)


class RefreshClass(IntEnum):
    """Entity refresh queue priorities (design §7.3). Lower is served first."""

    FOCUS = -1
    """An operator asked for it with ``entities add``: refreshed in full before anything else."""
    CHANGE = 0
    """A sweep saw the entity change, or found an entity we do not have."""
    ACTIVE = 1
    """On recent killmails, or in an alliance, with details older than the interval."""
    IDLE = 2
    """Refreshed; not due until something queues it again."""
    CRAWL = 3
    """History never fetched from ESI; filled with whatever budget is left."""
    DEFERRED = 4
    """A crawl entry whose entity is marked deleted or not stored at all (seen only on old
    killmails): still requested, after the rest of the crawl, because most of them answer
    404 and every 404 slows the pace."""


class Trigger(StrEnum):
    SCHEDULE = "schedule"
    MANUAL = "manual"
    CLI = "cli"


@dataclass(frozen=True, slots=True)
class DiscoveredObject:
    """What discovery learned about one upstream file from index.json and totals.json."""

    dataset: str
    object_key: str
    url: str
    logical_date: date | None
    etag: str | None
    size: int | None
    last_modified: datetime | None
    expected_count: int | None
    listing_unchanged: bool = False
    """Its listing answered 304: the entry is what the previous sync saw, so a difference
    from the registry was already checked by HEAD then. Not stored."""


@dataclass(frozen=True, slots=True)
class SourceObject:
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
    status: ObjectStatus
    last_error: str | None

    def differs_from(self, seen: DiscoveredObject) -> bool:
        return (
            self.upstream_etag != seen.etag
            or self.upstream_size != seen.size
            or _to_second(self.upstream_last_modified) != _to_second(seen.last_modified)
        )


def _to_second(value: datetime | None) -> datetime | None:
    """index.json carries milliseconds, HTTP Last-Modified only whole seconds."""
    return None if value is None else value.replace(microsecond=0)


@dataclass(frozen=True, slots=True)
class NewRevision:
    dataset: str
    object_key: str
    sha256: str
    size: int
    raw_path: str
    upstream_etag: str | None
    upstream_last_modified: datetime | None
    parser_version: int
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class SourceRevision:
    dataset: str
    object_key: str
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
    status: RevisionStatus


@dataclass(frozen=True, slots=True)
class ImportRun:
    run_id: RunId
    job: str
    trigger: Trigger
    started_at: datetime
    finished_at: datetime | None
    status: RunStatus
    params: dict[str, Any] = field(default_factory=lambda: {})
    objects_changed: int = 0
    rows_written: int = 0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RefreshEntry:
    kind: str
    entity_id: int
    priority: int
    next_due_at: datetime
    last_refreshed_at: datetime | None = None
    etag: str | None = None
    failures: int = 0
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class UpsertSummary:
    new: int = 0
    changed: int = 0
    unchanged: int = 0

    @property
    def total(self) -> int:
        return self.new + self.changed + self.unchanged


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    dataset: str
    objects_by_status: dict[str, int]
    oldest_imported: date | None
    newest_imported: date | None
