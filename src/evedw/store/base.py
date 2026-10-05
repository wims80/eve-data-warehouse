"""Store Protocols. See docs/design.md section 10.

Only packages under ``evedw.store`` implement these, and only they may import a database
driver. Everything crossing the boundary is a domain record or an Arrow table.
"""

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa

from evedw.domain.ids import RunId
from evedw.domain.registry import (
    DatasetSummary,
    DiscoveredObject,
    ImportRun,
    NewRevision,
    ObjectStatus,
    RefreshEntry,
    RevisionStatus,
    RunStatus,
    SourceObject,
    SourceRevision,
    Trigger,
    UpsertSummary,
)


class Registry(Protocol):
    """Source objects, revisions, run log and the entity refresh queue."""

    def migrate(self) -> int:
        """Apply pending schema migrations and return the resulting schema version."""
        ...

    def schema_version(self) -> int:
        """Current schema version, 0 for an empty store."""
        ...

    # -- source objects ------------------------------------------------------------------

    def upsert_objects(
        self, objects: Iterable[DiscoveredObject], *, now: datetime
    ) -> UpsertSummary:
        """Record discovery results.

        A key not seen before is inserted with status ``new``. An existing key whose etag,
        size or last_modified differs is set to ``changed``. An unchanged key only gets
        ``last_seen_at`` and ``expected_count`` refreshed.
        """
        ...

    def mark_missing(self, dataset: str, seen_keys: Collection[str], *, now: datetime) -> int:
        """Set status ``gone`` on dataset objects absent from ``seen_keys``. Returns count."""
        ...

    def mark(
        self, dataset: str, object_key: str, status: ObjectStatus, *, error: str | None = None
    ) -> None: ...

    def mark_parser_outdated(self, dataset: str, parser_version: int) -> int:
        """Set ``changed`` on imported objects whose current revision used an older parser."""
        ...

    def get_object(self, dataset: str, object_key: str) -> SourceObject | None: ...

    def objects(
        self,
        *,
        dataset: str | None = None,
        status: ObjectStatus | Collection[ObjectStatus] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        newest_first: bool = True,
    ) -> list[SourceObject]: ...

    # -- revisions -----------------------------------------------------------------------

    def add_revision(self, revision: NewRevision) -> SourceRevision:
        """Insert the next revision number for the object and return the stored row."""
        ...

    def promote(
        self,
        dataset: str,
        object_key: str,
        revision: int,
        *,
        observed_count: int,
        verified: bool | None,
        imported_at: datetime,
    ) -> None:
        """Make ``revision`` current. One transaction: revision -> imported, previous
        current -> superseded, object -> imported with ``current_revision`` set."""
        ...

    def set_revision_status(
        self, dataset: str, object_key: str, revision: int, status: RevisionStatus
    ) -> None: ...

    def revisions(self, dataset: str, object_key: str) -> list[SourceRevision]: ...

    def current_revision(self, dataset: str, object_key: str) -> SourceRevision | None: ...

    def objects_changed_since(self, dataset: str, since: datetime) -> list[SourceObject]:
        """Objects whose current revision was imported after ``since``."""
        ...

    # -- run log -------------------------------------------------------------------------

    def start_run(
        self, job: str, trigger: Trigger, params: Mapping[str, Any], *, now: datetime
    ) -> ImportRun: ...

    def finish_run(
        self,
        run_id: RunId,
        status: RunStatus,
        *,
        now: datetime,
        objects_changed: int = 0,
        rows_written: int = 0,
        error: str | None = None,
    ) -> None: ...

    def get_run(self, run_id: RunId) -> ImportRun | None: ...

    def runs(self, *, limit: int = 20, job: str | None = None) -> list[ImportRun]: ...

    # -- entity refresh queue ------------------------------------------------------------

    def refresh_push(self, entries: Iterable[RefreshEntry]) -> None:
        """Insert or merge. An existing entry keeps the lower priority value and the
        earlier ``next_due_at``."""
        ...

    def refresh_pop(self, *, limit: int, now: datetime) -> list[RefreshEntry]:
        """Due entries ordered by priority then ``next_due_at``. Does not remove them."""
        ...

    def refresh_update(self, entry: RefreshEntry) -> None: ...

    # -- reporting -----------------------------------------------------------------------

    def dataset_summaries(self) -> list[DatasetSummary]: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PartitionInfo:
    table: str
    partition: str
    path: Path
    row_count: int | None
    metadata: dict[str, str]


class Lake(Protocol):
    """Partitioned Parquet facts. Writes are atomic per partition."""

    def write_partition(
        self, table: str, partition: str, data: pa.Table, *, metadata: Mapping[str, str]
    ) -> PartitionInfo: ...

    def partitions(self, table: str) -> list[PartitionInfo]: ...

    def read(
        self,
        table: str,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        columns: Collection[str] | None = None,
    ) -> pa.Table: ...

    def view_sql(self) -> str:
        """DDL a consumer can run in its own DuckDB to get views over the lake."""
        ...


class EntityStore(Protocol):
    def upsert(self, table: str, rows: pa.Table) -> int:
        """Upsert by the table's primary key; returns rows affected."""
        ...

    def lookup(self, table: str, ids: Collection[int]) -> pa.Table: ...

    def export_parquet(self, directory: Path) -> list[Path]: ...


class Queries(Protocol):
    def names(self) -> list[str]: ...

    def run(self, name: str, params: Mapping[str, Any]) -> pa.Table: ...
