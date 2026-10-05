"""Registry implementation on a DuckDB file.

Timestamps are stored as naive TIMESTAMP columns holding UTC. Conversion to and from
aware datetimes happens at this boundary only.
"""

import json
import re
import threading
from collections.abc import Collection, Iterable, Mapping
from datetime import UTC, date, datetime
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from evedw.domain.ids import RunId, new_run_id
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

_MIGRATION_NAME = re.compile(r"^(\d{3})_.+\.sql$")

_OBJECT_COLUMNS = (
    "dataset, object_key, url, logical_date, upstream_etag, upstream_size, "
    "upstream_last_modified, expected_count, discovered_at, last_seen_at, "
    "current_revision, status, last_error"
)
_REVISION_COLUMNS = (
    "dataset, object_key, revision, sha256, size, raw_path, upstream_etag, "
    "upstream_last_modified, fetched_at, imported_at, parser_version, observed_count, "
    "verified, status"
)
_RUN_COLUMNS = (
    "run_id, job, trigger, started_at, finished_at, status, params_json, objects_changed, "
    "rows_written, error"
)
_REFRESH_COLUMNS = (
    "kind, entity_id, priority, last_refreshed_at, next_due_at, etag, failures, last_error"
)


def _to_db(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("naive datetime reached the store; use UTC-aware datetimes")
    return value.astimezone(UTC).replace(tzinfo=None)


def _from_db(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime from store, got {type(value).__name__}")
    return value.replace(tzinfo=UTC)


class DuckDBRegistry:
    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self._path = path
        self._con = duckdb.connect(str(path), read_only=read_only)
        self._lock = threading.RLock()

    # -- schema ---------------------------------------------------------------------------

    def schema_version(self) -> int:
        with self._lock:
            exists = self._con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'schema_version'"
            ).fetchone()
            if exists is None or exists[0] == 0:
                return 0
            row = self._con.execute(
                "SELECT coalesce(max(version), 0) FROM schema_version"
            ).fetchone()
            return int(row[0]) if row else 0

    def migrate(self) -> int:
        with self._lock:
            self._con.execute(
                "CREATE TABLE IF NOT EXISTS schema_version "
                "(version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL)"
            )
            current = self.schema_version()
            for version, sql in self._migrations():
                if version <= current:
                    continue
                self._con.begin()
                try:
                    self._con.execute(sql)
                    self._con.execute(
                        "INSERT INTO schema_version VALUES (?, ?)",
                        [version, _to_db(datetime.now(UTC))],
                    )
                    self._con.commit()
                except Exception:
                    self._con.rollback()
                    raise
                current = version
            return current

    @staticmethod
    def _migrations() -> list[tuple[int, str]]:
        package = resources.files("evedw.store.duckdb_lake") / "migrations"
        found: list[tuple[int, str]] = []
        for entry in package.iterdir():
            match = _MIGRATION_NAME.match(entry.name)
            if match:
                found.append((int(match.group(1)), entry.read_text(encoding="utf-8")))
        found.sort(key=lambda item: item[0])
        return found

    # -- source objects ------------------------------------------------------------------

    def upsert_objects(
        self, objects: Iterable[DiscoveredObject], *, now: datetime
    ) -> UpsertSummary:
        new = changed = unchanged = 0
        ts = _to_db(now)
        with self._lock:
            self._con.begin()
            try:
                for seen in objects:
                    existing = self.get_object(seen.dataset, seen.object_key)
                    if existing is None:
                        self._con.execute(
                            f"INSERT INTO source_object ({_OBJECT_COLUMNS}) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)",
                            [
                                seen.dataset,
                                seen.object_key,
                                seen.url,
                                seen.logical_date,
                                seen.etag,
                                seen.size,
                                _to_db(seen.last_modified),
                                seen.expected_count,
                                ts,
                                ts,
                                ObjectStatus.NEW.value,
                            ],
                        )
                        new += 1
                    elif existing.differs_from(seen) or existing.status is ObjectStatus.GONE:
                        self._con.execute(
                            "UPDATE source_object SET url = ?, upstream_etag = ?, "
                            "upstream_size = ?, upstream_last_modified = ?, expected_count = ?, "
                            "last_seen_at = ?, status = ?, last_error = NULL "
                            "WHERE dataset = ? AND object_key = ?",
                            [
                                seen.url,
                                seen.etag,
                                seen.size,
                                _to_db(seen.last_modified),
                                seen.expected_count,
                                ts,
                                ObjectStatus.CHANGED.value,
                                seen.dataset,
                                seen.object_key,
                            ],
                        )
                        changed += 1
                    else:
                        self._con.execute(
                            "UPDATE source_object SET expected_count = ?, last_seen_at = ? "
                            "WHERE dataset = ? AND object_key = ?",
                            [seen.expected_count, ts, seen.dataset, seen.object_key],
                        )
                        unchanged += 1
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise
        return UpsertSummary(new=new, changed=changed, unchanged=unchanged)

    def mark_missing(self, dataset: str, seen_keys: Collection[str], *, now: datetime) -> int:
        with self._lock:
            rows = self._con.execute(
                "SELECT object_key FROM source_object WHERE dataset = ? AND status <> ?",
                [dataset, ObjectStatus.GONE.value],
            ).fetchall()
            missing = [str(r[0]) for r in rows if r[0] not in seen_keys]
            for key in missing:
                self._con.execute(
                    "UPDATE source_object SET status = ?, last_seen_at = ? "
                    "WHERE dataset = ? AND object_key = ?",
                    [ObjectStatus.GONE.value, _to_db(now), dataset, key],
                )
            return len(missing)

    def mark(
        self, dataset: str, object_key: str, status: ObjectStatus, *, error: str | None = None
    ) -> None:
        with self._lock:
            self._con.execute(
                "UPDATE source_object SET status = ?, last_error = ? "
                "WHERE dataset = ? AND object_key = ?",
                [status.value, error, dataset, object_key],
            )

    def mark_parser_outdated(self, dataset: str, parser_version: int) -> int:
        with self._lock:
            rows = self._con.execute(
                "SELECT o.object_key FROM source_object o "
                "JOIN source_revision r ON r.dataset = o.dataset AND r.object_key = o.object_key "
                "AND r.revision = o.current_revision "
                "WHERE o.dataset = ? AND o.status = ? AND r.parser_version < ?",
                [dataset, ObjectStatus.IMPORTED.value, parser_version],
            ).fetchall()
            for row in rows:
                self.mark(dataset, str(row[0]), ObjectStatus.CHANGED)
            return len(rows)

    def get_object(self, dataset: str, object_key: str) -> SourceObject | None:
        with self._lock:
            row = self._con.execute(
                f"SELECT {_OBJECT_COLUMNS} FROM source_object WHERE dataset = ? AND object_key = ?",
                [dataset, object_key],
            ).fetchone()
        return self._object(row) if row else None

    def objects(
        self,
        *,
        dataset: str | None = None,
        status: ObjectStatus | Collection[ObjectStatus] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        newest_first: bool = True,
    ) -> list[SourceObject]:
        clauses: list[str] = []
        params: list[Any] = []
        if dataset is not None:
            clauses.append("dataset = ?")
            params.append(dataset)
        if status is not None:
            statuses = [status] if isinstance(status, ObjectStatus) else list(status)
            clauses.append("status IN (" + ", ".join("?" for _ in statuses) + ")")
            params.extend(s.value for s in statuses)
        if date_from is not None:
            clauses.append("logical_date >= ?")
            params.append(date_from)
        if date_to is not None:
            clauses.append("logical_date <= ?")
            params.append(date_to)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        order = "DESC" if newest_first else "ASC"
        with self._lock:
            rows = self._con.execute(
                f"SELECT {_OBJECT_COLUMNS} FROM source_object{where} "
                f"ORDER BY logical_date {order} NULLS LAST, dataset, object_key {order}",
                params,
            ).fetchall()
        return [self._object(r) for r in rows]

    @staticmethod
    def _object(row: tuple[Any, ...]) -> SourceObject:
        return SourceObject(
            dataset=row[0],
            object_key=row[1],
            url=row[2],
            logical_date=row[3],
            upstream_etag=row[4],
            upstream_size=row[5],
            upstream_last_modified=_from_db(row[6]),
            expected_count=row[7],
            discovered_at=_from_db(row[8]) or datetime.min.replace(tzinfo=UTC),
            last_seen_at=_from_db(row[9]) or datetime.min.replace(tzinfo=UTC),
            current_revision=row[10],
            status=ObjectStatus(row[11]),
            last_error=row[12],
        )

    # -- revisions -----------------------------------------------------------------------

    def add_revision(self, revision: NewRevision) -> SourceRevision:
        with self._lock:
            self._con.begin()
            try:
                row = self._con.execute(
                    "SELECT coalesce(max(revision), 0) + 1 FROM source_revision "
                    "WHERE dataset = ? AND object_key = ?",
                    [revision.dataset, revision.object_key],
                ).fetchone()
                number = int(row[0]) if row else 1
                self._con.execute(
                    f"INSERT INTO source_revision ({_REVISION_COLUMNS}) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?)",
                    [
                        revision.dataset,
                        revision.object_key,
                        number,
                        revision.sha256,
                        revision.size,
                        revision.raw_path,
                        revision.upstream_etag,
                        _to_db(revision.upstream_last_modified),
                        _to_db(revision.fetched_at),
                        revision.parser_version,
                        RevisionStatus.FETCHED.value,
                    ],
                )
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise
        stored = self._revision_row(revision.dataset, revision.object_key, number)
        if stored is None:
            raise RuntimeError("revision vanished after insert")
        return stored

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
        with self._lock:
            self._con.begin()
            try:
                self._con.execute(
                    "UPDATE source_revision SET status = ? WHERE dataset = ? AND object_key = ? "
                    "AND status = ? AND revision <> ?",
                    [
                        RevisionStatus.SUPERSEDED.value,
                        dataset,
                        object_key,
                        RevisionStatus.IMPORTED.value,
                        revision,
                    ],
                )
                self._con.execute(
                    "UPDATE source_revision SET status = ?, imported_at = ?, observed_count = ?, "
                    "verified = ? WHERE dataset = ? AND object_key = ? AND revision = ?",
                    [
                        RevisionStatus.IMPORTED.value,
                        _to_db(imported_at),
                        observed_count,
                        verified,
                        dataset,
                        object_key,
                        revision,
                    ],
                )
                self._con.execute(
                    "UPDATE source_object SET current_revision = ?, status = ?, last_error = NULL "
                    "WHERE dataset = ? AND object_key = ?",
                    [revision, ObjectStatus.IMPORTED.value, dataset, object_key],
                )
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise

    def set_revision_status(
        self, dataset: str, object_key: str, revision: int, status: RevisionStatus
    ) -> None:
        with self._lock:
            self._con.execute(
                "UPDATE source_revision SET status = ? "
                "WHERE dataset = ? AND object_key = ? AND revision = ?",
                [status.value, dataset, object_key, revision],
            )

    def revisions(self, dataset: str, object_key: str) -> list[SourceRevision]:
        with self._lock:
            rows = self._con.execute(
                f"SELECT {_REVISION_COLUMNS} FROM source_revision "
                "WHERE dataset = ? AND object_key = ? ORDER BY revision",
                [dataset, object_key],
            ).fetchall()
        return [self._revision(r) for r in rows]

    def current_revision(self, dataset: str, object_key: str) -> SourceRevision | None:
        obj = self.get_object(dataset, object_key)
        if obj is None or obj.current_revision is None:
            return None
        return self._revision_row(dataset, object_key, obj.current_revision)

    def objects_changed_since(self, dataset: str, since: datetime) -> list[SourceObject]:
        with self._lock:
            rows = self._con.execute(
                f"SELECT {', '.join('o.' + c.strip() for c in _OBJECT_COLUMNS.split(','))} "
                "FROM source_object o JOIN source_revision r ON r.dataset = o.dataset "
                "AND r.object_key = o.object_key AND r.revision = o.current_revision "
                "WHERE o.dataset = ? AND r.imported_at > ? ORDER BY r.imported_at",
                [dataset, _to_db(since)],
            ).fetchall()
        return [self._object(r) for r in rows]

    def _revision_row(self, dataset: str, object_key: str, revision: int) -> SourceRevision | None:
        with self._lock:
            row = self._con.execute(
                f"SELECT {_REVISION_COLUMNS} FROM source_revision "
                "WHERE dataset = ? AND object_key = ? AND revision = ?",
                [dataset, object_key, revision],
            ).fetchone()
        return self._revision(row) if row else None

    @staticmethod
    def _revision(row: tuple[Any, ...]) -> SourceRevision:
        return SourceRevision(
            dataset=row[0],
            object_key=row[1],
            revision=row[2],
            sha256=row[3],
            size=row[4],
            raw_path=row[5],
            upstream_etag=row[6],
            upstream_last_modified=_from_db(row[7]),
            fetched_at=_from_db(row[8]) or datetime.min.replace(tzinfo=UTC),
            imported_at=_from_db(row[9]),
            parser_version=row[10],
            observed_count=row[11],
            verified=row[12],
            status=RevisionStatus(row[13]),
        )

    # -- run log -------------------------------------------------------------------------

    def start_run(
        self, job: str, trigger: Trigger, params: Mapping[str, Any], *, now: datetime
    ) -> ImportRun:
        run_id = new_run_id()
        params_json = json.dumps(dict(params), sort_keys=True, default=str)
        with self._lock:
            self._con.execute(
                f"INSERT INTO import_run ({_RUN_COLUMNS}) "
                "VALUES (?, ?, ?, ?, NULL, ?, ?, 0, 0, NULL)",
                [run_id, job, trigger.value, _to_db(now), RunStatus.RUNNING.value, params_json],
            )
        run = self.get_run(run_id)
        if run is None:
            raise RuntimeError("run vanished after insert")
        return run

    def finish_run(
        self,
        run_id: RunId,
        status: RunStatus,
        *,
        now: datetime,
        objects_changed: int = 0,
        rows_written: int = 0,
        error: str | None = None,
    ) -> None:
        with self._lock:
            self._con.execute(
                "UPDATE import_run SET finished_at = ?, status = ?, objects_changed = ?, "
                "rows_written = ?, error = ? WHERE run_id = ?",
                [_to_db(now), status.value, objects_changed, rows_written, error, run_id],
            )

    def get_run(self, run_id: RunId) -> ImportRun | None:
        with self._lock:
            row = self._con.execute(
                f"SELECT {_RUN_COLUMNS} FROM import_run WHERE run_id = ?", [run_id]
            ).fetchone()
        return self._run(row) if row else None

    def runs(self, *, limit: int = 20, job: str | None = None) -> list[ImportRun]:
        where = " WHERE job = ?" if job is not None else ""
        params: list[Any] = [job] if job is not None else []
        params.append(limit)
        with self._lock:
            rows = self._con.execute(
                f"SELECT {_RUN_COLUMNS} FROM import_run{where} ORDER BY started_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._run(r) for r in rows]

    @staticmethod
    def _run(row: tuple[Any, ...]) -> ImportRun:
        return ImportRun(
            run_id=RunId(row[0]),
            job=row[1],
            trigger=Trigger(row[2]),
            started_at=_from_db(row[3]) or datetime.min.replace(tzinfo=UTC),
            finished_at=_from_db(row[4]),
            status=RunStatus(row[5]),
            params=json.loads(row[6]),
            objects_changed=row[7],
            rows_written=row[8],
            error=row[9],
        )

    # -- entity refresh queue ------------------------------------------------------------

    def refresh_push(self, entries: Iterable[RefreshEntry]) -> None:
        items = list(entries)
        if not items:
            return
        incoming = pa.table(
            {
                "kind": pa.array([e.kind for e in items], pa.string()),
                "entity_id": pa.array([e.entity_id for e in items], pa.int64()),
                "priority": pa.array([e.priority for e in items], pa.int32()),
                "last_refreshed_at": pa.array(
                    [_to_db(e.last_refreshed_at) for e in items], pa.timestamp("us")
                ),
                "next_due_at": pa.array([_to_db(e.next_due_at) for e in items], pa.timestamp("us")),
                "etag": pa.array([e.etag for e in items], pa.string()),
                "failures": pa.array([e.failures for e in items], pa.int32()),
                "last_error": pa.array([e.last_error for e in items], pa.string()),
            }
        )
        with self._lock:
            self._con.register("incoming_refresh", incoming)
            try:
                self._con.execute(
                    f"INSERT INTO entity_refresh ({_REFRESH_COLUMNS}) "
                    f"SELECT {_REFRESH_COLUMNS} FROM incoming_refresh "
                    "QUALIFY row_number() OVER (PARTITION BY kind, entity_id "
                    "ORDER BY priority, next_due_at) = 1 "
                    "ON CONFLICT (kind, entity_id) DO UPDATE SET "
                    "priority = least(priority, excluded.priority), "
                    "next_due_at = least(next_due_at, excluded.next_due_at)"
                )
            finally:
                self._con.unregister("incoming_refresh")

    def refresh_pop(self, *, limit: int, now: datetime) -> list[RefreshEntry]:
        with self._lock:
            rows = self._con.execute(
                f"SELECT {_REFRESH_COLUMNS} FROM entity_refresh WHERE next_due_at <= ? "
                "ORDER BY priority, last_refreshed_at NULLS FIRST, next_due_at, kind, entity_id "
                "LIMIT ?",
                [_to_db(now), limit],
            ).fetchall()
        return [self._refresh(r) for r in rows]

    def refreshed_since(self, since: datetime) -> set[tuple[str, int]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT kind, entity_id FROM entity_refresh WHERE last_refreshed_at >= ?",
                [_to_db(since)],
            ).fetchall()
        return {(str(r[0]), int(r[1])) for r in rows}

    def refresh_update(self, entry: RefreshEntry) -> None:
        with self._lock:
            self._con.execute(
                "UPDATE entity_refresh SET priority = ?, last_refreshed_at = ?, next_due_at = ?, "
                "etag = ?, failures = ?, last_error = ? WHERE kind = ? AND entity_id = ?",
                [
                    entry.priority,
                    _to_db(entry.last_refreshed_at),
                    _to_db(entry.next_due_at),
                    entry.etag,
                    entry.failures,
                    entry.last_error,
                    entry.kind,
                    entry.entity_id,
                ],
            )

    @staticmethod
    def _refresh(row: tuple[Any, ...]) -> RefreshEntry:
        return RefreshEntry(
            kind=row[0],
            entity_id=row[1],
            priority=row[2],
            last_refreshed_at=_from_db(row[3]),
            next_due_at=_from_db(row[4]) or datetime.min.replace(tzinfo=UTC),
            etag=row[5],
            failures=row[6],
            last_error=row[7],
        )

    # -- reporting -----------------------------------------------------------------------

    def dataset_summaries(self) -> list[DatasetSummary]:
        with self._lock:
            counts = self._con.execute(
                "SELECT dataset, status, count(*) FROM source_object GROUP BY dataset, status"
            ).fetchall()
            ranges = self._con.execute(
                "SELECT dataset, min(logical_date), max(logical_date) FROM source_object "
                "WHERE status = ? GROUP BY dataset",
                [ObjectStatus.IMPORTED.value],
            ).fetchall()
        by_dataset: dict[str, dict[str, int]] = {}
        for dataset, status, count in counts:
            by_dataset.setdefault(str(dataset), {})[str(status)] = int(count)
        range_by_dataset = {str(r[0]): (r[1], r[2]) for r in ranges}
        return [
            DatasetSummary(
                dataset=name,
                objects_by_status=statuses,
                oldest_imported=range_by_dataset.get(name, (None, None))[0],
                newest_imported=range_by_dataset.get(name, (None, None))[1],
            )
            for name, statuses in sorted(by_dataset.items())
        ]

    def close(self) -> None:
        with self._lock:
            self._con.close()
