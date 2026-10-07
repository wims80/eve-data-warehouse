"""In-memory store backend for the contract tests.

A second implementation of the Registry, EntityStore and ResponseCache Protocols in plain
Python. It exists to prove the Protocols are complete: every contract test and every job
test that runs on the ``registry`` fixture runs against it as well, so a job that reached
past the Protocol into DuckDB would fail here. It is not a product backend.
"""

import json
import os
import threading
from collections.abc import Collection, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

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
from evedw.domain.schemas import ENTITY_PRIMARY_KEYS, ENTITY_TABLES
from evedw.store.base import CachedResponse

_SCHEMA_VERSION = 1

ObjectId = tuple[str, str]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("naive datetime reached the store; use UTC-aware datetimes")
    return value.astimezone(UTC)


def _utc_or_none(value: datetime | None) -> datetime | None:
    return None if value is None else _utc(value)


class MemoryRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._version = 0
        self._objects: dict[ObjectId, SourceObject] = {}
        self._revisions: dict[ObjectId, list[SourceRevision]] = {}
        self._runs: dict[RunId, ImportRun] = {}
        self._refresh: dict[tuple[str, int], RefreshEntry] = {}
        self._state: dict[str, str] = {}

    # -- schema ---------------------------------------------------------------------------

    def migrate(self) -> int:
        self._version = _SCHEMA_VERSION
        return self._version

    def schema_version(self) -> int:
        return self._version

    # -- source objects ------------------------------------------------------------------

    def upsert_objects(
        self, objects: Iterable[DiscoveredObject], *, now: datetime
    ) -> UpsertSummary:
        new = changed = unchanged = 0
        ts = _utc(now)
        with self._lock:
            for seen in objects:
                key = (seen.dataset, seen.object_key)
                existing = self._objects.get(key)
                if existing is None:
                    self._objects[key] = SourceObject(
                        dataset=seen.dataset,
                        object_key=seen.object_key,
                        url=seen.url,
                        logical_date=seen.logical_date,
                        upstream_etag=seen.etag,
                        upstream_size=seen.size,
                        upstream_last_modified=_utc_or_none(seen.last_modified),
                        expected_count=seen.expected_count,
                        discovered_at=ts,
                        last_seen_at=ts,
                        current_revision=None,
                        status=ObjectStatus.NEW,
                        last_error=None,
                    )
                    new += 1
                elif existing.differs_from(seen) or existing.status is ObjectStatus.GONE:
                    self._objects[key] = replace(
                        existing,
                        url=seen.url,
                        upstream_etag=seen.etag,
                        upstream_size=seen.size,
                        upstream_last_modified=_utc_or_none(seen.last_modified),
                        expected_count=seen.expected_count,
                        last_seen_at=ts,
                        status=ObjectStatus.CHANGED,
                        last_error=None,
                    )
                    changed += 1
                else:
                    self._objects[key] = replace(
                        existing, expected_count=seen.expected_count, last_seen_at=ts
                    )
                    unchanged += 1
        return UpsertSummary(new=new, changed=changed, unchanged=unchanged)

    def mark_missing(self, dataset: str, seen_keys: Collection[str], *, now: datetime) -> int:
        with self._lock:
            missing = [
                obj
                for obj in self._objects.values()
                if obj.dataset == dataset
                and obj.status is not ObjectStatus.GONE
                and obj.object_key not in seen_keys
            ]
            for obj in missing:
                self._objects[(dataset, obj.object_key)] = replace(
                    obj, status=ObjectStatus.GONE, last_seen_at=_utc(now)
                )
            return len(missing)

    def mark(
        self, dataset: str, object_key: str, status: ObjectStatus, *, error: str | None = None
    ) -> None:
        with self._lock:
            obj = self._objects.get((dataset, object_key))
            if obj is not None:
                self._objects[(dataset, object_key)] = replace(obj, status=status, last_error=error)

    def mark_parser_outdated(self, dataset: str, parser_version: int) -> int:
        with self._lock:
            outdated = [
                obj
                for obj in self._objects.values()
                if obj.dataset == dataset
                and obj.status is ObjectStatus.IMPORTED
                and (current := self.current_revision(dataset, obj.object_key)) is not None
                and current.parser_version < parser_version
            ]
            for obj in outdated:
                self.mark(dataset, obj.object_key, ObjectStatus.CHANGED)
            return len(outdated)

    def get_object(self, dataset: str, object_key: str) -> SourceObject | None:
        with self._lock:
            return self._objects.get((dataset, object_key))

    def objects(
        self,
        *,
        dataset: str | None = None,
        status: ObjectStatus | Collection[ObjectStatus] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        newest_first: bool = True,
    ) -> list[SourceObject]:
        statuses: set[ObjectStatus] | None = None
        if status is not None:
            statuses = {status} if isinstance(status, ObjectStatus) else set(status)
        with self._lock:
            found = [
                obj
                for obj in self._objects.values()
                if (dataset is None or obj.dataset == dataset)
                and (statuses is None or obj.status in statuses)
                and (
                    date_from is None
                    or (obj.logical_date is not None and obj.logical_date >= date_from)
                )
                and (
                    date_to is None
                    or (obj.logical_date is not None and obj.logical_date <= date_to)
                )
            ]
        # Same order as the SQL backends: logical_date, nulls last, then dataset ascending,
        # then object_key in the requested direction.
        found.sort(key=lambda o: o.object_key, reverse=newest_first)
        found.sort(key=lambda o: o.dataset)
        dated = sorted(
            (o for o in found if o.logical_date is not None),
            key=lambda o: o.logical_date or date.min,
            reverse=newest_first,
        )
        return dated + [o for o in found if o.logical_date is None]

    # -- revisions -----------------------------------------------------------------------

    def add_revision(self, revision: NewRevision) -> SourceRevision:
        with self._lock:
            chain = self._revisions.setdefault((revision.dataset, revision.object_key), [])
            stored = SourceRevision(
                dataset=revision.dataset,
                object_key=revision.object_key,
                revision=len(chain) + 1,
                sha256=revision.sha256,
                size=revision.size,
                raw_path=revision.raw_path,
                upstream_etag=revision.upstream_etag,
                upstream_last_modified=_utc_or_none(revision.upstream_last_modified),
                fetched_at=_utc(revision.fetched_at),
                imported_at=None,
                parser_version=revision.parser_version,
                observed_count=None,
                verified=None,
                status=RevisionStatus.FETCHED,
            )
            chain.append(stored)
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
            chain = self._revisions.get((dataset, object_key), [])
            for index, r in enumerate(chain):
                if r.revision == revision:
                    chain[index] = replace(
                        r,
                        status=RevisionStatus.IMPORTED,
                        imported_at=_utc(imported_at),
                        observed_count=observed_count,
                        verified=verified,
                    )
                elif r.status is RevisionStatus.IMPORTED:
                    chain[index] = replace(r, status=RevisionStatus.SUPERSEDED)
            obj = self._objects.get((dataset, object_key))
            if obj is not None:
                self._objects[(dataset, object_key)] = replace(
                    obj, current_revision=revision, status=ObjectStatus.IMPORTED, last_error=None
                )

    def set_revision_status(
        self, dataset: str, object_key: str, revision: int, status: RevisionStatus
    ) -> None:
        with self._lock:
            chain = self._revisions.get((dataset, object_key), [])
            for index, r in enumerate(chain):
                if r.revision == revision:
                    chain[index] = replace(r, status=status)

    def revisions(self, dataset: str, object_key: str) -> list[SourceRevision]:
        with self._lock:
            return list(self._revisions.get((dataset, object_key), []))

    def current_revision(self, dataset: str, object_key: str) -> SourceRevision | None:
        with self._lock:
            obj = self._objects.get((dataset, object_key))
            if obj is None or obj.current_revision is None:
                return None
            for r in self._revisions.get((dataset, object_key), []):
                if r.revision == obj.current_revision:
                    return r
            return None

    def objects_changed_since(self, dataset: str, since: datetime) -> list[SourceObject]:
        cutoff = _utc(since)
        with self._lock:
            found: list[tuple[datetime, SourceObject]] = []
            for obj in self._objects.values():
                if obj.dataset != dataset:
                    continue
                current = self.current_revision(dataset, obj.object_key)
                if (
                    current is not None
                    and current.imported_at is not None
                    and current.imported_at > cutoff
                ):
                    found.append((current.imported_at, obj))
        found.sort(key=lambda item: item[0])
        return [obj for _, obj in found]

    # -- run log -------------------------------------------------------------------------

    def start_run(
        self, job: str, trigger: Trigger, params: Mapping[str, Any], *, now: datetime
    ) -> ImportRun:
        return self._insert_run(job, trigger, params, now=now, status=RunStatus.RUNNING)

    def queue_run(
        self, job: str, trigger: Trigger, params: Mapping[str, Any], *, now: datetime
    ) -> ImportRun:
        return self._insert_run(job, trigger, params, now=now, status=RunStatus.QUEUED)

    def _insert_run(
        self,
        job: str,
        trigger: Trigger,
        params: Mapping[str, Any],
        *,
        now: datetime,
        status: RunStatus,
    ) -> ImportRun:
        # Round-trip through JSON like a stored params column would.
        stored: dict[str, Any] = json.loads(json.dumps(dict(params), sort_keys=True, default=str))
        run = ImportRun(
            run_id=new_run_id(),
            job=job,
            trigger=trigger,
            started_at=_utc(now),
            finished_at=None,
            status=status,
            params=stored,
        )
        with self._lock:
            self._runs[run.run_id] = run
        return run

    def begin_run(self, run_id: RunId, *, now: datetime) -> None:
        with self._lock:
            run = self._runs.get(run_id)
            if run is not None and run.status is RunStatus.QUEUED:
                self._runs[run_id] = replace(run, status=RunStatus.RUNNING, started_at=_utc(now))

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
            run = self._runs.get(run_id)
            if run is not None:
                self._runs[run_id] = replace(
                    run,
                    finished_at=_utc(now),
                    status=status,
                    objects_changed=objects_changed,
                    rows_written=rows_written,
                    error=error,
                )

    def get_run(self, run_id: RunId) -> ImportRun | None:
        with self._lock:
            return self._runs.get(run_id)

    def runs(self, *, limit: int = 20, job: str | None = None) -> list[ImportRun]:
        with self._lock:
            found = [r for r in self._runs.values() if job is None or r.job == job]
        found.sort(key=lambda r: r.started_at, reverse=True)
        return found[:limit]

    # -- entity refresh queue ------------------------------------------------------------

    def refresh_push(self, entries: Iterable[RefreshEntry]) -> None:
        with self._lock:
            for entry in entries:
                key = (entry.kind, entry.entity_id)
                entry = replace(
                    entry,
                    next_due_at=_utc(entry.next_due_at),
                    last_refreshed_at=_utc_or_none(entry.last_refreshed_at),
                )
                existing = self._refresh.get(key)
                if existing is None:
                    self._refresh[key] = entry
                else:
                    self._refresh[key] = replace(
                        existing,
                        priority=min(existing.priority, entry.priority),
                        next_due_at=min(existing.next_due_at, entry.next_due_at),
                    )

    def refresh_pop(self, *, limit: int, now: datetime) -> list[RefreshEntry]:
        cutoff = _utc(now)
        with self._lock:
            due = [e for e in self._refresh.values() if e.next_due_at <= cutoff]
        within: dict[tuple[int, str], list[RefreshEntry]] = {}
        for entry in due:
            within.setdefault((entry.priority, entry.kind), []).append(entry)
        ranked: list[tuple[int, int, RefreshEntry]] = []
        for (priority, _kind), entries in within.items():
            entries.sort(
                key=lambda e: (
                    e.last_refreshed_at is not None,
                    e.last_refreshed_at or datetime.min.replace(tzinfo=UTC),
                    e.next_due_at,
                    e.entity_id,
                )
            )
            ranked.extend((priority, turn, e) for turn, e in enumerate(entries))
        ranked.sort(key=lambda item: (item[0], item[1], item[2].next_due_at, item[2].kind))
        return [item[2] for item in ranked[:limit]]

    def refresh_update(self, entry: RefreshEntry) -> None:
        key = (entry.kind, entry.entity_id)
        with self._lock:
            if key in self._refresh:
                self._refresh[key] = replace(
                    entry,
                    next_due_at=_utc(entry.next_due_at),
                    last_refreshed_at=_utc_or_none(entry.last_refreshed_at),
                )

    def refresh_filter(
        self, kind: str, ids: Collection[int], *, refreshed_before: datetime | None = None
    ) -> list[int]:
        cutoff = None if refreshed_before is None else _utc(refreshed_before)
        wanted: list[int] = []
        with self._lock:
            for entity_id in sorted({int(i) for i in ids}):
                entry = self._refresh.get((kind, entity_id))
                if entry is None or (
                    cutoff is not None
                    and (entry.last_refreshed_at is None or entry.last_refreshed_at < cutoff)
                ):
                    wanted.append(entity_id)
        return wanted

    def refresh_counts(self, *, now: datetime) -> dict[int, tuple[int, int]]:
        cutoff = _utc(now)
        counts: dict[int, tuple[int, int]] = {}
        with self._lock:
            for entry in self._refresh.values():
                total, due = counts.get(entry.priority, (0, 0))
                counts[entry.priority] = (total + 1, due + int(entry.next_due_at <= cutoff))
        return dict(sorted(counts.items()))

    # -- sweep state ---------------------------------------------------------------------

    def state_get(self, key: str) -> str | None:
        with self._lock:
            return self._state.get(key)

    def state_put(self, key: str, value: str, *, now: datetime) -> None:
        _utc(now)
        with self._lock:
            self._state[key] = value

    # -- reporting -----------------------------------------------------------------------

    def dataset_summaries(self) -> list[DatasetSummary]:
        by_dataset: dict[str, dict[str, int]] = {}
        imported: dict[str, list[date]] = {}
        with self._lock:
            for obj in self._objects.values():
                statuses = by_dataset.setdefault(obj.dataset, {})
                statuses[obj.status.value] = statuses.get(obj.status.value, 0) + 1
                if obj.status is ObjectStatus.IMPORTED and obj.logical_date is not None:
                    imported.setdefault(obj.dataset, []).append(obj.logical_date)
        return [
            DatasetSummary(
                dataset=name,
                objects_by_status=statuses,
                oldest_imported=min(imported[name]) if name in imported else None,
                newest_imported=max(imported[name]) if name in imported else None,
            )
            for name, statuses in sorted(by_dataset.items())
        ]

    def close(self) -> None:
        pass


def _schema(table: str) -> pa.Schema:
    try:
        return ENTITY_TABLES[table]
    except KeyError:
        raise KeyError(f"unknown entity table {table!r}") from None


class MemoryEntityStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tables: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {
            name: {} for name in ENTITY_TABLES
        }

    def upsert(self, table: str, rows: pa.Table) -> int:
        schema = _schema(table)
        missing = [name for name in schema.names if name not in rows.column_names]
        if missing:
            raise ValueError(f"{table}: rows lack columns {missing}")
        if rows.num_rows == 0:
            return 0
        data = rows.select(schema.names).cast(schema)
        keys = ENTITY_PRIMARY_KEYS[table]
        # Within a batch the newest observation of a key wins, as across batches.
        incoming: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in data.to_pylist():
            key = tuple(row[k] for k in keys)
            held = incoming.get(key)
            if held is None or row["observed_at"] > held["observed_at"]:
                incoming[key] = row
        with self._lock:
            stored = self._tables[table]
            for key, row in incoming.items():
                existing = stored.get(key)
                if existing is None or row["observed_at"] >= existing["observed_at"]:
                    stored[key] = row
        return data.num_rows

    def lookup(self, table: str, ids: Collection[int]) -> pa.Table:
        schema = _schema(table)
        wanted = {int(i) for i in ids}
        with self._lock:
            rows = [row for key, row in sorted(self._tables[table].items()) if key[0] in wanted]
        return pa.Table.from_pylist(rows, schema=schema)

    def ids(
        self,
        table: str,
        *,
        live_only: bool = False,
        where: Mapping[str, int] | None = None,
        after: int | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> list[int]:
        schema = _schema(table)
        for name in where or {}:
            if name not in schema.names:
                raise KeyError(f"{table} has no column {name!r}")
        with self._lock:
            rows = list(self._tables[table].items())
        found: set[int] = set()
        for key, row in rows:
            if live_only and row.get("deleted"):
                continue
            if any(row.get(name) != value for name, value in (where or {}).items()):
                continue
            entity_id = int(key[0])
            if after is not None and (entity_id >= after if descending else entity_id <= after):
                continue
            found.add(entity_id)
        ordered = sorted(found, reverse=descending)
        return ordered if limit is None else ordered[:limit]

    def count(self, table: str) -> int:
        _schema(table)
        with self._lock:
            return len(self._tables[table])

    def export_parquet(self, directory: Path) -> list[Path]:
        directory.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        for table, schema in ENTITY_TABLES.items():
            with self._lock:
                rows = [row for _, row in sorted(self._tables[table].items())]
            final = directory / f"{table}.parquet"
            tmp = directory / f"{table}.parquet.tmp"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp, compression="zstd")
            with tmp.open("rb") as fh:
                os.fsync(fh.fileno())
            tmp.replace(final)
            written.append(final)
        return written

    def close(self) -> None:
        pass


class MemoryResponseCache:
    def __init__(self) -> None:
        self._entries: dict[str, CachedResponse] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> CachedResponse | None:
        with self._lock:
            return self._entries.get(key)

    def put(self, entry: CachedResponse) -> None:
        _utc(entry.observed_at)
        _utc(entry.expires_at)
        with self._lock:
            self._entries[entry.key] = entry

    def delete(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def close(self) -> None:
        pass
