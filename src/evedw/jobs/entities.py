"""Entity jobs: seed from an EVE Ref backfill, refresh from ESI, export to Parquet.

Field sets below were pinned by inspecting ``eve-kill-com-karbowiak-2026-05-10.tar.bz2``
on 2026-10-05 (design §7.3). Reading uses explicit columns, so an extra upstream key is
reported, never silently absorbed, and a missing one becomes NULL.
"""

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from evedw.domain.datasets import ENTITIES_BACKFILL, Dataset
from evedw.domain.registry import RefreshEntry, SourceObject, SourceRevision
from evedw.domain.schemas import (
    ALLIANCES,
    CHARACTER_EMPLOYMENT,
    CHARACTERS,
    CORPORATION_ALLIANCE_HISTORY,
    CORPORATIONS,
    ENTITY_TABLES,
)
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.jobs.sync import ImportResult
from evedw.logs import log_context
from evedw.sources.archives import extract_members
from evedw.sources.esi import (
    EsiBudgetError,
    EsiClient,
    EsiPermanentError,
    EsiResponse,
    EsiTransientError,
)
from evedw.store.base import EntityStore, Lake, Registry

log = logging.getLogger(__name__)

DOOMHEIM = 1000001
"""The NPC corporation deleted characters are moved to."""

MEMBERS = ("characters.json", "corporations.json", "alliances.json")

EXPECTED_KEYS: dict[str, frozenset[str]] = {
    "characters": frozenset(
        {
            "alliance_id",
            "birthday",
            "bloodline_id",
            "character_id",
            "corporation_id",
            "createdAt",
            "deleted",
            "description",
            "faction_id",
            "gender",
            "history",
            "last_active",
            "name",
            "race_id",
            "security_status",
            "updatedAt",
        }
    ),
    "corporations": frozenset(
        {
            "alliance_id",
            "ceo_id",
            "corporation_id",
            "createdAt",
            "creator_id",
            "date_founded",
            "deleted",
            "description",
            "faction_id",
            "history",
            "home_station_id",
            "member_count",
            "name",
            "shares",
            "tax_rate",
            "ticker",
            "updatedAt",
            "url",
            "war_eligible",
        }
    ),
    "alliances": frozenset(
        {
            "alliance_id",
            "corporation_count",
            "createdAt",
            "creator_corporation_id",
            "creator_id",
            "date_founded",
            "deleted",
            "executor_corporation_id",
            "faction_id",
            "member_count",
            "name",
            "ticker",
            "updatedAt",
        }
    ),
}

HISTORY_KEYS: dict[str, frozenset[str]] = {
    "characters": frozenset({"corporation_id", "record_id", "start_date"}),
    "corporations": frozenset({"alliance_id", "record_id", "start_date"}),
}


class SourceContentError(ValueError):
    """The archive does not contain what a backfill must contain."""


def _columns(spec: Mapping[str, str]) -> str:
    return "{" + ", ".join(f"'{k}': '{v}'" for k, v in spec.items()) + "}"


def _read_json(path: Path, spec: Mapping[str, str]) -> str:
    return (
        f"read_json('{path}', format = 'array', auto_detect = false, "
        f"columns = {_columns(spec)}, maximum_object_size = 268435456)"
    )


# Each entry: (entity table, SQL producing the table's columns, history table, history SQL).
# ``$observed`` and ``$source`` are bound parameters; ``{src}`` is the read_json call.

CHARACTER_SPEC = {
    "character_id": "BIGINT",
    "name": "VARCHAR",
    "corporation_id": "BIGINT",
    "alliance_id": "BIGINT",
    "faction_id": "BIGINT",
    "birthday": "VARCHAR",
    "security_status": "DOUBLE",
    "deleted": "BOOLEAN",
    "updatedAt": "VARCHAR",
    "history": "JSON",
}
CHARACTER_SQL = """
SELECT character_id, name, corporation_id, alliance_id, faction_id,
       try_cast(birthday AS TIMESTAMPTZ) AS birthday,
       security_status,
       coalesce(deleted, false) AS deleted,
       coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
       $source::VARCHAR AS source
FROM {src}
WHERE character_id IS NOT NULL
"""
EMPLOYMENT_SQL = """
SELECT character_id, h.record_id AS record_id, h.corporation_id AS corporation_id,
       try_cast(h.start_date AS TIMESTAMPTZ) AS start_date, observed_at,
       $source::VARCHAR AS source
FROM (
    SELECT character_id,
           coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
           unnest(from_json(history,
               '[{{"record_id": "BIGINT", "corporation_id": "BIGINT", "start_date": "VARCHAR"}}]'
           )) AS h
    FROM {src}
    WHERE character_id IS NOT NULL AND json_array_length(history) > 0
)
WHERE h.record_id IS NOT NULL
"""

CORPORATION_SPEC = {
    "corporation_id": "BIGINT",
    "name": "VARCHAR",
    "ticker": "VARCHAR",
    "alliance_id": "BIGINT",
    "ceo_id": "BIGINT",
    "member_count": "BIGINT",
    "date_founded": "VARCHAR",
    "deleted": "BOOLEAN",
    "updatedAt": "VARCHAR",
    "history": "JSON",
}
CORPORATION_SQL = """
SELECT corporation_id, name, ticker, alliance_id, ceo_id, member_count,
       try_cast(date_founded AS TIMESTAMPTZ) AS date_founded,
       coalesce(deleted, false) AS deleted,
       coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
       $source::VARCHAR AS source
FROM {src}
WHERE corporation_id IS NOT NULL
"""
ALLIANCE_HISTORY_SQL = """
SELECT corporation_id, h.record_id AS record_id, h.alliance_id AS alliance_id,
       try_cast(h.start_date AS TIMESTAMPTZ) AS start_date,
       NULL::BOOLEAN AS is_deleted, observed_at, $source::VARCHAR AS source
FROM (
    SELECT corporation_id,
           coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
           unnest(from_json(history,
               '[{{"record_id": "BIGINT", "alliance_id": "BIGINT", "start_date": "VARCHAR"}}]'
           )) AS h
    FROM {src}
    WHERE corporation_id IS NOT NULL AND json_array_length(history) > 0
)
WHERE h.record_id IS NOT NULL
"""

ALLIANCE_SPEC = {
    "alliance_id": "BIGINT",
    "name": "VARCHAR",
    "ticker": "VARCHAR",
    "executor_corporation_id": "BIGINT",
    "date_founded": "VARCHAR",
    "deleted": "BOOLEAN",
    "updatedAt": "VARCHAR",
}
ALLIANCE_SQL = """
SELECT alliance_id, name, ticker, executor_corporation_id,
       try_cast(date_founded AS TIMESTAMPTZ) AS date_founded,
       coalesce(deleted, false) AS deleted,
       coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
       $source::VARCHAR AS source
FROM {src}
WHERE alliance_id IS NOT NULL
"""

KEYS_SQL = """
SELECT DISTINCT unnest(json_keys(json)) AS k
FROM read_json_objects('{path}', format = 'array', maximum_object_size = 268435456)
ORDER BY k
"""
HISTORY_KEYS_SQL = """
SELECT DISTINCT unnest(json_keys(h)) AS k FROM (
    SELECT unnest(from_json(json->'history', '["JSON"]')) AS h
    FROM read_json_objects('{path}', format = 'array', maximum_object_size = 268435456)
    WHERE json_array_length(json->'history') > 0
) ORDER BY k
"""


@dataclass(frozen=True, slots=True)
class MemberPlan:
    kind: str
    spec: Mapping[str, str]
    entity_table: str
    entity_sql: str
    history_table: str | None
    history_sql: str | None


PLANS: dict[str, MemberPlan] = {
    "characters.json": MemberPlan(
        "characters",
        CHARACTER_SPEC,
        "characters",
        CHARACTER_SQL,
        "character_employment",
        EMPLOYMENT_SQL,
    ),
    "corporations.json": MemberPlan(
        "corporations",
        CORPORATION_SPEC,
        "corporations",
        CORPORATION_SQL,
        "corporation_alliance_history",
        ALLIANCE_HISTORY_SQL,
    ),
    "alliances.json": MemberPlan("alliances", ALLIANCE_SPEC, "alliances", ALLIANCE_SQL, None, None),
}


@dataclass(frozen=True, slots=True)
class SeedCounts:
    rows: dict[str, int]
    unknown_keys: dict[str, list[str]]
    """Member kind -> keys the archive has that the plan does not."""


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET preserve_insertion_order = false")
    return con


def unknown_keys(con: duckdb.DuckDBPyConnection, path: Path, kind: str) -> list[str]:
    keys = {row[0] for row in con.execute(KEYS_SQL.format(path=path)).fetchall()}
    unknown = keys - EXPECTED_KEYS[kind]
    if kind in HISTORY_KEYS:
        found = {row[0] for row in con.execute(HISTORY_KEYS_SQL.format(path=path)).fetchall()}
        unknown |= {f"history.{k}" for k in found - HISTORY_KEYS[kind]}
    return sorted(unknown)


def _stream(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: Mapping[str, Any],
    schema: pa.Schema,
    batch_rows: int,
) -> Iterable[pa.Table]:
    reader = con.execute(sql, dict(params)).to_arrow_reader(batch_rows)
    for batch in reader:
        yield pa.Table.from_batches([batch]).select(schema.names).cast(schema)


def seed_members(
    members: Mapping[str, Path],
    entities: EntityStore,
    *,
    source: str,
    snapshot: datetime,
    batch_rows: int = 250_000,
    check_keys: bool = True,
) -> SeedCounts:
    """Load the three JSON arrays into the entity store. ``members`` maps base name to
    extracted file."""
    rows: dict[str, int] = {}
    unknown: dict[str, list[str]] = {}
    con = _connect()
    try:
        for name, plan in PLANS.items():
            path = members[name]
            with log_context(member=name):
                if check_keys:
                    extra = unknown_keys(con, path, plan.kind)
                    if extra:
                        unknown[plan.kind] = extra
                        log.warning("unknown keys in %s: %s", name, ", ".join(extra))
                params = {"observed": snapshot, "source": source}
                src = _read_json(path, plan.spec)
                written = 0
                for table in _stream(
                    con,
                    plan.entity_sql.format(src=src),
                    params,
                    ENTITY_TABLES[plan.entity_table],
                    batch_rows,
                ):
                    written += entities.upsert(plan.entity_table, table)
                rows[plan.entity_table] = written
                log.info("%s: %d rows", plan.entity_table, written)
                if plan.history_table and plan.history_sql:
                    written = 0
                    for table in _stream(
                        con,
                        plan.history_sql.format(src=src),
                        params,
                        ENTITY_TABLES[plan.history_table],
                        batch_rows,
                    ):
                        written += entities.upsert(plan.history_table, table)
                    rows[plan.history_table] = written
                    log.info("%s: %d rows", plan.history_table, written)
    finally:
        con.close()
    return SeedCounts(rows=rows, unknown_keys=unknown)


class BackfillImporter:
    """``Importer`` for the ``entities_backfill`` dataset."""

    def __init__(self, entities: EntityStore) -> None:
        self._entities = entities

    @property
    def dataset(self) -> Dataset:
        return ENTITIES_BACKFILL

    def import_revision(
        self,
        ctx: RunContext,
        obj: SourceObject,
        revision: SourceRevision,
        *,
        raw_file: Path,
        scratch_dir: Path,
    ) -> ImportResult:
        members = extract_members(raw_file, scratch_dir / "backfill", MEMBERS)
        missing = [name for name in MEMBERS if name not in members]
        if missing:
            raise SourceContentError(f"{raw_file.name}: members missing: {missing}")
        if obj.logical_date is None:
            raise SourceContentError(f"{obj.object_key}: no snapshot date")
        snapshot = datetime.combine(obj.logical_date, datetime.min.time(), tzinfo=UTC)
        counts = seed_members(
            members,
            self._entities,
            source=f"everef_backfill:{revision.sha256}",
            snapshot=snapshot,
        )
        for path in members.values():
            path.unlink(missing_ok=True)
        entity_rows = sum(
            counts.rows.get(t, 0) for t in ("characters", "corporations", "alliances")
        )
        return ImportResult(rows=entity_rows)


# --- refresh ------------------------------------------------------------------------------

KINDS = ("character", "corporation", "alliance")

KILLMAIL_ID_COLUMNS: dict[str, dict[str, str]] = {
    "killmails": {
        "victim_character_id": "character",
        "victim_corporation_id": "corporation",
        "victim_alliance_id": "alliance",
    },
    "attackers": {
        "character_id": "character",
        "corporation_id": "corporation",
        "alliance_id": "alliance",
    },
}

PERMANENT_RETRY = timedelta(days=365)
TRANSIENT_RETRY = timedelta(hours=1)
NOT_FOUND = frozenset({404, 410})
REQUEUE_AFTER = timedelta(days=1)
"""An entity refreshed more recently than this is not re-queued by a killmail sighting."""
REQUEST_COST: dict[str, int] = {"character": 2, "corporation": 2, "alliance": 1}
"""Requests an entity refresh may send: the entity itself plus its history, if any."""


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _int(value: object) -> int | None:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _float(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _row(schema: pa.Schema, values: Mapping[str, object]) -> pa.Table:
    return pa.Table.from_pylist([dict(values)], schema=schema)


def ids_from_killmails(
    lake: Lake, *, date_from: date, date_to: date | None = None
) -> dict[str, set[int]]:
    """Distinct entity IDs appearing in killmail partitions in the range."""
    found: dict[str, set[int]] = {kind: set() for kind in KINDS}
    for table, columns in KILLMAIL_ID_COLUMNS.items():
        data = lake.read(table, date_from=date_from, date_to=date_to, columns=list(columns))
        for column, kind in columns.items():
            values = data.column(column).drop_null().unique().to_pylist()
            found[kind].update(int(v) for v in values if isinstance(v, int) and v > 0)
    return found


@dataclass(slots=True)
class RefreshJob:
    """Populate the refresh queue from recent killmails, then drain it within budget."""

    esi: EsiClient
    entities: EntityStore
    lake: Lake
    recent_days: int = 7
    refresh_interval: timedelta = timedelta(days=30)
    budget: int | None = None
    """Requests this run may send, on top of the client's daily budget."""
    populate: bool = True
    today: date | None = None
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    pop_size: int = 200

    def __call__(self, ctx: RunContext) -> JobOutcome:
        if self.populate:
            self.populate_queue(ctx.registry)
        return self.drain(ctx.registry, cancel=ctx.check_cancelled)

    def populate_queue(self, registry: Registry) -> int:
        """Queue entities seen in recent killmails with priority 1, except those refreshed
        within ``REQUEUE_AFTER``: their ESI data is still as fresh as a request would get."""
        today = self.today or self.now().date()
        since = today - timedelta(days=self.recent_days)
        found = ids_from_killmails(self.lake, date_from=since)
        now = self.now()
        fresh = registry.refreshed_since(now - REQUEUE_AFTER)
        entries = [
            RefreshEntry(kind=kind, entity_id=entity_id, priority=1, next_due_at=now)
            for kind, ids in found.items()
            for entity_id in sorted(ids)
            if (kind, entity_id) not in fresh
        ]
        registry.refresh_push(entries)
        log.info(
            "queued %d entities seen in killmails since %s (%s), %d refreshed recently",
            len(entries),
            since,
            ", ".join(f"{k}={len(v)}" for k, v in found.items()),
            sum(len(ids) for ids in found.values()) - len(entries),
        )
        return len(entries)

    def drain(self, registry: Registry, *, cancel: Callable[[], None] = lambda: None) -> JobOutcome:
        """``cancel`` is called before every entity; it raises to stop the drain."""
        refreshed = 0
        rows = 0
        sent_before = self.esi.policy.requests
        while True:
            due = registry.refresh_pop(limit=self.pop_size, now=self.now())
            if not due:
                log.info("refresh queue drained; %d entities refreshed", refreshed)
                return JobOutcome(objects_changed=refreshed, rows_written=rows)
            for entry in due:
                cancel()
                allowance = self.esi.budget_remaining()
                if self.budget is not None:
                    allowance = min(
                        allowance, self.budget - (self.esi.policy.requests - sent_before)
                    )
                if allowance < REQUEST_COST[entry.kind]:
                    log.info("request budget spent; %d entities refreshed", refreshed)
                    return JobOutcome(objects_changed=refreshed, rows_written=rows)
                with log_context(kind=entry.kind, entity_id=str(entry.entity_id)):
                    try:
                        resolved, written = self.refresh_one(registry, entry)
                    except EsiBudgetError as exc:
                        log.info("%s", exc)
                        return JobOutcome(objects_changed=refreshed, rows_written=rows)
                rows += written
                refreshed += int(resolved)

    # -- one entity ----------------------------------------------------------------------

    def refresh_one(self, registry: Registry, entry: RefreshEntry) -> tuple[bool, int]:
        """Returns (resolved, rows written). Resolved means the entity's state is settled:
        refreshed, or found gone upstream. A transient failure is not resolved."""
        now = self.now()
        try:
            rows, expires_at, etag = self._fetch(entry.kind, entry.entity_id)
        except EsiPermanentError as exc:
            if exc.status in NOT_FOUND:
                rows = self._mark_deleted(entry.kind, entry.entity_id, now)
                log.info("HTTP %d: marked deleted", exc.status)
            else:
                rows = 0
                log.warning("permanent failure: %s", exc)
            registry.refresh_update(
                RefreshEntry(
                    kind=entry.kind,
                    entity_id=entry.entity_id,
                    priority=max(entry.priority, 2),
                    next_due_at=now + PERMANENT_RETRY,
                    last_refreshed_at=entry.last_refreshed_at,
                    etag=entry.etag,
                    failures=entry.failures + 1,
                    last_error=f"HTTP {exc.status}",
                )
            )
            return True, rows
        except EsiTransientError as exc:
            failures = entry.failures + 1
            registry.refresh_update(
                RefreshEntry(
                    kind=entry.kind,
                    entity_id=entry.entity_id,
                    priority=entry.priority,
                    next_due_at=now + TRANSIENT_RETRY * min(2 ** (failures - 1), 24),
                    last_refreshed_at=entry.last_refreshed_at,
                    etag=entry.etag,
                    failures=failures,
                    last_error=str(exc)[:500],
                )
            )
            log.warning("transient failure: %s", exc)
            return False, 0
        registry.refresh_update(
            RefreshEntry(
                kind=entry.kind,
                entity_id=entry.entity_id,
                priority=2,
                next_due_at=max(expires_at, now + self.refresh_interval),
                last_refreshed_at=now,
                etag=etag,
                failures=0,
                last_error=None,
            )
        )
        return True, rows

    def _fetch(self, kind: str, entity_id: int) -> tuple[int, datetime, str | None]:
        """Returns rows written, the earliest cache expiry, and the entity's etag."""
        now = self.now()
        if kind == "character":
            entity = self.esi.get(f"/characters/{entity_id}")
            rows = self._store(entity, "characters", lambda b: character_row(entity_id, b, now))
            history = self.esi.get(f"/characters/{entity_id}/corporationhistory")
            rows += self._store(
                history, "character_employment", lambda b: employment_rows(entity_id, b, now)
            )
            return rows, min(entity.expires_at, history.expires_at), entity.etag
        if kind == "corporation":
            entity = self.esi.get(f"/corporations/{entity_id}")
            rows = self._store(entity, "corporations", lambda b: corporation_row(entity_id, b, now))
            history = self.esi.get(f"/corporations/{entity_id}/alliancehistory")
            rows += self._store(
                history,
                "corporation_alliance_history",
                lambda b: alliance_history_rows(entity_id, b, now),
            )
            return rows, min(entity.expires_at, history.expires_at), entity.etag
        if kind == "alliance":
            entity = self.esi.get(f"/alliances/{entity_id}")
            rows = self._store(entity, "alliances", lambda b: alliance_row(entity_id, b, now))
            return rows, entity.expires_at, entity.etag
        raise ValueError(f"unknown entity kind {kind!r}")

    def _store(self, response: EsiResponse, table: str, rows: Callable[[Any], pa.Table]) -> int:
        if response.from_cache or response.not_modified:
            return 0
        return self.entities.upsert(table, rows(response.body))

    def _mark_deleted(self, kind: str, entity_id: int, now: datetime) -> int:
        table = f"{kind}s" if kind != "alliance" else "alliances"
        existing = self.entities.lookup(table, [entity_id])
        if existing.num_rows == 0:
            return 0
        row = existing.slice(0, 1).to_pylist()[0]
        row.update({"deleted": True, "observed_at": now, "source": "esi"})
        return self.entities.upsert(table, _row(ENTITY_TABLES[table], row))


# --- ESI body -> rows ---------------------------------------------------------------------


def _object(body: Any) -> Mapping[str, object]:
    if not isinstance(body, Mapping):
        raise ValueError("ESI returned a non-object body")
    return body  # pyright: ignore[reportUnknownVariableType]


def _array(body: Any) -> list[Mapping[str, object]]:
    if not isinstance(body, list):
        raise ValueError("ESI returned a non-array body")
    return [item for item in body if isinstance(item, Mapping)]  # pyright: ignore[reportUnknownVariableType]


def character_row(character_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    corporation_id = _int(data.get("corporation_id"))
    return _row(
        CHARACTERS,
        {
            "character_id": character_id,
            "name": _str(data.get("name")),
            "corporation_id": corporation_id,
            "alliance_id": _int(data.get("alliance_id")),
            "faction_id": _int(data.get("faction_id")),
            "birthday": _timestamp(data.get("birthday")),
            "security_status": _float(data.get("security_status")),
            "deleted": corporation_id == DOOMHEIM,
            "observed_at": now,
            "source": "esi",
        },
    )


def employment_rows(character_id: int, body: Any, now: datetime) -> pa.Table:
    rows = [
        {
            "character_id": character_id,
            "record_id": _int(item.get("record_id")),
            "corporation_id": _int(item.get("corporation_id")),
            "start_date": _timestamp(item.get("start_date")),
            "observed_at": now,
            "source": "esi",
        }
        for item in _array(body)
        if _int(item.get("record_id")) is not None
    ]
    return pa.Table.from_pylist(rows, schema=CHARACTER_EMPLOYMENT)


def corporation_row(corporation_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    return _row(
        CORPORATIONS,
        {
            "corporation_id": corporation_id,
            "name": _str(data.get("name")),
            "ticker": _str(data.get("ticker")),
            "alliance_id": _int(data.get("alliance_id")),
            "ceo_id": _int(data.get("ceo_id")),
            "member_count": _int(data.get("member_count")),
            "date_founded": _timestamp(data.get("date_founded")),
            "deleted": data.get("state") == "closed",
            "observed_at": now,
            "source": "esi",
        },
    )


def alliance_history_rows(corporation_id: int, body: Any, now: datetime) -> pa.Table:
    rows = [
        {
            "corporation_id": corporation_id,
            "record_id": _int(item.get("record_id")),
            "alliance_id": _int(item.get("alliance_id")),
            "start_date": _timestamp(item.get("start_date")),
            "is_deleted": bool(item.get("is_deleted", False)),
            "observed_at": now,
            "source": "esi",
        }
        for item in _array(body)
        if _int(item.get("record_id")) is not None
    ]
    return pa.Table.from_pylist(rows, schema=CORPORATION_ALLIANCE_HISTORY)


def alliance_row(alliance_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    return _row(
        ALLIANCES,
        {
            "alliance_id": alliance_id,
            "name": _str(data.get("name")),
            "ticker": _str(data.get("ticker")),
            "executor_corporation_id": _int(data.get("executor_corporation_id")),
            "date_founded": _timestamp(data.get("date_founded")),
            "deleted": False,
            "observed_at": now,
            "source": "esi",
        },
    )


# --- export -----------------------------------------------------------------------------


def export_entities(entities: EntityStore, directory: Path) -> tuple[list[Path], int]:
    """Write every entity table to ``directory``; returns the files and total rows."""
    paths = entities.export_parquet(directory)
    total = sum(entities.count(table) for table in ENTITY_TABLES)
    return paths, total


@dataclass(slots=True)
class ExportJob:
    entities: EntityStore
    directory: Path

    def __call__(self, ctx: RunContext) -> JobOutcome:
        paths, total = export_entities(self.entities, self.directory)
        log.info("exported %d files, %d rows, to %s", len(paths), total, self.directory)
        return JobOutcome(objects_changed=len(paths), rows_written=total)
