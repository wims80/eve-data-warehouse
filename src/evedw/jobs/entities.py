"""Entity jobs: seed from an EVE Ref backfill and export to Parquet. The ESI refresh is in
``jobs/refresh.py``.

Field sets below were pinned by inspecting ``eve-kill-com-karbowiak-2026-05-10.tar.bz2``
on 2026-10-05 (design §7.3). Reading uses explicit columns, so an extra upstream key is
reported, never silently absorbed, and a missing one becomes NULL.
"""

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from evedw.domain.datasets import ENTITIES_BACKFILL, Dataset
from evedw.domain.registry import SourceObject, SourceRevision
from evedw.domain.schemas import (
    ENTITY_TABLES,
)
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.jobs.sync import ImportResult
from evedw.logs import log_context
from evedw.sources.archives import extract_members
from evedw.store.base import EntityStore

log = logging.getLogger(__name__)

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
# ``$observed`` and ``$source`` are bound parameters; ``{src}`` is the read_json call and
# ``{real}`` the filter below.

REAL_RECORD = "error IS NULL AND name IS NOT NULL"
"""eve-kill exports contain placeholders for entities it failed to fetch: an ``error`` key
or no name. They describe nothing and are skipped (parser_version 2)."""

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
    "error": "JSON",
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
WHERE character_id IS NOT NULL AND {real}
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
    WHERE character_id IS NOT NULL AND {real} AND json_array_length(history) > 0
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
    "error": "JSON",
    "history": "JSON",
}
CORPORATION_SQL = """
SELECT corporation_id, name, ticker, alliance_id, ceo_id, member_count,
       try_cast(date_founded AS TIMESTAMPTZ) AS date_founded,
       coalesce(deleted, false) AS deleted,
       coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
       $source::VARCHAR AS source
FROM {src}
WHERE corporation_id IS NOT NULL AND {real}
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
    WHERE corporation_id IS NOT NULL AND {real} AND json_array_length(history) > 0
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
    "error": "JSON",
}
ALLIANCE_SQL = """
SELECT alliance_id, name, ticker, executor_corporation_id,
       try_cast(date_founded AS TIMESTAMPTZ) AS date_founded,
       coalesce(deleted, false) AS deleted,
       coalesce(try_cast(updatedAt AS TIMESTAMPTZ), $observed::TIMESTAMPTZ) AS observed_at,
       $source::VARCHAR AS source
FROM {src}
WHERE alliance_id IS NOT NULL AND {real}
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
                    plan.entity_sql.format(src=src, real=REAL_RECORD),
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
                        plan.history_sql.format(src=src, real=REAL_RECORD),
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
