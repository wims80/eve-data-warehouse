"""Killmail importer: one ``killmails-<date>.tar.bz2`` becomes three partitions,
``killmails``, ``attackers`` and ``items``, all keyed by ``source_date``.

The archive's JSON members are concatenated into one NDJSON file and read by DuckDB with
an explicit schema. Schema inference is never used: it drifts between days. Keys that are
present in the data but absent from the schema are recorded in the partition metadata
under ``evedw.unknown_keys`` so ``verify`` can surface them.

Measured on killmails-2026-10-01 (14,807 killmails): about one second per day.
"""

import json
import logging
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa

from evedw.domain.datasets import KILLMAILS, Dataset
from evedw.domain.registry import SourceObject, SourceRevision
from evedw.domain.schemas import ATTACKERS, ITEMS
from evedw.domain.schemas import KILLMAILS as KILLMAILS_SCHEMA
from evedw.jobs.runner import RunContext
from evedw.jobs.sync import ImportResult, partition_metadata
from evedw.sources.archives import tar_json_to_ndjson
from evedw.store.base import Lake
from evedw.store.duckdb_lake.lake import partition_name

log = logging.getLogger(__name__)


class NestingError(ValueError):
    """Items nested deeper than the schema allows. Extend the schema, bump parser_version."""


ITEM_FIELDS = (
    ("flag", "INTEGER"),
    ("item_type_id", "BIGINT"),
    ("quantity_destroyed", "BIGINT"),
    ("quantity_dropped", "BIGINT"),
    ("singleton", "INTEGER"),
    ("items", "JSON"),
)
ITEM_STRUCT = "STRUCT(" + ", ".join(f"{k} {v}" for k, v in ITEM_FIELDS) + ")"
ITEM_LIST_JSON = json.dumps([dict(ITEM_FIELDS)])

VICTIM_STRUCT = (
    "STRUCT(alliance_id BIGINT, character_id BIGINT, corporation_id BIGINT, "
    "faction_id BIGINT, damage_taken HUGEINT, ship_type_id BIGINT, "
    f"position STRUCT(x DOUBLE, y DOUBLE, z DOUBLE), items {ITEM_STRUCT}[])"
)
ATTACKER_STRUCT = (
    "STRUCT(alliance_id BIGINT, character_id BIGINT, corporation_id BIGINT, "
    "faction_id BIGINT, damage_done HUGEINT, final_blow BOOLEAN, security_status DOUBLE, "
    "ship_type_id BIGINT, weapon_type_id BIGINT)"
)
COLUMNS = {
    "killmail_id": "BIGINT",
    "killmail_hash": "VARCHAR",
    "killmail_time": "TIMESTAMP",
    "http_last_modified": "TIMESTAMP",
    "solar_system_id": "BIGINT",
    "moon_id": "BIGINT",
    "war_id": "BIGINT",
    "victim": VICTIM_STRUCT,
    "attackers": f"{ATTACKER_STRUCT}[]",
}
COLUMNS_SQL = "{" + ", ".join(f"{k}: '{v}'" for k, v in COLUMNS.items()) + "}"

EXPECTED_KEYS: dict[str, frozenset[str]] = {
    "top": frozenset(COLUMNS),
    "victim": frozenset(
        {
            "alliance_id",
            "character_id",
            "corporation_id",
            "damage_taken",
            "faction_id",
            "items",
            "position",
            "ship_type_id",
        }
    ),
    "attacker": frozenset(
        {
            "alliance_id",
            "character_id",
            "corporation_id",
            "damage_done",
            "faction_id",
            "final_blow",
            "security_status",
            "ship_type_id",
            "weapon_type_id",
        }
    ),
    "item": frozenset(k for k, _ in ITEM_FIELDS),
}

_KEY_QUERIES = {
    "top": "SELECT DISTINCT unnest(json_keys(json)) FROM src",
    "victim": "SELECT DISTINCT unnest(json_keys(json -> 'victim')) FROM src",
    "attacker": (
        "SELECT DISTINCT unnest(json_keys(a)) FROM "
        "(SELECT unnest(json_extract(json, '$.attackers[*]')) AS a FROM src)"
    ),
    "item": (
        "SELECT DISTINCT unnest(json_keys(i)) FROM "
        "(SELECT unnest(json_extract(json, '$.victim.items[*]')) AS i FROM src)"
    ),
}

KILLMAILS_SQL = """
SELECT ?::DATE AS source_date, killmail_id, killmail_hash, killmail_time, http_last_modified,
       solar_system_id, moon_id, war_id,
       victim.character_id AS victim_character_id,
       victim.corporation_id AS victim_corporation_id,
       victim.alliance_id AS victim_alliance_id,
       victim.faction_id AS victim_faction_id,
       victim.ship_type_id AS victim_ship_type_id,
       victim.damage_taken::BIGINT AS damage_taken,
       victim.position.x AS pos_x, victim.position.y AS pos_y, victim.position.z AS pos_z,
       len(attackers)::INTEGER AS attacker_count
FROM raw
ORDER BY killmail_id
"""

# generate_subscripts and unnest in one select list expand in lockstep.
ATTACKERS_SQL = """
SELECT ?::DATE AS source_date, killmail_id, ordinal::INTEGER AS ordinal,
       a.character_id, a.corporation_id, a.alliance_id, a.faction_id,
       a.ship_type_id, a.weapon_type_id, a.damage_done::BIGINT AS damage_done,
       a.final_blow, a.security_status
FROM (SELECT killmail_id, generate_subscripts(attackers, 1) AS ordinal,
             unnest(attackers) AS a FROM raw)
ORDER BY killmail_id, ordinal
"""

ITEMS_SQL = f"""
WITH l0 AS (
    SELECT killmail_id, generate_subscripts(victim.items, 1) AS ordinal,
           unnest(victim.items) AS it
    FROM raw
), l1 AS (
    SELECT killmail_id, ordinal AS parent_ordinal,
           generate_subscripts(from_json(it.items, '{ITEM_LIST_JSON}'), 1) AS ordinal,
           unnest(from_json(it.items, '{ITEM_LIST_JSON}')) AS it
    FROM l0 WHERE it.items IS NOT NULL
)
SELECT ?::DATE AS source_date, killmail_id, NULL::INTEGER AS parent_ordinal,
       ordinal::INTEGER AS ordinal, it.flag, it.item_type_id, it.quantity_destroyed,
       it.quantity_dropped, it.singleton
FROM l0
UNION ALL
SELECT ?::DATE, killmail_id, parent_ordinal::INTEGER, ordinal::INTEGER, it.flag,
       it.item_type_id, it.quantity_destroyed, it.quantity_dropped, it.singleton
FROM l1
ORDER BY killmail_id, parent_ordinal NULLS FIRST, ordinal
"""

DEEPER_SQL = f"""
SELECT count(*) FROM (
    SELECT unnest(from_json(it.items, '{ITEM_LIST_JSON}')) AS it2
    FROM (SELECT unnest(victim.items) AS it FROM raw)
    WHERE it.items IS NOT NULL
) WHERE it2.items IS NOT NULL
"""


def normalise_day(
    ndjson: Path, source_date: date
) -> tuple[dict[str, pa.Table], dict[str, list[str]]]:
    """Read one day's NDJSON and return the three tables plus any unknown keys found."""
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        if ndjson.stat().st_size == 0:
            return (
                {
                    "killmails": KILLMAILS_SCHEMA.empty_table(),
                    "attackers": ATTACKERS.empty_table(),
                    "items": ITEMS.empty_table(),
                },
                {},
            )
        con.execute(
            f"CREATE TABLE raw AS SELECT * FROM read_ndjson(?, columns = {COLUMNS_SQL})",
            [str(ndjson)],
        )
        con.execute("CREATE TABLE src AS SELECT * FROM read_ndjson_objects(?)", [str(ndjson)])

        unknown: dict[str, list[str]] = {}
        for scope, sql in _KEY_QUERIES.items():
            seen = {str(r[0]) for r in con.execute(sql).fetchall()}
            extra = sorted(seen - EXPECTED_KEYS[scope])
            if extra:
                unknown[scope] = extra

        deeper = con.execute(DEEPER_SQL).fetchone()
        if deeper and deeper[0]:
            raise NestingError(f"{deeper[0]} items are nested deeper than one level")

        tables = {
            "killmails": con.execute(KILLMAILS_SQL, [source_date])
            .to_arrow_table()
            .cast(KILLMAILS_SCHEMA),
            "attackers": con.execute(ATTACKERS_SQL, [source_date]).to_arrow_table().cast(ATTACKERS),
            "items": con.execute(ITEMS_SQL, [source_date, source_date])
            .to_arrow_table()
            .cast(ITEMS),
        }
        return tables, unknown
    finally:
        con.close()


class KillmailImporter:
    def __init__(self, lake: Lake) -> None:
        self._lake = lake

    @property
    def dataset(self) -> Dataset:
        return KILLMAILS

    def import_revision(
        self,
        ctx: RunContext,
        obj: SourceObject,
        revision: SourceRevision,
        *,
        raw_file: Path,
        scratch_dir: Path,
    ) -> ImportResult:
        if obj.logical_date is None:
            raise ValueError(f"{obj.object_key} has no logical date")
        ndjson = scratch_dir / f"{obj.object_key.replace('/', '_')}.ndjson"
        try:
            extracted = tar_json_to_ndjson(raw_file, ndjson)
            tables, unknown = normalise_day(ndjson, obj.logical_date)
        finally:
            ndjson.unlink(missing_ok=True)
        if unknown:
            log.warning("unknown JSON keys in %s: %s", obj.object_key, unknown)
        if tables["killmails"].num_rows != extracted.documents:
            raise ValueError(
                f"{extracted.documents} documents in archive but {tables['killmails'].num_rows} "
                "killmail rows parsed"
            )
        metadata = partition_metadata(
            self.dataset,
            obj,
            revision,
            extra=[("evedw.unknown_keys", json.dumps(unknown, sort_keys=True))],
        )
        partitions = [
            self._lake.write_partition(
                table, partition_name(table, obj.logical_date), data, metadata=metadata
            )
            for table, data in (
                ("attackers", tables["attackers"]),
                ("items", tables["items"]),
                ("killmails", tables["killmails"]),
            )
        ]
        return ImportResult(rows=tables["killmails"].num_rows, partitions=partitions)
