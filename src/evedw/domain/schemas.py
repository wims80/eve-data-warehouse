"""Arrow schemas for every lake table and registry table.

These are the contract between sources, store and consumers. Change a lake schema only
together with a ``parser_version`` bump in ``datasets.py``.
"""

from datetime import date

import pyarrow as pa

TIMESTAMP = pa.timestamp("us", tz="UTC")

# --- lake: killmails -------------------------------------------------------------------

KILLMAILS = pa.schema(
    [
        ("source_date", pa.date32()),
        ("killmail_id", pa.int64()),
        ("killmail_hash", pa.string()),
        ("killmail_time", TIMESTAMP),
        ("http_last_modified", TIMESTAMP),
        ("solar_system_id", pa.int64()),
        ("moon_id", pa.int64()),
        ("war_id", pa.int64()),
        ("victim_character_id", pa.int64()),
        ("victim_corporation_id", pa.int64()),
        ("victim_alliance_id", pa.int64()),
        ("victim_faction_id", pa.int64()),
        ("victim_ship_type_id", pa.int64()),
        ("damage_taken", pa.int64()),
        ("pos_x", pa.float64()),
        ("pos_y", pa.float64()),
        ("pos_z", pa.float64()),
        ("attacker_count", pa.int32()),
    ]
)

ATTACKERS = pa.schema(
    [
        ("source_date", pa.date32()),
        ("killmail_id", pa.int64()),
        ("ordinal", pa.int32()),
        ("character_id", pa.int64()),
        ("corporation_id", pa.int64()),
        ("alliance_id", pa.int64()),
        ("faction_id", pa.int64()),
        ("ship_type_id", pa.int64()),
        ("weapon_type_id", pa.int64()),
        ("damage_done", pa.int64()),
        ("final_blow", pa.bool_()),
        ("security_status", pa.float64()),
    ]
)

ITEMS = pa.schema(
    [
        ("source_date", pa.date32()),
        ("killmail_id", pa.int64()),
        ("parent_ordinal", pa.int32()),
        ("ordinal", pa.int32()),
        ("flag", pa.int32()),
        ("item_type_id", pa.int64()),
        ("quantity_destroyed", pa.int64()),
        ("quantity_dropped", pa.int64()),
        ("singleton", pa.int32()),
    ]
)

# --- lake: market history --------------------------------------------------------------

MARKET_HISTORY = pa.schema(
    [
        ("date", pa.date32()),
        ("region_id", pa.int64()),
        ("type_id", pa.int64()),
        ("average", pa.float64()),
        ("highest", pa.float64()),
        ("lowest", pa.float64()),
        ("order_count", pa.int64()),
        ("volume", pa.int64()),
        ("http_last_modified", TIMESTAMP),
    ]
)

# --- entities ---------------------------------------------------------------------------

_OBSERVATION = [("observed_at", TIMESTAMP), ("source", pa.string())]

CHARACTERS = pa.schema(
    [
        ("character_id", pa.int64()),
        ("name", pa.string()),
        ("corporation_id", pa.int64()),
        ("alliance_id", pa.int64()),
        ("faction_id", pa.int64()),
        ("birthday", TIMESTAMP),
        ("security_status", pa.float64()),
        ("deleted", pa.bool_()),
        *_OBSERVATION,
    ]
)

CORPORATIONS = pa.schema(
    [
        ("corporation_id", pa.int64()),
        ("name", pa.string()),
        ("ticker", pa.string()),
        ("alliance_id", pa.int64()),
        ("ceo_id", pa.int64()),
        ("member_count", pa.int64()),
        ("date_founded", TIMESTAMP),
        ("deleted", pa.bool_()),
        *_OBSERVATION,
    ]
)

ALLIANCES = pa.schema(
    [
        ("alliance_id", pa.int64()),
        ("name", pa.string()),
        ("ticker", pa.string()),
        ("executor_corporation_id", pa.int64()),
        ("date_founded", TIMESTAMP),
        ("deleted", pa.bool_()),
        *_OBSERVATION,
    ]
)

CHARACTER_EMPLOYMENT = pa.schema(
    [
        ("character_id", pa.int64()),
        ("record_id", pa.int64()),
        ("corporation_id", pa.int64()),
        ("start_date", TIMESTAMP),
        *_OBSERVATION,
    ]
)

CORPORATION_ALLIANCE_HISTORY = pa.schema(
    [
        ("corporation_id", pa.int64()),
        ("record_id", pa.int64()),
        ("alliance_id", pa.int64()),
        ("start_date", TIMESTAMP),
        ("is_deleted", pa.bool_()),
        *_OBSERVATION,
    ]
)

MERGED_KILLMAILS = KILLMAILS.append(pa.field("sources", pa.list_(pa.string())))
"""The merged killmail table: one row per killmail, ``sources`` naming every source that
has it (design §7.1)."""

KILLMAIL_SOURCES: tuple[str, ...] = ("everef", "zkillboard", "live")
"""Killmail sources in order of precedence: where several have a killmail, the first one's
rows go into the merged layer. ``live`` is zKillboard's live feed (design §7.1), last so a
day's daily files take over from it."""

DAILY_SOURCES: tuple[str, ...] = ("everef", "zkillboard")
"""Sources that publish whole days; ``verify`` compares their day counts."""

KILLMAIL_ZKB = pa.schema(
    [
        ("source_date", pa.date32()),
        ("killmail_id", pa.int64()),
        ("sequence_id", pa.int64()),
        ("uploaded_at", TIMESTAMP),
        ("killmail_hash", pa.string()),
        ("location_id", pa.int64()),
        ("total_value", pa.float64()),
        ("fitted_value", pa.float64()),
        ("dropped_value", pa.float64()),
        ("destroyed_value", pa.float64()),
        ("total_droppable_value", pa.float64()),
        ("points", pa.int64()),
        ("npc", pa.bool_()),
        ("solo", pa.bool_()),
        ("awox", pa.bool_()),
        ("attacker_count", pa.int32()),
        ("labels", pa.list_(pa.string())),
    ]
)
"""zKillboard's metadata of a killmail from the live feed, one row per killmail, the day
from its ``killmail_time``; the newest sequence wins (design §7.1)."""

KILLMAIL_TABLES: tuple[str, ...] = ("killmails", "attackers", "items")

SOURCE_TABLES: dict[str, pa.Schema] = {
    f"{source}_{table}": schema
    for source in KILLMAIL_SOURCES
    for table, schema in (("killmails", KILLMAILS), ("attackers", ATTACKERS), ("items", ITEMS))
}
"""Per-source killmail tables, e.g. ``everef_killmails``. Not for consumers."""

LAKE_TABLES: dict[str, pa.Schema] = {
    "killmails": MERGED_KILLMAILS,
    "attackers": ATTACKERS,
    "items": ITEMS,
    "market_history": MARKET_HISTORY,
    "killmail_zkb": KILLMAIL_ZKB,
}
"""The tables consumers read."""

ALL_LAKE_TABLES: dict[str, pa.Schema] = {**LAKE_TABLES, **SOURCE_TABLES}


def source_table(source: str, table: str) -> str:
    """``source_table("everef", "killmails")`` -> ``everef_killmails``."""
    if source not in KILLMAIL_SOURCES or table not in KILLMAIL_TABLES:
        raise KeyError(f"no source table for {source!r}, {table!r}")
    return f"{source}_{table}"


def table_path(table: str) -> str:
    """Directory of a lake table relative to the lake: ``sources/everef/killmails`` for a
    source table, the table name otherwise."""
    if table in SOURCE_TABLES:
        source, _, name = table.partition("_")
        return f"sources/{source}/{name}"
    return table


ENTITY_TABLES: dict[str, pa.Schema] = {
    "characters": CHARACTERS,
    "corporations": CORPORATIONS,
    "alliances": ALLIANCES,
    "character_employment": CHARACTER_EMPLOYMENT,
    "corporation_alliance_history": CORPORATION_ALLIANCE_HISTORY,
}

ENTITY_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "characters": ("character_id",),
    "corporations": ("corporation_id",),
    "alliances": ("alliance_id",),
    "character_employment": ("character_id", "record_id"),
    "corporation_alliance_history": ("corporation_id", "record_id"),
}
"""Upsert keys of the entity tables. The first column is the entity the table is about,
which ``EntityStore.lookup`` filters on."""

PARTITION_COLUMN: dict[str, str] = {
    "killmails": "source_date",
    "attackers": "source_date",
    "items": "source_date",
    "market_history": "date",
    "killmail_zkb": "source_date",
    **dict.fromkeys(SOURCE_TABLES, "source_date"),
}


def partition_name(table: str, day: date) -> str:
    """``date=2026-10-01``: the hive directory of one day of a lake table."""
    return f"{PARTITION_COLUMN[table]}={day.isoformat()}"


def partition_date(partition: str) -> date | None:
    _, _, value = partition.partition("=")
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


DATASET_TABLES: dict[str, tuple[str, ...]] = {
    "killmails": ("everef_killmails", "everef_attackers", "everef_items"),
    "zkillboard_killmails": ("zkillboard_killmails", "zkillboard_attackers", "zkillboard_items"),
    "market_history": ("market_history",),
}
"""Lake tables each dataset fills. The first table is the one whose row count is the
object's ``observed_count``."""

# --- registry ---------------------------------------------------------------------------

SOURCE_OBJECT = pa.schema(
    [
        ("dataset", pa.string()),
        ("object_key", pa.string()),
        ("url", pa.string()),
        ("logical_date", pa.date32()),
        ("upstream_etag", pa.string()),
        ("upstream_size", pa.int64()),
        ("upstream_last_modified", TIMESTAMP),
        ("expected_count", pa.int64()),
        ("discovered_at", TIMESTAMP),
        ("last_seen_at", TIMESTAMP),
        ("current_revision", pa.int32()),
        ("status", pa.string()),
        ("last_error", pa.string()),
    ]
)

SOURCE_REVISION = pa.schema(
    [
        ("dataset", pa.string()),
        ("object_key", pa.string()),
        ("revision", pa.int32()),
        ("sha256", pa.string()),
        ("size", pa.int64()),
        ("raw_path", pa.string()),
        ("upstream_etag", pa.string()),
        ("upstream_last_modified", TIMESTAMP),
        ("fetched_at", TIMESTAMP),
        ("imported_at", TIMESTAMP),
        ("parser_version", pa.int32()),
        ("observed_count", pa.int64()),
        ("verified", pa.bool_()),
        ("status", pa.string()),
    ]
)

IMPORT_RUN = pa.schema(
    [
        ("run_id", pa.string()),
        ("job", pa.string()),
        ("trigger", pa.string()),
        ("started_at", TIMESTAMP),
        ("finished_at", TIMESTAMP),
        ("status", pa.string()),
        ("params_json", pa.string()),
        ("objects_changed", pa.int64()),
        ("rows_written", pa.int64()),
        ("error", pa.string()),
    ]
)

ENTITY_REFRESH = pa.schema(
    [
        ("kind", pa.string()),
        ("entity_id", pa.int64()),
        ("priority", pa.int32()),
        ("last_refreshed_at", TIMESTAMP),
        ("next_due_at", TIMESTAMP),
        ("etag", pa.string()),
        ("failures", pa.int32()),
        ("last_error", pa.string()),
    ]
)

ESI_CACHE = pa.schema(
    [
        ("key", pa.string()),
        ("status", pa.int32()),
        ("body", pa.string()),
        ("etag", pa.string()),
        ("last_modified", pa.string()),
        ("cache_control", pa.string()),
        ("observed_at", TIMESTAMP),
        ("expires_at", TIMESTAMP),
    ]
)
