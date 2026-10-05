"""Reference normaliser for one day of EVE Ref killmails using DuckDB.

Measured 2026-10-05 on killmails-2026-10-01.tar.bz2 (14,807 killmails):
concatenating members into one NDJSON took 0.09 s, reading it with the
explicit schema 0.05 s, and normalising plus writing three Parquet files
about 0.9 s. Reading the 14,807 files as a glob instead took 3.2 s, so the
job must always build a single NDJSON file per day.

Usage: python killmail_normalise.py <extracted-dir> <source-date> <out-dir>
"""

import pathlib
import sys

import duckdb

ITEM_INNER = (
    '[{"flag":"INTEGER","item_type_id":"BIGINT","quantity_destroyed":"BIGINT",'
    '"quantity_dropped":"BIGINT","singleton":"INTEGER","items":"JSON"}]'
)
ITEM = (
    "STRUCT(flag INTEGER, item_type_id BIGINT, quantity_destroyed BIGINT, "
    "quantity_dropped BIGINT, singleton INTEGER, items JSON)"
)
COLUMNS = f"""{{
  killmail_id: 'BIGINT', killmail_hash: 'VARCHAR', killmail_time: 'TIMESTAMP',
  http_last_modified: 'TIMESTAMP', solar_system_id: 'BIGINT', moon_id: 'BIGINT',
  war_id: 'BIGINT',
  victim: 'STRUCT(alliance_id BIGINT, character_id BIGINT, corporation_id BIGINT,
                  faction_id BIGINT, damage_taken HUGEINT, ship_type_id BIGINT,
                  position STRUCT(x DOUBLE, y DOUBLE, z DOUBLE), items {ITEM}[])',
  attackers: 'STRUCT(alliance_id BIGINT, character_id BIGINT, corporation_id BIGINT,
                     faction_id BIGINT, damage_done HUGEINT, final_blow BOOLEAN,
                     security_status DOUBLE, ship_type_id BIGINT,
                     weapon_type_id BIGINT)[]'
}}"""

EXPECTED_KEYS = {
    "top": {
        "attackers",
        "http_last_modified",
        "killmail_hash",
        "killmail_id",
        "killmail_time",
        "moon_id",
        "solar_system_id",
        "victim",
        "war_id",
    },
    "victim": {
        "alliance_id",
        "character_id",
        "corporation_id",
        "damage_taken",
        "faction_id",
        "items",
        "position",
        "ship_type_id",
    },
    "attacker": {
        "alliance_id",
        "character_id",
        "corporation_id",
        "damage_done",
        "faction_id",
        "final_blow",
        "security_status",
        "ship_type_id",
        "weapon_type_id",
    },
}


def build_ndjson(extracted: pathlib.Path, out: pathlib.Path) -> int:
    n = 0
    with out.open("wb") as fh:
        for member in sorted((extracted / "killmails").glob("*.json")):
            body = member.read_bytes().strip()
            if b"\n" in body:
                raise ValueError(f"{member} is not single-line JSON")
            fh.write(body)
            fh.write(b"\n")
            n += 1
    return n


def key_drift(con: duckdb.DuckDBPyConnection, ndjson: pathlib.Path) -> dict[str, set[str]]:
    src = f"read_ndjson_objects('{ndjson}')"
    top = {
        r[0] for r in con.execute(f"SELECT DISTINCT unnest(json_keys(json)) FROM {src}").fetchall()
    }
    victim = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT unnest(json_keys(json->'victim')) FROM {src}"
        ).fetchall()
    }
    attacker = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT unnest(json_keys(a)) FROM "
            f"(SELECT unnest(json_extract(json,'$.attackers[*]')) a FROM {src})"
        ).fetchall()
    }
    seen = {"top": top, "victim": victim, "attacker": attacker}
    return {k: seen[k] - EXPECTED_KEYS[k] for k in seen if seen[k] - EXPECTED_KEYS[k]}


def normalise(con: duckdb.DuckDBPyConnection, ndjson: pathlib.Path, source_date: str) -> None:
    con.execute(
        f"CREATE OR REPLACE TABLE raw AS SELECT * FROM read_ndjson('{ndjson}', columns={COLUMNS})"
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE killmails AS
        SELECT DATE '{source_date}' AS source_date, killmail_id, killmail_hash, killmail_time,
               http_last_modified, solar_system_id, moon_id, war_id,
               victim.character_id AS victim_character_id,
               victim.corporation_id AS victim_corporation_id,
               victim.alliance_id AS victim_alliance_id,
               victim.faction_id AS victim_faction_id,
               victim.ship_type_id AS victim_ship_type_id,
               victim.damage_taken::BIGINT AS damage_taken,
               victim.position.x AS pos_x, victim.position.y AS pos_y, victim.position.z AS pos_z,
               len(attackers) AS attacker_count
        FROM raw""")
    # generate_subscripts and unnest in the same select list expand in lockstep.
    con.execute(f"""
        CREATE OR REPLACE TABLE attackers AS
        SELECT DATE '{source_date}' AS source_date, killmail_id, ordinal,
               a.character_id, a.corporation_id, a.alliance_id, a.faction_id,
               a.ship_type_id, a.weapon_type_id, a.damage_done::BIGINT AS damage_done,
               a.final_blow, a.security_status
        FROM (SELECT killmail_id, generate_subscripts(attackers, 1) AS ordinal,
                     unnest(attackers) AS a FROM raw)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE items AS
        WITH l0 AS (
          SELECT killmail_id, generate_subscripts(victim.items, 1) AS ordinal,
                 unnest(victim.items) AS it FROM raw
        ), l1 AS (
          SELECT killmail_id, ordinal AS parent_ordinal,
                 generate_subscripts(from_json(it.items, '{ITEM_INNER}'), 1) AS ordinal,
                 unnest(from_json(it.items, '{ITEM_INNER}')) AS it
          FROM l0 WHERE it.items IS NOT NULL
        )
        SELECT DATE '{source_date}' AS source_date, killmail_id, NULL::INTEGER AS parent_ordinal,
               ordinal, it.flag, it.item_type_id, it.quantity_destroyed, it.quantity_dropped,
               it.singleton
        FROM l0
        UNION ALL
        SELECT DATE '{source_date}', killmail_id, parent_ordinal, ordinal, it.flag,
               it.item_type_id, it.quantity_destroyed, it.quantity_dropped, it.singleton
        FROM l1""")
    deeper = con.execute(
        "SELECT count(*) FROM (SELECT unnest(from_json(it.items, ?)) it2 FROM "
        "(SELECT unnest(victim.items) it FROM raw) WHERE it.items IS NOT NULL) "
        "WHERE it2.items IS NOT NULL",
        [ITEM_INNER],
    ).fetchone()[0]
    if deeper:
        raise ValueError(f"{deeper} items nested deeper than one level; schema needs extending")


def main() -> None:
    extracted, source_date, out_dir = (
        pathlib.Path(sys.argv[1]),
        sys.argv[2],
        pathlib.Path(sys.argv[3]),
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    ndjson = out_dir / "day.ndjson"
    print("members:", build_ndjson(extracted, ndjson))
    con = duckdb.connect()
    drift = key_drift(con, ndjson)
    if drift:
        print("unknown keys:", drift)
    normalise(con, ndjson, source_date)
    for table in ("killmails", "attackers", "items"):
        con.execute(
            f"COPY {table} TO '{out_dir / table}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        print(table, con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


if __name__ == "__main__":
    main()
