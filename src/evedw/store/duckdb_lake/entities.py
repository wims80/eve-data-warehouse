"""Entity tables in warehouse.duckdb: upserts keyed by primary key, lookups, Parquet export.

The upsert rule is "newer observation wins": a row is replaced only when the incoming
``observed_at`` is at least as recent as the stored one. Backfill snapshots carry their
record's update time, ESI rows carry the request time, so ESI always wins over a backfill.
"""

import os
import threading
from collections.abc import Collection
from pathlib import Path

import duckdb
import pyarrow as pa

from evedw.domain.schemas import ENTITY_PRIMARY_KEYS, ENTITY_TABLES, TIMESTAMP

ENTITY_ID: dict[str, str] = {table: keys[0] for table, keys in ENTITY_PRIMARY_KEYS.items()}
"""The column ``lookup`` filters on: the entity the table is about."""


def _check_table(table: str) -> pa.Schema:
    try:
        return ENTITY_TABLES[table]
    except KeyError:
        raise KeyError(f"unknown entity table {table!r}") from None


class DuckDBEntityStore:
    def __init__(self, path: Path) -> None:
        self._con = duckdb.connect(str(path))
        self._con.execute("SET TimeZone = 'UTC'")
        self._lock = threading.RLock()

    def close(self) -> None:
        self._con.close()

    def upsert(self, table: str, rows: pa.Table) -> int:
        schema = _check_table(table)
        missing = [name for name in schema.names if name not in rows.column_names]
        if missing:
            raise ValueError(f"{table}: rows lack columns {missing}")
        if rows.num_rows == 0:
            return 0
        data = rows.select(schema.names).cast(schema)
        keys = ENTITY_PRIMARY_KEYS[table]
        key_list = ", ".join(keys)
        columns = ", ".join(schema.names)
        # Naive UTC in the store; the arrow table carries tz-aware timestamps.
        select_list = ", ".join(
            f"CAST({name} AS TIMESTAMP) AS {name}" if schema.field(name).type == TIMESTAMP else name
            for name in schema.names
        )
        updates = ", ".join(
            f"{name} = excluded.{name}" for name in schema.names if name not in keys
        )
        with self._lock:
            self._con.register("incoming", data)
            try:
                self._con.begin()
                try:
                    self._con.execute(
                        f"INSERT INTO {table} ({columns}) "
                        f"SELECT {select_list} FROM ("
                        f"  SELECT * FROM incoming "
                        f"  QUALIFY row_number() OVER (PARTITION BY {key_list} "
                        f"    ORDER BY observed_at DESC) = 1"
                        f") ON CONFLICT ({key_list}) DO UPDATE SET {updates} "
                        f"WHERE excluded.observed_at >= {table}.observed_at"
                    )
                    self._con.commit()
                except Exception:
                    self._con.rollback()
                    raise
            finally:
                self._con.unregister("incoming")
        return data.num_rows

    def lookup(self, table: str, ids: Collection[int]) -> pa.Table:
        schema = _check_table(table)
        wanted = sorted({int(i) for i in ids})
        if not wanted:
            return schema.empty_table()
        column = ENTITY_ID[table]
        with self._lock:
            self._con.register("wanted", pa.table({"id": pa.array(wanted, pa.int64())}))
            try:
                result = self._con.execute(
                    f"SELECT {', '.join(schema.names)} FROM {table} "
                    f"WHERE {column} IN (SELECT id FROM wanted) "
                    f"ORDER BY {', '.join(ENTITY_PRIMARY_KEYS[table])}"
                ).to_arrow_table()
            finally:
                self._con.unregister("wanted")
        return result.cast(schema)

    def count(self, table: str) -> int:
        _check_table(table)
        with self._lock:
            row = self._con.execute(f"SELECT count(*) FROM {table}").fetchone()
        return int(row[0]) if row else 0

    def export_parquet(self, directory: Path) -> list[Path]:
        directory.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        for table, schema in ENTITY_TABLES.items():
            final = directory / f"{table}.parquet"
            tmp = directory / f"{table}.parquet.tmp"
            order = ", ".join(ENTITY_PRIMARY_KEYS[table])
            select_list = ", ".join(
                f"{name}::TIMESTAMPTZ AS {name}" if schema.field(name).type == TIMESTAMP else name
                for name in schema.names
            )
            with self._lock:
                self._con.execute(
                    f"COPY (SELECT {select_list} FROM {table} ORDER BY {order}) TO ? "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)",
                    [str(tmp)],
                )
            with tmp.open("rb") as fh:
                os.fsync(fh.fileno())
            tmp.replace(final)
            written.append(final)
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return written
