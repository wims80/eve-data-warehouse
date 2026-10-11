"""Parquet lake: one directory per table, one ``<column>=<date>`` directory per partition,
one ``data.parquet`` inside. Writes go to a temp file and are promoted with ``os.replace``
so readers never observe a partial partition.

Layout 2 (design §4): per-source killmail tables under ``sources/<source>/``, the merged
killmail tables at the top. ``LAYOUT`` holds the version; a lake without it is layout 1,
or a new lake when it has no killmail directories at all."""

import os
from collections.abc import Collection, Mapping
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from evedw.domain.schemas import (
    ALL_LAKE_TABLES,
    ENTITY_TABLES,
    KILLMAIL_TABLES,
    LAKE_TABLES,
    PARTITION_COLUMN,
    source_table,
    table_path,
)
from evedw.store.base import PartitionInfo

DATA_FILE = "data.parquet"
ENTITIES_DIR = "entities"
"""Subdirectory of the lake holding the entity table exports."""
LAYOUT_FILE = "LAYOUT"
LAYOUT_VERSION = 2


class ParquetLake:
    def __init__(self, lake_dir: Path) -> None:
        self.lake_dir = lake_dir

    def table_dir(self, table: str) -> Path:
        if table not in ALL_LAKE_TABLES:
            raise KeyError(f"unknown lake table {table!r}")
        return self.lake_dir / table_path(table)

    # -- layout --------------------------------------------------------------------------

    def layout_version(self) -> int:
        marker = self.lake_dir / LAYOUT_FILE
        if marker.is_file():
            return int(marker.read_text(encoding="utf-8").strip())
        old = any((self.lake_dir / t).is_dir() for t in KILLMAIL_TABLES)
        return 1 if old else LAYOUT_VERSION

    def migrate_layout(self) -> list[str]:
        """Layout 1 -> 2: EVE Ref's killmail partitions move under ``sources/everef/`` by
        directory rename, so nothing is copied and raw files are untouched. The merged
        layer is built afterwards by ``killmails:merge``."""
        done: list[str] = []
        if self.layout_version() < 2:
            for table in KILLMAIL_TABLES:
                old = self.lake_dir / table
                new = self.table_dir(source_table("everef", table))
                if not old.is_dir():
                    continue
                if new.exists():
                    raise RuntimeError(f"cannot move {old} to {new}: target exists")
                new.parent.mkdir(parents=True, exist_ok=True)
                old.rename(new)
                done.append(f"moved {table} to {new.relative_to(self.lake_dir)}")
        marker = self.lake_dir / LAYOUT_FILE
        if not marker.is_file() or marker.read_text(encoding="utf-8").strip() != str(
            LAYOUT_VERSION
        ):
            self.lake_dir.mkdir(parents=True, exist_ok=True)
            tmp = marker.with_suffix(".tmp")
            tmp.write_text(f"{LAYOUT_VERSION}\n", encoding="utf-8")
            tmp.replace(marker)
            done.append(f"lake layout version {LAYOUT_VERSION}")
        return done

    def partition_path(self, table: str, partition: str) -> Path:
        return self.table_dir(table) / partition / DATA_FILE

    def write_partition(
        self, table: str, partition: str, data: pa.Table, *, metadata: Mapping[str, str]
    ) -> PartitionInfo:
        self.table_dir(table)
        schema = ALL_LAKE_TABLES[table]
        missing = [name for name in schema.names if name not in data.column_names]
        if missing:
            raise ValueError(f"{table}: data lacks columns {missing}")
        cast = data.select(schema.names).cast(schema)
        cast = cast.replace_schema_metadata({k: v for k, v in metadata.items()})

        final = self.partition_path(table, partition)
        final.parent.mkdir(parents=True, exist_ok=True)
        tmp = final.with_name(DATA_FILE + ".tmp")
        pq.write_table(cast, tmp, compression="zstd")
        with tmp.open("rb") as fh:
            os.fsync(fh.fileno())
        tmp.replace(final)
        dir_fd = os.open(final.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return PartitionInfo(
            table=table,
            partition=partition,
            path=final,
            row_count=cast.num_rows,
            metadata=dict(metadata),
        )

    def partitions(self, table: str) -> list[PartitionInfo]:
        table_dir = self.table_dir(table)
        if not table_dir.is_dir():
            return []
        found: list[PartitionInfo] = []
        for entry in sorted(table_dir.iterdir()):
            if entry.is_dir() and (info := self.partition(table, entry.name)) is not None:
                found.append(info)
        return found

    def partition(self, table: str, partition: str) -> PartitionInfo | None:
        path = self.partition_path(table, partition)
        if not path.is_file():
            return None
        footer = pq.read_metadata(path)
        raw_meta = footer.metadata or {}
        metadata = {
            k.decode("utf-8"): v.decode("utf-8", errors="replace")
            for k, v in raw_meta.items()
            if not k.startswith(b"ARROW:")
        }
        return PartitionInfo(
            table=table,
            partition=partition,
            path=path,
            row_count=footer.num_rows,
            metadata=metadata,
        )

    def read(
        self,
        table: str,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        columns: Collection[str] | None = None,
    ) -> pa.Table:
        schema = ALL_LAKE_TABLES[table]
        selected = list(columns) if columns else schema.names
        unknown = [c for c in selected if c not in schema.names]
        if unknown:
            raise KeyError(f"{table}: unknown columns {unknown}")
        if not self.partitions(table):
            return schema.empty_table().select(selected)
        column = PARTITION_COLUMN[table]
        clauses: list[str] = []
        params: list[object] = [self._glob(table)]
        if date_from is not None:
            clauses.append(f"{column} >= ?")
            params.append(date_from)
        if date_to is not None:
            clauses.append(f"{column} <= ?")
            params.append(date_to)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        cols = ", ".join(f'"{c}"' for c in selected)
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            result = con.execute(
                f"SELECT {cols} FROM read_parquet(?, hive_partitioning = false){where} "
                f"ORDER BY {column}",
                params,
            ).to_arrow_table()
        finally:
            con.close()
        return result.cast(pa.schema([schema.field(c) for c in selected]))

    def killmail_day_counts(self, table: str) -> pa.Table:
        schema = pa.schema(
            [("day", pa.date32()), ("killmails", pa.int64()), ("distinct_ids", pa.int64())]
        )
        if "killmail_time" not in ALL_LAKE_TABLES[table].names:
            raise KeyError(f"{table} is not a killmail table")
        if not self.partitions(table):
            return schema.empty_table()
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            result = con.execute(
                "SELECT killmail_time::DATE AS day, count(*) AS killmails, "
                "count(DISTINCT killmail_id) AS distinct_ids "
                "FROM read_parquet(?, hive_partitioning = false) GROUP BY 1 ORDER BY 1",
                [self._glob(table)],
            ).to_arrow_table()
        finally:
            con.close()
        return result.cast(schema)

    def view_sql(self) -> str:
        lines = ["-- Views over the EVE data warehouse lake. Run in any DuckDB session."]
        populated = {table for table in LAKE_TABLES if self.partitions(table)}
        for table in LAKE_TABLES:
            if table not in populated:
                lines.append(f"-- {table}: no partitions yet, view skipped")
                continue
            lines.append(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM read_parquet('{self._glob(table)}', hive_partitioning = false);"
            )
        for table in ENTITY_TABLES:
            path = self.lake_dir / ENTITIES_DIR / f"{table}.parquet"
            if not path.is_file():
                lines.append(f"-- {table}: not exported yet, view skipped")
                continue
            lines.append(
                f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path.resolve()}');"
            )
        return "\n".join(lines) + "\n"

    def _glob(self, table: str) -> str:
        return str(self.table_dir(table).resolve() / "*" / DATA_FILE)
