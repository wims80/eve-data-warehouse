"""Parquet lake: one directory per table, one ``<column>=<date>`` directory per partition,
one ``data.parquet`` inside. Writes go to a temp file and are promoted with ``os.replace``
so readers never observe a partial partition."""

import os
from collections.abc import Collection, Mapping
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from evedw.domain.schemas import LAKE_TABLES, PARTITION_COLUMN
from evedw.store.base import PartitionInfo

DATA_FILE = "data.parquet"


def partition_name(table: str, day: date) -> str:
    return f"{PARTITION_COLUMN[table]}={day.isoformat()}"


def partition_date(partition: str) -> date | None:
    _, _, value = partition.partition("=")
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


class ParquetLake:
    def __init__(self, lake_dir: Path) -> None:
        self.lake_dir = lake_dir

    def table_dir(self, table: str) -> Path:
        if table not in LAKE_TABLES:
            raise KeyError(f"unknown lake table {table!r}")
        return self.lake_dir / table

    def partition_path(self, table: str, partition: str) -> Path:
        return self.table_dir(table) / partition / DATA_FILE

    def write_partition(
        self, table: str, partition: str, data: pa.Table, *, metadata: Mapping[str, str]
    ) -> PartitionInfo:
        self.table_dir(table)
        schema = LAKE_TABLES[table]
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
            path = entry / DATA_FILE
            if not entry.is_dir() or not path.is_file():
                continue
            footer = pq.read_metadata(path)
            raw_meta = footer.metadata or {}
            metadata = {
                k.decode("utf-8"): v.decode("utf-8", errors="replace")
                for k, v in raw_meta.items()
                if not k.startswith(b"ARROW:")
            }
            found.append(
                PartitionInfo(
                    table=table,
                    partition=entry.name,
                    path=path,
                    row_count=footer.num_rows,
                    metadata=metadata,
                )
            )
        return found

    def read(
        self,
        table: str,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        columns: Collection[str] | None = None,
    ) -> pa.Table:
        schema = LAKE_TABLES[table]
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
        if "killmails" in populated:
            lines.append(
                "CREATE OR REPLACE VIEW killmails_unique AS SELECT * FROM killmails "
                "QUALIFY row_number() OVER "
                "(PARTITION BY killmail_id ORDER BY source_date DESC) = 1;"
            )
        return "\n".join(lines) + "\n"

    def _glob(self, table: str) -> str:
        return str(self.table_dir(table).resolve() / "*" / DATA_FILE)
