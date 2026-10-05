"""Named read queries (design §9) on the DuckDB backend.

Each ``queries/<name>.sql`` file declares its parameters in leading comment lines::

    -- One line of description.
    -- param date_from: date required
    -- param region_id: int optional

and refers to them as ``$name``. Lake tables are temp views over the Parquet partitions,
entity tables are the live tables in ``warehouse.duckdb``; the query text only ever sees
table names, so the SQL stays portable to a backend that stores the facts differently.
"""

import re
import threading
from collections.abc import Mapping
from datetime import date
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from evedw.domain.schemas import LAKE_TABLES
from evedw.store.base import QueryInfo, QueryParam
from evedw.store.duckdb_lake.lake import DATA_FILE, ParquetLake

_PARAM_LINE = re.compile(r"^--\s*param\s+(\w+)\s*:\s*(date|int|ids|str)\s*(required|optional)?\s*$")
PARAM_KINDS = ("date", "int", "ids", "str")


def parse_query(name: str, text: str) -> tuple[QueryInfo, str]:
    """Split a query file into its declaration and the SQL to run."""
    description: list[str] = []
    params: list[QueryParam] = []
    body: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if body or not stripped.startswith("--"):
            body.append(line)
            continue
        match = _PARAM_LINE.match(stripped)
        if match:
            params.append(
                QueryParam(
                    name=match.group(1),
                    kind=match.group(2),
                    required=(match.group(3) or "required") == "required",
                )
            )
        else:
            description.append(stripped[2:].strip())
    return (
        QueryInfo(name=name, description=" ".join(description), params=tuple(params)),
        "\n".join(body).strip(),
    )


def convert_param(param: QueryParam, raw: str | None) -> Any:
    if raw is None or raw == "":
        if param.required:
            raise ValueError(f"missing required parameter {param.name!r}")
        return None
    try:
        if param.kind == "date":
            return date.fromisoformat(raw)
        if param.kind == "int":
            return int(raw)
        if param.kind == "ids":
            ids = sorted({int(part) for part in raw.split(",") if part.strip()})
            if not ids:
                raise ValueError("empty list")
            return ids
        return raw
    except ValueError:
        raise ValueError(f"parameter {param.name!r}: expected {param.kind}, got {raw!r}") from None


class DuckDBQueries:
    def __init__(self, warehouse_path: Path, lake: ParquetLake) -> None:
        self._con = duckdb.connect(str(warehouse_path))
        self._con.execute("SET TimeZone = 'UTC'")
        self._lake = lake
        self._lock = threading.RLock()
        self._populated: dict[str, bool | None] = dict.fromkeys(LAKE_TABLES)
        self._queries: dict[str, tuple[QueryInfo, str]] = {}
        package = resources.files("evedw.store.duckdb_lake") / "queries"
        for entry in sorted(package.iterdir(), key=lambda e: e.name):
            if entry.name.endswith(".sql"):
                name = entry.name.removesuffix(".sql")
                self._queries[name] = parse_query(name, entry.read_text(encoding="utf-8"))

    def close(self) -> None:
        with self._lock:
            self._con.close()

    def names(self) -> list[str]:
        return list(self._queries)

    def describe(self, name: str) -> QueryInfo:
        try:
            return self._queries[name][0]
        except KeyError:
            raise KeyError(f"unknown query {name!r}") from None

    def run(self, name: str, params: Mapping[str, str]) -> pa.Table:
        info, sql = self._queries.get(name) or self._unknown(name)
        declared = {p.name for p in info.params}
        unknown = sorted(set(params) - declared)
        if unknown:
            raise ValueError(f"unknown parameters {unknown}; {name} takes {sorted(declared)}")
        bound = {p.name: convert_param(p, params.get(p.name)) for p in info.params}
        with self._lock:
            self._refresh_views()
            return self._con.execute(sql, bound).to_arrow_table()

    @staticmethod
    def _unknown(name: str) -> tuple[QueryInfo, str]:
        raise KeyError(f"unknown query {name!r}")

    def _refresh_views(self) -> None:
        """Point each lake table name at its partitions, or at an empty table of the right
        schema while there are none, so a query never fails on a glob with no matches."""
        for table, schema in LAKE_TABLES.items():
            table_dir = self._lake.table_dir(table)
            populated = next(table_dir.glob(f"*/{DATA_FILE}"), None) is not None
            if self._populated[table] == populated:
                continue
            if populated:
                glob = str(table_dir.resolve() / "*" / DATA_FILE)
                self._con.execute(
                    f"CREATE OR REPLACE TEMP VIEW {table} AS "
                    f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
                )
            else:
                self._con.execute(f"DROP VIEW IF EXISTS {table}")
                self._con.register(table, schema.empty_table())
            self._populated[table] = populated
