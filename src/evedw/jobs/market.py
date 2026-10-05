"""Market history importer: one ``market-history-<date>.csv.bz2`` becomes one
``market_history/date=<date>`` partition."""

import logging
from pathlib import Path

import duckdb
import pyarrow as pa

from evedw.domain.datasets import MARKET_HISTORY, Dataset
from evedw.domain.registry import SourceObject, SourceRevision
from evedw.domain.schemas import MARKET_HISTORY as SCHEMA
from evedw.jobs.runner import RunContext
from evedw.jobs.sync import ImportResult, partition_metadata
from evedw.sources.archives import decompress_bz2
from evedw.store.base import Lake
from evedw.store.duckdb_lake.lake import partition_name

log = logging.getLogger(__name__)

CSV_TYPES = {
    "average": "DOUBLE",
    "date": "DATE",
    "highest": "DOUBLE",
    "lowest": "DOUBLE",
    "order_count": "BIGINT",
    "volume": "BIGINT",
    "http_last_modified": "VARCHAR",
    "region_id": "BIGINT",
    "type_id": "BIGINT",
}

_TYPES_SQL = "{" + ", ".join(f"'{k}': '{v}'" for k, v in CSV_TYPES.items()) + "}"

READ_SQL = f"""
SELECT
    date,
    region_id,
    type_id,
    average,
    highest,
    lowest,
    order_count,
    volume,
    strptime(http_last_modified, '%Y-%m-%dT%H:%M:%SZ')::TIMESTAMPTZ AS http_last_modified
FROM read_csv(?, header = true, types = {_TYPES_SQL})
ORDER BY region_id, type_id
"""


class SourceContentError(ValueError):
    """The file's content does not fit the object it was published as."""


def read_market_csv(path: Path) -> pa.Table:
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        return con.execute(READ_SQL, [str(path)]).to_arrow_table().cast(SCHEMA)
    finally:
        con.close()


class MarketHistoryImporter:
    def __init__(self, lake: Lake) -> None:
        self._lake = lake

    @property
    def dataset(self) -> Dataset:
        return MARKET_HISTORY

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
            raise SourceContentError(f"{obj.object_key} has no logical date")
        csv_path = scratch_dir / f"{obj.object_key.replace('/', '_')}.csv"
        try:
            decompress_bz2(raw_file, csv_path)
            table = read_market_csv(csv_path)
        finally:
            csv_path.unlink(missing_ok=True)

        dates = {d for d in table.column("date").to_pylist() if d is not None}
        if dates - {obj.logical_date}:
            raise SourceContentError(
                f"{obj.object_key} contains dates {sorted(dates)}, expected only {obj.logical_date}"
            )
        info = self._lake.write_partition(
            "market_history",
            partition_name("market_history", obj.logical_date),
            table,
            metadata=partition_metadata(self.dataset, obj, revision),
        )
        return ImportResult(rows=table.num_rows, partitions=[info])
