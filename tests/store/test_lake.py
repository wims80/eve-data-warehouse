from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from evedw.domain.schemas import MARKET_HISTORY
from evedw.store.duckdb_lake.lake import ParquetLake, partition_date, partition_name


def rows(day: date, n: int) -> pa.Table:
    return pa.table(  # pyright: ignore[reportUnknownMemberType]
        {
            "date": [day] * n,
            "region_id": [10000002] * n,
            "type_id": list(range(n)),
            "average": [1.5] * n,
            "highest": [2.0] * n,
            "lowest": [1.0] * n,
            "order_count": [3] * n,
            "volume": [100] * n,
            "http_last_modified": [datetime(2026, 10, 2, 11, tzinfo=UTC)] * n,
        }
    )


@pytest.fixture
def lake(tmp_path: Path) -> ParquetLake:
    return ParquetLake(tmp_path / "lake")


def test_partition_names() -> None:
    assert partition_name("market_history", date(2026, 10, 1)) == "date=2026-10-01"
    assert partition_name("killmails", date(2026, 10, 1)) == "source_date=2026-10-01"
    assert partition_date("date=2026-10-01") == date(2026, 10, 1)
    assert partition_date("junk") is None


def test_write_is_atomic_and_carries_metadata(lake: ParquetLake) -> None:
    info = lake.write_partition(
        "market_history",
        "date=2026-10-01",
        rows(date(2026, 10, 1), 4),
        metadata={"evedw.revision": "1"},
    )
    assert info.row_count == 4
    assert info.path == lake.lake_dir / "market_history" / "date=2026-10-01" / "data.parquet"
    assert sorted(p.name for p in info.path.parent.iterdir()) == ["data.parquet"]
    (listed,) = lake.partitions("market_history")
    assert listed.row_count == 4 and listed.metadata == {"evedw.revision": "1"}
    assert listed.partition == "date=2026-10-01"

    lake.write_partition(
        "market_history",
        "date=2026-10-01",
        rows(date(2026, 10, 1), 2),
        metadata={"evedw.revision": "2"},
    )
    (listed,) = lake.partitions("market_history")
    assert listed.row_count == 2 and listed.metadata["evedw.revision"] == "2"


def test_write_rejects_missing_columns_and_unknown_tables(lake: ParquetLake) -> None:
    with pytest.raises(ValueError, match="lacks columns"):
        lake.write_partition(
            "market_history", "date=2026-10-01", pa.table({"date": [1]}), metadata={}
        )
    with pytest.raises(KeyError, match="unknown lake table"):
        lake.write_partition("nope", "x", rows(date(2026, 10, 1), 1), metadata={})


def test_read_ranges_and_columns(lake: ParquetLake) -> None:
    empty = lake.read("market_history")
    assert empty.num_rows == 0 and empty.schema.equals(MARKET_HISTORY)

    for day, n in ((date(2026, 10, 1), 3), (date(2026, 10, 2), 2), (date(2026, 10, 3), 1)):
        lake.write_partition(
            "market_history", partition_name("market_history", day), rows(day, n), metadata={}
        )
    everything = lake.read("market_history")
    assert everything.num_rows == 6
    assert everything.schema.equals(MARKET_HISTORY)
    assert everything.column("date").to_pylist()[:3] == [date(2026, 10, 1)] * 3
    assert everything.column("http_last_modified")[0].as_py() == datetime(
        2026, 10, 2, 11, tzinfo=UTC
    )

    middle = lake.read("market_history", date_from=date(2026, 10, 2), date_to=date(2026, 10, 2))
    assert middle.num_rows == 2
    tail = lake.read("market_history", date_from=date(2026, 10, 2), columns=["date", "type_id"])
    assert tail.column_names == ["date", "type_id"] and tail.num_rows == 3
    with pytest.raises(KeyError, match="unknown columns"):
        lake.read("market_history", columns=["nope"])


def test_view_sql_covers_populated_tables_only(lake: ParquetLake) -> None:
    empty = lake.view_sql()
    assert "CREATE OR REPLACE VIEW" not in empty
    assert "-- market_history: no partitions yet" in empty

    lake.write_partition(
        "market_history", "date=2026-10-01", rows(date(2026, 10, 1), 1), metadata={}
    )
    sql = lake.view_sql()
    assert "CREATE OR REPLACE VIEW market_history AS" in sql
    assert "killmails_unique" not in sql
    assert str(lake.lake_dir.resolve() / "market_history" / "*" / "data.parquet") in sql
    con = duckdb.connect()
    con.execute(sql)
    assert con.execute("SELECT count(*) FROM market_history").fetchone() == (1,)
