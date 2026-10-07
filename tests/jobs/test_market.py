"""Market CSV reader across the header layouts EVE Ref has published."""

import bz2
from datetime import date
from pathlib import Path

import pytest

from evedw.domain.schemas import MARKET_HISTORY
from evedw.jobs.market import SourceContentError, read_market_csv
from tests.helpers import FIXTURES, MARKET_FIXTURE

LEGACY = {
    # date,region_id,type_id,average,highest,lowest,volume,order_count
    date(2003, 10, 1): FIXTURES / "market_history" / "market-history-2003-10-01.csv.bz2",
    # date,region_id,type_id,average,lowest,highest,volume,order_count
    date(2020, 6, 17): FIXTURES / "market_history" / "market-history-2020-06-17.csv.bz2",
}


def _csv(source: Path, into: Path) -> Path:
    into.write_bytes(bz2.decompress(source.read_bytes()))
    return into


def test_current_layout_keeps_http_last_modified(tmp_path: Path) -> None:
    table = read_market_csv(_csv(MARKET_FIXTURE, tmp_path / "day.csv"))
    assert table.schema == MARKET_HISTORY
    assert table.column("http_last_modified").null_count == 0


@pytest.mark.parametrize("day", sorted(LEGACY))
def test_legacy_layouts_read_by_name(tmp_path: Path, day: date) -> None:
    table = read_market_csv(_csv(LEGACY[day], tmp_path / "day.csv"))
    assert table.schema == MARKET_HISTORY
    assert table.num_rows == 20
    assert set(table.column("date").to_pylist()) == {day}
    assert table.column("http_last_modified").null_count == 20
    rows = table.to_pylist()
    assert all(r["lowest"] <= r["average"] <= r["highest"] for r in rows)


def test_2020_layout_maps_swapped_columns(tmp_path: Path) -> None:
    table = read_market_csv(_csv(LEGACY[date(2020, 6, 17)], tmp_path / "day.csv"))
    first = table.to_pylist()[0]
    # 2020-06-17,10000001,19,125,125,125,54,1 -> volume 54, one order.
    assert (first["region_id"], first["type_id"]) == (10000001, 19)
    assert (first["volume"], first["order_count"]) == (54, 1)


def test_unknown_header_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "day.csv"
    path.write_text("date,region_id,type_id,average,price\n2026-10-01,1,2,3,4\n")
    with pytest.raises(SourceContentError, match="unexpected CSV header"):
        read_market_csv(path)
