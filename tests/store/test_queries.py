"""Named read queries: parameter handling, empty lake, live entity tables."""

from collections.abc import Iterator
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from evedw.config import Settings
from evedw.domain.schemas import CHARACTERS, LAKE_TABLES
from evedw.store import open_lake, open_queries
from evedw.store.base import EntityStore, Queries, Registry
from evedw.store.duckdb_lake.queries import QueryParam, convert_param, parse_query
from tests.store.test_lake import rows


@pytest.fixture
def queries(settings: Settings, registry: Registry) -> Iterator[Queries]:
    q = open_queries(settings, open_lake(settings))
    yield q
    q.close()


def test_parse_query_header() -> None:
    info, sql = parse_query(
        "x",
        "-- First line.\n-- second line\n-- param a: date required\n-- param b: int optional\n"
        "-- param c: ids\nSELECT $a, $b, $c\n",
    )
    assert info.description == "First line. second line"
    assert [(p.name, p.kind, p.required) for p in info.params] == [
        ("a", "date", True),
        ("b", "int", False),
        ("c", "ids", True),
    ]
    assert sql == "SELECT $a, $b, $c"


def test_convert_param() -> None:
    assert convert_param(QueryParam("d", "date", True), "2026-10-01") == date(2026, 10, 1)
    assert convert_param(QueryParam("i", "int", False), None) is None
    assert convert_param(QueryParam("ids", "ids", True), "3,1,3") == [1, 3]
    with pytest.raises(ValueError, match="missing required parameter 'd'"):
        convert_param(QueryParam("d", "date", True), "")
    with pytest.raises(ValueError, match="expected date"):
        convert_param(QueryParam("d", "date", True), "yesterday")
    with pytest.raises(ValueError, match="expected ids"):
        convert_param(QueryParam("ids", "ids", True), ",")


def test_every_query_file_declares_itself(queries: Queries) -> None:
    names = queries.names()
    assert "killmails_by_date" in names and "characters_by_id" in names
    for name in names:
        info = queries.describe(name)
        assert info.description, name
        assert info.params, name
    with pytest.raises(KeyError, match="unknown query"):
        queries.describe("nope")
    with pytest.raises(KeyError, match="unknown query"):
        queries.run("nope", {})


def test_lake_queries_work_before_and_after_partitions_exist(
    settings: Settings, queries: Queries
) -> None:
    params = {"date_from": "2026-10-01", "date_to": "2026-10-02"}
    empty = queries.run("market_history_by_date", params)
    assert empty.num_rows == 0
    assert empty.schema.names == LAKE_TABLES["market_history"].names
    assert queries.run("killmails_by_date", params).num_rows == 0

    lake = open_lake(settings)
    lake.write_partition(
        "market_history", "date=2026-10-01", rows(date(2026, 10, 1), 3), metadata={}
    )
    lake.write_partition(
        "market_history", "date=2026-10-03", rows(date(2026, 10, 3), 2), metadata={}
    )
    found = queries.run("market_history_by_date", params)
    assert found.num_rows == 3
    assert found.column("date").to_pylist() == [date(2026, 10, 1)] * 3
    assert queries.run("market_history_by_date", {**params, "region_id": "1"}).num_rows == 0
    assert queries.run("market_history_by_date", {**params, "region_id": "10000002"}).num_rows == 3

    with pytest.raises(ValueError, match="missing required parameter 'date_to'"):
        queries.run("market_history_by_date", {"date_from": "2026-10-01"})
    with pytest.raises(ValueError, match="unknown parameters \\['limit'\\]"):
        queries.run("market_history_by_date", {**params, "limit": "5"})


def test_entity_queries_read_the_live_tables(queries: Queries, entities: EntityStore) -> None:
    observed = datetime(2026, 10, 5, 12, tzinfo=UTC)
    entities.upsert(
        "characters",
        pa.table(
            {
                "character_id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "corporation_id": [10, 11, 12],
                "alliance_id": [None, 99, None],
                "faction_id": [None, None, None],
                "birthday": [observed] * 3,
                "security_status": [0.5, 1.0, -1.0],
                "deleted": [False, False, True],
                "observed_at": [observed] * 3,
                "source": ["esi"] * 3,
            }
        ).cast(CHARACTERS),
    )
    found = queries.run("characters_by_id", {"ids": "3,1,7"})
    assert found.column("character_id").to_pylist() == [1, 3]
    assert found.schema.field("observed_at").type == CHARACTERS.field("observed_at").type
    assert found.column("observed_at").to_pylist() == [observed, observed]
    assert queries.run("character_employment_by_id", {"ids": "1"}).num_rows == 0
