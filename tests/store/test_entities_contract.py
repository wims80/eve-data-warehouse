"""Contract tests every EntityStore backend must pass."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from evedw.domain.schemas import ALLIANCES, CHARACTER_EMPLOYMENT, CHARACTERS, ENTITY_TABLES
from evedw.store.base import EntityStore

T0 = datetime(2026, 5, 10, tzinfo=UTC)
T1 = T0 + timedelta(days=1)


def characters(*rows: dict[str, object]) -> pa.Table:
    base: dict[str, object] = {
        "character_id": 1,
        "name": "One",
        "corporation_id": 98000001,
        "alliance_id": None,
        "faction_id": None,
        "birthday": T0,
        "security_status": 0.5,
        "deleted": False,
        "observed_at": T0,
        "source": "everef_backfill:abc",
    }
    return pa.Table.from_pylist([{**base, **row} for row in rows], schema=CHARACTERS)


def test_upsert_inserts_and_lookup_returns_schema(entities: EntityStore) -> None:
    assert entities.upsert("characters", characters({"character_id": 1}, {"character_id": 2})) == 2
    assert entities.count("characters") == 2
    found = entities.lookup("characters", [2, 3])
    assert found.schema == CHARACTERS
    assert found.to_pylist()[0]["character_id"] == 2
    assert found.to_pylist()[0]["birthday"] == T0
    assert entities.lookup("characters", []).num_rows == 0


def test_newer_observation_wins_older_is_ignored(entities: EntityStore) -> None:
    entities.upsert("characters", characters({"character_id": 1, "name": "Old", "observed_at": T0}))
    entities.upsert(
        "characters",
        characters({"character_id": 1, "name": "New", "observed_at": T1, "source": "esi"}),
    )
    row = entities.lookup("characters", [1]).to_pylist()[0]
    assert row["name"] == "New" and row["source"] == "esi"
    entities.upsert(
        "characters", characters({"character_id": 1, "name": "Stale", "observed_at": T0})
    )
    assert entities.lookup("characters", [1]).to_pylist()[0]["name"] == "New"
    # Equal observation time replaces: the later write is the later knowledge.
    entities.upsert(
        "characters", characters({"character_id": 1, "name": "Same", "observed_at": T1})
    )
    assert entities.lookup("characters", [1]).to_pylist()[0]["name"] == "Same"


def test_duplicate_keys_within_a_batch_keep_the_newest(entities: EntityStore) -> None:
    entities.upsert(
        "characters",
        characters(
            {"character_id": 1, "name": "A", "observed_at": T0},
            {"character_id": 1, "name": "B", "observed_at": T1},
            {"character_id": 1, "name": "C", "observed_at": T0},
        ),
    )
    assert entities.count("characters") == 1
    assert entities.lookup("characters", [1]).to_pylist()[0]["name"] == "B"


def test_composite_keys(entities: EntityStore) -> None:
    rows = pa.Table.from_pylist(
        [
            {
                "character_id": 1,
                "record_id": 10,
                "corporation_id": 5,
                "start_date": T0,
                "observed_at": T0,
                "source": "esi",
            },
            {
                "character_id": 1,
                "record_id": 11,
                "corporation_id": 6,
                "start_date": T1,
                "observed_at": T0,
                "source": "esi",
            },
            {
                "character_id": 2,
                "record_id": 10,
                "corporation_id": 7,
                "start_date": T0,
                "observed_at": T0,
                "source": "esi",
            },
        ],
        schema=CHARACTER_EMPLOYMENT,
    )
    assert entities.upsert("character_employment", rows) == 3
    found = entities.lookup("character_employment", [1])
    assert [r["record_id"] for r in found.to_pylist()] == [10, 11]


def test_missing_columns_and_unknown_tables_are_rejected(entities: EntityStore) -> None:
    with pytest.raises(ValueError):
        entities.upsert("characters", pa.table({"character_id": [1]}))
    with pytest.raises(KeyError):
        entities.upsert("people", characters({"character_id": 1}))
    with pytest.raises(KeyError):
        entities.count("people")


def test_empty_upsert_is_a_no_op(entities: EntityStore) -> None:
    assert entities.upsert("alliances", ALLIANCES.empty_table()) == 0


def test_export_parquet_writes_every_table_atomically(
    entities: EntityStore, tmp_path: Path
) -> None:
    entities.upsert("characters", characters({"character_id": 2}, {"character_id": 1}))
    out = tmp_path / "entities"
    paths = entities.export_parquet(out)
    assert sorted(p.name for p in paths) == sorted(f"{t}.parquet" for t in ENTITY_TABLES)
    assert not list(out.glob("*.tmp"))
    table = pq.read_table(out / "characters.parquet")
    assert table.column("character_id").to_pylist() == [1, 2]
    assert table.schema.field("observed_at").type == CHARACTERS.field("observed_at").type
    assert pq.read_table(out / "alliances.parquet").num_rows == 0
