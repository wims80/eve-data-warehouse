"""Killmail normalisation on the real fixture, plus end-to-end sync."""

import io
import json
import tarfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from evedw.domain.datasets import KILLMAILS
from evedw.domain.registry import ObjectStatus, RunStatus
from evedw.domain.schemas import ATTACKERS, ITEMS
from evedw.domain.schemas import KILLMAILS as KILLMAILS_SCHEMA
from evedw.jobs.killmails import NestingError, normalise_day
from evedw.jobs.sync import SyncError
from evedw.jobs.verify import verify_dataset
from evedw.sources.archives import tar_json_to_ndjson
from tests.helpers import KILLMAIL_FIXTURE, FakeEveRef, SyncEnv, killmail_name

DAY = date(2026, 10, 1)


def fixture_documents() -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    with tarfile.open(KILLMAIL_FIXTURE, "r:bz2") as tar:
        for member in tar:
            fh = tar.extractfile(member)
            if fh is not None:
                docs.append(json.loads(fh.read()))
    return docs


def make_archive(path: Path, docs: list[dict[str, Any]]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:bz2") as tar:
        for doc in docs:
            body = json.dumps(doc, separators=(",", ":")).encode()
            info = tarfile.TarInfo(f"killmails/{doc['killmail_id']}.json")
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    path.write_bytes(buf.getvalue())
    return buf.getvalue()


@pytest.fixture
def ndjson(tmp_path: Path) -> Iterator[Path]:
    out = tmp_path / "day.ndjson"
    tar_json_to_ndjson(KILLMAIL_FIXTURE, out)
    yield out


def test_normalise_matches_independent_json_counts(ndjson: Path) -> None:
    docs = fixture_documents()
    tables, unknown = normalise_day(ndjson, DAY)
    assert unknown == {}
    assert tables["killmails"].schema.equals(KILLMAILS_SCHEMA)
    assert tables["attackers"].schema.equals(ATTACKERS)
    assert tables["items"].schema.equals(ITEMS)

    expected_attackers = sum(len(d["attackers"]) for d in docs)
    expected_top = sum(len(d["victim"].get("items", [])) for d in docs)
    expected_nested = sum(
        len(i.get("items", [])) for d in docs for i in d["victim"].get("items", [])
    )
    assert tables["killmails"].num_rows == len(docs) == 50
    assert tables["attackers"].num_rows == expected_attackers == 348
    items = tables["items"]
    assert items.num_rows == expected_top + expected_nested == 719 + 195
    parent = items.column("parent_ordinal").to_pylist()
    assert sum(1 for p in parent if p is None) == expected_top
    assert sum(1 for p in parent if p is not None) == expected_nested
    assert all(d == DAY for d in tables["killmails"].column("source_date").to_pylist())


def test_normalise_preserves_values(ndjson: Path) -> None:
    docs = {d["killmail_id"]: d for d in fixture_documents()}
    tables, _ = normalise_day(ndjson, DAY)
    km = tables["killmails"].to_pylist()
    by_id = {row["killmail_id"]: row for row in km}

    synthetic = by_id[900000001]
    assert synthetic["damage_taken"] == -42
    assert synthetic["killmail_hash"] == "deadbeef" * 5
    neg = [
        r
        for r in tables["attackers"].to_pylist()
        if r["killmail_id"] == 900000001 and r["ordinal"] == 1
    ]
    assert neg[0]["damage_done"] == -7

    with_moon = next(d for d in docs.values() if "moon_id" in d)
    assert by_id[with_moon["killmail_id"]]["moon_id"] == with_moon["moon_id"]
    with_war = next(d for d in docs.values() if "war_id" in d)
    assert by_id[with_war["killmail_id"]]["war_id"] == with_war["war_id"]
    no_char = next(d for d in docs.values() if "character_id" not in d["victim"])
    assert by_id[no_char["killmail_id"]]["victim_character_id"] is None
    no_pos = next(d for d in docs.values() if "position" not in d["victim"])
    assert by_id[no_pos["killmail_id"]]["pos_x"] is None

    some = docs[with_war["killmail_id"]]
    row = by_id[some["killmail_id"]]
    assert row["killmail_time"] == datetime.fromisoformat(some["killmail_time"]).astimezone(UTC)
    assert row["http_last_modified"] == datetime.fromisoformat(
        some["http_last_modified"]
    ).astimezone(UTC)
    assert row["attacker_count"] == len(some["attackers"])
    assert row["victim_ship_type_id"] == some["victim"]["ship_type_id"]
    assert row["solar_system_id"] == some["solar_system_id"]

    # Attackers keep their order and final_blow.
    rows = sorted(
        (r for r in tables["attackers"].to_pylist() if r["killmail_id"] == some["killmail_id"]),
        key=lambda r: r["ordinal"],
    )
    assert [r["final_blow"] for r in rows] == [a["final_blow"] for a in some["attackers"]]
    assert [r["damage_done"] for r in rows] == [a["damage_done"] for a in some["attackers"]]
    assert rows[0]["ordinal"] == 1

    # A container's children point at the container's ordinal.
    container_doc = next(
        d for d in docs.values() if any(i.get("items") for i in d["victim"].get("items", []))
    )
    container_index = next(
        i for i, it in enumerate(container_doc["victim"]["items"], start=1) if it.get("items")
    )
    children = [
        r
        for r in tables["items"].to_pylist()
        if r["killmail_id"] == container_doc["killmail_id"]
        and r["parent_ordinal"] == container_index
    ]
    expected_children = container_doc["victim"]["items"][container_index - 1]["items"]
    assert [c["item_type_id"] for c in sorted(children, key=lambda r: r["ordinal"])] == [
        c["item_type_id"] for c in expected_children
    ]


def test_unknown_keys_are_reported_and_deeper_nesting_rejected(tmp_path: Path) -> None:
    docs = fixture_documents()[:3]
    docs[0]["zkb_value"] = 1
    docs[1]["victim"]["warp_scrambled"] = True
    docs[2]["attackers"][0]["mood"] = "grim"
    docs[2]["victim"]["items"] = [{"flag": 5, "item_type_id": 34, "singleton": 0, "colour": "red"}]
    out = tmp_path / "day.ndjson"
    out.write_bytes(b"".join(json.dumps(d, separators=(",", ":")).encode() + b"\n" for d in docs))
    tables, unknown = normalise_day(out, DAY)
    assert unknown == {
        "attacker": ["mood"],
        "item": ["colour"],
        "top": ["zkb_value"],
        "victim": ["warp_scrambled"],
    }
    assert tables["killmails"].num_rows == 3

    docs[2]["victim"]["items"] = [
        {
            "flag": 5,
            "item_type_id": 1,
            "singleton": 0,
            "items": [
                {
                    "flag": 5,
                    "item_type_id": 2,
                    "singleton": 0,
                    "items": [{"flag": 5, "item_type_id": 3, "singleton": 0}],
                }
            ],
        }
    ]
    out.write_bytes(b"".join(json.dumps(d, separators=(",", ":")).encode() + b"\n" for d in docs))
    with pytest.raises(NestingError, match="deeper than one level"):
        normalise_day(out, DAY)


def test_empty_day(tmp_path: Path) -> None:
    out = tmp_path / "empty.ndjson"
    out.write_bytes(b"")
    tables, unknown = normalise_day(out, DAY)
    assert unknown == {} and all(t.num_rows == 0 for t in tables.values())


def test_sync_killmails_end_to_end(env: SyncEnv, tmp_path: Path) -> None:
    fake: FakeEveRef = env.fake_for(KILLMAILS, killmail_name)
    archive = KILLMAIL_FIXTURE.read_bytes()
    fake.put(DAY, archive, expected=50)
    day2 = date(2026, 10, 2)
    docs = fixture_documents()[:10]
    for doc in docs:
        doc["killmail_time"] = "2026-10-02T01:00:00Z"
    fake.put(day2, make_archive(tmp_path / "d2.tar.bz2", docs), expected=11)

    run = env.sync(KILLMAILS)
    assert run.status is RunStatus.SUCCEEDED
    assert run.objects_changed == 2 and run.rows_written == 60

    obj = env.registry.get_object("killmails", "2026/killmails-2026-10-01.tar.bz2")
    assert obj is not None and obj.status is ObjectStatus.IMPORTED
    rev = env.registry.current_revision("killmails", obj.object_key)
    assert rev is not None and rev.observed_count == 50 and rev.verified is True
    rev2 = env.registry.current_revision("killmails", "2026/killmails-2026-10-02.tar.bz2")
    assert rev2 is not None and rev2.observed_count == 10 and rev2.verified is False

    for table in ("killmails", "attackers", "items"):
        names = [p.partition for p in env.lake.partitions(table)]
        assert names == ["source_date=2026-10-01", "source_date=2026-10-02"]
    assert env.lake.read("killmails").num_rows == 60
    assert env.lake.read("attackers", date_from=DAY, date_to=DAY).num_rows == 348
    assert env.lake.read("items", date_from=DAY, date_to=DAY).num_rows == 914
    (info,) = [p for p in env.lake.partitions("killmails") if p.partition.endswith("10-01")]
    assert info.metadata["evedw.unknown_keys"] == "{}"

    report = verify_dataset(env.settings, env.registry, env.lake, KILLMAILS, totals=fake.totals)
    assert [i.kind for i in report.issues] == ["expected_count"]
    assert report.objects_checked == 2

    # The same ids appear on both days; the unique view keeps the newest source_date.
    import duckdb

    con = duckdb.connect()
    con.execute(env.lake.view_sql())
    assert con.execute("SELECT count(*) FROM killmails").fetchone() == (60,)
    assert con.execute("SELECT count(*) FROM killmails_unique").fetchone() == (50,)
    assert con.execute(
        "SELECT count(*) FROM killmails_unique WHERE source_date = DATE '2026-10-02'"
    ).fetchone() == (10,)


def test_sync_rejects_deeper_nesting_as_a_failed_object(env: SyncEnv, tmp_path: Path) -> None:
    fake = env.fake_for(KILLMAILS, killmail_name)
    docs = fixture_documents()[:2]
    docs[0]["victim"]["items"] = [
        {
            "flag": 5,
            "item_type_id": 1,
            "singleton": 0,
            "items": [
                {
                    "flag": 5,
                    "item_type_id": 2,
                    "singleton": 0,
                    "items": [{"flag": 5, "item_type_id": 3, "singleton": 0}],
                }
            ],
        }
    ]
    fake.put(DAY, make_archive(tmp_path / "bad.tar.bz2", docs))
    with pytest.raises(SyncError, match="NestingError"):
        env.sync(KILLMAILS)
    assert env.lake.partitions("killmails") == []
    obj = env.registry.get_object("killmails", "2026/killmails-2026-10-01.tar.bz2")
    assert obj is not None and obj.status is ObjectStatus.FAILED
