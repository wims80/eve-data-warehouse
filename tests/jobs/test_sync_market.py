"""End-to-end sync of market history against a fake EVE Ref."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from evedw.domain.datasets import MARKET_HISTORY
from evedw.domain.registry import ObjectStatus, RevisionStatus, RunStatus
from evedw.jobs.sync import SyncError
from evedw.jobs.verify import verify_dataset
from tests.helpers import D1, D2, D3, SyncEnv, market_csv_bz2, publish_three_days


def test_initial_sync_imports_everything_newest_first(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED
    assert run.objects_changed == 3 and run.rows_written == 600

    for day, rows in ((D1, 300), (D2, 200), (D3, 100)):
        obj = env.obj(day)
        assert obj.status is ObjectStatus.IMPORTED and obj.current_revision == 1
        rev = env.registry.current_revision("market_history", obj.object_key)
        assert rev is not None
        assert rev.observed_count == rows and rev.verified is True
        assert rev.status is RevisionStatus.IMPORTED
        raw = env.settings.data_dir / rev.raw_path
        assert raw.is_file() and raw.parent == (
            env.settings.raw_dir / "market_history" / "2026" / f"market-history-{day.isoformat()}"
        )
        assert raw.name == f"{rev.sha256}.csv.bz2"
        assert env.partition(day).is_file()

    # Newest first: the first run row's import order shows in imported_at ordering.
    imported = [
        env.registry.current_revision("market_history", env.obj(d).object_key) for d in (D3, D2, D1)
    ]
    stamps = [r.imported_at for r in imported if r is not None and r.imported_at is not None]
    assert stamps == sorted(stamps)

    table = env.lake.read("market_history")
    assert table.num_rows == 600
    assert env.lake.read("market_history", date_from=D2).num_rows == 300
    (partition,) = [
        p for p in env.lake.partitions("market_history") if p.partition == "date=2026-10-02"
    ]
    assert partition.metadata["evedw.object_key"] == "2026/market-history-2026-10-02.csv.bz2"
    assert partition.metadata["evedw.revision"] == "1"
    assert partition.metadata["evedw.parser_version"] == "1"
    assert not list(env.settings.scratch_dir.iterdir())

    report = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, totals=env.fake.totals
    )
    assert report.ok, report.to_text()
    assert report.objects_checked == 3 and report.raw_files_hashed == 3


def test_backfilled_day_gets_a_new_revision_and_only_that_day_is_rewritten(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.sync()
    between = datetime.now(UTC)
    before = {day: env.partition(day).stat() for day in (D1, D2, D3)}
    downloads_before = env.fake.downloads()

    env.fake.put(D2, market_csv_bz2(D2, rows=250), expected=250)
    run = env.sync()
    assert (
        run.status is RunStatus.SUCCEEDED and run.objects_changed == 1 and run.rows_written == 250
    )

    obj = env.obj(D2)
    assert obj.current_revision == 2
    chain = env.registry.revisions("market_history", obj.object_key)
    assert [(r.revision, r.status) for r in chain] == [
        (1, RevisionStatus.SUPERSEDED),
        (2, RevisionStatus.IMPORTED),
    ]
    assert chain[0].sha256 != chain[1].sha256
    assert (env.settings.data_dir / chain[0].raw_path).is_file()  # old revision retained
    assert env.obj(D1).current_revision == 1 and env.obj(D3).current_revision == 1

    after = {day: env.partition(day).stat() for day in (D1, D2, D3)}
    for day in (D1, D3):
        assert (before[day].st_ino, before[day].st_mtime_ns) == (
            after[day].st_ino,
            after[day].st_mtime_ns,
        )
    assert before[D2].st_ino != after[D2].st_ino
    assert env.lake.read("market_history", date_from=D2, date_to=D2).num_rows == 250
    assert not list(env.partition(D2).parent.glob("*.tmp"))
    downloads_after = env.fake.downloads()
    assert downloads_after == downloads_before + 1

    assert env.registry.objects_changed_since("market_history", between) == [obj]
    report = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, totals=env.fake.totals
    )
    assert report.ok, report.to_text()


def test_unchanged_upstream_is_a_no_op(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.sync()
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 0 and run.rows_written == 0
    assert env.obj(D1).current_revision == 1


def test_count_mismatch_is_recorded_not_fatal(env: SyncEnv) -> None:
    env.fake.put(D1, market_csv_bz2(D1, rows=50), expected=999)
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED
    rev = env.registry.current_revision("market_history", env.obj(D1).object_key)
    assert rev is not None and rev.observed_count == 50 and rev.verified is False
    report = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, totals=env.fake.totals
    )
    assert [i.kind for i in report.issues] == ["expected_count"]


def test_failed_import_keeps_previous_partition_and_next_run_recovers(
    env: SyncEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    publish_three_days(env.fake)
    env.sync()
    env.fake.put(D2, market_csv_bz2(D2, rows=250), expected=250)

    real_write = env.lake.write_partition

    def explode(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(env.lake, "write_partition", explode)
    with pytest.raises(SyncError, match="disk on fire"):
        env.sync()
    (run,) = env.registry.runs(limit=1)
    assert run.status is RunStatus.FAILED
    obj = env.obj(D2)
    assert obj.status is ObjectStatus.FAILED and obj.current_revision == 1
    assert "disk on fire" in (obj.last_error or "")
    chain = env.registry.revisions("market_history", obj.object_key)
    assert [(r.revision, r.status) for r in chain] == [
        (1, RevisionStatus.IMPORTED),
        (2, RevisionStatus.FAILED),
    ]
    assert env.lake.read("market_history", date_from=D2, date_to=D2).num_rows == 200
    assert not list(env.partition(D2).parent.glob("*.tmp"))

    monkeypatch.setattr(env.lake, "write_partition", real_write)
    downloads_before = env.fake.downloads()
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    obj = env.obj(D2)
    assert obj.status is ObjectStatus.IMPORTED and obj.current_revision == 3
    chain = env.registry.revisions("market_history", obj.object_key)
    assert chain[2].sha256 == chain[1].sha256 and chain[2].raw_path == chain[1].raw_path
    # The retained file was reused; nothing was downloaded again.
    assert env.fake.downloads() == downloads_before
    assert env.lake.read("market_history", date_from=D2, date_to=D2).num_rows == 250


def test_force_reimports_range_without_downloading(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.sync()
    downloads_before = env.fake.downloads()
    run = env.sync(date_from=D2, date_to=D3, force=True)
    assert run.objects_changed == 2 and run.rows_written == 300
    assert env.obj(D1).current_revision == 1
    assert env.obj(D2).current_revision == 2 and env.obj(D3).current_revision == 2
    assert env.fake.downloads() == downloads_before


def test_parser_bump_reimports_from_retained_raw(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.sync()
    downloads_before = env.fake.downloads()
    run = env.sync(dataset=replace(MARKET_HISTORY, parser_version=2))
    assert run.objects_changed == 3
    rev = env.registry.current_revision("market_history", env.obj(D1).object_key)
    assert rev is not None and rev.revision == 2 and rev.parser_version == 2
    assert env.fake.downloads() == downloads_before
    (partition,) = [
        p for p in env.lake.partitions("market_history") if p.partition == "date=2026-10-01"
    ]
    assert partition.metadata["evedw.parser_version"] == "2"


def test_upstream_rewritten_between_discovery_and_fetch_fails_that_object(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.fake.serve_instead[D2] = market_csv_bz2(D2, rows=10)
    with pytest.raises(SyncError, match="1 of 3 objects failed"):
        env.sync()
    assert env.obj(D2).status is ObjectStatus.FAILED
    assert "UpstreamChangedError" in (env.obj(D2).last_error or "")
    assert (
        env.obj(D1).status is ObjectStatus.IMPORTED and env.obj(D3).status is ObjectStatus.IMPORTED
    )
    assert env.registry.revisions("market_history", env.obj(D2).object_key) == []

    del env.fake.serve_instead[D2]
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1


def test_date_range_limits_work_and_never_marks_gone(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    run = env.sync(date_from=D3, date_to=D3)
    assert run.objects_changed == 1
    assert env.obj(D3).status is ObjectStatus.IMPORTED
    assert env.obj(D1).status is ObjectStatus.NEW

    del env.fake.files[D1]
    env.sync(date_from=D3, date_to=D3)
    assert env.obj(D1).status is ObjectStatus.NEW
    env.sync()
    assert env.obj(D1).status is ObjectStatus.GONE
    assert env.obj(D2).status is ObjectStatus.IMPORTED


def test_wrong_date_inside_file_is_rejected(env: SyncEnv) -> None:
    env.fake.put(D2, market_csv_bz2(D1, rows=10))
    with pytest.raises(SyncError, match="SourceContentError"):
        env.sync()
    assert not env.partition(D2).exists()
