"""Contract tests every Registry backend must pass. Parametrised through the ``registry``
fixture in conftest; add new backends there."""

from datetime import UTC, date, datetime, timedelta

import pytest

from evedw.domain.registry import (
    DiscoveredObject,
    NewRevision,
    ObjectStatus,
    RefreshEntry,
    RevisionStatus,
    RunStatus,
    Trigger,
)
from evedw.store.base import Registry

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def discovered(
    key: str = "2026/killmails-2026-10-01.tar.bz2", **overrides: object
) -> DiscoveredObject:
    base: dict[str, object] = {
        "dataset": "killmails",
        "object_key": key,
        "url": f"https://example.test/killmails/{key}",
        "logical_date": date(2026, 10, 1),
        "etag": "etag-1",
        "size": 1000,
        "last_modified": datetime(2026, 10, 2, 3, 0, tzinfo=UTC),
        "expected_count": 15000,
    }
    base.update(overrides)
    return DiscoveredObject(**base)  # type: ignore[arg-type]


def new_revision(
    key: str = "2026/killmails-2026-10-01.tar.bz2", sha: str = "a" * 64
) -> NewRevision:
    return NewRevision(
        dataset="killmails",
        object_key=key,
        sha256=sha,
        size=1000,
        raw_path=f"raw/killmails/{key}/{sha}.tar.bz2",
        upstream_etag="etag-1",
        upstream_last_modified=datetime(2026, 10, 2, 3, 0, tzinfo=UTC),
        parser_version=1,
        fetched_at=NOW,
    )


def test_migrate_is_idempotent(registry: Registry) -> None:
    first = registry.schema_version()
    assert first >= 1
    assert registry.migrate() == first
    assert registry.schema_version() == first


def test_discovery_inserts_new_objects(registry: Registry) -> None:
    summary = registry.upsert_objects([discovered()], now=NOW)
    assert (summary.new, summary.changed, summary.unchanged) == (1, 0, 0)
    obj = registry.get_object("killmails", "2026/killmails-2026-10-01.tar.bz2")
    assert obj is not None
    assert obj.status is ObjectStatus.NEW
    assert obj.current_revision is None
    assert obj.upstream_etag == "etag-1"
    assert obj.upstream_last_modified == datetime(2026, 10, 2, 3, 0, tzinfo=UTC)
    assert obj.discovered_at == NOW
    assert obj.logical_date == date(2026, 10, 1)


def test_unchanged_discovery_only_refreshes_seen_and_count(registry: Registry) -> None:
    registry.upsert_objects([discovered()], now=NOW)
    registry.mark("killmails", "2026/killmails-2026-10-01.tar.bz2", ObjectStatus.IMPORTED)
    later = NOW + timedelta(hours=1)
    summary = registry.upsert_objects([discovered(expected_count=15010)], now=later)
    assert (summary.new, summary.changed, summary.unchanged) == (0, 0, 1)
    obj = registry.get_object("killmails", "2026/killmails-2026-10-01.tar.bz2")
    assert obj is not None
    assert obj.status is ObjectStatus.IMPORTED
    assert obj.expected_count == 15010
    assert obj.last_seen_at == later
    assert obj.discovered_at == NOW


@pytest.mark.parametrize(
    "change",
    [{"etag": "etag-2"}, {"size": 1001}, {"last_modified": datetime(2026, 10, 3, tzinfo=UTC)}],
)
def test_changed_upstream_marks_object_changed(
    registry: Registry, change: dict[str, object]
) -> None:
    registry.upsert_objects([discovered()], now=NOW)
    registry.mark("killmails", "2026/killmails-2026-10-01.tar.bz2", ObjectStatus.IMPORTED)
    summary = registry.upsert_objects([discovered(**change)], now=NOW)  # type: ignore[arg-type]
    assert summary.changed == 1
    obj = registry.get_object("killmails", "2026/killmails-2026-10-01.tar.bz2")
    assert obj is not None
    assert obj.status is ObjectStatus.CHANGED


def test_mark_missing_sets_gone_and_rediscovery_restores(registry: Registry) -> None:
    registry.upsert_objects(
        [discovered(), discovered("2026/killmails-2026-10-02.tar.bz2")], now=NOW
    )
    gone = registry.mark_missing("killmails", {"2026/killmails-2026-10-01.tar.bz2"}, now=NOW)
    assert gone == 1
    obj = registry.get_object("killmails", "2026/killmails-2026-10-02.tar.bz2")
    assert obj is not None and obj.status is ObjectStatus.GONE
    registry.upsert_objects([discovered("2026/killmails-2026-10-02.tar.bz2")], now=NOW)
    obj = registry.get_object("killmails", "2026/killmails-2026-10-02.tar.bz2")
    assert obj is not None and obj.status is ObjectStatus.CHANGED


def test_objects_filters_and_ordering(registry: Registry) -> None:
    registry.upsert_objects(
        [
            discovered("2026/killmails-2026-10-01.tar.bz2", logical_date=date(2026, 10, 1)),
            discovered("2026/killmails-2026-10-02.tar.bz2", logical_date=date(2026, 10, 2)),
            discovered("2026/killmails-2026-10-03.tar.bz2", logical_date=date(2026, 10, 3)),
        ],
        now=NOW,
    )
    registry.mark("killmails", "2026/killmails-2026-10-02.tar.bz2", ObjectStatus.FAILED, error="x")
    newest_first = registry.objects(dataset="killmails")
    assert [o.logical_date for o in newest_first] == [
        date(2026, 10, 3),
        date(2026, 10, 2),
        date(2026, 10, 1),
    ]
    assert [o.logical_date for o in registry.objects(dataset="killmails", newest_first=False)] == [
        date(2026, 10, 1),
        date(2026, 10, 2),
        date(2026, 10, 3),
    ]
    failed = registry.objects(dataset="killmails", status=ObjectStatus.FAILED)
    assert len(failed) == 1 and failed[0].last_error == "x"
    ranged = registry.objects(
        dataset="killmails", date_from=date(2026, 10, 2), date_to=date(2026, 10, 2)
    )
    assert [o.object_key for o in ranged] == ["2026/killmails-2026-10-02.tar.bz2"]
    both = registry.objects(status=[ObjectStatus.NEW, ObjectStatus.FAILED])
    assert len(both) == 3
    assert registry.objects(dataset="market_history") == []


def test_revisions_number_sequentially_and_promote_swaps_current(registry: Registry) -> None:
    key = "2026/killmails-2026-10-01.tar.bz2"
    registry.upsert_objects([discovered(key)], now=NOW)
    first = registry.add_revision(new_revision(key, "a" * 64))
    assert first.revision == 1 and first.status is RevisionStatus.FETCHED
    registry.promote("killmails", key, 1, observed_count=15000, verified=True, imported_at=NOW)
    obj = registry.get_object("killmails", key)
    assert obj is not None
    assert obj.status is ObjectStatus.IMPORTED and obj.current_revision == 1
    current = registry.current_revision("killmails", key)
    assert current is not None and current.observed_count == 15000 and current.verified is True
    assert current.imported_at == NOW

    second = registry.add_revision(new_revision(key, "b" * 64))
    assert second.revision == 2
    later = NOW + timedelta(days=1)
    registry.promote("killmails", key, 2, observed_count=15020, verified=None, imported_at=later)
    chain = registry.revisions("killmails", key)
    assert [(r.revision, r.status) for r in chain] == [
        (1, RevisionStatus.SUPERSEDED),
        (2, RevisionStatus.IMPORTED),
    ]
    obj = registry.get_object("killmails", key)
    assert obj is not None and obj.current_revision == 2

    assert registry.objects_changed_since("killmails", NOW) == [obj]
    assert registry.objects_changed_since("killmails", later) == []


def test_failed_revision_keeps_previous_current(registry: Registry) -> None:
    key = "2026/killmails-2026-10-01.tar.bz2"
    registry.upsert_objects([discovered(key)], now=NOW)
    registry.add_revision(new_revision(key, "a" * 64))
    registry.promote("killmails", key, 1, observed_count=1, verified=True, imported_at=NOW)
    registry.add_revision(new_revision(key, "b" * 64))
    registry.set_revision_status("killmails", key, 2, RevisionStatus.FAILED)
    registry.mark("killmails", key, ObjectStatus.FAILED, error="boom")
    current = registry.current_revision("killmails", key)
    assert current is not None and current.revision == 1
    assert [r.status for r in registry.revisions("killmails", key)] == [
        RevisionStatus.IMPORTED,
        RevisionStatus.FAILED,
    ]


def test_parser_bump_marks_imported_objects_changed(registry: Registry) -> None:
    key = "2026/killmails-2026-10-01.tar.bz2"
    registry.upsert_objects([discovered(key)], now=NOW)
    registry.add_revision(new_revision(key))
    registry.promote("killmails", key, 1, observed_count=1, verified=True, imported_at=NOW)
    assert registry.mark_parser_outdated("killmails", 1) == 0
    assert registry.mark_parser_outdated("killmails", 2) == 1
    obj = registry.get_object("killmails", key)
    assert obj is not None and obj.status is ObjectStatus.CHANGED
    assert obj.current_revision == 1


def test_run_log_round_trip(registry: Registry) -> None:
    run = registry.start_run("sync:killmails", Trigger.CLI, {"from": date(2026, 10, 1)}, now=NOW)
    assert run.status is RunStatus.RUNNING and run.finished_at is None
    assert run.params == {"from": "2026-10-01"}
    registry.finish_run(
        run.run_id,
        RunStatus.SUCCEEDED,
        now=NOW + timedelta(minutes=1),
        objects_changed=3,
        rows_written=42,
    )
    stored = registry.get_run(run.run_id)
    assert stored is not None
    assert stored.status is RunStatus.SUCCEEDED
    assert stored.objects_changed == 3 and stored.rows_written == 42
    assert stored.finished_at == NOW + timedelta(minutes=1)

    failed = registry.start_run("verify", Trigger.SCHEDULE, {}, now=NOW + timedelta(hours=1))
    registry.finish_run(failed.run_id, RunStatus.FAILED, now=NOW + timedelta(hours=2), error="bad")
    assert [r.job for r in registry.runs()] == ["verify", "sync:killmails"]
    assert [r.job for r in registry.runs(job="verify")] == ["verify"]
    assert registry.runs(limit=1)[0].error == "bad"


def test_refresh_queue_merges_and_orders(registry: Registry) -> None:
    soon = NOW + timedelta(hours=1)
    registry.refresh_push(
        [
            RefreshEntry("character", 1, priority=5, next_due_at=NOW),
            RefreshEntry("character", 2, priority=1, next_due_at=soon),
            RefreshEntry("corporation", 3, priority=1, next_due_at=NOW),
        ]
    )
    # Re-push keeps the better priority and the earlier due time.
    registry.refresh_push([RefreshEntry("character", 1, priority=1, next_due_at=soon)])
    due_now = registry.refresh_pop(limit=10, now=NOW)
    assert [(e.kind, e.entity_id, e.priority) for e in due_now] == [
        ("character", 1, 1),
        ("corporation", 3, 1),
    ]
    assert len(registry.refresh_pop(limit=10, now=soon)) == 3
    assert len(registry.refresh_pop(limit=1, now=soon)) == 1

    registry.refresh_update(
        RefreshEntry(
            "character",
            1,
            priority=9,
            next_due_at=soon + timedelta(days=1),
            last_refreshed_at=NOW,
            etag="e",
            failures=1,
            last_error="x",
        )
    )
    assert [e.entity_id for e in registry.refresh_pop(limit=10, now=soon)] == [3, 2]


def test_refresh_queue_orders_never_refreshed_and_stalest_first(registry: Registry) -> None:
    registry.refresh_push(
        [
            RefreshEntry("character", 1, priority=1, next_due_at=NOW),
            RefreshEntry("character", 2, priority=1, next_due_at=NOW),
            RefreshEntry("character", 3, priority=1, next_due_at=NOW),
        ]
    )
    registry.refresh_update(
        RefreshEntry(
            "character", 1, priority=1, next_due_at=NOW, last_refreshed_at=NOW - timedelta(days=2)
        )
    )
    registry.refresh_update(
        RefreshEntry(
            "character", 3, priority=1, next_due_at=NOW, last_refreshed_at=NOW - timedelta(days=9)
        )
    )
    assert [e.entity_id for e in registry.refresh_pop(limit=10, now=NOW)] == [2, 3, 1]
    assert registry.refreshed_since(NOW - timedelta(days=3)) == {("character", 1)}
    assert registry.refreshed_since(NOW - timedelta(days=30)) == {
        ("character", 1),
        ("character", 3),
    }
    assert registry.refreshed_since(NOW) == set()


def test_dataset_summaries(registry: Registry) -> None:
    assert registry.dataset_summaries() == []
    k1, k2 = "2026/killmails-2026-10-01.tar.bz2", "2026/killmails-2026-10-02.tar.bz2"
    registry.upsert_objects(
        [
            discovered(k1, logical_date=date(2026, 10, 1)),
            discovered(k2, logical_date=date(2026, 10, 2)),
        ],
        now=NOW,
    )
    registry.add_revision(new_revision(k1))
    registry.promote("killmails", k1, 1, observed_count=1, verified=True, imported_at=NOW)
    (summary,) = registry.dataset_summaries()
    assert summary.dataset == "killmails"
    assert summary.objects_by_status == {"imported": 1, "new": 1}
    assert summary.oldest_imported == summary.newest_imported == date(2026, 10, 1)


def test_naive_datetimes_are_rejected(registry: Registry) -> None:
    with pytest.raises(ValueError, match="naive"):
        registry.upsert_objects([discovered()], now=datetime(2026, 1, 1))  # noqa: DTZ001
