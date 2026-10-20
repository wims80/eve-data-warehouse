"""Discovery must not trust the index's etag; the file's own headers decide."""

from datetime import date, timedelta

import pytest

from evedw.domain.datasets import MARKET_HISTORY
from evedw.domain.registry import RunStatus, Trigger
from evedw.jobs import sweep as sweeps
from evedw.jobs.importers import importer_for
from evedw.jobs.runner import JobCancelled, JobRunner
from evedw.jobs.sync import SyncJob
from tests.helpers import TODAY, SyncEnv, market_csv_bz2


def file_requests(env: SyncEnv) -> int:
    return env.fake.requests("GET", suffix=".csv.bz2") + env.fake.requests(
        "HEAD", suffix=".csv.bz2"
    )


def test_stale_index_entry_for_recent_day_is_caught_by_head(env: SyncEnv) -> None:
    day = TODAY - timedelta(days=3)
    old = market_csv_bz2(day, rows=100)
    env.fake.put(day, old, expected=100)
    env.sync()
    assert env.obj(day).current_revision == 1

    # The file is rewritten but the index still describes the old bytes.
    new = market_csv_bz2(day, rows=150)
    env.fake.put(day, new, expected=150)
    env.fake.stale_index[day] = old
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    assert env.obj(day).current_revision == 2
    assert env.lake.read("market_history", date_from=day, date_to=day).num_rows == 150


def test_old_day_with_stale_index_needs_a_sweep(env: SyncEnv) -> None:
    day = TODAY - timedelta(days=400)
    old = market_csv_bz2(day, rows=100)
    env.fake.put(day, old, expected=100)
    env.sync()
    requests_after_first = file_requests(env)

    new = market_csv_bz2(day, rows=150)
    env.fake.put(day, new, expected=150)
    env.fake.stale_index[day] = old
    run = env.sync()
    assert run.objects_changed == 0
    assert env.obj(day).current_revision == 1
    assert file_requests(env) == requests_after_first  # no HEAD, no download: the day is old

    run = env.sync(sweep=True)
    assert run.objects_changed == 1
    assert env.obj(day).current_revision == 2

    # An explicit range also refreshes headers regardless of age.
    env.fake.put(day, market_csv_bz2(day, rows=160), expected=160)
    env.fake.stale_index[day] = old
    run = env.sync(date_from=day, date_to=day)
    assert run.objects_changed == 1 and env.obj(day).current_revision == 3


def test_index_change_alone_triggers_head_not_download_when_file_is_unchanged(env: SyncEnv) -> None:
    day = TODAY - timedelta(days=400)
    data = market_csv_bz2(day, rows=100)
    env.fake.put(day, data, expected=100)
    env.sync()
    before = file_requests(env)
    # Index now claims different bytes, but the file served is the same as before.
    env.fake.stale_index[day] = market_csv_bz2(day, rows=90)
    run = env.sync()
    assert run.objects_changed == 0
    assert file_requests(env) == before + 1  # one HEAD, no GET
    assert env.obj(day).current_revision == 1


def test_head_days_window(env: SyncEnv) -> None:
    inside = TODAY - timedelta(days=10)
    outside = TODAY - timedelta(days=200)
    for day in (inside, outside):
        env.fake.put(day, market_csv_bz2(day, rows=50), expected=50)
    env.sync()
    before = dict(env.fake.hits)
    env.sync()
    delta = {
        k: v - before.get(k, 0)
        for k, v in env.fake.hits.items()
        if k.endswith(".csv.bz2") and v != before.get(k, 0)
    }
    assert delta == {
        f"HEAD /market-history/{inside.year}/market-history-{inside.isoformat()}.csv.bz2": 1
    }
    assert MARKET_HISTORY.logical_date(f"market-history-{outside.isoformat()}.csv.bz2") == outside
    assert date(2026, 10, 5) == TODAY

    # A range refreshes only the objects inside it.
    before = dict(env.fake.hits)
    env.sync(date_from=outside, date_to=outside)
    delta = {
        k: v - before.get(k, 0)
        for k, v in env.fake.hits.items()
        if k.endswith(".csv.bz2") and v != before.get(k, 0)
    }
    assert delta == {
        f"HEAD /market-history/{outside.year}/market-history-{outside.isoformat()}.csv.bz2": 1
    }


def test_pending_old_day_gets_its_headers_checked_before_download(env: SyncEnv) -> None:
    """A day registered from the index but never fetched must not be downloaded against
    the index's stale etag."""
    # Same year as the ranged sync below, but outside the header window.
    old_day, recent = TODAY - timedelta(days=200), TODAY - timedelta(days=3)
    old = market_csv_bz2(old_day, rows=100)
    env.fake.put(old_day, old, expected=100)
    env.fake.put(recent, market_csv_bz2(recent, rows=10), expected=10)
    env.sync(date_from=recent, date_to=recent)  # registers old_day as new, from the index
    assert env.obj(old_day).current_revision is None

    env.fake.put(old_day, market_csv_bz2(old_day, rows=150), expected=150)
    env.fake.stale_index[old_day] = old
    run = env.sync()
    assert run.status is RunStatus.SUCCEEDED and run.objects_changed == 1
    assert env.lake.read("market_history", date_from=old_day, date_to=old_day).num_rows == 150


def test_a_stop_during_header_refresh_sends_no_further_heads(env: SyncEnv) -> None:
    for offset in range(10):
        day = TODAY - timedelta(days=offset)
        env.fake.put(day, market_csv_bz2(day, rows=5), expected=5)
    asked: list[int] = []

    def cancel() -> None:
        asked.append(1)
        if len(asked) > 3:
            raise JobCancelled("stop requested")

    found = env.client.discover(MARKET_HISTORY, [TODAY.year])
    with pytest.raises(JobCancelled):
        env.client.refresh_headers(found, workers=1, cancel=cancel)
    assert env.fake.requests("HEAD") == 3


def test_a_sliced_sweep_checks_old_days_oldest_first_and_resumes(
    env: SyncEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("evedw.jobs.sync.SWEEP_CHUNK", 2)
    days = [TODAY - timedelta(days=400 + n) for n in range(5)]  # outside the header window
    for day in days:
        env.fake.put(day, market_csv_bz2(day, rows=100), expected=100)
    env.sync()
    heads = {day: env.fake.hits[head_path(day)] for day in days}

    ticks = [0.0]

    def clock() -> float:
        ticks[0] += 10.0  # each budget check costs ten seconds
        return ticks[0]

    def slice_run() -> None:
        job = SyncJob(
            dataset=MARKET_HISTORY,
            client=env.client,
            importer=importer_for(MARKET_HISTORY, env.lake),
            today=TODAY,
            sweep=True,
            sweep_slice=timedelta(seconds=25),
            clock=clock,
        )
        JobRunner(env.settings, env.registry, env.lock).run(
            "sync:market_history", job, trigger=Trigger.SCHEDULE, params={"sweep": True}
        )

    slice_run()  # two chunks of two fit the budget: the four oldest days
    checked = [day for day in days if env.fake.hits[head_path(day)] > heads[day]]
    assert sorted(checked) == sorted(days)[:4]
    progress = sweeps.load(env.registry, "market_history")
    assert progress.in_progress and progress.finished_at is None

    slice_run()  # the newest day is left; the pass completes
    assert all(env.fake.hits[head_path(day)] == heads[day] + 1 for day in days)
    progress = sweeps.load(env.registry, "market_history")
    assert not progress.in_progress and progress.finished_at is not None


def head_path(day: date) -> str:
    return f"HEAD /market-history/{day.year}/market-history-{day.isoformat()}.csv.bz2"
