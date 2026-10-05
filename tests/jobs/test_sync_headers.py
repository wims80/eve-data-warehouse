"""Discovery must not trust the index's etag; the file's own headers decide."""

from datetime import date, timedelta

from evedw.domain.datasets import MARKET_HISTORY
from evedw.domain.registry import RunStatus
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
