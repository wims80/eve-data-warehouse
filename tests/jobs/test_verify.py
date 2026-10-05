from datetime import date

from evedw.domain.datasets import MARKET_HISTORY
from evedw.jobs.verify import verify_dataset
from tests.helpers import D1, D2, D3, SyncEnv, market_csv_bz2, publish_three_days


def test_verify_detects_each_kind_of_drift(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.sync()
    clean = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, totals=env.fake.totals
    )
    assert clean.ok and clean.objects_checked == 3

    # Missing partition.
    env.partition(D1).unlink()
    # Corrupted raw file.
    rev = env.registry.current_revision("market_history", env.obj(D2).object_key)
    assert rev is not None
    (env.settings.data_dir / rev.raw_path).write_bytes(market_csv_bz2(D2, rows=1))
    # Orphan partition.
    orphan = env.settings.lake_dir / "market_history" / "date=2020-01-01"
    orphan.mkdir()
    (orphan / "data.parquet").write_bytes(env.partition(D3).read_bytes())
    # Upstream totals changed.
    env.fake.totals[MARKET_HISTORY.totals_key(D3)] = 101

    report = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, totals=env.fake.totals
    )
    kinds = sorted((i.kind, i.object_key) for i in report.issues)
    assert kinds == [
        ("expected_count", "2026/market-history-2026-10-03.csv.bz2"),
        ("missing_partition", "2026/market-history-2026-10-01.csv.bz2"),
        ("orphan_partition", None),
        ("raw_hash", "2026/market-history-2026-10-02.csv.bz2"),
    ]
    assert "2020-01-01" in report.to_text()
    assert '"ok": false' in report.to_json()

    no_hash = verify_dataset(env.settings, env.registry, env.lake, MARKET_HISTORY, hash_raw=False)
    assert no_hash.raw_files_hashed == 0
    assert "raw_hash" not in {i.kind for i in no_hash.issues}

    ranged = verify_dataset(
        env.settings, env.registry, env.lake, MARKET_HISTORY, date_from=D2, date_to=D2
    )
    assert [i.kind for i in ranged.issues] == ["raw_hash"]
    assert date(2026, 10, 2) == D2
