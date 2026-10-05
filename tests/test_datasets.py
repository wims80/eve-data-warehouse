from datetime import date

import pytest

from evedw.domain.datasets import DATASETS, KILLMAILS, MARKET_HISTORY, get_dataset


def test_killmails_urls_and_dates() -> None:
    assert KILLMAILS.index_url("https://data.everef.net/", 2026) == (
        "https://data.everef.net/killmails/2026/index.json"
    )
    assert KILLMAILS.totals_url("https://data.everef.net") == (
        "https://data.everef.net/killmails/totals.json"
    )
    assert KILLMAILS.logical_date("killmails-2026-10-01.tar.bz2") == date(2026, 10, 1)
    assert KILLMAILS.logical_date("index.json") is None
    assert KILLMAILS.object_key(2026, "killmails-2026-10-01.tar.bz2") == (
        "2026/killmails-2026-10-01.tar.bz2"
    )
    assert KILLMAILS.totals_key(date(2026, 10, 1)) == "20261001"
    assert KILLMAILS.years(date(2026, 10, 5)) == range(2007, 2027)


def test_market_history_totals_key_uses_dashes() -> None:
    assert MARKET_HISTORY.totals_key(date(2026, 10, 1)) == "2026-10-01"
    assert MARKET_HISTORY.logical_date("market-history-2026-10-01.csv.bz2") == date(2026, 10, 1)


def test_entities_backfill_has_no_index() -> None:
    backfill = get_dataset("entities_backfill")
    assert backfill.logical_date("eve-kill-com-karbowiak-2026-05-10.tar.bz2") == date(2026, 5, 10)
    with pytest.raises(ValueError, match="no index"):
        backfill.index_url("https://data.everef.net", 2026)


def test_unknown_dataset() -> None:
    with pytest.raises(KeyError, match="unknown dataset 'nope'"):
        get_dataset("nope")
    assert set(DATASETS) == {"killmails", "market_history", "entities_backfill"}
