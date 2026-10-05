import hashlib
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from evedw.domain.datasets import MARKET_HISTORY
from evedw.sources.everef import (
    DownloadError,
    EveRefClient,
    UpstreamChangedError,
    normalize_etag,
    parse_timestamp,
)
from tests.helpers import BASE_URL, FakeEveRef, market_csv_bz2, md5


@pytest.fixture
def router() -> Iterator[respx.Router]:
    with respx.mock(assert_all_called=False) as mock:
        yield mock


@pytest.fixture
def fake(router: respx.Router) -> FakeEveRef:
    return FakeEveRef(router, MARKET_HISTORY)


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(sleeps: list[float]) -> Iterator[EveRefClient]:
    c = EveRefClient(
        BASE_URL, client=httpx.Client(), attempts=3, backoff_seconds=0.0, sleep=sleeps.append
    )
    yield c
    c.close()


def test_normalizers() -> None:
    assert normalize_etag('"abc"') == "abc"
    assert normalize_etag('W/"abc"') == "abc"
    assert normalize_etag("abc") == "abc"
    assert normalize_etag(None) is None
    assert parse_timestamp("2026-04-05T17:12:20.959Z") == datetime(
        2026, 4, 5, 17, 12, 20, 959000, tzinfo=UTC
    )
    assert parse_timestamp("2026-04-05T17:12:20") is None
    assert parse_timestamp(None) is None


def test_year_index_and_totals(fake: FakeEveRef, client: EveRefClient) -> None:
    day = date(2026, 10, 1)
    data = market_csv_bz2(day)
    fake.put(day, data, expected=300)
    fake.missing_years.add(2003)

    entries = client.year_index(MARKET_HISTORY, 2026)
    assert entries is not None and len(entries) == 1
    entry = entries[0]
    assert entry.name == "market-history-2026-10-01.csv.bz2"
    assert entry.size == len(data) and entry.etag == md5(data)
    assert entry.last_modified == datetime(2026, 10, 1, 12, tzinfo=UTC)
    assert entry.url == fake.url_for(day)

    assert client.year_index(MARKET_HISTORY, 2003) is None
    assert client.totals(MARKET_HISTORY) == {"2026-10-01": 300}


def test_discover_joins_index_and_totals(fake: FakeEveRef, client: EveRefClient) -> None:
    d1, d2 = date(2025, 12, 31), date(2026, 1, 1)
    fake.put(d1, market_csv_bz2(d1), expected=10)
    fake.put(d2, market_csv_bz2(d2))
    found = client.discover(MARKET_HISTORY, [2025, 2026])
    assert [(o.object_key, o.logical_date, o.expected_count) for o in found] == [
        ("2025/market-history-2025-12-31.csv.bz2", d1, 10),
        ("2026/market-history-2026-01-01.csv.bz2", d2, None),
    ]
    assert all(o.dataset == "market_history" and o.etag for o in found)


def test_download_verifies_and_names_by_hash(
    fake: FakeEveRef, client: EveRefClient, tmp_path: Path
) -> None:
    day = date(2026, 10, 1)
    data = market_csv_bz2(day)
    fake.put(day, data)
    result = client.download(
        fake.url_for(day),
        into_dir=tmp_path / "raw",
        suffix=".csv.bz2",
        expected_size=len(data),
        expected_etag=md5(data),
    )
    sha = hashlib.sha256(data).hexdigest()
    assert result.sha256 == sha and result.size == len(data)
    assert result.path == tmp_path / "raw" / f"{sha}.csv.bz2"
    assert result.path.read_bytes() == data
    assert result.etag == md5(data)
    assert result.last_modified == datetime(2026, 10, 1, 12, tzinfo=UTC)
    assert list((tmp_path / "raw").iterdir()) == [result.path]


def test_download_detects_rewritten_upstream(
    fake: FakeEveRef, client: EveRefClient, tmp_path: Path
) -> None:
    day = date(2026, 10, 1)
    data = market_csv_bz2(day)
    fake.put(day, data)
    fake.serve_instead[day] = market_csv_bz2(day, rows=5)
    with pytest.raises(UpstreamChangedError, match="etag"):
        client.download(
            fake.url_for(day),
            into_dir=tmp_path,
            suffix=".csv.bz2",
            expected_size=len(data),
            expected_etag=md5(data),
        )
    with pytest.raises(UpstreamChangedError, match="size"):
        client.download(
            fake.url_for(day),
            into_dir=tmp_path,
            suffix=".csv.bz2",
            expected_size=len(data),
            expected_etag=None,
        )
    assert not any(tmp_path.glob("*.part"))


def test_retries_then_succeeds(
    router: respx.Router, client: EveRefClient, sleeps: list[float]
) -> None:
    url = f"{BASE_URL}/market-history/totals.json"
    router.get(url).mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectError("boom"),
            httpx.Response(200, json={"a": 1}),
        ]
    )
    assert client.totals(MARKET_HISTORY) == {"a": 1}
    assert len(sleeps) == 2


def test_gives_up_after_attempts(
    router: respx.Router, client: EveRefClient, tmp_path: Path
) -> None:
    url = f"{BASE_URL}/market-history/2026/market-history-2026-10-01.csv.bz2"
    router.get(url).mock(return_value=httpx.Response(502))
    with pytest.raises(DownloadError, match="3 attempts"):
        client.download(
            url, into_dir=tmp_path, suffix=".csv.bz2", expected_size=None, expected_etag=None
        )
    assert not any(tmp_path.iterdir())
