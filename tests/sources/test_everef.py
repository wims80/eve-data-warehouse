import hashlib
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from evedw.domain.datasets import MARKET_HISTORY
from evedw.domain.registry import DiscoveredObject
from evedw.sources.everef import (
    DownloadError,
    EveRefClient,
    UpstreamChangedError,
    normalize_etag,
    parse_timestamp,
)
from tests.helpers import BASE_URL, FakeEveRef, market_csv_bz2, md5, publish_three_days


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
        BASE_URL,
        client=httpx.Client(),
        attempts=3,
        backoff_seconds=0.0,
        sleep=sleeps.append,
        spacing=0.0,
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


BACKFILL_HTML = (
    '<html><body><div class="container"><pre><table class="table table-sm">'
    '<thead><th>File</th><th class="text-right">Size</th><th class="text-right">Size (bytes)</th>'
    '<th class="text-right">Last modified</th></thead><tbody>'
    '<tr class="data-file"><td><a class="data-file-url" '
    'href="/characters-corporations-alliances/backfills/eve-kill-com-karbowiak-2025-03-13.tar.bz2">'
    "eve-kill-com-karbowiak-2025-03-13.tar.bz2</a></td>"
    '<td class="data-file-size-formatted text-right">620.47 MiB</td>'
    '<td class="data-file-size-bytes text-right">650,606,182</td>'
    '<td class="data-file-last-modified text-right">'
    '<time datetime="2025-03-13T00:00:00Z">2025-03-13 00:00:00 UTC</time></td></tr>'
    '<tr class="data-file"><td><a class="data-file-url" '
    'href="/characters-corporations-alliances/backfills/eve-kill-com-karbowiak-2026-05-10.tar.bz2">'
    "eve-kill-com-karbowiak-2026-05-10.tar.bz2</a></td>"
    '<td class="data-file-size-formatted text-right">819.18 MiB</td>'
    '<td class="data-file-size-bytes text-right">858,967,779</td>'
    '<td class="data-file-last-modified text-right">'
    '<time datetime="2026-05-10T00:00:00Z">2026-05-10 00:00:00 UTC</time></td></tr>'
    "</tbody></table></pre></div></body></html>"
)


def test_parse_html_listing() -> None:
    from evedw.sources.everef import parse_html_listing

    base = "https://everef.test/characters-corporations-alliances/backfills/"
    entries = parse_html_listing(BACKFILL_HTML, base)
    assert [e.name for e in entries] == [
        "eve-kill-com-karbowiak-2025-03-13.tar.bz2",
        "eve-kill-com-karbowiak-2026-05-10.tar.bz2",
    ]
    assert entries[1].size == 858_967_779 and entries[1].etag is None
    assert entries[1].url == base + "eve-kill-com-karbowiak-2026-05-10.tar.bz2"
    assert entries[1].last_modified == datetime(2026, 5, 10, tzinfo=UTC)


def test_discover_listing_dataset_tries_index_then_html(
    router: respx.Router, client: EveRefClient
) -> None:
    from evedw.domain.datasets import ENTITIES_BACKFILL

    base = f"{BASE_URL}/characters-corporations-alliances/backfills/"
    router.get(base + "index.json").mock(return_value=httpx.Response(404))
    router.get(base).mock(return_value=httpx.Response(200, text=BACKFILL_HTML))
    found = client.discover(ENTITIES_BACKFILL, [])
    assert [(o.object_key, o.logical_date, o.size) for o in found] == [
        ("2025/eve-kill-com-karbowiak-2025-03-13.tar.bz2", date(2025, 3, 13), 650_606_182),
        ("2026/eve-kill-com-karbowiak-2026-05-10.tar.bz2", date(2026, 5, 10), 858_967_779),
    ]
    assert all(o.expected_count is None and o.etag is None for o in found)


def test_rate_limit_waits_as_asked_and_at_least_the_floor(
    router: respx.Router, client: EveRefClient, sleeps: list[float]
) -> None:
    url = f"{BASE_URL}/market-history/2026/x.csv.bz2"
    router.head(url).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "90"}),
            httpx.Response(429),
            httpx.Response(200, headers={"ETag": '"e"'}),
        ]
    )
    assert client._request("HEAD", url) is not None  # pyright: ignore[reportPrivateUsage]
    # Retry-After 90 on the first attempt; no header on the second: the 30 s floor, doubled.
    assert sleeps == [90.0, 60.0]


def test_failed_head_keeps_index_metadata_instead_of_failing(
    router: respx.Router, client: EveRefClient
) -> None:
    def obj(name: str) -> DiscoveredObject:
        return DiscoveredObject(
            dataset="market_history",
            object_key=f"2026/{name}",
            url=f"{BASE_URL}/market-history/2026/{name}",
            logical_date=date(2026, 10, 1),
            etag="index-etag",
            size=10,
            last_modified=None,
            expected_count=None,
        )

    router.head(f"{BASE_URL}/market-history/2026/a").mock(return_value=httpx.Response(429))
    router.head(f"{BASE_URL}/market-history/2026/b").mock(
        return_value=httpx.Response(200, headers={"ETag": '"served"', "Content-Length": "11"})
    )
    a, b = client.refresh_headers([obj("a"), obj("b")])
    assert a.etag == "index-etag" and a.size == 10
    assert b.etag == "served" and b.size == 11


def test_requests_start_at_least_spacing_apart(fake: FakeEveRef) -> None:
    now = [100.0]
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    client = EveRefClient(
        BASE_URL, client=httpx.Client(), sleep=sleep, spacing=0.5, clock=lambda: now[0]
    )
    publish_three_days(fake)
    client.totals(MARKET_HISTORY)
    client.totals(MARKET_HISTORY)
    now[0] += 2.0  # a pause longer than the spacing costs nothing
    client.totals(MARKET_HISTORY)
    client.close()
    assert slept == [0.5]
