"""EVE Ref data client: per-year index discovery, totals, verified downloads.

EVE Ref publishes ``<dataset>/<year>/index.json`` listing every file with ``etag``,
``size`` and ``last_modified``. That listing is the change feed; the download verifies the
served file against the listing so a file rewritten between discovery and fetch is
detected rather than imported under stale metadata.

Listings (year indexes, ``totals.json``, directory pages) are fetched with conditional
GETs when a ``ResponseCache`` is given (design §6): the last body and its validators are
kept, and a ``304`` reuses the body, so an unchanged year costs a request but no download.
"""

import hashlib
import json
import logging
import os
import random
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx

from evedw import __version__
from evedw.domain.datasets import Dataset
from evedw.domain.registry import DiscoveredObject
from evedw.store.base import CachedResponse, ResponseCache

log = logging.getLogger(__name__)

RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})
RATE_LIMITED_WAIT = 30.0
"""Least wait after a 429 without a usable ``Retry-After``, doubled per attempt."""
CONCURRENCY = 2
"""Requests in flight at once. EVE Ref's download guide uses ``rclone --checkers 2``."""
SPACING = 0.5
"""Least seconds between request starts, across threads. At full speed a sync's ~55 requests
drew 429s from EVE Ref's Cloudflare front around the 55th (2026-10-09)."""


class UpstreamChangedError(RuntimeError):
    """The served file does not match what the index described. Re-discover the object."""


class DownloadError(RuntimeError):
    """A download failed after all attempts."""


class _Retryable(Exception):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _retryable(response: httpx.Response) -> _Retryable:
    retry_after: float | None = None
    if response.status_code == 429:
        header = response.headers.get("Retry-After")
        if header and header.strip().isdigit():
            retry_after = float(header.strip())
        else:
            when = parse_http_date(header)
            if when is not None:
                retry_after = max(0.0, (when - datetime.now(UTC)).total_seconds())
        retry_after = max(retry_after or 0.0, RATE_LIMITED_WAIT)
    return _Retryable(f"HTTP {response.status_code}", retry_after=retry_after)


@dataclass(frozen=True, slots=True)
class Traffic:
    """Requests a client has sent, for the sync log line."""

    listings: int = 0
    """Listing GETs: year indexes, totals, directory pages."""
    not_modified: int = 0
    """Listing GETs answered 304, whose cached body was reused."""
    heads: int = 0
    downloads: int = 0
    """File GETs, retries included."""

    def since(self, earlier: "Traffic") -> "Traffic":
        return Traffic(
            self.listings - earlier.listings,
            self.not_modified - earlier.not_modified,
            self.heads - earlier.heads,
            self.downloads - earlier.downloads,
        )

    def describe(self) -> str:
        return (
            f"{self.listings} listing requests ({self.not_modified} answered 304), "
            f"{self.heads} HEADs, {self.downloads} downloads"
        )


def listing_key(url: str) -> str:
    """Response cache key of a listing; distinct from every ESI key."""
    return hashlib.sha256(json.dumps(["everef", "GET", url]).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class IndexEntry:
    name: str
    url: str
    size: int | None
    etag: str | None
    last_modified: datetime | None
    file_time: datetime | None


@dataclass(frozen=True, slots=True)
class Download:
    path: Path
    sha256: str
    size: int
    etag: str | None
    last_modified: datetime | None


def normalize_etag(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if value.startswith("W/"):
        value = value[2:]
    return value.strip('"') or None


def parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def parse_http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None


def user_agent(contact: str | None) -> str:
    who = contact or "no contact configured"
    return f"evedw/{__version__} (local EVE data warehouse; {who})"


class _ListingParser(HTMLParser):
    """Rows of EVE Ref's directory page: ``<tr class="data-file">`` with the file link,
    a ``data-file-size-bytes`` cell and a ``<time datetime="...">``."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[dict[str, str]] = []
        self._row: dict[str, str] | None = None
        self._capture: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {k: v or "" for k, v in attrs}
        classes = attributes.get("class", "").split()
        if tag == "tr" and "data-file" in classes:
            self._row = {}
        elif self._row is None:
            return
        elif tag == "a" and "data-file-url" in classes:
            self._row["href"] = attributes.get("href", "")
            self._capture = "name"
        elif tag == "td" and "data-file-size-bytes" in classes:
            self._capture = "size"
        elif tag == "time" and "datetime" in attributes:
            self._row["last_modified"] = attributes["datetime"]

    def handle_data(self, data: str) -> None:
        if self._row is not None and self._capture is not None:
            self._row[self._capture] = self._row.get(self._capture, "") + data

    def handle_endtag(self, tag: str) -> None:
        if tag in ("a", "td"):
            self._capture = None
        if tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


def parse_html_listing(html: str, base_url: str) -> list[IndexEntry]:
    parser = _ListingParser()
    parser.feed(html)
    entries: list[IndexEntry] = []
    for row in parser.rows:
        href = row.get("href", "")
        name = (row.get("name") or href).strip().rsplit("/", 1)[-1]
        if not name:
            continue
        digits = row.get("size", "").replace(",", "").strip()
        entries.append(
            IndexEntry(
                name=name,
                url=urljoin(base_url, href) if href else f"{base_url}{name}",
                size=int(digits) if digits.isdigit() else None,
                etag=None,
                last_modified=parse_timestamp(row.get("last_modified")),
                file_time=None,
            )
        )
    return entries


class EveRefClient:
    def __init__(
        self,
        base_url: str,
        *,
        contact: str | None = None,
        client: httpx.Client | None = None,
        attempts: int = 5,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        cache: ResponseCache | None = None,
        spacing: float = SPACING,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """``cache`` keeps listing bodies for conditional GETs; the client closes it."""
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(120.0, connect=15.0),
            follow_redirects=True,
            headers={"User-Agent": user_agent(contact)},
        )
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._cache = cache
        self._traffic = Traffic()
        self._traffic_lock = threading.Lock()
        self._spacing = spacing
        self._clock = clock
        self._next_start = 0.0
        self._pace_lock = threading.Lock()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
        if self._cache is not None:
            self._cache.close()

    @property
    def traffic(self) -> Traffic:
        with self._traffic_lock:
            return self._traffic

    def _pace(self) -> None:
        """Wait for this request's start slot: one every ``spacing`` seconds, whichever
        thread asks. The slot is taken under the lock and waited for outside it."""
        with self._pace_lock:
            now = self._clock()
            start = max(now, self._next_start)
            self._next_start = start + self._spacing
        if start > now:
            self._sleep(start - now)

    def _count(self, **fields: int) -> None:
        with self._traffic_lock:
            self._traffic = replace(
                self._traffic,
                **{name: getattr(self._traffic, name) + n for name, n in fields.items()},
            )

    # -- discovery -----------------------------------------------------------------------

    def year_index(self, dataset: Dataset, year: int) -> list[IndexEntry] | None:
        """Entries listed for a year, or ``None`` when the year has no index (404)."""
        found = self._year_index(dataset, year)
        return None if found is None else found[0]

    def _year_index(self, dataset: Dataset, year: int) -> tuple[list[IndexEntry], bool] | None:
        url = dataset.index_url(self.base_url, year)
        fetched = self._listing(url)
        if fetched is None:
            return None
        text, unchanged = fetched
        return self._index_entries(json.loads(text), url), unchanged

    def listing(self, dataset: Dataset) -> list[IndexEntry]:
        """Entries of a directory that has no per-year index. ``index.json`` is tried
        first; EVE Ref's HTML directory listing is the fallback."""
        return self._directory(dataset)[0]

    def _directory(self, dataset: Dataset) -> tuple[list[IndexEntry], bool]:
        base = dataset.listing_url(self.base_url)
        fetched = self._listing(base + "index.json")
        if fetched is not None:
            return self._index_entries(json.loads(fetched[0]), base + "index.json"), fetched[1]
        fetched = self._listing(base)
        if fetched is None:
            return [], False
        return parse_html_listing(fetched[0], base), fetched[1]

    @staticmethod
    def _index_entries(payload: Any, url: str) -> list[IndexEntry]:
        # EVE Ref serves {"files": [...], "path": "..."}; accept a bare array as well.
        if isinstance(payload, dict):
            payload = payload.get("files")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not isinstance(payload, list):
            raise ValueError(f"{url}: expected an object with a files array")
        entries: list[IndexEntry] = []
        for item in payload:  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(item, dict):
                continue
            raw: dict[str, Any] = item  # pyright: ignore[reportUnknownVariableType]
            name = raw.get("name")
            if not isinstance(name, str):
                continue
            size = raw.get("size")
            entries.append(
                IndexEntry(
                    name=name,
                    url=str(raw.get("url") or f"{url.rsplit('/', 1)[0]}/{name}"),
                    size=int(size) if isinstance(size, int | float | str) and size != "" else None,
                    etag=normalize_etag(raw.get("etag")),
                    last_modified=parse_timestamp(raw.get("last_modified")),
                    file_time=parse_timestamp(raw.get("file_time")),
                )
            )
        return entries

    def totals(self, dataset: Dataset) -> dict[str, int]:
        """Expected record counts keyed as the dataset's totals.json keys them."""
        if dataset.totals_path is None:
            return {}
        fetched = self._listing(dataset.totals_url(self.base_url))
        if fetched is None:
            return {}
        payload: Any = json.loads(fetched[0])
        if not isinstance(payload, dict):
            return {}
        result: dict[str, int] = {}
        for key, value in payload.items():  # pyright: ignore[reportUnknownVariableType]
            if isinstance(key, str) and isinstance(value, int):
                result[key] = value
        return result

    def discover(self, dataset: Dataset, years: Iterable[int]) -> list[DiscoveredObject]:
        totals = self.totals(dataset)
        found: list[DiscoveredObject] = []
        listed: list[tuple[int | None, list[IndexEntry], bool]]
        if dataset.index_path is None:
            listed = [(None, *self._directory(dataset))]
        else:
            listed = []
            for year in years:
                indexed = self._year_index(dataset, year)
                if indexed is None:
                    log.info("no index for year %d", year)
                    continue
                listed.append((year, *indexed))
        for year, entries, unchanged in listed:
            for entry in entries:
                day = dataset.logical_date(entry.name)
                if day is None:
                    continue
                expected = totals.get(dataset.totals_key(day)) if totals else None
                found.append(
                    DiscoveredObject(
                        dataset=dataset.name,
                        object_key=dataset.object_key(
                            year if year is not None else day.year, entry.name
                        ),
                        url=entry.url,
                        logical_date=day,
                        etag=entry.etag,
                        size=entry.size,
                        last_modified=entry.last_modified,
                        expected_count=expected,
                        listing_unchanged=unchanged,
                    )
                )
        return found

    def refresh_headers(
        self, objects: Iterable[DiscoveredObject], *, workers: int = CONCURRENCY
    ) -> list[DiscoveredObject]:
        """Replace etag, size and last_modified with what a HEAD of each file reports now.

        EVE Ref regenerates a year's index.json on its own schedule, which lags the files
        it lists by hours for the current year and by months for past years. The file's
        own headers are the truth; the index is only the listing.
        """
        items = list(objects)
        if not items:
            return []

        def refresh(obj: DiscoveredObject) -> DiscoveredObject:
            try:
                response = self._request("HEAD", obj.url, count="heads")
            except DownloadError as exc:
                # One unreachable file must not fail the whole discovery; the download
                # checks the served etag anyway, and the next sync checks it again.
                log.warning("HEAD failed, keeping index metadata: %s", exc)
                return obj
            if response is None:
                log.warning("HEAD %s returned 404; keeping index metadata", obj.url)
                return obj
            length = response.headers.get("Content-Length")
            size = int(length) if length and length.isdigit() else obj.size
            return replace(
                obj,
                etag=normalize_etag(response.headers.get("ETag")) or obj.etag,
                size=size,
                last_modified=parse_http_date(response.headers.get("Last-Modified"))
                or obj.last_modified,
            )

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            return list(pool.map(refresh, items))

    # -- download ------------------------------------------------------------------------

    def download(
        self,
        url: str,
        *,
        into_dir: Path,
        suffix: str,
        expected_size: int | None,
        expected_etag: str | None,
    ) -> Download:
        """Stream ``url`` into ``into_dir/<sha256><suffix>``.

        Raises ``UpstreamChangedError`` when the served Content-Length, ETag or byte count
        disagrees with what discovery recorded.
        """
        into_dir.mkdir(parents=True, exist_ok=True)
        part = into_dir / f".download-{os.getpid()}{suffix}.part"
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                return self._download_once(
                    url,
                    part=part,
                    into_dir=into_dir,
                    suffix=suffix,
                    expected_size=expected_size,
                    expected_etag=expected_etag,
                )
            except (httpx.TransportError, _Retryable) as exc:
                last_error = exc
                self._pause(attempt, url, exc)
            finally:
                part.unlink(missing_ok=True)
        raise DownloadError(f"{url}: giving up after {self._attempts} attempts: {last_error}")

    def _download_once(
        self,
        url: str,
        *,
        part: Path,
        into_dir: Path,
        suffix: str,
        expected_size: int | None,
        expected_etag: str | None,
    ) -> Download:
        digest = hashlib.sha256()
        written = 0
        self._count(downloads=1)
        self._pace()
        with self._client.stream("GET", url) as response:
            if response.status_code in RETRY_STATUS:
                raise _retryable(response)
            response.raise_for_status()
            served_etag = normalize_etag(response.headers.get("ETag"))
            if expected_etag and served_etag and served_etag != expected_etag:
                raise UpstreamChangedError(
                    f"{url}: served etag {served_etag} differs from index etag {expected_etag}"
                )
            content_length = response.headers.get("Content-Length")
            declared = int(content_length) if content_length and content_length.isdigit() else None
            if expected_size is not None and declared is not None and declared != expected_size:
                raise UpstreamChangedError(
                    f"{url}: served size {declared} differs from index size {expected_size}"
                )
            with part.open("wb") as fh:
                for chunk in response.iter_bytes(1 << 20):
                    fh.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
                fh.flush()
                os.fsync(fh.fileno())
            last_modified = parse_http_date(response.headers.get("Last-Modified"))
        if declared is not None and written != declared:
            raise _Retryable(f"short read: {written} of {declared} bytes")
        if expected_size is not None and written != expected_size:
            raise UpstreamChangedError(
                f"{url}: downloaded {written} bytes, index said {expected_size}"
            )
        sha256 = digest.hexdigest()
        final = into_dir / f"{sha256}{suffix}"
        part.replace(final)
        return Download(
            path=final, sha256=sha256, size=written, etag=served_etag, last_modified=last_modified
        )

    # -- http ----------------------------------------------------------------------------

    def _listing(self, url: str) -> tuple[str, bool] | None:
        """A listing body and whether it was answered 304, or ``None`` on 404. With a cache
        the GET is conditional and a ``304`` returns the cached body."""
        key = listing_key(url)
        cached = self._cache.get(key) if self._cache is not None else None
        headers: dict[str, str] = {}
        if cached is not None:
            if cached.etag:
                # Cloudflare weakens the ETag of a body it compresses on the fly and then
                # ignores the weak form in If-None-Match (checked 2026-10-09); the tag is
                # the file's content hash, so its strong form is the right validator.
                headers["If-None-Match"] = cached.etag.removeprefix("W/")
            if cached.last_modified:
                headers["If-Modified-Since"] = cached.last_modified
        response = self._request("GET", url, headers=headers, count="listings")
        if response is None:
            if self._cache is not None and cached is not None:
                self._cache.delete(key)
            return None
        if response.status_code == 304:
            if cached is None:
                raise DownloadError(f"{url}: 304 without a cached body")
            self._count(not_modified=1)
            return cached.body, True
        text = response.text
        if self._cache is not None:
            now = datetime.now(UTC)
            self._cache.put(
                CachedResponse(
                    key=key,
                    status=response.status_code,
                    body=text,
                    etag=response.headers.get("ETag"),
                    last_modified=response.headers.get("Last-Modified"),
                    cache_control=response.headers.get("Cache-Control"),
                    observed_at=now,
                    expires_at=now,  # always revalidated
                )
            )
        return text, False

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        count: str | None = None,
    ) -> httpx.Response | None:
        """With retries. Returns ``None`` on 404; a ``304`` is returned as is."""
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                if count is not None:
                    self._count(**{count: 1})
                self._pace()
                response = self._client.request(method, url, headers=headers)
                if response.status_code == 404:
                    return None
                if response.status_code == 304:
                    return response
                if response.status_code in RETRY_STATUS:
                    raise _retryable(response)
                response.raise_for_status()
                return response
            except (httpx.TransportError, _Retryable) as exc:
                last_error = exc
                self._pause(attempt, url, exc)
        raise DownloadError(f"{url}: giving up after {self._attempts} attempts: {last_error}")

    def _pause(self, attempt: int, url: str, exc: Exception) -> None:
        if attempt + 1 >= self._attempts:
            return
        delay = self._backoff * (2**attempt) + random.uniform(0, self._backoff)
        if isinstance(exc, _Retryable) and exc.retry_after is not None:
            # Rate limited: wait at least what the server asks, longer on each attempt.
            delay = max(delay, exc.retry_after * (2**attempt))
        log.warning(
            "retrying %s after %s (attempt %d, sleeping %.1fs)", url, exc, attempt + 1, delay
        )
        self._sleep(delay)
