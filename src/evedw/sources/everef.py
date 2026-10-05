"""EVE Ref data client: per-year index discovery, totals, verified downloads.

EVE Ref publishes ``<dataset>/<year>/index.json`` listing every file with ``etag``,
``size`` and ``last_modified``. That listing is the change feed; the download verifies the
served file against the listing so a file rewritten between discovery and fetch is
detected rather than imported under stale metadata.
"""

import hashlib
import logging
import os
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from evedw import __version__
from evedw.domain.datasets import Dataset
from evedw.domain.registry import DiscoveredObject

log = logging.getLogger(__name__)

RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class UpstreamChangedError(RuntimeError):
    """The served file does not match what the index described. Re-discover the object."""


class DownloadError(RuntimeError):
    """A download failed after all attempts."""


class _Retryable(Exception):
    pass


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
    ) -> None:
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

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    # -- discovery -----------------------------------------------------------------------

    def year_index(self, dataset: Dataset, year: int) -> list[IndexEntry] | None:
        """Entries listed for a year, or ``None`` when the year has no index (404)."""
        url = dataset.index_url(self.base_url, year)
        response = self._get(url)
        if response is None:
            return None
        payload: Any = response.json()
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
        response = self._get(dataset.totals_url(self.base_url))
        if response is None:
            return {}
        payload: Any = response.json()
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
        for year in years:
            entries = self.year_index(dataset, year)
            if entries is None:
                log.info("no index for year %d", year)
                continue
            for entry in entries:
                day = dataset.logical_date(entry.name)
                if day is None:
                    continue
                expected = totals.get(dataset.totals_key(day)) if totals else None
                found.append(
                    DiscoveredObject(
                        dataset=dataset.name,
                        object_key=dataset.object_key(year, entry.name),
                        url=entry.url,
                        logical_date=day,
                        etag=entry.etag,
                        size=entry.size,
                        last_modified=entry.last_modified,
                        expected_count=expected,
                    )
                )
        return found

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
        with self._client.stream("GET", url) as response:
            if response.status_code in RETRY_STATUS:
                raise _Retryable(f"HTTP {response.status_code}")
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

    def _get(self, url: str) -> httpx.Response | None:
        """GET with retries. Returns ``None`` on 404."""
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                response = self._client.get(url)
                if response.status_code == 404:
                    return None
                if response.status_code in RETRY_STATUS:
                    raise _Retryable(f"HTTP {response.status_code}")
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
        log.warning(
            "retrying %s after %s (attempt %d, sleeping %.1fs)", url, exc, attempt + 1, delay
        )
        self._sleep(delay)
