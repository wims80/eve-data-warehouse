"""zKillboard R2Z2 client (design §2.4) under the zKillboard policy of design §11a.

Discovery reads ``history/totals.json``; a day whose count did not change since the cached
body is flagged ``listing_unchanged`` so the sync takes its file metadata from the registry
instead of a HEAD. A day is downloaded with its compact index of hashes; both are stored
gzip-compressed, named by the sha256 of the day file as served.

A 403 or 429 is never retried: zKillboard work pauses for an hour, and the third refusal
in a UTC day stops it until an operator resumes it. The state is ``zkillboard/policy.json``.
"""

import gzip
import hashlib
import json
import logging
import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from evedw import __version__
from evedw.domain.datasets import Dataset
from evedw.domain.registry import DiscoveredObject
from evedw.sources.everef import (
    RETRY_STATUS,
    Download,
    DownloadError,
    EveRefClient,
    UpstreamChangedError,
    listing_key,
    normalize_etag,
    parse_http_date,
)
from evedw.store.base import ResponseCache

log = logging.getLogger(__name__)

PROJECT_URL = "https://github.com/wims80/eve-data-warehouse"
REFUSED = frozenset({403, 429})
PAUSE_SECONDS = 3600
STOP_AFTER = 3
"""Refusals in one UTC day that stop zKillboard work until an operator resumes it."""


class ZKillboardRefused(RuntimeError):
    """zKillboard answered 403 or 429, or a pause or stop from earlier ones is in force."""


def user_agent(contact: str | None) -> str:
    """ "Send a descriptive `User-Agent` with a maintainer or project URL" (zKillboard API
    documentation)."""
    who = f"; {contact}" if contact else ""
    return f"eve-data-warehouse/{__version__} (+{PROJECT_URL}{who})"


@dataclass(slots=True)
class ZKillboardPolicy:
    blocked_until: float = 0
    refusals: list[float] = field(default_factory=lambda: [])
    stopped: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "blocked_until": self.blocked_until,
            "refusals": list(self.refusals),
            "stopped": self.stopped,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ZKillboardPolicy":
        refusals: list[Any] = list(raw.get("refusals") or [])
        stopped = raw.get("stopped")
        return cls(
            blocked_until=float(raw.get("blocked_until", 0)),
            refusals=[float(r) for r in refusals if isinstance(r, int | float)],  # pyright: ignore[reportUnknownVariableType]
            stopped=str(stopped) if stopped else None,
        )

    def refuse(self, now: float, reason: str) -> None:
        today = datetime.fromtimestamp(now, tz=UTC).date()
        self.refusals = [
            r for r in self.refusals if datetime.fromtimestamp(r, tz=UTC).date() == today
        ]
        self.refusals.append(now)
        self.blocked_until = max(self.blocked_until, now + PAUSE_SECONDS)
        if len(self.refusals) >= STOP_AFTER:
            self.stopped = f"{len(self.refusals)} refusals on {today}, the last {reason}"


def load_policy(path: Path) -> ZKillboardPolicy:
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ZKillboardPolicy()
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return ZKillboardPolicy.from_dict(raw)  # pyright: ignore[reportUnknownArgumentType]


def save_policy(path: Path, policy: ZKillboardPolicy) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(policy.to_dict(), fh, indent=1, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(path)


class ZKillboardClient(EveRefClient):
    source_name = "zKillboard"

    def __init__(
        self,
        base_url: str,
        *,
        policy_path: Path,
        contact: str | None = None,
        client: httpx.Client | None = None,
        cache: ResponseCache | None = None,
        spacing: float = 0.2,
        download_spacing: float = 1.0,
        attempts: int = 5,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__(
            base_url,
            contact=contact,
            client=client
            or httpx.Client(
                timeout=httpx.Timeout(300.0, connect=15.0),
                follow_redirects=True,
                headers={"User-Agent": user_agent(contact), "Accept-Encoding": "gzip"},
            ),
            attempts=attempts,
            backoff_seconds=backoff_seconds,
            sleep=sleep,
            cache=cache,
            spacing=spacing,
            clock=clock,
        )
        if client is None:
            self._owns_client = True
        self._policy_path = policy_path
        self._download_spacing = download_spacing
        self._next_download = 0.0
        self._wall_clock = wall_clock

    # -- policy --------------------------------------------------------------------------

    def _check_policy(self) -> None:
        policy = load_policy(self._policy_path)
        if policy.stopped:
            raise ZKillboardRefused(
                f"zKillboard work is stopped: {policy.stopped}; `evedw zkillboard resume`"
            )
        wait = policy.blocked_until - self._wall_clock()
        if wait > 0:
            raise ZKillboardRefused(f"zKillboard is paused for another {wait:.0f}s")

    def _refused(self, url: str, status: int) -> ZKillboardRefused:
        policy = load_policy(self._policy_path)
        policy.refuse(self._wall_clock(), f"HTTP {status} on {url}")
        save_policy(self._policy_path, policy)
        log.warning(
            "zKillboard answered HTTP %d on %s; pausing zKillboard for an hour", status, url
        )
        if policy.stopped:
            log.warning("zKillboard work stopped: %s", policy.stopped)
        return ZKillboardRefused(f"{url}: HTTP {status}")

    def _pace(self) -> None:
        self._check_policy()
        super()._pace()

    def refresh_headers(
        self,
        objects: Iterable[DiscoveredObject],
        *,
        workers: int = 1,
        cancel: Callable[[], None] = lambda: None,
    ) -> list[DiscoveredObject]:
        """One request in flight (design §11a), whatever the caller asks for."""
        del workers
        return super().refresh_headers(objects, workers=1, cancel=cancel)

    # -- discovery -----------------------------------------------------------------------

    def discover(self, dataset: Dataset, years: Iterable[int]) -> list[DiscoveredObject]:
        if dataset.totals_path is None:
            raise ValueError(f"{dataset.name}: zKillboard discovery needs totals")
        url = dataset.totals_url(self.base_url)
        cached = self._cache.get(listing_key(url)) if self._cache is not None else None
        before = _counts(cached.body) if cached is not None else {}
        fetched = self._listing(url)
        if fetched is None:
            return []
        text, unchanged = fetched
        wanted = set(years)
        found: list[DiscoveredObject] = []
        for key, count in sorted(_counts(text).items()):
            name = f"{key}.json"
            day = dataset.logical_date(name)
            if day is None or day.year not in wanted:
                continue
            found.append(
                DiscoveredObject(
                    dataset=dataset.name,
                    object_key=dataset.object_key(day.year, name),
                    url=f"{self.base_url}/history/raw/{name}",
                    logical_date=day,
                    etag=None,
                    size=None,
                    last_modified=None,
                    expected_count=count,
                    listing_unchanged=unchanged or before.get(key) == count,
                )
            )
        return found

    def totals(self, dataset: Dataset) -> dict[str, int]:
        if dataset.totals_path is None:
            return {}
        fetched = self._listing(dataset.totals_url(self.base_url))
        return {} if fetched is None else _counts(fetched[0])

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
        """The day file, gzip-compressed as ``<sha256>.json.gz``, and its index beside it
        as ``<sha256>.index.json.gz``. ``expected_size`` is not checked: zKillboard sends
        the files compressed, so no uncompressed length is announced."""
        del suffix, expected_size
        into_dir.mkdir(parents=True, exist_ok=True)
        self._wait_for_download()
        part = into_dir / f".download-{os.getpid()}.json.gz.part"
        try:
            sha256, written, etag, last_modified = self._fetch_gzipped(url, part, expected_etag)
            final = into_dir / f"{sha256}.json.gz"
            part.replace(final)
            index_url = url.replace("/history/raw/", "/history/")
            index_part = into_dir / f".download-{os.getpid()}.index.json.gz.part"
            try:
                self._fetch_gzipped(index_url, index_part, None)
                index_part.replace(into_dir / f"{sha256}.index.json.gz")
            finally:
                index_part.unlink(missing_ok=True)
        finally:
            part.unlink(missing_ok=True)
        return Download(
            path=final, sha256=sha256, size=written, etag=etag, last_modified=last_modified
        )

    def _wait_for_download(self) -> None:
        now = self._clock()
        start = max(now, self._next_download)
        self._next_download = start + self._download_spacing
        if start > now:
            self._sleep(start - now)

    def _fetch_gzipped(
        self, url: str, part: Path, expected_etag: str | None
    ) -> tuple[str, int, str | None, datetime | None]:
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            self._count(downloads=1)
            self._pace()
            digest = hashlib.sha256()
            written = 0
            try:
                with self._client.stream("GET", url) as response:
                    if response.status_code in REFUSED:
                        raise self._refused(url, response.status_code)
                    if response.status_code in RETRY_STATUS:
                        raise DownloadError(f"HTTP {response.status_code}")
                    response.raise_for_status()
                    etag = normalize_etag(response.headers.get("ETag"))
                    if expected_etag and etag and etag != expected_etag:
                        raise UpstreamChangedError(
                            f"{url}: served etag {etag} differs from discovered {expected_etag}",
                            served_etag=etag,
                        )
                    with gzip.open(part, "wb", compresslevel=6) as out:
                        for chunk in response.iter_bytes(1 << 20):
                            out.write(chunk)
                            digest.update(chunk)
                            written += len(chunk)
                    last_modified = parse_http_date(response.headers.get("Last-Modified"))
                with part.open("rb") as fh:
                    os.fsync(fh.fileno())
                return digest.hexdigest(), written, etag, last_modified
            except (httpx.TransportError, DownloadError) as exc:
                last_error = exc
                self._pause_retry(attempt, url, exc)
        raise DownloadError(f"{url}: giving up after {self._attempts} attempts: {last_error}")

    def _pause_retry(self, attempt: int, url: str, exc: Exception) -> None:
        if attempt + 1 >= self._attempts:
            return
        delay = self._backoff * (2**attempt)
        log.warning(
            "retrying %s after %s (attempt %d, sleeping %.1fs)", url, exc, attempt + 1, delay
        )
        self._sleep(delay)

    # -- http ----------------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        count: str | None = None,
    ) -> httpx.Response | None:
        """As EVE Ref's, except that a 403 or 429 is never retried (design §11a)."""
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                if count is not None:
                    self._count(**{count: 1})
                self._pace()
                response = self._client.request(method, url, headers=headers)
                if response.status_code in REFUSED:
                    raise self._refused(url, response.status_code)
                if response.status_code == 404:
                    return None
                if response.status_code == 304:
                    return response
                if response.status_code in RETRY_STATUS:
                    raise DownloadError(f"HTTP {response.status_code}")
                response.raise_for_status()
                return response
            except (httpx.TransportError, DownloadError) as exc:
                last_error = exc
                self._pause_retry(attempt, url, exc)
        raise DownloadError(f"{url}: giving up after {self._attempts} attempts: {last_error}")


def _counts(text: str) -> dict[str, int]:
    """``totals.json`` as ``{YYYYMMDD: count}``."""
    payload: Any = json.loads(text)
    if not isinstance(payload, dict):
        return {}
    return {
        str(k): int(v)  # pyright: ignore[reportUnknownArgumentType]
        for k, v in payload.items()  # pyright: ignore[reportUnknownVariableType]
        if isinstance(v, int) and not isinstance(v, bool)
    }
