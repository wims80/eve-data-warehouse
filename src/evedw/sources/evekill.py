"""eve-kill.com's public read-only API (design §2.3) under the policy of design §11b.

Used for discovery only: it lists characters ESI cannot enumerate and the members of a
corporation or alliance. Nothing it says is stored; what it names is refreshed from ESI.
Lists page by ascending id with an ``after`` cursor, up to 1,000 entries a page.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import httpx

from evedw.sources.agent import user_agent
from evedw.sources.everef import parse_http_date
from evedw.sources.gate import RequestGate

log = logging.getLogger(__name__)

PAGE = 1000
"""Entries a list page holds at most."""
RETRY_STATUS = frozenset({500, 502, 503, 504, 520, 521, 522, 523, 524})
RATE_LIMITED_WAIT = 30.0
"""Least wait after a 429 without a usable ``Retry-After``, doubled per attempt."""

PROCESS_GATE = RequestGate()
"""The gate every eve-kill client of the service shares (design §11b)."""


class EveKillError(RuntimeError):
    """A request failed after all attempts, or the answer was not what the API lists."""


class EveKillRefused(EveKillError):
    """eve-kill answered 403: no retry, the job stops (design §11b)."""


@dataclass(frozen=True, slots=True)
class Page:
    rows: list[dict[str, Any]]
    cursor: int | None
    """The ``after`` value of the next page."""
    has_more: bool


class EveKillClient:
    def __init__(
        self,
        base_url: str,
        *,
        contact: str | None = None,
        client: httpx.Client | None = None,
        spacing: float = 1.0,
        attempts: int = 5,
        backoff_seconds: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        gate: RequestGate | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            headers={"User-Agent": user_agent(contact), "Accept-Encoding": "gzip"},
        )
        self._spacing = spacing
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._clock = clock
        self._gate = gate or RequestGate()
        self.requests = 0
        """Requests sent, for the job's log line."""

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    # -- lists -----------------------------------------------------------------------------

    def characters(self, *, after: int | None) -> Page:
        """Characters with an id above ``after``, ascending."""
        return self._page("/characters", after)

    def members(self, kind: str, entity_id: int, *, after: int | None) -> Page:
        """Characters eve-kill lists in a corporation or alliance, ascending by id."""
        if kind not in ("corporation", "alliance"):
            raise ValueError(f"no member list for {kind!r}")
        return self._page(f"/{kind}s/{entity_id}/members", after)

    def all_members(self, kind: str, entity_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        after: int | None = None
        while True:
            page = self.members(kind, entity_id, after=after)
            rows.extend(page.rows)
            if not page.has_more or page.cursor is None or page.cursor == after:
                return rows
            after = page.cursor

    def _page(self, path: str, after: int | None) -> Page:
        params: dict[str, str | int] = {"limit": PAGE}
        if after is not None:
            params["after"] = after
        body = self._get(path, params)
        if body is None:
            return Page(rows=[], cursor=None, has_more=False)
        if not isinstance(body, dict):
            raise EveKillError(f"{path}: expected a JSON object")
        payload = cast(dict[str, Any], body)
        data = payload.get("data")
        pagination = payload.get("pagination")
        if not isinstance(data, list):
            raise EveKillError(f"{path}: no data list")
        rows = [cast(dict[str, Any], r) for r in cast(list[Any], data) if isinstance(r, dict)]
        cursor: Any = None
        has_more = False
        if isinstance(pagination, dict):
            paging = cast(dict[str, Any], pagination)
            cursor = paging.get("cursor")
            has_more = bool(paging.get("hasMore"))
        return Page(
            rows=rows,
            cursor=cursor if isinstance(cursor, int) and not isinstance(cursor, bool) else None,
            has_more=has_more,
        )

    # -- http ------------------------------------------------------------------------------

    def _get(self, path: str, params: dict[str, str | int]) -> Any | None:
        url = f"{self.base_url}{path}"
        last: str = ""
        for attempt in range(self._attempts):
            try:
                with self._gate.turn(self._spacing, self._clock, self._sleep):
                    self.requests += 1
                    response = self._client.get(url, params=params)
            except httpx.TransportError as exc:
                last = f"{type(exc).__name__}: {exc}"
                self._pause(attempt, url, last, None)
                continue
            status = response.status_code
            if status == 200:
                return response.json()
            if status == 404:
                return None
            if status == 403:
                log.warning("eve-kill answered HTTP 403 on %s; stopping", url)
                raise EveKillRefused(f"{url}: HTTP 403")
            if status == 429:
                last = "HTTP 429"
                log.warning("eve-kill answered HTTP 429 on %s", url)
                self._pause(attempt, url, last, _retry_after(response))
                continue
            if status in RETRY_STATUS:
                last = f"HTTP {status}"
                self._pause(attempt, url, last, None)
                continue
            raise EveKillError(f"{url}: HTTP {status}")
        raise EveKillError(f"{url}: giving up after {self._attempts} attempts: {last}")

    def _pause(self, attempt: int, url: str, reason: str, retry_after: float | None) -> None:
        if attempt + 1 >= self._attempts:
            return
        delay = self._backoff * (2**attempt)
        if retry_after is not None:
            # Rate limited: wait at least what the server asks, longer on each attempt.
            delay = max(delay, retry_after * (2**attempt))
        log.warning(
            "retrying %s after %s (attempt %d, sleeping %.1fs)", url, reason, attempt + 1, delay
        )
        self._sleep(delay)


def _retry_after(response: httpx.Response) -> float:
    header = response.headers.get("Retry-After")
    wait: float | None = None
    if header and header.strip().isdigit():
        wait = float(header.strip())
    else:
        when = parse_http_date(header)
        if when is not None:
            wait = max(0.0, (when - datetime.now(UTC)).total_seconds())
    return max(wait or 0.0, RATE_LIMITED_WAIT)
