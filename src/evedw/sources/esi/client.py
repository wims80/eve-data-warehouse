"""ESI HTTP client: conditional requests, response cache, pacing, persistent cooldowns.

Design §11 is binding. Every request goes through ``_request``; nothing else in the
project talks to ESI. The policy state is written to ``policy.json`` after every change
so a restart never forgets a cooldown or a stop. Response bodies and validators live in a
``ResponseCache`` supplied by the store; the client never imports a database driver.
"""

import hashlib
import json
import logging
import os
import random
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from evedw.sources.esi.policy import Policy, header, http_date, number, policy_route
from evedw.sources.everef import user_agent
from evedw.store.base import CachedResponse, ResponseCache

log = logging.getLogger(__name__)

RETRY_STATUS = frozenset({408, 429})
STOP_STATUS = frozenset({403, 420})
MAX_BATCH = 1000
MAX_WAIT_STEP = 30.0


class EsiError(RuntimeError):
    pass


class EsiStoppedError(EsiError):
    """All ESI work is stopped after a 403 or 420 until an operator resumes it."""


class EsiPermanentError(EsiError):
    """A 4xx that will not change on retry. Record it on the entity, do not retry."""

    def __init__(self, route: str, status: int, body: str) -> None:
        super().__init__(f"ESI {route}: HTTP {status}: {body[:200]}")
        self.route = route
        self.status = status
        self.body = body


class EsiTransientError(EsiError):
    """Retries exhausted; the cooldown has been persisted."""


class EsiBudgetError(EsiError):
    """The daily request budget is spent."""


@dataclass(frozen=True, slots=True)
class EsiResponse:
    status: int
    body: Any
    observed_at: datetime
    expires_at: datetime
    etag: str | None
    last_modified: str | None
    from_cache: bool
    """Served from the cache without a request."""
    not_modified: bool
    """A conditional request came back 304; ``body`` is the cached one."""


def expiry(headers: Mapping[str, str], now: int, fallback: int) -> int:
    """When a response stops being fresh, from ``Cache-Control``, ``Age``, ``Date`` and
    ``Expires``, else ``now + fallback``."""
    control = (header(headers, "cache-control") or "").lower()
    directives = [part.strip() for part in control.split(",")]
    if "no-cache" in directives or "no-store" in directives:
        return now
    age = max(0, number(headers, "age") or 0)
    date_header = header(headers, "date")
    served = (http_date(date_header) if date_header else None) or now
    elapsed = max(age, now - served)
    for directive in directives:
        if directive.startswith("max-age="):
            try:
                ttl = int(directive[len("max-age=") :].strip('"'))
            except ValueError:
                continue
            return now + max(0, ttl - elapsed)
    expires_header = header(headers, "expires")
    expires = http_date(expires_header) if expires_header else None
    if expires is not None:
        return now + max(0, expires - served - elapsed)
    return now + fallback


def cache_key(base_url: str, method: str, route: str, body: str | None) -> str:
    material = json.dumps([base_url, method, route, body], separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def load_policy(path: Path) -> Policy:
    """The persisted policy state, or a fresh one when the file does not exist."""
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return Policy()
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return Policy.from_dict(raw)  # pyright: ignore[reportUnknownArgumentType]


class EsiClient:
    def __init__(
        self,
        base_url: str,
        *,
        compatibility_date: str,
        contact: str | None,
        cache: ResponseCache,
        policy_path: Path,
        daily_budget: int,
        client: httpx.Client | None = None,
        attempts: int = 5,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._compatibility_date = compatibility_date
        self._cache = cache
        self._policy_path = policy_path
        self._budget = daily_budget
        self._owns_client = client is None
        self._headers = {"User-Agent": user_agent(contact), "Accept": "application/json"}
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=False
        )
        self._attempts = attempts
        self._sleep = sleep
        self._clock = clock
        self._policy = self._load_policy()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    # -- policy state --------------------------------------------------------------------

    @property
    def policy(self) -> Policy:
        return self._policy

    def budget_remaining(self) -> int:
        return self._policy.budget_remaining(self._budget, self._today())

    def resume(self) -> None:
        """Operator action: clear a 403/420 stop."""
        self._policy.stopped = None
        self._save_policy()

    def _load_policy(self) -> Policy:
        return load_policy(self._policy_path)

    def _save_policy(self) -> None:
        self._policy_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._policy_path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._policy.to_dict(), fh, indent=1, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(self._policy_path)

    def _now(self) -> int:
        return int(self._clock())

    def _today(self) -> date:
        return datetime.fromtimestamp(self._clock(), tz=UTC).date()

    # -- public requests -----------------------------------------------------------------

    def get(self, route: str, *, fallback_seconds: int = 3600) -> EsiResponse:
        if not route.startswith("/") or "?" in route or "#" in route:
            raise ValueError(f"ESI GET needs a canonical path, got {route!r}")
        return self._request("GET", route, body=None, fallback_seconds=fallback_seconds)

    def post_ids(
        self, route: str, ids: Collection[int], *, fallback_seconds: int = 3600
    ) -> EsiResponse:
        """POST a batch of at most 1,000 distinct positive IDs, e.g. ``/universe/names/``."""
        unique = sorted({int(i) for i in ids})
        if not unique or len(unique) > MAX_BATCH or unique[0] <= 0:
            raise ValueError(f"ESI batches take 1 to {MAX_BATCH} distinct positive IDs")
        body = json.dumps(unique, separators=(",", ":"))
        return self._request("POST", route, body=body, fallback_seconds=fallback_seconds)

    # -- the one request path ------------------------------------------------------------

    def _request(
        self, method: str, route: str, *, body: str | None, fallback_seconds: int
    ) -> EsiResponse:
        policy = self._policy
        if policy.stopped:
            raise EsiStoppedError(f"ESI access is stopped: {policy.stopped}; resume to continue")
        key = cache_key(self.base_url, method, route, body)
        cached = self._cache.get(key)
        now = self._now()
        if cached is not None and int(cached.expires_at.timestamp()) > now:
            policy.cache_hits += 1
            self._save_policy()
            return self._from_cache(cached, not_modified=False)
        if policy.budget_remaining(self._budget, self._today()) <= 0:
            raise EsiBudgetError(f"daily ESI budget of {self._budget} requests is spent")

        bucket_route = policy_route(route) if method == "GET" else route
        url = f"{self.base_url}{route}"
        headers = dict(self._headers)
        headers["X-Compatibility-Date"] = self._compatibility_date
        if cached is not None:
            if cached.etag:
                headers["If-None-Match"] = cached.etag
            elif cached.last_modified:
                headers["If-Modified-Since"] = cached.last_modified
        last_error: str = "no attempt made"
        for attempt in range(self._attempts):
            self._wait_until(policy.ready_at(bucket_route, self._now()))
            now = self._now()
            policy.requests += 1
            policy.retries += int(attempt > 0)
            policy.reserve(bucket_route, now)
            policy.charge(self._today())
            self._save_policy()
            try:
                response = self._client.request(
                    method,
                    url,
                    headers=headers,
                    content=body.encode("utf-8") if body is not None else None,
                )
            except httpx.TransportError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self._back_off(attempt, route, last_error)
                continue
            now = self._now()
            status = response.status_code
            response_headers = dict(response.headers)
            policy.observe(bucket_route, status, response_headers, now)
            if status in STOP_STATUS:
                policy.stopped = f"HTTP {status} on {route} at {now}"
                self._save_policy()
                raise EsiStoppedError(
                    f"ESI {route}: HTTP {status}; all ESI work stopped until an operator resumes"
                )
            if status in RETRY_STATUS or status >= 500:
                last_error = f"HTTP {status}"
                self._back_off(attempt, route, last_error)
                continue
            self._save_policy()
            if status == 304:
                if cached is None:
                    raise EsiTransientError(f"ESI {route}: 304 without a cached response")
                merged_headers = dict(response_headers)
                if header(merged_headers, "cache-control") is None and cached.cache_control:
                    merged_headers["cache-control"] = cached.cache_control
                entry = self._entry(
                    key,
                    status=cached.status,
                    body=cached.body,
                    headers=merged_headers,
                    now=now,
                    fallback=0,
                    previous=cached,
                )
                self._store(key, entry, merged_headers)
                return self._from_cache(entry, not_modified=True)
            text = response.text
            if 400 <= status < 500:
                raise EsiPermanentError(route, status, text)
            try:
                json.loads(text)
            except ValueError as exc:
                raise EsiTransientError(f"ESI {route}: invalid JSON body: {exc}") from exc
            entry = self._entry(
                key,
                status=status,
                body=text,
                headers=response_headers,
                now=now,
                fallback=fallback_seconds,
                previous=None,
            )
            self._store(key, entry, response_headers)
            return self._from_cache(entry, not_modified=False, fresh=True)
        raise EsiTransientError(
            f"ESI {route}: giving up after {self._attempts} attempts ({last_error}); "
            "cooldown persisted"
        )

    # -- helpers -------------------------------------------------------------------------

    def _wait_until(self, deadline: int) -> None:
        waited = False
        while True:
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            if not waited:
                self._policy.pauses += 1
                waited = True
                log.info("ESI pacing: waiting %.0fs", remaining)
            self._sleep(min(MAX_WAIT_STEP, max(0.05, remaining)))

    def _back_off(self, attempt: int, route: str, reason: str) -> None:
        delay = (1 << min(attempt, 5)) + random.randint(0, 2)
        self._policy.blocked_until = max(self._policy.blocked_until, self._now() + delay)
        self._save_policy()
        log.warning(
            "ESI %s: %s (attempt %d of %d), pausing %ds",
            route,
            reason,
            attempt + 1,
            self._attempts,
            delay,
        )

    @staticmethod
    def _entry(
        key: str,
        *,
        status: int,
        body: str,
        headers: Mapping[str, str],
        now: int,
        fallback: int,
        previous: CachedResponse | None,
    ) -> CachedResponse:
        etag = header(headers, "etag") or (previous.etag if previous else None)
        last_modified = header(headers, "last-modified") or (
            previous.last_modified if previous else None
        )
        return CachedResponse(
            key=key,
            status=status,
            body=body,
            etag=etag,
            last_modified=last_modified,
            cache_control=header(headers, "cache-control"),
            observed_at=datetime.fromtimestamp(now, tz=UTC),
            expires_at=datetime.fromtimestamp(expiry(headers, now, fallback), tz=UTC),
        )

    def _store(self, key: str, entry: CachedResponse, headers: Mapping[str, str]) -> None:
        control = (header(headers, "cache-control") or "").lower()
        cacheable = "no-store" not in [part.strip() for part in control.split(",")]
        if cacheable:
            self._cache.put(entry)
        else:
            self._cache.delete(key)

    @staticmethod
    def _from_cache(
        entry: CachedResponse, *, not_modified: bool, fresh: bool = False
    ) -> EsiResponse:
        return EsiResponse(
            status=entry.status,
            body=json.loads(entry.body),
            observed_at=entry.observed_at,
            expires_at=entry.expires_at,
            etag=entry.etag,
            last_modified=entry.last_modified,
            from_cache=not fresh and not not_modified,
            not_modified=not_modified,
        )
