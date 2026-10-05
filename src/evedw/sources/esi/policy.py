"""ESI pacing and cooldown state. Pure logic over header values and a clock; no I/O.

Ported from kat's ``infrastructure/esi/policy.rs``. The state is small enough to be
serialised to JSON after every request so a restart never forgets a cooldown. Times are
Unix seconds (integers) because that is what the headers deal in.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from email.utils import parsedate_to_datetime
from typing import Any

LEGACY_ERROR_REMAIN = "x-esi-error-limit-remain"
LEGACY_ERROR_RESET = "x-esi-error-limit-reset"

LOW_LEGACY_ALLOWANCE = 20
"""Below this many remaining errors in the legacy window we pause until it resets."""

RESERVE_PER_REQUEST = 5
"""Bucket units reserved before a request is sent, so a crash cannot reset the allowance."""

UNKNOWN_LIMIT_PAUSE = 3601
"""Pause when a rate-limit header has a shape we do not understand: fail closed."""


def header(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup over a plain mapping."""
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def number(headers: Mapping[str, str], name: str) -> int | None:
    value = header(headers, name)
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def http_date(value: str) -> int | None:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.timestamp())


def parse_limit(value: str) -> tuple[int, int] | None:
    """``150/15m`` -> (150, 900 seconds)."""
    limit_text, sep, window = value.partition("/")
    if not sep or len(window) < 2:
        return None
    unit = {"s": 1, "m": 60, "h": 3600}.get(window[-1])
    if unit is None:
        return None
    try:
        limit = int(limit_text)
        amount = int(window[:-1])
    except ValueError:
        return None
    seconds = amount * unit
    if limit <= 0 or seconds <= 0:
        return None
    return limit, seconds


def policy_route(route: str) -> str:
    """``/characters/123/`` -> ``/characters/{id}/`` so IDs do not create separate buckets."""
    parts = [("{id}" if part.isdigit() else part) for part in route.split("/")]
    return "/".join(parts)


@dataclass(slots=True)
class Bucket:
    remaining: int
    limit: int
    window: int
    reset_at: int

    def to_dict(self) -> dict[str, int]:
        return {
            "remaining": self.remaining,
            "limit": self.limit,
            "window": self.window,
            "reset_at": self.reset_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Bucket":
        return cls(
            remaining=int(raw.get("remaining", 0)),
            limit=int(raw.get("limit", 0)),
            window=int(raw.get("window", 0)),
            reset_at=int(raw.get("reset_at", 0)),
        )


@dataclass(slots=True)
class Policy:
    stopped: str | None = None
    """Why all ESI work is stopped, or ``None``. Cleared only by an operator."""
    next_request: int = 0
    blocked_until: int = 0
    routes: dict[str, str] = field(default_factory=lambda: {})
    """Policy route -> rate-limit group, learned from ``X-RateLimit-Group``."""
    buckets: dict[str, Bucket] = field(default_factory=lambda: {})
    requests: int = 0
    cache_hits: int = 0
    retries: int = 0
    pauses: int = 0
    budget_day: str | None = None
    budget_used: int = 0

    # -- serialisation -------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "stopped": self.stopped,
            "next_request": self.next_request,
            "blocked_until": self.blocked_until,
            "routes": dict(self.routes),
            "buckets": {k: v.to_dict() for k, v in self.buckets.items()},
            "requests": self.requests,
            "cache_hits": self.cache_hits,
            "retries": self.retries,
            "pauses": self.pauses,
            "budget_day": self.budget_day,
            "budget_used": self.budget_used,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Policy":
        stopped = raw.get("stopped")
        routes: dict[str, str] = {}
        for key, value in dict(raw.get("routes") or {}).items():
            routes[str(key)] = str(value)
        buckets: dict[str, Bucket] = {}
        for key, value in dict(raw.get("buckets") or {}).items():
            if isinstance(value, Mapping):
                buckets[str(key)] = Bucket.from_dict(value)
        budget_day = raw.get("budget_day")
        return cls(
            stopped=str(stopped) if stopped else None,
            next_request=int(raw.get("next_request", 0)),
            blocked_until=int(raw.get("blocked_until", 0)),
            routes=routes,
            buckets=buckets,
            requests=int(raw.get("requests", 0)),
            cache_hits=int(raw.get("cache_hits", 0)),
            retries=int(raw.get("retries", 0)),
            pauses=int(raw.get("pauses", 0)),
            budget_day=str(budget_day) if budget_day else None,
            budget_used=int(raw.get("budget_used", 0)),
        )

    # -- budget --------------------------------------------------------------------------

    def budget_remaining(self, budget: int, today: date) -> int:
        if self.budget_day != today.isoformat():
            return budget
        return max(0, budget - self.budget_used)

    def charge(self, today: date) -> None:
        if self.budget_day != today.isoformat():
            self.budget_day = today.isoformat()
            self.budget_used = 0
        self.budget_used += 1

    # -- pacing --------------------------------------------------------------------------

    def ready_at(self, route: str, now: int) -> int:
        """Earliest time a request on ``route`` may be sent."""
        ready = max(now, self.next_request, self.blocked_until)
        group = self.routes.get(route)
        bucket = self.buckets.get(group) if group else None
        if bucket is not None and bucket.remaining < RESERVE_PER_REQUEST:
            ready = max(ready, bucket.reset_at)
        return ready

    def reserve(self, route: str, now: int) -> None:
        """Account for a request before sending it."""
        self.next_request = now + 1
        group = self.routes.get(route)
        bucket = self.buckets.get(group) if group else None
        if bucket is None:
            return
        if now >= bucket.reset_at:
            bucket.remaining = bucket.limit
        bucket.remaining = max(0, bucket.remaining - RESERVE_PER_REQUEST)

    def observe(self, route: str, status: int, headers: Mapping[str, str], now: int) -> None:
        """Update cooldowns from a response. Every branch only extends ``blocked_until``."""
        self.next_request = now + 1

        retry_after = header(headers, "retry-after")
        if retry_after is not None:
            until: int | None
            try:
                until = now + max(0, int(retry_after.strip()))
            except ValueError:
                until = http_date(retry_after)
            if until is not None:
                self.blocked_until = max(self.blocked_until, until + 1)
            elif status == 429:
                self.blocked_until = max(self.blocked_until, now + 60)
        elif status == 429:
            self.blocked_until = max(self.blocked_until, now + 60)

        remain = number(headers, LEGACY_ERROR_REMAIN)
        if (remain is not None and remain <= LOW_LEGACY_ALLOWANCE) or status == 420:
            reset = number(headers, LEGACY_ERROR_RESET)
            self.blocked_until = max(
                self.blocked_until, now + max(1, reset if reset is not None else 60) + 1
            )
        for name in (LEGACY_ERROR_REMAIN, LEGACY_ERROR_RESET):
            value = header(headers, name)
            if value is not None:
                parsed = number(headers, name)
                if parsed is None or parsed < 0:
                    self.blocked_until = max(self.blocked_until, now + 61)

        group = header(headers, "x-ratelimit-group")
        if group is not None:
            self.routes[route] = group
            limit_header = header(headers, "x-ratelimit-limit")
            parsed_limit = parse_limit(limit_header) if limit_header is not None else None
            if parsed_limit is None:
                self.blocked_until = max(self.blocked_until, now + UNKNOWN_LIMIT_PAUSE)
            else:
                limit, window = parsed_limit
                remaining = max(0, number(headers, "x-ratelimit-remaining") or 0)
                self.buckets[group] = Bucket(
                    remaining=remaining, limit=limit, window=window, reset_at=now + window + 1
                )
                # Waiting a full window near exhaustion is conservative for floating buckets.
                if remaining <= max(5, limit // 5):
                    self.blocked_until = max(self.blocked_until, now + window + 1)
