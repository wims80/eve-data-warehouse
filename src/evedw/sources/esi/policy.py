"""ESI pacing and cooldown state. Pure logic over header values and a clock; no I/O.

Ported from kat's ``infrastructure/esi/policy.rs``. The state is small enough to be
serialised to JSON after every request so a restart never forgets a cooldown. Times are
Unix seconds, integers where they come from headers; the request spacing is fractional.

The pace adapts (design §11): requests start ``floor`` seconds apart, the spacing doubles
up to ``MAX_SPACING`` when ESI shows a warning sign, and halves back towards the floor
after ``CALM_FOR`` seconds without one.
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

ERROR_WARN = 90
"""Below this many remaining errors in the legacy window, errors are piling up: slow down.
The window allows 100."""

MAX_SPACING = 2.0
"""The slowest the pace adapts to, in seconds between requests."""

SLOW_DOWN_EVERY = 60.0
"""At most one doubling per legacy error window, so one bad minute is one step."""

CALM_FOR = 300.0
"""Seconds without a warning sign before the spacing halves back towards the floor."""


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
    next_request: float = 0
    blocked_until: int = 0
    floor: float = 1.0
    """Configured spacing between requests; not persisted, the client sets it."""
    spacing: float = 1.0
    """Current spacing: ``floor`` when calm, up to ``MAX_SPACING`` after warning signs."""
    last_warning: float = 0
    paced_at: float = 0
    """When ``spacing`` last changed."""
    slowed_at: float = 0
    slowdowns: int = 0
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
            "spacing": self.spacing,
            "last_warning": self.last_warning,
            "paced_at": self.paced_at,
            "slowed_at": self.slowed_at,
            "slowdowns": self.slowdowns,
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
            next_request=float(raw.get("next_request", 0)),
            blocked_until=int(raw.get("blocked_until", 0)),
            spacing=float(raw.get("spacing", 1.0)),
            last_warning=float(raw.get("last_warning", 0)),
            paced_at=float(raw.get("paced_at", 0)),
            slowed_at=float(raw.get("slowed_at", 0)),
            slowdowns=int(raw.get("slowdowns", 0)),
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

    def ready_at(self, route: str, now: float) -> float:
        """Earliest time a request on ``route`` may be sent."""
        ready = max(now, self.next_request, self.blocked_until)
        group = self.routes.get(route)
        bucket = self.buckets.get(group) if group else None
        if bucket is not None and bucket.remaining < RESERVE_PER_REQUEST:
            ready = max(ready, bucket.reset_at)
        return ready

    def reserve(self, route: str, now: float) -> None:
        """Account for a request before sending it."""
        self.next_request = now + self.pace()
        group = self.routes.get(route)
        bucket = self.buckets.get(group) if group else None
        if bucket is None:
            return
        if now >= bucket.reset_at:
            bucket.remaining = bucket.limit
        bucket.remaining = max(0, bucket.remaining - RESERVE_PER_REQUEST)

    def pace(self) -> float:
        """Seconds between requests now; never below the floor."""
        self.spacing = min(MAX_SPACING, max(self.floor, self.spacing))
        return self.spacing

    def warn(self, now: float) -> None:
        """A warning sign: double the spacing, at most once per ``SLOW_DOWN_EVERY``."""
        self.last_warning = now
        if now - self.slowed_at >= SLOW_DOWN_EVERY and self.pace() < MAX_SPACING:
            self.spacing = min(MAX_SPACING, self.spacing * 2)
            self.paced_at = self.slowed_at = now
            self.slowdowns += 1

    def relax(self, now: float) -> None:
        """No warning sign: after ``CALM_FOR`` quiet seconds, halve the spacing."""
        if (
            self.pace() > self.floor
            and now - self.last_warning >= CALM_FOR
            and now - self.paced_at >= CALM_FOR
        ):
            self.spacing = max(self.floor, self.spacing / 2)
            self.paced_at = now

    def observe(self, route: str, status: int, headers: Mapping[str, str], at: float) -> str | None:
        """Update cooldowns and the pace from a response. Every branch only extends
        ``blocked_until``. Returns the warning sign seen, if any."""
        now = int(at)
        warning = self._warning(status, headers)
        if warning is None:
            self.relax(at)
        else:
            self.warn(at)
        self.next_request = at + self.pace()

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
        return warning

    @staticmethod
    def _warning(status: int, headers: Mapping[str, str]) -> str | None:
        """A sign that ESI is struggling or that we are using up an allowance."""
        if status == 429 or status == 420 or status >= 500:
            return f"HTTP {status}"
        remain = number(headers, LEGACY_ERROR_REMAIN)
        if remain is not None and remain < ERROR_WARN:
            return f"{remain} errors left in the window"
        bucket_remaining = number(headers, "x-ratelimit-remaining")
        limit_header = header(headers, "x-ratelimit-limit")
        parsed = parse_limit(limit_header) if limit_header is not None else None
        if bucket_remaining is not None and parsed is not None and bucket_remaining * 2 < parsed[0]:
            return f"rate-limit bucket at {bucket_remaining} of {parsed[0]}"
        return None
