"""A fake ESI served through respx, a fake clock, and an in-memory response cache."""

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx
import respx

from evedw.store.base import CachedResponse

ESI_URL = "https://esi.test"


@dataclass(slots=True)
class FakeClock:
    """``time()`` and ``sleep()`` over a counter, so pacing is observable and instant."""

    now: float = 1_800_000_000.0
    slept: list[float] = field(default_factory=lambda: [])

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class MemoryCache:
    def __init__(self) -> None:
        self.entries: dict[str, CachedResponse] = {}

    def get(self, key: str) -> CachedResponse | None:
        return self.entries.get(key)

    def put(self, entry: CachedResponse) -> None:
        self.entries[entry.key] = entry

    def delete(self, key: str) -> None:
        self.entries.pop(key, None)

    def close(self) -> None:
        pass


@dataclass(slots=True)
class Canned:
    status: int
    body: Any
    headers: dict[str, str]


class FakeEsi:
    """Routes map to a queue of canned responses; the last one repeats. A GET carrying an
    ``If-None-Match`` equal to the current response's ETag gets a 304."""

    def __init__(self, router: respx.Router, base_url: str = ESI_URL) -> None:
        self.base_url = base_url
        self.queues: dict[str, list[Canned]] = {}
        self.hits: Counter[str] = Counter()
        self.requests: list[httpx.Request] = []
        router.route(url__regex=rf"{base_url}/.*").mock(side_effect=self._handle)

    def put(
        self,
        route: str,
        body: Any,
        *,
        status: int = 200,
        etag: str | None = None,
        max_age: int | None = 3600,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Set the steady-state response of ``route``."""
        combined: dict[str, str] = {}
        if max_age is not None:
            combined["Cache-Control"] = f"public, max-age={max_age}"
        if etag is not None:
            combined["ETag"] = f'"{etag}"'
        combined.update(headers or {})
        self.queues[route] = [Canned(status, body, combined)]

    def queue(
        self,
        route: str,
        status: int,
        body: Any = None,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Prepend a one-shot response before the steady state."""
        self.queues.setdefault(route, [])
        self.queues[route].insert(
            len(self.queues[route]) - 1 if self.queues[route] else 0,
            Canned(status, body, dict(headers or {})),
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        route = request.url.path
        self.hits[f"{request.method} {route}"] += 1
        self.requests.append(request)
        queue = self.queues.get(route)
        if not queue:
            return httpx.Response(404, json={"error": "no such route in the fake"})
        canned = queue.pop(0) if len(queue) > 1 else queue[0]
        etag = canned.headers.get("ETag")
        if (
            canned.status == 200
            and etag is not None
            and request.headers.get("If-None-Match") == etag
        ):
            return httpx.Response(304, headers=canned.headers)
        if canned.body is None:
            return httpx.Response(canned.status, headers=canned.headers)
        content = canned.body if isinstance(canned.body, str) else json.dumps(canned.body)
        return httpx.Response(canned.status, content=content, headers=canned.headers)
