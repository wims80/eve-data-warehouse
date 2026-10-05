"""Test helpers: fixture data generation and a fake EVE Ref served through respx."""

import bz2
import hashlib
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from evedw.config import Settings
from evedw.domain.datasets import MARKET_HISTORY, Dataset
from evedw.domain.registry import Trigger
from evedw.jobs.importers import importer_for
from evedw.jobs.runner import JobRunner, WriterLock
from evedw.jobs.sync import SyncJob
from evedw.sources.everef import EveRefClient
from evedw.store.base import Lake, Registry

FIXTURES = Path(__file__).parent / "fixtures"
MARKET_FIXTURE = FIXTURES / "market_history" / "market-history-2026-10-01.csv.bz2"
BASE_URL = "https://everef.test"


def market_csv_bz2(day: date, *, rows: int | None = None) -> bytes:
    """The trimmed real market history day, re-dated and optionally truncated."""
    text = bz2.decompress(MARKET_FIXTURE.read_bytes()).decode("utf-8")
    header, *lines = text.splitlines()
    lines = [line.replace(",2026-10-01,", f",{day.isoformat()},") for line in lines if line]
    if rows is not None:
        lines = lines[:rows]
    return bz2.compress(("\n".join([header, *lines]) + "\n").encode("utf-8"))


def market_name(day: date) -> str:
    return f"market-history-{day.isoformat()}.csv.bz2"


def killmail_name(day: date) -> str:
    return f"killmails-{day.isoformat()}.tar.bz2"


KILLMAIL_FIXTURE = FIXTURES / "killmails" / "killmails-2026-10-01.tar.bz2"
"""49 real killmails from 2026-10-01 (containers, moon, war, NPC attackers, no items, no
position, one with over 100 attackers) plus one synthetic member, id 900000001, carrying
negative damage values as ESI history sometimes does."""


def md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


class FakeEveRef:
    """Serves per-year index.json, totals.json and files for one dataset.

    ``put`` publishes or rewrites a day; ``serve_instead`` makes the file endpoint serve
    different bytes from what the index describes, to simulate a mid-run rewrite.
    """

    def __init__(
        self,
        router: respx.Router,
        dataset: Dataset,
        *,
        name_for: Callable[[date], str] = market_name,
        base_url: str = BASE_URL,
    ) -> None:
        if dataset.index_path is None:
            raise ValueError("dataset has no index")
        self.dataset = dataset
        self.base_url = base_url
        self.prefix = dataset.index_path.split("/")[0]
        self.name_for = name_for
        self.files: dict[date, bytes] = {}
        self.modified: dict[date, datetime] = {}
        self.totals: dict[str, int] = {}
        self.serve_instead: dict[date, bytes] = {}
        """Bytes a GET returns instead of the published file; HEAD is unaffected."""
        self.stale_index: dict[date, bytes] = {}
        """Days whose index entry still describes these older bytes."""
        self.hits: Counter[str] = Counter()
        """Request counts keyed by ``"<METHOD> <path>"``."""
        self.missing_years: set[int] = set()
        router.route(method__in=["GET", "HEAD"], url__regex=rf"{base_url}/{self.prefix}/.*").mock(
            side_effect=self._handle
        )

    def put(
        self,
        day: date,
        data: bytes,
        *,
        expected: int | None = None,
        modified: datetime | None = None,
    ) -> None:
        self.files[day] = data
        self.modified[day] = modified or datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
        if expected is not None:
            self.totals[self.dataset.totals_key(day)] = expected

    def requests(self, method: str, *, suffix: str = "") -> int:
        """Number of requests of ``method`` whose path ends with ``suffix``."""
        return sum(
            n
            for key, n in self.hits.items()
            if key.startswith(f"{method} ") and key.endswith(suffix)
        )

    def downloads(self) -> int:
        return self.requests("GET", suffix=self.name_for(date(2000, 1, 1))[-8:])

    def url_for(self, day: date) -> str:
        return f"{self.base_url}/{self.prefix}/{day.year}/{self.name_for(day)}"

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.hits[f"{request.method} {path}"] += 1
        parts = path.strip("/").split("/")
        if parts[-1] == "totals.json":
            return httpx.Response(200, json=self.totals)
        if parts[-1] == "index.json":
            year = int(parts[-2])
            if year in self.missing_years:
                return httpx.Response(404)
            entries = [
                {
                    "name": self.name_for(day),
                    "size": len(data),
                    "etag": md5(data),
                    "last_modified": self.modified[day].isoformat().replace("+00:00", "Z"),
                    "file_time": f"{day.isoformat()}T00:00:00Z",
                    "type": self.prefix,
                    "url": self.url_for(day),
                }
                for day, data in sorted(
                    (d, self.stale_index.get(d, b)) for d, b in self.files.items()
                )
                if day.year == year
            ]
            body = {"files": entries, "path": f"{self.prefix}/{year}"}
            return httpx.Response(200, content=json.dumps(body).encode())
        for day, data in self.files.items():
            if parts[-1] == self.name_for(day):
                # HEAD describes the published file; serve_instead only affects the GET body,
                # modelling a rewrite between header refresh and download.
                body = data if request.method == "HEAD" else self.serve_instead.get(day, data)
                headers = {
                    "ETag": f'"{md5(body)}"',
                    "Content-Length": str(len(body)),
                    "Last-Modified": self.modified[day].strftime("%a, %d %b %Y %H:%M:%S GMT"),
                }
                if request.method == "HEAD":
                    return httpx.Response(200, headers=headers)
                return httpx.Response(200, content=body, headers=headers)
        return httpx.Response(404)


TODAY = date(2026, 10, 5)
D1, D2, D3 = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)


@dataclass
class SyncEnv:
    settings: Settings
    registry: Registry
    lake: Lake
    lock: WriterLock
    client: EveRefClient
    fake: FakeEveRef
    router: respx.Router

    def fake_for(self, dataset: Dataset, name_for: Callable[[date], str]) -> FakeEveRef:
        return FakeEveRef(self.router, dataset, name_for=name_for)

    def sync(self, dataset: Dataset = MARKET_HISTORY, **kwargs: Any) -> Any:
        job = SyncJob(
            dataset=dataset,
            client=self.client,
            importer=importer_for(dataset, self.lake),
            today=TODAY,
            **kwargs,
        )
        runner = JobRunner(self.settings, self.registry, self.lock)
        return runner.run(f"sync:{dataset.name}", job, trigger=Trigger.MANUAL, params=kwargs)

    def obj(self, day: date) -> Any:
        key = f"{day.year}/market-history-{day.isoformat()}.csv.bz2"
        obj = self.registry.get_object("market_history", key)
        assert obj is not None
        return obj

    def partition(self, day: date) -> Path:
        return (
            self.settings.lake_dir / "market_history" / f"date={day.isoformat()}" / "data.parquet"
        )


def publish_three_days(fake: FakeEveRef) -> None:
    fake.put(D1, market_csv_bz2(D1, rows=300), expected=300)
    fake.put(D2, market_csv_bz2(D2, rows=200), expected=200)
    fake.put(D3, market_csv_bz2(D3, rows=100), expected=100)
