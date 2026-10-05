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
from evedw.jobs.market import MarketHistoryImporter
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
        self.hits: Counter[str] = Counter()
        self.missing_years: set[int] = set()
        router.get(url__regex=rf"{base_url}/{self.prefix}/.*").mock(side_effect=self._handle)

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

    def url_for(self, day: date) -> str:
        return f"{self.base_url}/{self.prefix}/{day.year}/{self.name_for(day)}"

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.hits[path] += 1
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
                for day, data in sorted(self.files.items())
                if day.year == year
            ]
            body = {"files": entries, "path": f"{self.prefix}/{year}"}
            return httpx.Response(200, content=json.dumps(body).encode())
        for day, data in self.files.items():
            if parts[-1] == self.name_for(day):
                body = self.serve_instead.get(day, data)
                return httpx.Response(
                    200,
                    content=body,
                    headers={
                        "ETag": f'"{md5(body)}"',
                        "Last-Modified": self.modified[day].strftime("%a, %d %b %Y %H:%M:%S GMT"),
                    },
                )
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

    def sync(self, dataset: Dataset = MARKET_HISTORY, **kwargs: Any) -> Any:
        job = SyncJob(
            dataset=dataset,
            client=self.client,
            importer=MarketHistoryImporter(self.lake),
            today=TODAY,
            **kwargs,
        )
        runner = JobRunner(self.settings, self.registry, self.lock)
        return runner.run("sync:market_history", job, trigger=Trigger.MANUAL, params=kwargs)

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
