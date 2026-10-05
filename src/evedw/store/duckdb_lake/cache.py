"""ESI response cache in warehouse.duckdb, table ``esi_cache``."""

import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from evedw.store.base import CachedResponse

_COLUMNS = "key, status, body, etag, last_modified, cache_control, observed_at, expires_at"


def _naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("naive datetime reached the store; use UTC-aware datetimes")
    return value.astimezone(UTC).replace(tzinfo=None)


def _aware(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime from store, got {type(value).__name__}")
    return value.replace(tzinfo=UTC)


class DuckDBResponseCache:
    def __init__(self, path: Path) -> None:
        self._con = duckdb.connect(str(path))
        self._lock = threading.RLock()

    def close(self) -> None:
        self._con.close()

    def get(self, key: str) -> CachedResponse | None:
        with self._lock:
            row = self._con.execute(
                f"SELECT {_COLUMNS} FROM esi_cache WHERE key = ?", [key]
            ).fetchone()
        if row is None:
            return None
        return CachedResponse(
            key=row[0],
            status=int(row[1]),
            body=row[2],
            etag=row[3],
            last_modified=row[4],
            cache_control=row[5],
            observed_at=_aware(row[6]),
            expires_at=_aware(row[7]),
        )

    def put(self, entry: CachedResponse) -> None:
        with self._lock:
            self._con.execute(
                f"INSERT OR REPLACE INTO esi_cache ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    entry.key,
                    entry.status,
                    entry.body,
                    entry.etag,
                    entry.last_modified,
                    entry.cache_control,
                    _naive(entry.observed_at),
                    _naive(entry.expires_at),
                ],
            )

    def delete(self, key: str) -> None:
        with self._lock:
            self._con.execute("DELETE FROM esi_cache WHERE key = ?", [key])
