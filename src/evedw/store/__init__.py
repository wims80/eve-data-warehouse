"""Store backends. The ``open_*`` functions are the only factories the rest of the code
uses."""

from evedw.config import Settings
from evedw.store.base import (
    CachedResponse,
    EntityStore,
    Lake,
    PartitionInfo,
    Queries,
    QueryInfo,
    QueryParam,
    Registry,
    ResponseCache,
)

__all__ = [
    "CachedResponse",
    "EntityStore",
    "Lake",
    "PartitionInfo",
    "Queries",
    "QueryInfo",
    "QueryParam",
    "Registry",
    "ResponseCache",
    "open_entity_store",
    "open_lake",
    "open_queries",
    "open_registry",
    "open_response_cache",
]


def _unknown(settings: Settings) -> ValueError:
    return ValueError(f"unknown store backend {settings.store_backend!r}")


def open_registry(settings: Settings, *, read_only: bool = False) -> Registry:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.registry import DuckDBRegistry

        settings.ensure_dirs()
        return DuckDBRegistry(settings.warehouse_path, read_only=read_only)
    raise _unknown(settings)


def open_lake(settings: Settings) -> Lake:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.lake import ParquetLake

        settings.ensure_dirs()
        return ParquetLake(settings.lake_dir)
    raise _unknown(settings)


def open_entity_store(settings: Settings) -> EntityStore:
    """Requires a migrated registry: the tables live in the same database."""
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.entities import DuckDBEntityStore

        return DuckDBEntityStore(settings.warehouse_path)
    raise _unknown(settings)


def open_response_cache(settings: Settings) -> ResponseCache:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.cache import DuckDBResponseCache

        return DuckDBResponseCache(settings.warehouse_path)
    raise _unknown(settings)


def open_queries(settings: Settings, lake: Lake) -> Queries:
    """Named read queries over the lake and the live entity tables. Requires a migrated
    registry; the lake must come from ``open_lake`` of the same backend."""
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.lake import ParquetLake
        from evedw.store.duckdb_lake.queries import DuckDBQueries

        if not isinstance(lake, ParquetLake):
            raise TypeError("the DuckDB query backend needs the Parquet lake")
        return DuckDBQueries(settings.warehouse_path, lake)
    raise _unknown(settings)
