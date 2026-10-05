"""Store backends. ``open_registry`` and ``open_lake`` are the only factories the rest of
the code uses."""

from evedw.config import Settings
from evedw.store.base import EntityStore, Lake, PartitionInfo, Queries, Registry

__all__ = [
    "EntityStore",
    "Lake",
    "PartitionInfo",
    "Queries",
    "Registry",
    "open_lake",
    "open_registry",
]


def open_registry(settings: Settings, *, read_only: bool = False) -> Registry:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.registry import DuckDBRegistry

        settings.ensure_dirs()
        return DuckDBRegistry(settings.warehouse_path, read_only=read_only)
    raise ValueError(f"unknown store backend {settings.store_backend!r}")


def open_lake(settings: Settings) -> Lake:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.lake import ParquetLake

        settings.ensure_dirs()
        return ParquetLake(settings.lake_dir)
    raise ValueError(f"unknown store backend {settings.store_backend!r}")
