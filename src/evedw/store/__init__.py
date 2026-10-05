"""Store backends. ``open_registry`` is the only factory the rest of the code uses."""

from evedw.config import Settings
from evedw.store.base import EntityStore, Lake, PartitionInfo, Queries, Registry

__all__ = ["EntityStore", "Lake", "PartitionInfo", "Queries", "Registry", "open_registry"]


def open_registry(settings: Settings, *, read_only: bool = False) -> Registry:
    if settings.store_backend == "duckdb":
        from evedw.store.duckdb_lake.registry import DuckDBRegistry

        settings.ensure_dirs()
        return DuckDBRegistry(settings.warehouse_path, read_only=read_only)
    raise ValueError(f"unknown store backend {settings.store_backend!r}")
