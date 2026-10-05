"""Application settings. Every key is overridable with an EVEDW_ environment variable."""

from datetime import timedelta
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EVEDW_", env_file=".env", extra="ignore")

    data_dir: Path = Path("data")
    bind: str = "127.0.0.1:8470"
    min_free_gb: float = 20.0
    store_backend: str = "duckdb"

    esi_contact: str | None = None
    esi_daily_budget: int = 20_000
    # Verify against https://developers.eveonline.com/api-explorer before milestone M3.
    esi_compatibility_date: str = "2025-08-26"

    everef_base_url: str = "https://data.everef.net"
    esi_base_url: str = "https://esi.evetech.net"

    sync_interval_killmails: timedelta = Field(default=timedelta(hours=6))
    sync_interval_market: timedelta = Field(default=timedelta(hours=6))

    log_level: str = "INFO"

    @field_validator("sync_interval_killmails", "sync_interval_market", mode="before")
    @classmethod
    def _seconds_as_timedelta(cls, value: object) -> object:
        """Accept plain seconds from the environment, e.g. ``EVEDW_SYNC_INTERVAL_MARKET=3600``."""
        if isinstance(value, str) and value.strip().isdigit():
            return int(value)
        return value

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / "writer.lock"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def scratch_dir(self) -> Path:
        return self.data_dir / "scratch"

    def ensure_dirs(self) -> None:
        for directory in (self.data_dir, self.raw_dir, self.lake_dir, self.scratch_dir):
            directory.mkdir(parents=True, exist_ok=True)
