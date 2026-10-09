"""Application settings. Every key is overridable with an EVEDW_ environment variable."""

import os
from datetime import timedelta
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def project_home() -> Path:
    """Where ``.env`` is read and a relative data dir resolves (design §13), so ``evedw``
    works from any directory: ``EVEDW_HOME``, else the source checkout this package runs
    from (an editable install), else the current directory."""
    explicit = os.environ.get("EVEDW_HOME", "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / "pyproject.toml").is_file() and (checkout / "src" / "evedw").is_dir():
        return checkout
    return Path.cwd()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EVEDW_", env_file=".env", extra="ignore")

    data_dir: Path = Path("data")
    bind: str = "127.0.0.1:8470"
    min_free_gb: float = 20.0
    store_backend: str = "duckdb"

    esi_contact: str | None = None
    esi_daily_budget: int = 800_000
    esi_spacing: float = 0.05
    """Seconds between ESI requests when calm; warning signs slow it to 2 s (design §11)."""
    # Latest date listed by https://esi.evetech.net/meta/compatibility-dates on 2026-10-05.
    # Re-check the endpoint specification when changing it (design §11).
    esi_compatibility_date: str = "2026-08-18"
    esi_refresh_interval: timedelta = Field(default=timedelta(days=30))
    """How long a refreshed entity stays off the queue unless a killmail brings it back."""
    esi_recent_days: int = 7
    """Killmail days scanned for entity IDs when the refresh queue is populated."""

    everef_base_url: str = "https://data.everef.net"
    everef_spacing: float = 0.5
    """Least seconds between EVE Ref request starts (design §6)."""
    esi_base_url: str = "https://esi.evetech.net"

    sync_interval_killmails: timedelta = Field(default=timedelta(hours=6))
    sync_interval_market: timedelta = Field(default=timedelta(hours=6))
    sweep_interval: timedelta = Field(default=timedelta(days=7))
    refresh_interval: timedelta = Field(default=timedelta(minutes=1))
    """Pause between scheduled ``entities:refresh`` slices."""
    refresh_slice: int = 5_000
    """Requests one scheduled refresh slice may send before yielding to other jobs."""
    export_interval: timedelta = Field(default=timedelta(days=1))
    seed_interval: timedelta = Field(default=timedelta(days=1))
    """How often the backfill listing is checked for archives not yet seeded."""
    alliance_sweep_interval: timedelta = Field(default=timedelta(days=1))
    """How often every live alliance's member corporations are listed."""
    affiliation_cycle: timedelta = Field(default=timedelta(days=7))
    """How often every live character's corporation is checked, 1,000 per request."""
    verify_interval: timedelta = Field(default=timedelta(days=7))
    head_days: int = 30
    """Days back from today whose file headers are re-checked on every sync."""

    log_level: str = "INFO"

    @classmethod
    def load(cls, **overrides: object) -> "Settings":
        """Settings from the environment and ``<home>/.env``; a relative data dir from
        either is taken relative to the home. Overrides are used as given."""
        home = project_home()
        settings = cls(_env_file=home / ".env", **overrides)  # type: ignore[call-arg]
        if not settings.data_dir.is_absolute():
            settings = settings.model_copy(update={"data_dir": home / settings.data_dir})
        return settings

    @field_validator("esi_contact", mode="before")
    @classmethod
    def _blank_contact_is_unset(cls, value: object) -> object:
        """``.env.example`` ships ``EVEDW_ESI_CONTACT=``; blank must not enable ESI refresh."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator(
        "sync_interval_killmails",
        "sync_interval_market",
        "sweep_interval",
        "refresh_interval",
        "export_interval",
        "seed_interval",
        "alliance_sweep_interval",
        "affiliation_cycle",
        "verify_interval",
        "esi_refresh_interval",
        mode="before",
    )
    @classmethod
    def _seconds_as_timedelta(cls, value: object) -> object:
        """Accept plain seconds from the environment, e.g. ``EVEDW_SYNC_INTERVAL_MARKET=3600``."""
        if isinstance(value, str) and value.strip().isdigit():
            return int(value)
        return value

    @property
    def service_url(self) -> str:
        """Where ``evedw serve`` listens and where a CLI sends triggers."""
        return f"http://{self.bind}"

    @property
    def bind_host(self) -> str:
        host, _, _ = self.bind.rpartition(":")
        return host or "127.0.0.1"

    @property
    def bind_port(self) -> int:
        _, _, port = self.bind.rpartition(":")
        return int(port) if port.isdigit() else 8470

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

    @property
    def esi_dir(self) -> Path:
        return self.data_dir / "esi"

    @property
    def esi_policy_path(self) -> Path:
        return self.esi_dir / "policy.json"

    @property
    def entities_dir(self) -> Path:
        """Parquet exports of the entity tables, for consumers."""
        return self.lake_dir / "entities"

    def ensure_dirs(self) -> None:
        for directory in (
            self.data_dir,
            self.raw_dir,
            self.lake_dir,
            self.scratch_dir,
            self.esi_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
