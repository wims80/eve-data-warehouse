"""Progress of a header sweep that runs in slices (design §6).

A sweep re-checks the headers of every object of a dataset, oldest first. EVE Ref answers
HEADs of old files at about two a minute (measured 2026-10-20), so a full pass takes days;
each run checks objects for at most one slice of time and records where it stopped, and the
scheduler runs the next slice soon after until the pass is complete.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from evedw.store.base import Registry

STATE_PREFIX = "sweep"
"""Registry state key prefix; the key is ``sweep:<dataset>``."""


@dataclass(frozen=True, slots=True)
class SweepProgress:
    cursor: str | None = None
    """The last object key checked in the current pass, or None between passes."""
    started_at: datetime | None = None
    """When the current pass began."""
    finished_at: datetime | None = None
    """When the last complete pass ended."""

    @property
    def in_progress(self) -> bool:
        return self.cursor is not None


def state_key(dataset: str) -> str:
    return f"{STATE_PREFIX}:{dataset}"


def load(registry: Registry, dataset: str) -> SweepProgress:
    raw = registry.state_get(state_key(dataset))
    if raw is None:
        return SweepProgress()
    data = json.loads(raw)

    def when(name: str) -> datetime | None:
        value = data.get(name)
        return datetime.fromisoformat(value) if isinstance(value, str) else None

    cursor = data.get("cursor")
    return SweepProgress(
        cursor=cursor if isinstance(cursor, str) else None,
        started_at=when("started_at"),
        finished_at=when("finished_at"),
    )


def save(registry: Registry, dataset: str, progress: SweepProgress, *, now: datetime) -> None:
    def iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    payload = {
        "cursor": progress.cursor,
        "started_at": iso(progress.started_at),
        "finished_at": iso(progress.finished_at),
    }
    registry.state_put(state_key(dataset), json.dumps(payload, sort_keys=True), now=now)
