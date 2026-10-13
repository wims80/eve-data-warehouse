"""Entity discovery from eve-kill (design §7.3): characters ESI cannot enumerate.

Each run first walks eve-kill's character list past the highest id seen, so characters
created since the last run are found without anyone running a command; then it continues
the one-time full walk of the list from where the previous run stopped, at most
``full_pages`` pages a run, until it reaches the end. Characters we do not store, and the
corporations and alliances they name that we do not store, go on the refresh queue as
changes; ESI then supplies everything that is stored. Progress is saved after every page,
so a stopped run resumes where it was.
"""

import json
import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from evedw.domain.registry import RefreshClass, RefreshEntry
from evedw.jobs.refresh import DISCOVERY_STATE
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.sources.evekill import EveKillClient, Page
from evedw.store.base import EntityStore, Registry

log = logging.getLogger(__name__)


_TABLES = {"character": "characters", "corporation": "corporations", "alliance": "alliances"}
_COLUMNS = {"character": "character_id", "corporation": "corporation_id", "alliance": "alliance_id"}


def load_state(registry: Registry) -> dict[str, Any]:
    raw = registry.state_get(DISCOVERY_STATE)
    if raw is None:
        return {}
    try:
        value: Any = json.loads(raw)
    except ValueError:
        return {}
    return dict(value) if isinstance(value, dict) else {}  # pyright: ignore[reportUnknownArgumentType]


@dataclass(slots=True)
class DiscoveryReport:
    pages: int = 0
    listed: int = 0
    queued: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_TABLES, 0))


@dataclass(slots=True)
class DiscoveryJob:
    """``entities:discover``."""

    evekill: EveKillClient
    entities: EntityStore
    full_pages: int | None = None
    """Pages of the full walk this run may read; ``None`` reads to the end, 0 skips it."""
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    def __call__(self, ctx: RunContext) -> JobOutcome:
        registry = ctx.registry
        state = load_state(registry)
        report = DiscoveryReport()
        sent = self.evekill.requests

        # 1. New characters: past the highest id seen, to the end of the list.
        after = state.get("after")
        if not isinstance(after, int):
            highest = self.entities.ids("characters", descending=True, limit=1)
            after = highest[0] if highest else None
        seeding = after is None
        new_found = 0
        # With no character stored there is no "past": the full walk covers the list and
        # leaves its highest id as the starting point.
        while after is not None:
            ctx.check_cancelled()
            page = self.evekill.characters(after=after)
            new_found += self._take(registry, page, report)
            listed = [i for r in page.rows if (i := _id(r, "character_id")) is not None]
            if listed:
                after = max([after or 0, *listed])
            if page.cursor is not None:
                after = max(after or 0, page.cursor)
            state["after"] = after
            self._save(registry, state)
            if not page.has_more or page.cursor is None:
                break
        state["new_found"] = int(state.get("new_found", 0)) + new_found
        log.info("discovery: %d characters past the highest id were new to us", new_found)

        # 2. The full walk, a slice per run, until it has reached the end once.
        full_found = 0
        if state.get("full_done_at") is None and self.full_pages != 0:
            cursor = state.get("full_after")
            cursor = cursor if isinstance(cursor, int) else None
            read = 0
            while self.full_pages is None or read < self.full_pages:
                ctx.check_cancelled()
                page = self.evekill.characters(after=cursor)
                read += 1
                found = self._take(registry, page, report)
                full_found += found
                state["full_pages"] = int(state.get("full_pages", 0)) + 1
                state["full_found"] = int(state.get("full_found", 0)) + found
                last = [i for r in page.rows if (i := _id(r, "character_id")) is not None]
                if seeding and last:
                    state["after"] = max(int(state.get("after") or 0), *last)
                if not page.has_more or page.cursor is None:
                    state["full_done_at"] = self.now().isoformat()
                    state.pop("full_after", None)
                    self._save(registry, state)
                    log.info(
                        "discovery: full walk complete, %d pages, %d characters new to us",
                        state["full_pages"],
                        state["full_found"],
                    )
                    break
                cursor = page.cursor
                state["full_after"] = cursor
                self._save(registry, state)
            log.info(
                "discovery: full walk read %d pages this run, %d characters new to us",
                read,
                full_found,
            )
        state["last_run"] = self.now().isoformat()
        self._save(registry, state)
        log.info(
            "discovery: %d requests, %d listed, queued %d characters, %d corporations, "
            "%d alliances",
            self.evekill.requests - sent,
            report.listed,
            report.queued["character"],
            report.queued["corporation"],
            report.queued["alliance"],
        )
        return JobOutcome(objects_changed=sum(report.queued.values()), rows_written=0)

    def _take(self, registry: Registry, page: Page, report: DiscoveryReport) -> int:
        """Queue what a page names that we do not store; returns the new characters."""
        report.pages += 1
        report.listed += len(page.rows)
        wanted: dict[str, set[int]] = {kind: set() for kind in _TABLES}
        for row in page.rows:
            for kind, column in _COLUMNS.items():
                value = _id(row, column)
                if value is not None:
                    wanted[kind].add(value)
        found = 0
        for kind, ids in wanted.items():
            unknown = self._unknown(kind, ids)
            if kind == "character":
                found = len(unknown)
            self._queue(registry, kind, unknown)
            report.queued[kind] += len(unknown)
        return found

    def _unknown(self, kind: str, ids: Collection[int]) -> set[int]:
        if not ids:
            return set()
        known = self.entities.lookup(_TABLES[kind], ids).column(_COLUMNS[kind]).to_pylist()
        return set(ids) - {int(i) for i in known if i is not None}

    def _queue(self, registry: Registry, kind: str, ids: Collection[int]) -> None:
        if not ids:
            return
        now = self.now()
        registry.refresh_push(
            [
                RefreshEntry(
                    kind=kind, entity_id=i, priority=int(RefreshClass.CHANGE), next_due_at=now
                )
                for i in sorted(ids)
            ]
        )

    def _save(self, registry: Registry, state: dict[str, Any]) -> None:
        registry.state_put(DISCOVERY_STATE, json.dumps(state, sort_keys=True), now=self.now())


def _id(row: dict[str, Any], column: str) -> int | None:
    value = row.get(column)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None
