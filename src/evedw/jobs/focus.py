"""Operator focus on chosen entities (design §7.3): ``entities add`` puts them, and on
request the members of a corporation or alliance, at the front of the refresh queue, and
with ``refresh`` fetches them at once, in the same run, within the ESI policy. Names
resolve through ESI's ``POST /universe/ids``; members come from our own tables, ESI's
alliance corporation list and, when a client is given, eve-kill's member lists.
"""

import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from evedw.domain.registry import RefreshClass, RefreshEntry
from evedw.jobs.refresh import FULL_COST, RefreshJob, body_ids
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.sources.esi import EsiClient
from evedw.sources.esi.client import MAX_NAMES
from evedw.sources.evekill import EveKillClient
from evedw.store.base import EntityStore, Lake

log = logging.getLogger(__name__)

KINDS = ("character", "corporation", "alliance")


@dataclass(slots=True)
class FocusJob:
    esi: EsiClient
    entities: EntityStore
    kind: str
    ids: Collection[int]
    members: bool = False
    """For a corporation: its known characters. For an alliance: the corporations ESI
    lists for it now and the ones stored with it, and their known characters. With an
    eve-kill client, also every character eve-kill lists as a member."""
    names: Collection[str] = ()
    """Names to resolve through ESI, in addition to ``ids``."""
    evekill: EveKillClient | None = None
    lake: Lake | None = None
    refresh: bool = False
    """Fetch the queued entities in this run instead of leaving them to the slices; needs
    ``lake`` for the refresh job."""
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    def __call__(self, ctx: RunContext) -> JobOutcome:
        ids = set(self.ids) | self._resolve(self.names)
        if not ids:
            raise ValueError(f"no {self.kind} to add: nothing resolved")
        wanted: dict[str, set[int]] = {kind: set() for kind in KINDS}
        wanted[self.kind] |= ids
        if self.members:
            for entity_id in sorted(ids):
                if self.kind == "alliance":
                    corporations = self._alliance_corporations(entity_id)
                    wanted["corporation"] |= corporations
                    wanted["character"] |= self._characters("alliance_id", [entity_id])
                    wanted["character"] |= self._characters("corporation_id", corporations)
                elif self.kind == "corporation":
                    wanted["character"] |= self._characters("corporation_id", [entity_id])
                if self.evekill is not None and self.kind in ("corporation", "alliance"):
                    listed = self._listed_members(entity_id)
                    log.info(
                        "eve-kill lists %d members of %s %d", len(listed), self.kind, entity_id
                    )
                    wanted["character"] |= listed
        now = self.now()
        entries = [
            RefreshEntry(kind=kind, entity_id=i, priority=int(RefreshClass.FOCUS), next_due_at=now)
            for kind in KINDS
            for i in sorted(wanted[kind])
        ]
        ctx.registry.refresh_push(entries)
        log.info(
            "focus queued: %d characters, %d corporations, %d alliances",
            len(wanted["character"]),
            len(wanted["corporation"]),
            len(wanted["alliance"]),
        )
        if not self.refresh:
            return JobOutcome(objects_changed=len(entries), rows_written=0)
        if self.lake is None:
            raise ValueError("refreshing at once needs the lake")
        # Focus entries are served first; a budget of exactly their requests stops the
        # drain once they are fetched.
        cost = sum(FULL_COST[e.kind] for e in entries)
        refreshed = RefreshJob(
            self.esi, self.entities, self.lake, budget=cost, populate=False, now=self.now
        )(ctx)
        return JobOutcome(objects_changed=len(entries), rows_written=refreshed.rows_written)

    def _resolve(self, names: Collection[str]) -> set[int]:
        """Exact-match ids of ``names`` in this job's kind; names that match nothing are
        logged and left out."""
        wanted = sorted({n.strip() for n in names if n.strip()})
        found: set[int] = set()
        resolved: set[str] = set()
        key = f"{self.kind}s"
        for start in range(0, len(wanted), MAX_NAMES):
            chunk = wanted[start : start + MAX_NAMES]
            body: Any = self.esi.post_names("/universe/ids", chunk).body
            payload = cast(dict[str, Any], body) if isinstance(body, dict) else {}
            matches = payload.get(key)
            for match in cast(list[Any], matches) if isinstance(matches, list) else []:
                if not isinstance(match, dict):
                    continue
                entry = cast(dict[str, Any], match)
                entity_id = entry.get("id")
                if isinstance(entity_id, int) and not isinstance(entity_id, bool):
                    found.add(entity_id)
                    resolved.add(str(entry.get("name", "")).lower())
        missing = [n for n in wanted if n.lower() not in resolved]
        if missing:
            log.warning("no %s named %s (ESI matches names exactly)", self.kind, ", ".join(missing))
        return found

    def _listed_members(self, entity_id: int) -> set[int]:
        assert self.evekill is not None
        rows = self.evekill.all_members(self.kind, entity_id)
        return {
            int(r["character_id"])
            for r in rows
            if isinstance(r.get("character_id"), int) and r["character_id"] > 0
        }

    def _alliance_corporations(self, alliance_id: int) -> set[int]:
        """ESI's member list now, plus what is stored, so one that just left is
        refreshed too and its new alliance written."""
        listed = body_ids(self.esi.get(f"/alliances/{alliance_id}/corporations", store=False).body)
        stored = self.entities.ids(
            "corporations", live_only=True, where={"alliance_id": alliance_id}
        )
        return set(listed) | set(stored)

    def _characters(self, column: str, values: Collection[int]) -> set[int]:
        found: set[int] = set()
        for value in values:
            found |= set(self.entities.ids("characters", live_only=True, where={column: value}))
        return found
