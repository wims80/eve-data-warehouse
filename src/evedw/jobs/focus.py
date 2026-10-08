"""Operator focus on chosen entities (design §7.3): ``entities add`` puts them, and on
request the known members of a corporation or alliance, at the front of the refresh
queue. The job only queues; the scheduled refresh slices do the requests, within the
budget and the ESI policy, before anything else on the queue.
"""

import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime

from evedw.domain.registry import RefreshClass, RefreshEntry
from evedw.jobs.refresh import body_ids
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.sources.esi import EsiClient
from evedw.store.base import EntityStore

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
    lists for it now and the ones stored with it, and their known characters."""
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    def __call__(self, ctx: RunContext) -> JobOutcome:
        wanted: dict[str, set[int]] = {kind: set() for kind in KINDS}
        wanted[self.kind] |= set(self.ids)
        if self.members:
            for entity_id in sorted(self.ids):
                if self.kind == "alliance":
                    corporations = self._alliance_corporations(entity_id)
                    wanted["corporation"] |= corporations
                    wanted["character"] |= self._characters("alliance_id", [entity_id])
                    wanted["character"] |= self._characters("corporation_id", corporations)
                elif self.kind == "corporation":
                    wanted["character"] |= self._characters("corporation_id", [entity_id])
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
        return JobOutcome(objects_changed=len(entries), rows_written=0)

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
