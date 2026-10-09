"""Entity refresh from ESI (design §7.3).

Change is detected in bulk: an alliance sweep maps every corporation that is in an
alliance, and an affiliation sweep checks the corporation of every live character, 1,000
per request. Only what changed is fetched in full. Whatever budget is left goes to a crawl
that fetches the history nobody has fetched yet. One run is a slice; progress is kept in
the registry's sweep state, so a restart resumes where a slice stopped.
"""

import json
import logging
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pyarrow as pa

from evedw.domain.registry import RefreshClass, RefreshEntry
from evedw.domain.schemas import (
    ALLIANCES,
    CHARACTER_EMPLOYMENT,
    CHARACTERS,
    CORPORATION_ALLIANCE_HISTORY,
    CORPORATIONS,
    ENTITY_TABLES,
)
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.logs import log_context
from evedw.sources.esi import (
    EsiBudgetError,
    EsiClient,
    EsiDowntimeError,
    EsiPermanentError,
    EsiTransientError,
)
from evedw.store.base import EntityStore, Lake, Registry

log = logging.getLogger(__name__)

DOOMHEIM = 1000001
"""The NPC corporation deleted characters are moved to."""

KINDS = ("character", "corporation", "alliance")
TABLE = {"character": "characters", "corporation": "corporations", "alliance": "alliances"}

KILLMAIL_ID_COLUMNS: dict[str, dict[str, str]] = {
    "killmails": {
        "victim_character_id": "character",
        "victim_corporation_id": "corporation",
        "victim_alliance_id": "alliance",
    },
    "attackers": {
        "character_id": "character",
        "corporation_id": "corporation",
        "alliance_id": "alliance",
    },
}

ALLIANCE_STATE = "refresh.alliance_sweep"
RECENT_STATE = "refresh.affiliation_recent"
CYCLE_STATE = "refresh.affiliation_cycle"
CRAWL_STATE = "refresh.crawl"

AFFILIATION_BATCH = 1000
SPLIT_WAYS = 10
"""A rejected affiliation batch is retried as tenths: about four errors isolate one invalid
id in a batch of 1,000, where halving would cost about ten."""
SLICE_ERROR_CAP = 100
"""ESI error responses a slice may cause before it stops early. ESI's error limit is 100
per minute; repeated failing requests are what its best practices warn against."""
FIRST_KILLMAIL_DAY = date(2007, 12, 1)
PERMANENT_RETRY = timedelta(days=365)
TRANSIENT_RETRY = timedelta(hours=1)
SETTLED_FOR = timedelta(days=3650)
"""A refreshed entry is idle until a sweep or a killmail queues it again."""
NOT_FOUND = frozenset({404, 410})
FULL_COST: dict[str, int] = {"character": 2, "corporation": 2, "alliance": 1}
HISTORY_ONLY = frozenset({RefreshClass.CRAWL, RefreshClass.DEFERRED})
"""Classes that fetch history alone for an entity we already have."""
"""Requests a full refresh sends: the entity plus its history, if it has one."""


class _OutOfBudget(Exception):
    """The slice's allowance is spent, or it caused too many ESI errors; unwind to the end
    of the run and leave the rest for the next slice."""


# --- small helpers ------------------------------------------------------------------------


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _int(value: object) -> int | None:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _float(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def body_ids(body: Any) -> list[int]:
    """The distinct positive ids in an ESI array body, ascending."""
    if not isinstance(body, list):
        raise ValueError("ESI returned a non-array body")
    return sorted({v for v in body if isinstance(v, int) and v > 0})  # pyright: ignore[reportUnknownVariableType]


def _object(body: Any) -> Mapping[str, object]:
    if not isinstance(body, Mapping):
        raise ValueError("ESI returned a non-object body")
    return body  # pyright: ignore[reportUnknownVariableType]


def _array(body: Any) -> list[Mapping[str, object]]:
    if not isinstance(body, list):
        raise ValueError("ESI returned a non-array body")
    return [item for item in body if isinstance(item, Mapping)]  # pyright: ignore[reportUnknownVariableType]


def _load(registry: Registry, key: str) -> dict[str, Any]:
    raw = registry.state_get(key)
    if raw is None:
        return {}
    value = json.loads(raw)
    return value if isinstance(value, dict) else {}  # pyright: ignore[reportUnknownVariableType]


def _save(registry: Registry, key: str, value: Mapping[str, Any], now: datetime) -> None:
    registry.state_put(key, json.dumps(value, sort_keys=True, default=str), now=now)


def _months(first: date, last: date) -> Iterable[tuple[date, date]]:
    start = date(first.year, first.month, 1)
    while start <= last:
        following = date(start.year + start.month // 12, start.month % 12 + 1, 1)
        yield start, following - timedelta(days=1)
        start = following


def ids_from_killmails(
    lake: Lake, *, date_from: date, date_to: date | None = None
) -> dict[str, set[int]]:
    """Distinct entity IDs appearing in killmail partitions in the range."""
    found: dict[str, set[int]] = {kind: set() for kind in KINDS}
    for table, columns in KILLMAIL_ID_COLUMNS.items():
        data = lake.read(table, date_from=date_from, date_to=date_to, columns=list(columns))
        for column, kind in columns.items():
            values = data.column(column).drop_null().unique().to_pylist()
            found[kind].update(int(v) for v in values if isinstance(v, int) and v > 0)
    return found


def refresh_status(
    registry: Registry, *, now: datetime
) -> tuple[dict[str, dict[str, int]], dict[str, Any]]:
    """Queue size per class and the sweep state, for ``evedw entities status``."""
    counts = registry.refresh_counts(now=now)
    queue = {
        cls.name.lower(): {
            "entries": counts.get(int(cls), (0, 0))[0],
            "due": counts.get(int(cls), (0, 0))[1],
        }
        for cls in RefreshClass
    }
    state: dict[str, Any] = {}
    for key in (ALLIANCE_STATE, RECENT_STATE, CYCLE_STATE, CRAWL_STATE):
        value = _load(registry, key)
        if isinstance(value.get("ids"), list):
            value["alliances"] = len(value.pop("ids"))  # pyright: ignore[reportUnknownArgumentType]
        state[key.removeprefix("refresh.")] = value
    return queue, state


# --- ESI body -> rows ---------------------------------------------------------------------


def _row(schema: pa.Schema, values: Mapping[str, object]) -> pa.Table:
    return pa.Table.from_pylist([dict(values)], schema=schema)


def character_row(character_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    corporation_id = _int(data.get("corporation_id"))
    return _row(
        CHARACTERS,
        {
            "character_id": character_id,
            "name": _str(data.get("name")),
            "corporation_id": corporation_id,
            "alliance_id": _int(data.get("alliance_id")),
            "faction_id": _int(data.get("faction_id")),
            "birthday": _timestamp(data.get("birthday")),
            "security_status": _float(data.get("security_status")),
            "deleted": corporation_id == DOOMHEIM,
            "observed_at": now,
            "source": "esi",
        },
    )


def employment_rows(character_id: int, body: Any, now: datetime) -> pa.Table:
    rows = [
        {
            "character_id": character_id,
            "record_id": _int(item.get("record_id")),
            "corporation_id": _int(item.get("corporation_id")),
            "start_date": _timestamp(item.get("start_date")),
            "observed_at": now,
            "source": "esi",
        }
        for item in _array(body)
        if _int(item.get("record_id")) is not None
    ]
    return pa.Table.from_pylist(rows, schema=CHARACTER_EMPLOYMENT)


def corporation_row(corporation_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    return _row(
        CORPORATIONS,
        {
            "corporation_id": corporation_id,
            "name": _str(data.get("name")),
            "ticker": _str(data.get("ticker")),
            "alliance_id": _int(data.get("alliance_id")),
            "ceo_id": _int(data.get("ceo_id")),
            "member_count": _int(data.get("member_count")),
            "date_founded": _timestamp(data.get("date_founded")),
            "deleted": data.get("state") == "closed",
            "observed_at": now,
            "source": "esi",
        },
    )


def alliance_history_rows(corporation_id: int, body: Any, now: datetime) -> pa.Table:
    rows = [
        {
            "corporation_id": corporation_id,
            "record_id": _int(item.get("record_id")),
            "alliance_id": _int(item.get("alliance_id")),
            "start_date": _timestamp(item.get("start_date")),
            "is_deleted": bool(item.get("is_deleted", False)),
            "observed_at": now,
            "source": "esi",
        }
        for item in _array(body)
        if _int(item.get("record_id")) is not None
    ]
    return pa.Table.from_pylist(rows, schema=CORPORATION_ALLIANCE_HISTORY)


def alliance_row(alliance_id: int, body: Any, now: datetime) -> pa.Table:
    data = _object(body)
    return _row(
        ALLIANCES,
        {
            "alliance_id": alliance_id,
            "name": _str(data.get("name")),
            "ticker": _str(data.get("ticker")),
            "executor_corporation_id": _int(data.get("executor_corporation_id")),
            "date_founded": _timestamp(data.get("date_founded")),
            "deleted": False,
            "observed_at": now,
            "source": "esi",
        },
    )


# --- the job ------------------------------------------------------------------------------


@dataclass(slots=True)
class SliceReport:
    alliances_swept: int = 0
    characters_checked: int = 0
    changes: int = 0
    active_queued: int = 0
    crawl_queued: int = 0
    refreshed: int = 0
    rows: int = 0
    esi_errors: int = 0
    """Error responses this slice caused: 4xx answers and exhausted retries."""
    splits: int = 0
    deferred: int = 0


@dataclass(slots=True)
class RefreshJob:
    """One refresh slice: sweeps, active queueing, crawl feed, then the queue drain."""

    esi: EsiClient
    entities: EntityStore
    lake: Lake
    recent_days: int = 7
    refresh_interval: timedelta = timedelta(days=30)
    alliance_sweep_interval: timedelta = timedelta(days=1)
    affiliation_cycle: timedelta = timedelta(days=7)
    budget: int | None = None
    """Requests this run may send, on top of the client's daily budget."""
    populate: bool = True
    """False only drains the queue: no sweeps, no new work."""
    today: date | None = None
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    pop_size: int = 200
    crawl_low_water: int = 70_000
    """Feed the crawl when fewer entries than this are due: about a day of requests."""
    crawl_chunk: int = 100_000
    _sent_at_start: int = field(default=0, init=False)
    _report: SliceReport = field(default_factory=SliceReport, init=False)

    def __call__(self, ctx: RunContext) -> JobOutcome:
        self._sent_at_start = self.esi.policy.requests
        self._report = SliceReport()
        registry = ctx.registry
        try:
            # Before any work: a slice during downtime is one /status check at most.
            self.esi.check_downtime()
            if self.populate:
                today = self.today or self.now().date()
                recent = ids_from_killmails(
                    self.lake, date_from=today - timedelta(days=self.recent_days)
                )
                self.sweep_alliances(registry, cancel=ctx.check_cancelled)
                self.affiliate_recent(registry, sorted(recent["character"]))
                self.affiliation_cycle_step(registry, cancel=ctx.check_cancelled)
                self.queue_active(registry, recent)
                self.feed_crawl(registry)
            self.drain(registry, cancel=ctx.check_cancelled)
        except (_OutOfBudget, EsiBudgetError) as exc:
            log.info("request budget spent: %s", str(exc) or "slice allowance")
        except EsiDowntimeError as exc:
            # Nothing was sent, so nothing failed: the entity or batch in hand stays due and
            # the sweeps resume from their saved position once ESI is back.
            log.info("slice ends early: %s", exc)
        r = self._report
        log.info(
            "refresh slice: %d alliances swept, %d characters checked, %d changes, "
            "%d active and %d crawl queued, %d entities refreshed, %d requests, "
            "%d ESI errors, %d batch splits, %d deleted deferred",
            r.alliances_swept,
            r.characters_checked,
            r.changes,
            r.active_queued,
            r.crawl_queued,
            r.refreshed,
            self.esi.policy.requests - self._sent_at_start,
            r.esi_errors,
            r.splits,
            r.deferred,
        )
        return JobOutcome(objects_changed=r.refreshed + r.changes, rows_written=r.rows)

    # -- budget --------------------------------------------------------------------------

    def allowance(self) -> int:
        remaining = self.esi.budget_remaining()
        if self.budget is not None:
            remaining = min(
                remaining, self.budget - (self.esi.policy.requests - self._sent_at_start)
            )
        return remaining

    def _need(self, requests: int) -> None:
        if self.allowance() < requests:
            raise _OutOfBudget()

    def _error(self, *, stop: bool = True) -> None:
        """Count an ESI error response; stop the slice at the cap. ``stop=False`` only
        counts, for work that must finish and save its progress first."""
        self._report.esi_errors += 1
        if stop:
            self._check_errors()

    def _check_errors(self) -> None:
        if self._report.esi_errors >= SLICE_ERROR_CAP:
            raise _OutOfBudget(f"{self._report.esi_errors} ESI errors in this slice")

    # -- queueing ------------------------------------------------------------------------

    def _queue(self, registry: Registry, kind: str, ids: Collection[int], cls: RefreshClass) -> int:
        now = self.now()
        entries = [
            RefreshEntry(kind=kind, entity_id=i, priority=int(cls), next_due_at=now)
            for i in sorted(set(ids))
        ]
        registry.refresh_push(entries)
        if cls is RefreshClass.CHANGE:
            self._report.changes += len(entries)
        return len(entries)

    def _write(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        now = self.now()
        for row in rows:
            row.update({"observed_at": now, "source": "esi"})
        self._report.rows += self.entities.upsert(
            table, pa.Table.from_pylist(rows, schema=ENTITY_TABLES[table])
        )

    def _rows_by_id(self, table: str, ids: Collection[int]) -> dict[int, dict[str, Any]]:
        key = {"characters": "character_id", "corporations": "corporation_id"}.get(
            table, "alliance_id"
        )
        return {int(r[key]): r for r in self.entities.lookup(table, ids).to_pylist()}

    # -- 1. alliance sweep ---------------------------------------------------------------

    def sweep_alliances(self, registry: Registry, *, cancel: Callable[[], None]) -> None:
        state: dict[str, Any] = _load(registry, ALLIANCE_STATE)
        now = self.now()
        if state.get("ids") is None:
            started = _timestamp(state.get("started_at"))
            if started is not None and now < started + self.alliance_sweep_interval:
                return
            self._need(1)
            live = body_ids(self.esi.get("/alliances", store=False).body)
            self._start_alliance_sweep(registry, live)
            state = {"started_at": now.isoformat(), "ids": live, "pos": 0}
            _save(registry, ALLIANCE_STATE, state, now)
            log.info("alliance sweep started: %d live alliances", len(live))
        ids: list[int] = list(state["ids"])
        pos = int(state.get("pos", 0))
        while pos < len(ids):
            cancel()
            self._need(1)
            alliance_id = ids[pos]
            try:
                members = body_ids(
                    self.esi.get(f"/alliances/{alliance_id}/corporations", store=False).body
                )
            except EsiPermanentError as exc:
                if exc.status not in NOT_FOUND:
                    raise
                self._error()
                members = []
            except EsiTransientError as exc:
                self._error()
                log.warning("alliance %d members: %s; resuming next slice", alliance_id, exc)
                return
            self._apply_alliance(registry, alliance_id, members)
            pos += 1
            self._report.alliances_swept += 1
            state["pos"] = pos
            _save(registry, ALLIANCE_STATE, state, self.now())
        state.update({"ids": None, "pos": 0, "finished_at": self.now().isoformat()})
        _save(registry, ALLIANCE_STATE, state, self.now())
        log.info("alliance sweep finished: %d alliances", len(ids))

    def _start_alliance_sweep(self, registry: Registry, live: list[int]) -> None:
        """Alliances gone from the live list are closed; unknown ones are changes; live
        alliances with stale details are queued as active."""
        known_live = set(self.entities.ids("alliances", live_only=True))
        known = set(self.entities.ids("alliances"))
        closed = sorted(known_live - set(live))
        rows = list(self._rows_by_id("alliances", closed).values())
        for row in rows:
            row["deleted"] = True
        self._write("alliances", rows)
        self._queue(registry, "alliance", set(live) - known, RefreshClass.CHANGE)
        stale = registry.refresh_filter(
            "alliance",
            sorted(set(live) & known),
            refreshed_before=self.now() - self.refresh_interval,
        )
        self._report.active_queued += self._queue(registry, "alliance", stale, RefreshClass.ACTIVE)
        if closed:
            log.info("%d alliances closed since the last sweep", len(closed))

    def _apply_alliance(self, registry: Registry, alliance_id: int, members: list[int]) -> None:
        listed = set(members)
        stored = set(self.entities.ids("corporations", where={"alliance_id": alliance_id}))
        # A member no longer listed left, or switched to an alliance processed later in this
        # sweep: queue it without writing, the full refresh settles where it went.
        changed = stored - listed
        rows = self._rows_by_id("corporations", listed)
        joined = [row for row in rows.values() if row.get("alliance_id") != alliance_id]
        for row in joined:
            row["alliance_id"] = alliance_id
        self._write("corporations", joined)
        changed |= {int(row["corporation_id"]) for row in joined}
        changed |= listed - set(rows)
        self._queue(registry, "corporation", changed, RefreshClass.CHANGE)
        stale = registry.refresh_filter(
            "corporation",
            sorted(listed - changed),
            refreshed_before=self.now() - self.refresh_interval,
        )
        self._report.active_queued += self._queue(
            registry, "corporation", stale, RefreshClass.ACTIVE
        )

    # -- 2 and 3. affiliation ------------------------------------------------------------

    def affiliate_recent(self, registry: Registry, character_ids: list[int]) -> None:
        state = _load(registry, RECENT_STATE)
        now = self.now()
        last = _timestamp(state.get("last_at"))
        if last is not None and now < last + timedelta(days=1):
            return
        batches = [
            character_ids[i : i + AFFILIATION_BATCH]
            for i in range(0, len(character_ids), AFFILIATION_BATCH)
        ]
        if batches:
            # All or nothing, with room for a few batch splits; otherwise next slice.
            self._need(len(batches) + 20)
        for batch in batches:
            results = self._affiliate(batch)
            self._apply_affiliations(registry, results)
        _save(
            registry,
            RECENT_STATE,
            {"last_at": now.isoformat(), "characters": len(character_ids)},
            now,
        )
        self._check_errors()

    def affiliation_cycle_step(self, registry: Registry, *, cancel: Callable[[], None]) -> None:
        state: dict[str, Any] = _load(registry, CYCLE_STATE)
        now = self.now()
        started = _timestamp(state.get("started_at"))
        if started is None or (
            state.get("finished_at") and now >= started + self.affiliation_cycle
        ):
            state = {"started_at": now.isoformat(), "cursor": None, "checked": 0, "changed": 0}
            _save(registry, CYCLE_STATE, state, now)
            log.info("affiliation cycle started")
        if state.get("finished_at"):
            return
        while True:
            cancel()
            self._need(1)
            cursor = state.get("cursor")
            batch = self.entities.ids(
                "characters",
                live_only=True,
                after=None if cursor is None else int(cursor),
                limit=AFFILIATION_BATCH,
            )
            if not batch:
                state["finished_at"] = self.now().isoformat()
                _save(registry, CYCLE_STATE, state, self.now())
                log.info(
                    "affiliation cycle finished: %d characters checked, %d changed",
                    state["checked"],
                    state["changed"],
                )
                return
            changes_before = self._report.changes
            self._apply_affiliations(registry, self._affiliate(batch))
            state["cursor"] = batch[-1]
            state["checked"] = int(state["checked"]) + len(batch)
            state["changed"] = int(state["changed"]) + self._report.changes - changes_before
            _save(registry, CYCLE_STATE, state, self.now())
            self._check_errors()

    def _affiliate(self, ids: list[int]) -> list[Mapping[str, object]]:
        """Affiliations of ``ids``. A ``400`` batch is retried in tenths until the invalid
        ids are isolated; those come back as ``{"character_id": id, "invalid": True}``."""
        self._need(1)
        try:
            body = self.esi.post_ids("/characters/affiliation", ids, store=False).body
        except EsiPermanentError as exc:
            if exc.status != 400:
                raise
            # Finish isolating: stopping halfway would re-send the same bad batch next time.
            self._error(stop=False)
            if len(ids) == 1:
                log.info("character %d: ESI calls the id invalid", ids[0])
                return [{"character_id": ids[0], "invalid": True}]
            self._report.splits += 1
            size = -(-len(ids) // SPLIT_WAYS)
            results: list[Mapping[str, object]] = []
            for i in range(0, len(ids), size):
                results += self._affiliate(ids[i : i + size])
            return results
        return _array(body)

    def _apply_affiliations(self, registry: Registry, results: list[Mapping[str, object]]) -> None:
        now_rows = self._rows_by_id(
            "characters", [i for r in results if (i := _int(r.get("character_id")))]
        )
        writes: list[dict[str, Any]] = []
        changed: set[int] = set()
        corporations: set[int] = set()
        alliances: set[int] = set()
        for result in results:
            character_id = _int(result.get("character_id"))
            if character_id is None:
                continue
            row = now_rows.get(character_id)
            if result.get("invalid"):
                if row is not None and not row["deleted"]:
                    writes.append(dict(row, deleted=True))
                continue
            corporation_id = _int(result.get("corporation_id"))
            alliance_id = _int(result.get("alliance_id"))
            faction_id = _int(result.get("faction_id"))
            if row is None:
                changed.add(character_id)
            elif corporation_id == DOOMHEIM:
                if not row["deleted"] or row["corporation_id"] != DOOMHEIM:
                    writes.append(
                        dict(
                            row,
                            corporation_id=DOOMHEIM,
                            alliance_id=None,
                            faction_id=None,
                            deleted=True,
                        )
                    )
                continue
            elif corporation_id != row["corporation_id"] or row["deleted"]:
                writes.append(
                    dict(
                        row,
                        corporation_id=corporation_id,
                        alliance_id=alliance_id,
                        faction_id=faction_id,
                        deleted=False,
                    )
                )
                changed.add(character_id)
            elif alliance_id != row["alliance_id"] or faction_id != row["faction_id"]:
                writes.append(dict(row, alliance_id=alliance_id, faction_id=faction_id))
            if corporation_id is not None:
                corporations.add(corporation_id)
            if alliance_id is not None:
                alliances.add(alliance_id)
        self._write("characters", writes)
        self._report.characters_checked += len(results)
        self._queue(registry, "character", changed, RefreshClass.CHANGE)
        unknown_corps = corporations - set(self._rows_by_id("corporations", corporations))
        self._queue(registry, "corporation", unknown_corps, RefreshClass.CHANGE)
        unknown_alliances = alliances - set(self._rows_by_id("alliances", alliances))
        self._queue(registry, "alliance", unknown_alliances, RefreshClass.CHANGE)

    # -- 4. active -----------------------------------------------------------------------

    def queue_active(self, registry: Registry, recent: Mapping[str, set[int]]) -> None:
        before = self.now() - self.refresh_interval
        for kind, ids in recent.items():
            wanted = registry.refresh_filter(kind, ids, refreshed_before=before)
            self._report.active_queued += self._queue(registry, kind, wanted, RefreshClass.ACTIVE)

    # -- 5. crawl feed -------------------------------------------------------------------

    def feed_crawl(self, registry: Registry) -> None:
        now = self.now()
        due = registry.refresh_counts(now=now).get(int(RefreshClass.CRAWL), (0, 0))[1]
        if due >= self.crawl_low_water:
            return
        state: dict[str, Any] = _load(registry, CRAWL_STATE)
        phase = state.get("phase", "killmails")
        if phase == "killmails":
            self._crawl_killmail_entities(registry)
            state = {"phase": "all", "cursor": {}}
            _save(registry, CRAWL_STATE, state, now)
            return
        if phase != "all":
            return
        cursors: dict[str, int | None] = dict(state.get("cursor") or {})
        exhausted = 0
        for kind in ("character", "corporation"):
            if cursors.get(kind) == -1:
                exhausted += 1
                continue
            pushed = 0
            while pushed < self.crawl_chunk:
                ids = self.entities.ids(
                    TABLE[kind],
                    live_only=True,
                    after=cursors.get(kind),
                    descending=True,
                    limit=self.crawl_chunk,
                )
                if not ids:
                    cursors[kind] = -1
                    exhausted += 1
                    break
                cursors[kind] = ids[-1]
                pushed += self._queue(
                    registry, kind, registry.refresh_filter(kind, ids), RefreshClass.CRAWL
                )
            self._report.crawl_queued += pushed
        state = {"phase": "done" if exhausted == 2 else "all", "cursor": cursors}
        _save(registry, CRAWL_STATE, state, self.now())
        if exhausted == 2:
            log.info("history crawl: every live character and corporation is queued")

    def _crawl_killmail_entities(self, registry: Registry) -> None:
        """Queue every character and corporation that appears on any killmail, first."""
        today = self.today or self.now().date()
        found: dict[str, set[int]] = {"character": set(), "corporation": set()}
        for first, last in _months(FIRST_KILLMAIL_DAY, today):
            month = ids_from_killmails(self.lake, date_from=first, date_to=last)
            for kind in found:
                found[kind] |= month[kind]
        for kind, ids in found.items():
            ordered = sorted(ids, reverse=True)
            queued = 0
            for i in range(0, len(ordered), self.crawl_chunk):
                chunk = registry.refresh_filter(kind, ordered[i : i + self.crawl_chunk])
                queued += self._queue(registry, kind, chunk, RefreshClass.CRAWL)
            self._report.crawl_queued += queued
            log.info("history crawl: %d killmail %ss queued of %d seen", queued, kind, len(ids))

    # -- 6. drain ------------------------------------------------------------------------

    def drain(self, registry: Registry, *, cancel: Callable[[], None]) -> None:
        while True:
            due = registry.refresh_pop(limit=self.pop_size, now=self.now())
            if not due:
                return
            crawled = {
                kind: {e.entity_id for e in due if e.kind == kind and e.priority in HISTORY_ONLY}
                for kind in ("character", "corporation")
            }
            known = {kind: self._rows_by_id(TABLE[kind], ids) for kind, ids in crawled.items()}
            for entry in due:
                cancel()
                row = known.get(entry.kind, {}).get(entry.entity_id)
                if entry.priority == RefreshClass.CRAWL and (row is None or row["deleted"]):
                    # Deleted or never stored: mostly 404s. Nothing is skipped; the entry
                    # moves behind the crawl and is requested there (in full if not stored),
                    # so live entities do not wait behind a run of 404s.
                    registry.refresh_update(replace(entry, priority=int(RefreshClass.DEFERRED)))
                    self._report.deferred += 1
                    continue
                full = entry.priority not in HISTORY_ONLY or row is None
                self._need(FULL_COST[entry.kind] if full else 1)
                with log_context(kind=entry.kind, entity_id=str(entry.entity_id)):
                    self._refresh_one(registry, entry, full=full)

    def _refresh_one(self, registry: Registry, entry: RefreshEntry, *, full: bool) -> None:
        now = self.now()
        try:
            self._report.rows += self._fetch(entry.kind, entry.entity_id, full=full)
        except EsiPermanentError as exc:
            if exc.status in NOT_FOUND:
                self._mark_deleted(entry.kind, entry.entity_id)
                log.info("HTTP %d: marked deleted", exc.status)
            else:
                log.warning("permanent failure: %s", exc)
            registry.refresh_update(
                RefreshEntry(
                    kind=entry.kind,
                    entity_id=entry.entity_id,
                    priority=int(RefreshClass.IDLE),
                    next_due_at=now + PERMANENT_RETRY,
                    # A definitive answer counts as a refresh: without it queue_active sees
                    # the entity as never refreshed and asks again every slice.
                    last_refreshed_at=now,
                    failures=entry.failures + 1,
                    last_error=f"HTTP {exc.status}",
                )
            )
            self._report.refreshed += 1
            self._error()  # counted after the entry is parked, so it is never retried
            return
        except EsiTransientError as exc:
            failures = entry.failures + 1
            registry.refresh_update(
                RefreshEntry(
                    kind=entry.kind,
                    entity_id=entry.entity_id,
                    priority=entry.priority,
                    next_due_at=now + TRANSIENT_RETRY * min(2 ** (failures - 1), 24),
                    last_refreshed_at=entry.last_refreshed_at,
                    failures=failures,
                    last_error=str(exc)[:500],
                )
            )
            log.warning("transient failure: %s", exc)
            self._error()
            return
        registry.refresh_update(
            RefreshEntry(
                kind=entry.kind,
                entity_id=entry.entity_id,
                priority=int(RefreshClass.IDLE),
                next_due_at=now + SETTLED_FOR,
                last_refreshed_at=now,
            )
        )
        self._report.refreshed += 1

    def _fetch(self, kind: str, entity_id: int, *, full: bool) -> int:
        """Rows written. ``full`` fetches details and history, otherwise history only."""
        now = self.now()
        esi = self.esi
        rows = 0
        if kind == "character":
            if full:
                body = esi.get(f"/characters/{entity_id}", store=False).body
                rows += self.entities.upsert("characters", character_row(entity_id, body, now))
            body = esi.get(f"/characters/{entity_id}/corporationhistory", store=False).body
            return rows + self.entities.upsert(
                "character_employment", employment_rows(entity_id, body, now)
            )
        if kind == "corporation":
            if full:
                body = esi.get(f"/corporations/{entity_id}", store=False).body
                rows += self.entities.upsert("corporations", corporation_row(entity_id, body, now))
            body = esi.get(f"/corporations/{entity_id}/alliancehistory", store=False).body
            return rows + self.entities.upsert(
                "corporation_alliance_history", alliance_history_rows(entity_id, body, now)
            )
        if kind == "alliance":
            body = esi.get(f"/alliances/{entity_id}", store=False).body
            return self.entities.upsert("alliances", alliance_row(entity_id, body, now))
        raise ValueError(f"unknown entity kind {kind!r}")

    def _mark_deleted(self, kind: str, entity_id: int) -> None:
        table = TABLE[kind]
        rows = list(self._rows_by_id(table, [entity_id]).values())
        for row in rows:
            row["deleted"] = True
        self._write(table, rows)
