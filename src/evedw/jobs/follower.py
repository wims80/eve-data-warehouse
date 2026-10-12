"""The live feed follower (design §7.1, §8): runs in the service beside the scheduler, not
as a queued job, and follows zKillboard's R2Z2 sequence one object at a time under the
policy of design §11a.

It resumes from the sequence stored in the registry's state, or starts from
``sequence.json`` the first time and after more than a day away (the feed keeps objects at
least 24 hours; the daily history covers anything older). Every object is appended to the
raw hourly files before the stored sequence moves past it, so a restart can fetch an
object twice but never skip one, and the flush keeps one row per killmail. Every
``flush_interval`` the new objects are written to the lake.
"""

import json
import logging
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

from evedw.jobs.live_flush import LiveFlush
from evedw.jobs.live_raw import LiveRaw, sequence_of
from evedw.logs import log_context
from evedw.sources.zkillboard import ZKillboardClient, ZKillboardRefused
from evedw.store.base import Registry

log = logging.getLogger(__name__)

SEQUENCE_STATE = "zkillboard.live.sequence"
"""``{"next": N, "at": ISO time of the last object fetched}``."""
FLUSHED_STATE = "zkillboard.live.flushed"
"""Sequence of the last object written to the lake."""

AFTER_SUCCESS = 0.1
"""Seconds after an object before asking for the next ("about 100 ms between successes")."""
AFTER_NOT_FOUND = 6.0
""" "After a 404, wait at least six seconds before trying again" (zKillboard API docs)."""
AFTER_ERROR = 30.0
AFTER_REFUSAL = 60.0
"""How often a paused or stopped follower looks at the zKillboard policy again."""
POINTER_REFRESH = timedelta(minutes=1)
"""Least time between ``sequence.json`` reads while the feed answers 404."""
RESUME_WITHIN = timedelta(hours=24)
"""Objects stay on the feed "at least 24 hours"; a longer absence starts over."""


@dataclass(frozen=True, slots=True)
class LiveStatus:
    state: str = "starting"
    """``starting``, ``following``, ``paused`` (zKillboard pause or stop), ``stopped``."""
    next_sequence: int | None = None
    pointer: int | None = None
    """Last ``sequence.json`` value read."""
    last_object_at: datetime | None = None
    last_uploaded_at: datetime | None = None
    today: date | None = None
    objects_today: int = 0
    flushed_sequence: int | None = None
    last_flush_at: datetime | None = None
    skipped_sequences: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LiveFollower:
    def __init__(
        self,
        *,
        client: ZKillboardClient,
        registry: Registry,
        raw: LiveRaw,
        flush: LiveFlush,
        flush_interval: timedelta,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        wait: Callable[[threading.Event, float], object] = threading.Event.wait,
    ) -> None:
        self._client = client
        self._wait = wait
        self._registry = registry
        self._raw = raw
        self._flush = flush
        self._flush_interval = flush_interval
        self._now = now
        self._status = LiveStatus()
        self._status_lock = threading.Lock()
        self._pointer_at: datetime | None = None
        self._last_flush: datetime | None = None
        self._pending = False

    @property
    def status(self) -> LiveStatus:
        with self._status_lock:
            return self._status

    def _update(self, **changes: Any) -> None:
        with self._status_lock:
            self._status = replace(self._status, **changes)

    def _error(self, message: str) -> None:
        log.warning("live feed: %s", message)
        self._update(last_error=message, last_error_at=self._now())

    # -- state -------------------------------------------------------------------------

    def _stored_sequence(self) -> tuple[int, datetime] | None:
        raw = self._registry.state_get(SEQUENCE_STATE)
        if raw is None:
            return None
        try:
            payload: Any = json.loads(raw)
            return int(payload["next"]), datetime.fromisoformat(payload["at"])
        except (ValueError, KeyError, TypeError):
            log.warning("ignoring unreadable %s: %r", SEQUENCE_STATE, raw)
            return None

    def _store_sequence(self, next_sequence: int, at: datetime) -> None:
        payload = json.dumps({"next": next_sequence, "at": at.isoformat()})
        self._registry.state_put(SEQUENCE_STATE, payload, now=at)

    def _flushed(self) -> int | None:
        raw = self._registry.state_get(FLUSHED_STATE)
        return int(raw) if raw is not None and raw.lstrip("-").isdigit() else None

    # -- loop --------------------------------------------------------------------------

    def run(self, stop: threading.Event) -> None:
        """Follow until ``stop`` is set, then write what was fetched and close the hour."""
        with log_context(dataset="zkillboard_live"):
            try:
                self._run(stop)
            finally:
                self._raw.close(now=self._now())
                self.flush()
                self._client.close()
                self._update(state="stopped")
                log.info("live feed follower stopped")

    def _start_sequence(self, stop: threading.Event) -> int | None:
        stored = self._stored_sequence()
        now = self._now()
        if stored is not None and now - stored[1] <= RESUME_WITHIN:
            log.info("live feed resumes at sequence %d", stored[0])
            return stored[0]
        while not stop.is_set():
            try:
                pointer = self._read_pointer()
            except ZKillboardRefused as exc:
                self._update(state="paused")
                self._error(str(exc))
                self._wait(stop, AFTER_REFUSAL)
                continue
            except Exception as exc:
                self._error(f"sequence.json: {type(exc).__name__}: {exc}")
                self._wait(stop, AFTER_ERROR)
                continue
            if stored is None:
                log.info("live feed starts at sequence %d", pointer)
            else:
                log.info(
                    "live feed starts over at sequence %d: last object %s, more than %s ago",
                    pointer,
                    stored[1].isoformat(),
                    RESUME_WITHIN,
                )
            return pointer
        return None

    def _read_pointer(self) -> int:
        pointer = self._client.live_sequence()
        self._pointer_at = self._now()
        self._update(pointer=pointer)
        return pointer

    def _run(self, stop: threading.Event) -> None:
        sequence = self._start_sequence(stop)
        if sequence is None:
            return
        self._raw.compress_closed(now=self._now())
        # Objects fetched before a restart and not yet written go out with the first flush.
        self._pending = True
        self._update(state="following", next_sequence=sequence, flushed_sequence=self._flushed())
        while not stop.is_set():
            try:
                obj = self._client.live_object(sequence)
            except ZKillboardRefused as exc:
                self._update(state="paused")
                self._error(str(exc))
                self._maybe_flush()
                self._wait(stop, AFTER_REFUSAL)
                continue
            except Exception as exc:
                self._error(f"sequence {sequence}: {type(exc).__name__}: {exc}")
                self._wait(stop, AFTER_ERROR)
                continue
            now = self._now()
            if obj is None:
                if self._is_gap(sequence):
                    log.warning("live feed skips sequence %d: none was published", sequence)
                    sequence += 1
                    self._store_sequence(sequence, now)
                    self._update(
                        next_sequence=sequence,
                        skipped_sequences=self.status.skipped_sequences + 1,
                    )
                    continue
                self._update(state="following")
                self._maybe_flush()
                self._wait(stop, AFTER_NOT_FOUND)
                continue
            served = sequence_of(obj)
            if served is not None and served != sequence:
                log.warning("live feed asked for %d and was served %d", sequence, served)
            self._raw.append(obj, now=now)
            sequence += 1
            self._store_sequence(sequence, now)
            self._pending = True
            self._count(obj, now, sequence)
            self._maybe_flush()
            self._wait(stop, AFTER_SUCCESS)

    def _count(self, obj: dict[str, Any], now: datetime, next_sequence: int) -> None:
        uploaded: Any = obj.get("uploaded_at")
        status = self.status
        today = now.date()
        self._update(
            state="following",
            next_sequence=next_sequence,
            last_object_at=now,
            last_uploaded_at=(
                datetime.fromtimestamp(uploaded, tz=UTC)
                if isinstance(uploaded, int | float) and not isinstance(uploaded, bool)
                else status.last_uploaded_at
            ),
            today=today,
            objects_today=(status.objects_today + 1 if status.today == today else 1),
        )

    def _is_gap(self, sequence: int) -> bool:
        """A 404 below the published pointer means zKillboard never published that
        sequence (or it expired): waiting would never end. The pointer is read again at
        most once a minute while the feed answers 404."""
        pointer = self.status.pointer
        now = self._now()
        if pointer is not None and pointer > sequence:
            return True
        if self._pointer_at is not None and now - self._pointer_at < POINTER_REFRESH:
            return False
        try:
            return self._read_pointer() > sequence
        except Exception as exc:
            self._error(f"sequence.json: {type(exc).__name__}: {exc}")
            return False

    # -- flush -------------------------------------------------------------------------

    def _maybe_flush(self) -> None:
        now = self._now()
        if not self._pending:
            return
        if self._last_flush is not None and now - self._last_flush < self._flush_interval:
            return
        self.flush()

    def flush(self) -> None:
        """Write the objects fetched since the last flush to the lake."""
        now = self._now()
        self._last_flush = now
        try:
            report = self._flush.flush(self._flushed())
        except Exception as exc:
            log.exception("live flush failed")
            self._update(last_error=f"flush: {type(exc).__name__}: {exc}", last_error_at=now)
            return
        self._pending = False
        if report.last_sequence is not None and report.objects:
            self._registry.state_put(FLUSHED_STATE, str(report.last_sequence), now=now)
            log.info(
                "live flush: %d objects, %d killmails over %d days, up to sequence %d",
                report.objects,
                report.killmails,
                len(report.days),
                report.last_sequence,
            )
        self._update(flushed_sequence=report.last_sequence, last_flush_at=now)
