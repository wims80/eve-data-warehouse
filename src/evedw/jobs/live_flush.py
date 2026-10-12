"""Turns raw live objects into lake partitions (design §7.1).

Each flush reads the objects fetched since its checkpoint, keeps the newest sequence of
each killmail (a reprocessed killmail comes back under a new sequence), and for every UTC
day of ``killmail_time`` among them rewrites that day's ``live`` source partitions and its
``killmail_zkb`` partition with the new killmails in place of any earlier copy. The merged
layer is then rebuilt for those days, where daily files, when present, take precedence.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from evedw.domain.schemas import KILLMAIL_ZKB, partition_name, source_table
from evedw.jobs.killmail_merge import KillmailMerge
from evedw.jobs.killmails import normalise_day
from evedw.jobs.live_raw import LiveRaw, sequence_of
from evedw.store.base import Lake

log = logging.getLogger(__name__)

LIVE = "live"
DATASET = "zkillboard_live"
"""Names the live partitions' origin in their metadata, as datasets do for imported ones."""

TOP_KEYS = frozenset(
    {"esi", "hash", "killmail_id", "sequence_id", "sequence_updated", "uploaded_at", "zkb"}
)
ZKB_KEYS = frozenset(
    {
        "attackerCount",
        "awox",
        "destroyedValue",
        "droppedValue",
        "fittedValue",
        "hash",
        "href",
        "labels",
        "locationID",
        "npc",
        "points",
        "solo",
        "totalDroppableValue",
        "totalValue",
    }
)
"""The fields of a live object as served on 2026-10-12. ``href`` is ESI's URL of the
killmail, derived from id and hash, so it is not stored. Other keys are reported, not
stored (design §7.1)."""

_SORT: dict[str, tuple[str, ...]] = {
    "killmails": ("killmail_id",),
    "attackers": ("killmail_id", "ordinal"),
    "items": ("killmail_id", "parent_ordinal", "ordinal"),
    "killmail_zkb": ("killmail_id",),
}
"""Row order of each table as the importers write it; top-level items (no parent) first."""


def _ordered(table: pa.Table, keys: tuple[str, ...]) -> pa.Table:
    key = pa.table(
        {
            k: (
                pc.fill_null(table.column(k), pa.scalar(-1, table.schema.field(k).type))
                if k == "parent_ordinal"
                else table.column(k)
            )
            for k in keys
        }
    )
    return table.take(pc.sort_indices(key, sort_keys=[(k, "ascending") for k in keys]))


@dataclass(slots=True)
class FlushReport:
    objects: int = 0
    killmails: int = 0
    days: list[date] = field(default_factory=lambda: [])
    skipped: int = 0
    """Objects without a usable ESI killmail."""
    last_sequence: int | None = None
    unknown: dict[str, list[str]] = field(default_factory=lambda: {})


def _fields(value: Any) -> dict[str, Any]:
    """A JSON object as a typed dict; anything else as an empty one."""
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def killmail_day(obj: dict[str, Any]) -> date | None:
    when = _fields(obj.get("esi")).get("killmail_time")
    if not isinstance(when, str):
        return None
    try:
        return datetime.fromisoformat(when.replace("Z", "+00:00")).astimezone(UTC).date()
    except ValueError:
        return None


def killmail_id_of(obj: dict[str, Any]) -> int | None:
    value = obj.get("killmail_id")
    if value is None:
        value = _fields(obj.get("esi")).get("killmail_id")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def zkb_row(obj: dict[str, Any], day: date) -> dict[str, Any]:
    z = _fields(obj.get("zkb"))
    uploaded: Any = obj.get("uploaded_at")
    labels: Any = z.get("labels")
    return {
        "source_date": day,
        "killmail_id": killmail_id_of(obj),
        "sequence_id": sequence_of(obj),
        "uploaded_at": (
            datetime.fromtimestamp(uploaded, tz=UTC)
            if isinstance(uploaded, int | float) and not isinstance(uploaded, bool)
            else None
        ),
        "killmail_hash": obj.get("hash") or z.get("hash"),
        "location_id": z.get("locationID"),
        "total_value": z.get("totalValue"),
        "fitted_value": z.get("fittedValue"),
        "dropped_value": z.get("droppedValue"),
        "destroyed_value": z.get("destroyedValue"),
        "total_droppable_value": z.get("totalDroppableValue"),
        "points": z.get("points"),
        "npc": z.get("npc"),
        "solo": z.get("solo"),
        "awox": z.get("awox"),
        "attacker_count": z.get("attackerCount"),
        "labels": [str(x) for x in labels] if isinstance(labels, list) else None,  # pyright: ignore[reportUnknownVariableType]
    }


def unknown_keys(objects: list[dict[str, Any]]) -> dict[str, list[str]]:
    top: set[str] = set()
    zkb: set[str] = set()
    for obj in objects:
        top.update(k for k in obj if k not in TOP_KEYS)
        zkb.update(k for k in _fields(obj.get("zkb")) if k not in ZKB_KEYS)
    found = {"top": sorted(top), "zkb": sorted(zkb)}
    return {k: v for k, v in found.items() if v}


@dataclass(slots=True)
class LiveFlush:
    lake: Lake
    raw: LiveRaw
    scratch_dir: Path
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    def flush(self, since: int | None) -> FlushReport:
        """Write everything fetched after sequence ``since`` (everything for ``None``)."""
        objects = self.raw.read_since(since)
        report = FlushReport(objects=len(objects), last_sequence=since)
        if not objects:
            return report
        report.last_sequence = max(s for o in objects if (s := sequence_of(o)) is not None)
        newest: dict[int, dict[str, Any]] = {}
        for obj in objects:
            killmail_id = killmail_id_of(obj)
            if killmail_id is None or killmail_day(obj) is None:
                report.skipped += 1
                continue
            seen = newest.get(killmail_id)
            if seen is None or (sequence_of(obj) or 0) >= (sequence_of(seen) or 0):
                newest[killmail_id] = obj
        if report.skipped:
            log.warning("%d live objects carry no usable ESI killmail", report.skipped)
        report.unknown = unknown_keys(objects)
        if report.unknown:
            log.warning("unknown keys in live objects: %s", report.unknown)
        by_day: dict[date, list[dict[str, Any]]] = {}
        for obj in newest.values():
            day = killmail_day(obj)
            if day is not None:
                by_day.setdefault(day, []).append(obj)
        for day in sorted(by_day):
            self._write_day(day, by_day[day], report.unknown)
        report.days = sorted(by_day)
        report.killmails = len(newest)
        KillmailMerge(self.lake, now=self.now).merge(report.days)
        return report

    def _write_day(
        self, day: date, objects: list[dict[str, Any]], unknown: dict[str, list[str]]
    ) -> None:
        self.scratch_dir.mkdir(parents=True, exist_ok=True)
        ndjson = self.scratch_dir / f"live-{day.isoformat()}.ndjson"
        try:
            with ndjson.open("w", encoding="utf-8") as out:
                for obj in sorted(objects, key=lambda o: killmail_id_of(o) or 0):
                    row: dict[str, Any] = dict(obj["esi"])
                    hashed = obj.get("hash")
                    if isinstance(hashed, str):
                        row["killmail_hash"] = hashed
                    out.write(json.dumps(row, separators=(",", ":")) + "\n")
            tables, esi_unknown = normalise_day(ndjson, day)
        finally:
            ndjson.unlink(missing_ok=True)
        if esi_unknown:
            log.warning("unknown JSON keys in live killmails of %s: %s", day, esi_unknown)
        tables["killmail_zkb"] = pa.Table.from_pylist(
            [zkb_row(o, day) for o in objects], schema=KILLMAIL_ZKB
        )
        ids = pa.array(sorted(killmail_id_of(o) or 0 for o in objects), pa.int64())
        last = max(sequence_of(o) or 0 for o in objects)
        metadata = {
            "evedw.dataset": DATASET,
            "evedw.sequence_max": str(last),
            "evedw.written_at": self.now().isoformat(),
            "evedw.unknown_keys": json.dumps({**unknown, **esi_unknown}, sort_keys=True),
        }
        # Same order as the importers: the killmails partition last.
        for name in ("attackers", "items", "killmail_zkb", "killmails"):
            table = name if name == "killmail_zkb" else source_table(LIVE, name)
            kept = self._kept(table, day, ids)
            combined = pa.concat_tables([kept, tables[name].select(kept.schema.names)])
            combined = _ordered(combined, _SORT[name])
            self.lake.write_partition(
                table, partition_name(table, day), combined, metadata=metadata
            )

    def _kept(self, table: str, day: date, replaced: pa.Int64Array) -> pa.Table:
        """The rows of ``table`` on ``day`` whose killmail is not being replaced."""
        existing = self.lake.read(table, date_from=day, date_to=day)
        if existing.num_rows == 0:
            return existing
        mask = pc.invert(pc.is_in(existing.column("killmail_id"), value_set=replaced))
        return existing.filter(mask)
