"""The merged killmail layer (design §7.1): one row per killmail, the day from its
``killmail_time``, built from the per-source partitions.

A merged day reads every source's partitions of that day and the days either side, because
upstream files spill over (EVE Ref's 2026-01-06 file carries killmails of 2026-01-05). Where
several sources have a killmail, the first source in ``KILLMAIL_SOURCES`` supplies its rows;
``sources`` lists all of them. The ESI killmail is immutable, so the rows agree.
"""

import json
import logging
import threading
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow as pa

from evedw.domain.schemas import (
    ATTACKERS,
    ITEMS,
    KILLMAIL_SOURCES,
    KILLMAILS,
    MERGED_KILLMAILS,
    partition_date,
    partition_name,
    source_table,
)
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.store.base import Lake, PartitionInfo

log = logging.getLogger(__name__)

SPILL_DAYS = 1
"""Days either side of a merged day whose source files may hold its killmails. Measured
2026-10-11: every misplaced killmail sat one day late (14,043 of them, two files)."""

MERGE_LOCK = threading.Lock()
"""Held while one merged day is read from its sources and written: the live follower and a
sync in the archive lane can merge the same day, and the later write must see both."""

_KILLMAIL_COLUMNS = ", ".join(n for n in KILLMAILS.names if n != "source_date")
_ATTACKER_COLUMNS = ", ".join(n for n in ATTACKERS.names if n != "source_date")
_ITEM_COLUMNS = ", ".join(n for n in ITEMS.names if n != "source_date")


@dataclass(slots=True)
class MergeReport:
    days: int = 0
    killmails: int = 0
    from_several: int = 0
    """Killmails more than one source had."""
    only: dict[str, int] = field(default_factory=lambda: dict.fromkeys(KILLMAIL_SOURCES, 0))
    """Killmails only that source had."""


def _union(files: dict[str, list[str]], columns: str) -> str:
    """SQL selecting ``source``, its precedence ``rank``, ``source_date`` and ``columns``
    from every source's files; sources without files are left out."""
    parts = [
        f"SELECT '{source}' AS source, {rank} AS rank, source_date, {columns} "
        f"FROM read_parquet({json.dumps(paths)}, hive_partitioning = false)"
        for rank, source in enumerate(KILLMAIL_SOURCES)
        if (paths := files.get(source))
    ]
    return " UNION ALL ".join(parts)


def merge_day(lake: Lake, day: date) -> tuple[dict[str, pa.Table], list[dict[str, str]]] | None:
    """The three merged tables of ``day`` and the source partitions they came from, or
    ``None`` when no source has a partition near that day."""
    near = [day + timedelta(days=d) for d in range(-SPILL_DAYS, SPILL_DAYS + 1)]
    files: dict[str, dict[str, list[str]]] = {"killmails": {}, "attackers": {}, "items": {}}
    built_from: list[dict[str, str]] = []
    for source in KILLMAIL_SOURCES:
        for table in files:
            for d in near:
                info = lake.partition(source_table(source, table), partition_name("killmails", d))
                if info is None:
                    continue
                files[table].setdefault(source, []).append(str(info.path))
                if table == "killmails":
                    built_from.append(_provenance(info))
    if not files["killmails"]:
        return None
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute(
            f"""CREATE TEMP TABLE chosen AS
            SELECT * EXCLUDE (n) FROM (
                SELECT *, row_number() OVER (
                           PARTITION BY killmail_id ORDER BY rank, source_date) AS n,
                       list_sort(list_distinct(list(source) OVER (PARTITION BY killmail_id)))
                           AS sources
                FROM ({_union(files["killmails"], _KILLMAIL_COLUMNS)})
                WHERE killmail_time::DATE = $day
            ) WHERE n = 1""",
            {"day": day},
        )
        killmails = con.execute(
            f"SELECT $day::DATE AS source_date, {_KILLMAIL_COLUMNS}, sources "
            "FROM chosen ORDER BY killmail_id",
            {"day": day},
        ).to_arrow_table()
        tables = {"killmails": killmails.cast(MERGED_KILLMAILS)}
        for table, columns, order, schema in (
            ("attackers", _ATTACKER_COLUMNS, "a.killmail_id, a.ordinal", ATTACKERS),
            (
                "items",
                _ITEM_COLUMNS,
                "a.killmail_id, a.parent_ordinal NULLS FIRST, a.ordinal",
                ITEMS,
            ),
        ):
            if not files[table]:
                tables[table] = schema.empty_table()
                continue
            selected = ", ".join(f"a.{c.strip()}" for c in columns.split(","))
            tables[table] = (
                con.execute(
                    f"SELECT $day::DATE AS source_date, {selected} "
                    f"FROM ({_union(files[table], columns)}) a JOIN chosen c "
                    "ON a.killmail_id = c.killmail_id AND a.source = c.source "
                    f"AND a.source_date = c.source_date ORDER BY {order}",
                    {"day": day},
                )
                .to_arrow_table()
                .cast(schema)
            )
    finally:
        con.close()
    return tables, built_from


def _provenance(info: PartitionInfo) -> dict[str, str]:
    meta = info.metadata
    return {
        "table": info.table,
        "partition": info.partition,
        **{
            k: meta[k] for k in ("evedw.dataset", "evedw.object_key", "evedw.revision") if k in meta
        },
    }


@dataclass(slots=True)
class KillmailMerge:
    """Rebuilds merged days. Used by killmail syncs for the days they imported and by the
    ``killmails:merge`` job for any range."""

    lake: Lake
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    def affected(self, days: Iterable[date]) -> list[date]:
        """Merged days a change to these source days can reach."""
        out: set[date] = set()
        for day in days:
            out.update(day + timedelta(days=d) for d in range(-SPILL_DAYS, SPILL_DAYS + 1))
        return sorted(out)

    def merge(
        self, days: Collection[date], *, cancel: Callable[[], None] = lambda: None
    ) -> MergeReport:
        report = MergeReport()
        for day in sorted(days):
            cancel()
            with MERGE_LOCK:
                self._merge_one(report, day)
        return report

    def _merge_one(self, report: MergeReport, day: date) -> None:
        merged = merge_day(self.lake, day)
        if merged is None:
            if self.lake.partition("killmails", partition_name("killmails", day)) is None:
                return
            # Every source dropped the day: an empty partition, so no stale rows stay.
            tables = {
                "killmails": MERGED_KILLMAILS.empty_table(),
                "attackers": ATTACKERS.empty_table(),
                "items": ITEMS.empty_table(),
            }
            built_from: list[dict[str, str]] = []
        else:
            tables, built_from = merged
            existing = self.lake.partition("killmails", partition_name("killmails", day))
            if tables["killmails"].num_rows == 0 and existing is None:
                return  # a neighbour day with no killmail of its own
        metadata = {
            "evedw.layer": "merged",
            "evedw.built_from": json.dumps(built_from, sort_keys=True),
            "evedw.written_at": self.now().isoformat(),
        }
        name = partition_name("killmails", day)
        # Same order as the importers: the killmails partition last.
        for table in ("attackers", "items", "killmails"):
            self.lake.write_partition(table, name, tables[table], metadata=metadata)
        self._count(report, tables["killmails"])

    @staticmethod
    def _count(report: MergeReport, killmails: pa.Table) -> None:
        report.days += 1
        report.killmails += killmails.num_rows
        for sources in killmails.column("sources").to_pylist():
            names = list(sources or [])
            if len(names) > 1:
                report.from_several += 1
            elif names:
                report.only[names[0]] = report.only.get(names[0], 0) + 1


@dataclass(slots=True)
class MergeJob:
    """``killmails:merge``: rebuild the merged layer for a range, every day with a source
    partition by default."""

    lake: Lake
    date_from: date | None = None
    date_to: date | None = None

    def __call__(self, ctx: RunContext) -> JobOutcome:
        days: set[date] = set()
        for source in KILLMAIL_SOURCES:
            for info in self.lake.partitions(source_table(source, "killmails")):
                day = partition_date(info.partition)
                if day is not None:
                    days.add(day)
        merge = KillmailMerge(self.lake)
        wanted = [
            d
            for d in merge.affected(days)
            if (self.date_from is None or d >= self.date_from)
            and (self.date_to is None or d <= self.date_to)
        ]
        log.info("merging %d days", len(wanted))
        report = merge.merge(wanted, cancel=ctx.check_cancelled)
        log.info(
            "merged %d days: %d killmails, %d from several sources, only one source: %s",
            report.days,
            report.killmails,
            report.from_several,
            report.only,
        )
        return JobOutcome(objects_changed=report.days, rows_written=report.killmails)
