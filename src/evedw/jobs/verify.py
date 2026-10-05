"""Read-only consistency checks between registry, lake, raw files and upstream totals."""

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

from evedw.config import Settings
from evedw.domain.datasets import Dataset
from evedw.domain.registry import ObjectStatus
from evedw.domain.schemas import DATASET_TABLES
from evedw.store.base import Lake, Registry
from evedw.store.duckdb_lake.lake import partition_date, partition_name

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Issue:
    dataset: str
    object_key: str | None
    kind: str
    detail: str


@dataclass(slots=True)
class VerifyReport:
    dataset: str
    objects_checked: int = 0
    raw_files_hashed: int = 0
    issues: list[Issue] = field(default_factory=lambda: [])

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_json(self) -> str:
        return json.dumps(
            {
                "dataset": self.dataset,
                "objects_checked": self.objects_checked,
                "raw_files_hashed": self.raw_files_hashed,
                "ok": self.ok,
                "issues": [asdict(i) for i in self.issues],
            },
            indent=2,
        )

    def to_text(self) -> str:
        head = (
            f"{self.dataset}: {self.objects_checked} objects checked, "
            f"{self.raw_files_hashed} raw files hashed, {len(self.issues)} issues"
        )
        lines = [head]
        for issue in self.issues:
            where = issue.object_key or "-"
            lines.append(f"  {issue.kind:<18} {where:<44} {issue.detail}")
        return "\n".join(lines)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_dataset(
    settings: Settings,
    registry: Registry,
    lake: Lake,
    dataset: Dataset,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    hash_raw: bool = True,
    totals: Mapping[str, int] | None = None,
) -> VerifyReport:
    report = VerifyReport(dataset=dataset.name)
    tables = DATASET_TABLES.get(dataset.name, ())
    partitions = {table: {p.partition: p for p in lake.partitions(table)} for table in tables}
    objects = registry.objects(dataset=dataset.name, date_from=date_from, date_to=date_to)
    imported_dates: set[date] = set()

    for obj in objects:
        if obj.status is not ObjectStatus.IMPORTED or obj.current_revision is None:
            if obj.status is ObjectStatus.FAILED:
                report.issues.append(
                    Issue(dataset.name, obj.object_key, "failed_object", obj.last_error or "")
                )
            continue
        report.objects_checked += 1
        revision = registry.current_revision(dataset.name, obj.object_key)
        if revision is None:
            report.issues.append(
                Issue(
                    dataset.name, obj.object_key, "missing_revision", "current revision row absent"
                )
            )
            continue
        if obj.logical_date is not None:
            imported_dates.add(obj.logical_date)
            for index, table in enumerate(tables):
                name = partition_name(table, obj.logical_date)
                info = partitions[table].get(name)
                if info is None:
                    report.issues.append(
                        Issue(dataset.name, obj.object_key, "missing_partition", f"{table}/{name}")
                    )
                    continue
                if index == 0 and info.row_count != revision.observed_count:
                    report.issues.append(
                        Issue(
                            dataset.name,
                            obj.object_key,
                            "row_count",
                            f"{table}/{name} has {info.row_count} rows, "
                            f"registry says {revision.observed_count}",
                        )
                    )
                meta_rev = info.metadata.get("evedw.revision")
                if meta_rev is not None and meta_rev != str(revision.revision):
                    report.issues.append(
                        Issue(
                            dataset.name,
                            obj.object_key,
                            "stale_partition",
                            f"{table}/{name} was written by revision {meta_rev}, "
                            f"current is {revision.revision}",
                        )
                    )
        raw = settings.data_dir / revision.raw_path
        if not raw.is_file():
            report.issues.append(
                Issue(dataset.name, obj.object_key, "missing_raw", str(revision.raw_path))
            )
        elif hash_raw:
            report.raw_files_hashed += 1
            actual = sha256_file(raw)
            if actual != revision.sha256:
                report.issues.append(
                    Issue(
                        dataset.name,
                        obj.object_key,
                        "raw_hash",
                        f"{revision.raw_path} hashes to {actual[:12]}, "
                        f"registry says {revision.sha256[:12]}",
                    )
                )
        if totals is not None and obj.logical_date is not None:
            expected = totals.get(dataset.totals_key(obj.logical_date))
            if expected is not None and expected != revision.observed_count:
                report.issues.append(
                    Issue(
                        dataset.name,
                        obj.object_key,
                        "expected_count",
                        f"totals.json says {expected}, imported {revision.observed_count}",
                    )
                )

    if date_from is None and date_to is None:
        for table in tables:
            for name in partitions[table]:
                day = partition_date(name)
                if day is None or day not in imported_dates:
                    report.issues.append(
                        Issue(dataset.name, None, "orphan_partition", f"{table}/{name}")
                    )
    return report
