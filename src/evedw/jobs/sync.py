"""Generic dataset sync: discover, fetch, import, newest first.

The loop is the same for every index-driven dataset. What differs per dataset is the
``Importer`` that turns a retained raw file into lake partitions.
"""

import logging
import shutil
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

from evedw.config import Settings
from evedw.domain.datasets import Dataset
from evedw.domain.registry import (
    PENDING_STATUSES,
    NewRevision,
    ObjectStatus,
    RevisionStatus,
    SourceObject,
    SourceRevision,
)
from evedw.jobs.runner import JobOutcome, RunContext
from evedw.logs import log_context
from evedw.sources.everef import EveRefClient
from evedw.store.base import PartitionInfo

log = logging.getLogger(__name__)


class DiskSpaceError(RuntimeError):
    pass


class SyncError(RuntimeError):
    """One or more objects failed; the successful ones stay promoted."""


@dataclass(frozen=True, slots=True)
class ImportResult:
    rows: int
    partitions: list[PartitionInfo] = field(default_factory=lambda: [])


class Importer(Protocol):
    @property
    def dataset(self) -> Dataset: ...

    def import_revision(
        self,
        ctx: RunContext,
        obj: SourceObject,
        revision: SourceRevision,
        *,
        raw_file: Path,
        scratch_dir: Path,
    ) -> ImportResult:
        """Normalise ``raw_file`` into the lake. Must not touch the registry."""
        ...


def ensure_free_space(settings: Settings) -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    free_gb = shutil.disk_usage(settings.data_dir).free / 1e9
    if free_gb < settings.min_free_gb:
        raise DiskSpaceError(
            f"{free_gb:.1f} GB free under {settings.data_dir}, floor is {settings.min_free_gb} GB"
        )


def split_object_name(object_key: str) -> tuple[str, str, str]:
    """``2026/market-history-2026-10-01.csv.bz2`` -> (``2026``, ``market-history-2026-10-01``,
    ``.csv.bz2``)."""
    year, _, name = object_key.partition("/")
    suffix = "".join(Path(name).suffixes)
    stem = name[: -len(suffix)] if suffix else name
    return year, stem, suffix


def raw_dir_for(settings: Settings, dataset: Dataset, object_key: str) -> Path:
    year, stem, _ = split_object_name(object_key)
    return settings.raw_dir / dataset.name / year / stem


@dataclass(slots=True)
class SyncJob:
    """Callable job: ``runner.run("sync:<dataset>", SyncJob(...), ...)``."""

    dataset: Dataset
    client: EveRefClient
    importer: Importer
    date_from: date | None = None
    date_to: date | None = None
    force: bool = False
    today: date | None = None

    def __call__(self, ctx: RunContext) -> JobOutcome:
        with log_context(dataset=self.dataset.name):
            return self._run(ctx)

    # -- discovery -----------------------------------------------------------------------

    def _years(self) -> list[int]:
        today = self.today or datetime.now(UTC).date()
        years = list(self.dataset.years(today))
        if self.date_from is not None:
            years = [y for y in years if y >= self.date_from.year]
        if self.date_to is not None:
            years = [y for y in years if y <= self.date_to.year]
        return years

    def discover(self, ctx: RunContext) -> int:
        registry = ctx.registry
        now = datetime.now(UTC)
        found = self.client.discover(self.dataset, self._years())
        summary = registry.upsert_objects(found, now=now)
        gone = 0
        if self.date_from is None and self.date_to is None:
            gone = registry.mark_missing(self.dataset.name, {o.object_key for o in found}, now=now)
        outdated = registry.mark_parser_outdated(self.dataset.name, self.dataset.parser_version)
        forced = 0
        if self.force:
            for obj in registry.objects(
                dataset=self.dataset.name,
                status=ObjectStatus.IMPORTED,
                date_from=self.date_from,
                date_to=self.date_to,
            ):
                registry.mark(self.dataset.name, obj.object_key, ObjectStatus.CHANGED)
                forced += 1
        log.info(
            "discovered %d objects: %d new, %d changed, %d unchanged, %d gone, "
            "%d parser-outdated, %d forced",
            summary.total,
            summary.new,
            summary.changed,
            summary.unchanged,
            gone,
            outdated,
            forced,
        )
        return summary.new + summary.changed + outdated + forced

    # -- fetch and import ----------------------------------------------------------------

    def _run(self, ctx: RunContext) -> JobOutcome:
        self.discover(ctx)
        pending = ctx.registry.objects(
            dataset=self.dataset.name,
            status=PENDING_STATUSES,
            date_from=self.date_from,
            date_to=self.date_to,
            newest_first=True,
        )
        log.info("%d objects pending", len(pending))
        scratch = ctx.settings.scratch_dir / ctx.run_id
        scratch.mkdir(parents=True, exist_ok=True)
        imported = 0
        rows = 0
        failures: list[str] = []
        try:
            for obj in pending:
                with log_context(object_key=obj.object_key):
                    try:
                        rows += self._process(ctx, obj, scratch)
                        imported += 1
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:
                        log.exception("object failed")
                        failures.append(f"{obj.object_key}: {type(exc).__name__}: {exc}")
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        if failures:
            raise SyncError(
                f"{len(failures)} of {len(pending)} objects failed "
                f"({imported} imported): " + "; ".join(failures[:5])
            )
        return JobOutcome(objects_changed=imported, rows_written=rows)

    def _process(self, ctx: RunContext, obj: SourceObject, scratch: Path) -> int:
        registry = ctx.registry
        settings = ctx.settings
        revision: SourceRevision | None = None
        try:
            revision = self._reusable_revision(ctx, obj)
            if revision is None:
                ensure_free_space(settings)
                registry.mark(self.dataset.name, obj.object_key, ObjectStatus.FETCHING)
                revision = self._fetch(ctx, obj)
            registry.mark(self.dataset.name, obj.object_key, ObjectStatus.IMPORTING)
            raw_file = settings.data_dir / revision.raw_path
            result = self.importer.import_revision(
                ctx, obj, revision, raw_file=raw_file, scratch_dir=scratch
            )
            verified = None if obj.expected_count is None else result.rows == obj.expected_count
            registry.promote(
                self.dataset.name,
                obj.object_key,
                revision.revision,
                observed_count=result.rows,
                verified=verified,
                imported_at=datetime.now(UTC),
            )
            if verified is False:
                log.warning(
                    "imported revision %d: %d rows, totals.json expected %d",
                    revision.revision,
                    result.rows,
                    obj.expected_count,
                )
            else:
                log.info("imported revision %d: %d rows", revision.revision, result.rows)
            return result.rows
        except KeyboardInterrupt:
            registry.mark(
                self.dataset.name, obj.object_key, ObjectStatus.FAILED, error="interrupted"
            )
            raise
        except Exception as exc:
            if revision is not None and revision.status is RevisionStatus.FETCHED:
                registry.set_revision_status(
                    self.dataset.name, obj.object_key, revision.revision, RevisionStatus.FAILED
                )
            registry.mark(
                self.dataset.name,
                obj.object_key,
                ObjectStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

    def _reusable_revision(self, ctx: RunContext, obj: SourceObject) -> SourceRevision | None:
        """A retained raw file for this exact upstream etag, if we still have one.

        Covers a failed import of an already fetched file and a parser version bump. The
        latter gets a fresh revision row pointing at the same raw file so the chain shows
        which parser produced the live data.
        """
        if obj.upstream_etag is None:
            return None
        candidates = [
            r
            for r in reversed(ctx.registry.revisions(self.dataset.name, obj.object_key))
            if r.upstream_etag == obj.upstream_etag
            and (ctx.settings.data_dir / r.raw_path).is_file()
        ]
        if not candidates:
            return None
        latest = candidates[0]
        if (
            latest.status is RevisionStatus.FETCHED
            and latest.parser_version == self.dataset.parser_version
        ):
            log.info("reusing fetched revision %d", latest.revision)
            return latest
        log.info("reusing raw file of revision %d", latest.revision)
        return ctx.registry.add_revision(
            NewRevision(
                dataset=self.dataset.name,
                object_key=obj.object_key,
                sha256=latest.sha256,
                size=latest.size,
                raw_path=latest.raw_path,
                upstream_etag=latest.upstream_etag,
                upstream_last_modified=latest.upstream_last_modified,
                parser_version=self.dataset.parser_version,
                fetched_at=latest.fetched_at,
            )
        )

    def _fetch(self, ctx: RunContext, obj: SourceObject) -> SourceRevision:
        settings = ctx.settings
        _, _, suffix = split_object_name(obj.object_key)
        into_dir = raw_dir_for(settings, self.dataset, obj.object_key)
        download = self.client.download(
            obj.url,
            into_dir=into_dir,
            suffix=suffix,
            expected_size=obj.upstream_size,
            expected_etag=obj.upstream_etag,
        )
        log.info("fetched %d bytes sha256=%s", download.size, download.sha256[:12])
        revision = ctx.registry.add_revision(
            NewRevision(
                dataset=self.dataset.name,
                object_key=obj.object_key,
                sha256=download.sha256,
                size=download.size,
                raw_path=str(download.path.relative_to(settings.data_dir)),
                upstream_etag=download.etag or obj.upstream_etag,
                upstream_last_modified=download.last_modified or obj.upstream_last_modified,
                parser_version=self.dataset.parser_version,
                fetched_at=datetime.now(UTC),
            )
        )
        ctx.registry.mark(self.dataset.name, obj.object_key, ObjectStatus.FETCHED)
        return revision


def partition_metadata(
    dataset: Dataset,
    obj: SourceObject,
    revision: SourceRevision,
    *,
    extra: Iterable[tuple[str, str]] = (),
) -> dict[str, str]:
    meta = {
        "evedw.dataset": dataset.name,
        "evedw.object_key": obj.object_key,
        "evedw.revision": str(revision.revision),
        "evedw.source_sha256": revision.sha256,
        "evedw.parser_version": str(revision.parser_version),
        "evedw.written_at": datetime.now(UTC).isoformat(),
    }
    meta.update(dict(extra))
    return meta
