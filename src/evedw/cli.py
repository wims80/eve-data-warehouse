"""The ``evedw`` command line. Commands trigger jobs or report; import logic lives in jobs."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer

from evedw import __version__
from evedw.config import Settings
from evedw.domain.datasets import DATASETS, ENTITIES_BACKFILL, get_dataset
from evedw.domain.registry import ImportRun, Trigger
from evedw.domain.schemas import ENTITY_TABLES
from evedw.jobs.entities import ExportJob, RefreshJob
from evedw.jobs.importers import importer_for
from evedw.jobs.runner import JobRunner, LockHeldError, WriterLock
from evedw.jobs.sync import SyncJob
from evedw.jobs.verify import VerifyReport, verify_dataset
from evedw.logs import setup_logging
from evedw.sources.esi import EsiClient, load_policy
from evedw.sources.everef import EveRefClient
from evedw.store import open_entity_store, open_lake, open_registry, open_response_cache
from evedw.store.base import Registry

app = typer.Typer(no_args_is_help=True, add_completion=False, help="EVE data warehouse.")
entities_app = typer.Typer(no_args_is_help=True, help="Character, corporation and alliance tables.")
esi_app = typer.Typer(no_args_is_help=True, help="ESI policy state.")
app.add_typer(entities_app, name="entities")
app.add_typer(esi_app, name="esi")

DATE_FORMATS = ["%Y-%m-%d"]


@dataclass(slots=True)
class CliState:
    settings: Settings


def _state(ctx: typer.Context) -> CliState:
    state = ctx.obj
    if not isinstance(state, CliState):
        raise RuntimeError("CLI state missing")
    return state


def _day(value: datetime | None) -> date | None:
    return value.date() if value is not None else None


@contextmanager
def _writer(settings: Settings) -> Generator[tuple[WriterLock, Registry]]:
    settings.ensure_dirs()
    try:
        lock = WriterLock(settings.lock_path)
        lock.acquire()
    except LockHeldError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    registry = open_registry(settings)
    try:
        yield lock, registry
    finally:
        registry.close()
        lock.release()


def _require_migrated(registry: Registry) -> None:
    if registry.schema_version() == 0:
        typer.echo("error: registry is not migrated; run `evedw migrate`", err=True)
        raise typer.Exit(code=1)


def _run(runner: JobRunner, job: str, fn: Any, params: dict[str, Any]) -> ImportRun:
    try:
        return runner.run(job, fn, trigger=Trigger.CLI, params=params)
    except Exception as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None


def _print_run(run: ImportRun, *, objects: str = "objects imported") -> None:
    print(
        f"run {run.run_id} {run.status.value}: {run.objects_changed} {objects}, "
        f"{run.rows_written} rows written"
    )


def _esi_client(settings: Settings) -> EsiClient:
    return EsiClient(
        settings.esi_base_url,
        compatibility_date=settings.esi_compatibility_date,
        contact=settings.esi_contact,
        cache=open_response_cache(settings),
        policy_path=settings.esi_policy_path,
        daily_budget=settings.esi_daily_budget,
    )


@app.callback()
def main(
    ctx: typer.Context,
    data_dir: Annotated[
        Path | None, typer.Option("--data-dir", help="Overrides EVEDW_DATA_DIR.")
    ] = None,
    log_level: Annotated[
        str | None, typer.Option("--log-level", help="Overrides EVEDW_LOG_LEVEL.")
    ] = None,
) -> None:
    overrides: dict[str, object] = {}
    if data_dir is not None:
        overrides["data_dir"] = data_dir
    if log_level is not None:
        overrides["log_level"] = log_level
    settings = Settings(**overrides)  # type: ignore[arg-type]
    setup_logging(settings.log_level)
    ctx.obj = CliState(settings=settings)


@app.command()
def version() -> None:
    """Print the version."""
    print(__version__)


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Create or upgrade the registry schema."""
    settings = _state(ctx).settings
    with _writer(settings) as (_, registry):
        before = registry.schema_version()
        after = registry.migrate()
    if after == before:
        print(f"registry schema is current at version {after}")
    else:
        print(f"registry schema upgraded from version {before} to {after}")


@app.command()
def status(ctx: typer.Context) -> None:
    """Show datasets, object counts by status and recent runs."""
    settings = _state(ctx).settings
    if not settings.warehouse_path.exists():
        print(f"no warehouse at {settings.warehouse_path}; run `evedw migrate` first")
        raise typer.Exit(code=1)
    registry = open_registry(settings, read_only=True)
    try:
        version = registry.schema_version()
        if version == 0:
            print("registry is not migrated; run `evedw migrate`")
            raise typer.Exit(code=1)
        print(f"data dir      {settings.data_dir.resolve()}")
        print(f"schema        version {version}")
        summaries = {s.dataset: s for s in registry.dataset_summaries()}
        print()
        print(f"{'dataset':<20} {'parser':>6} {'imported':>9} {'pending':>8} {'other':>6}  range")
        for name, dataset in sorted(DATASETS.items()):
            summary = summaries.get(name)
            if summary is None:
                print(f"{name:<20} {dataset.parser_version:>6} {0:>9} {0:>8} {0:>6}  -")
                continue
            counts = summary.objects_by_status
            imported = counts.get("imported", 0)
            pending = sum(counts.get(s, 0) for s in ("new", "changed", "fetched", "failed"))
            other = sum(counts.values()) - imported - pending
            if summary.oldest_imported and summary.newest_imported:
                span = f"{summary.oldest_imported} .. {summary.newest_imported}"
            else:
                span = "-"
            print(
                f"{name:<20} {dataset.parser_version:>6} {imported:>9} {pending:>8} "
                f"{other:>6}  {span}"
            )
        runs = registry.runs(limit=10)
        print()
        if not runs:
            print("no runs recorded")
        else:
            print(f"{'started (UTC)':<20} {'job':<24} {'trigger':<8} {'status':<10} detail")
            for run in runs:
                started = run.started_at.strftime("%Y-%m-%d %H:%M:%S")
                detail = run.error or f"objects={run.objects_changed} rows={run.rows_written}"
                print(
                    f"{started:<20} {run.job:<24} {run.trigger.value:<8} "
                    f"{run.status.value:<10} {detail}"
                )
    finally:
        registry.close()


@app.command()
def sync(
    ctx: typer.Context,
    dataset: Annotated[str, typer.Argument(help="Dataset name, e.g. market_history.")],
    date_from: Annotated[
        datetime | None,
        typer.Option("--from", formats=DATE_FORMATS, help="Earliest day, inclusive."),
    ] = None,
    date_to: Annotated[
        datetime | None,
        typer.Option("--to", formats=DATE_FORMATS, help="Latest day, inclusive."),
    ] = None,
    force: Annotated[
        bool, typer.Option("--force", help="Re-import already imported days in range.")
    ] = False,
    sweep: Annotated[
        bool,
        typer.Option("--sweep", help="Re-check the headers of every file, not only recent ones."),
    ] = False,
) -> None:
    """Discover, fetch and import changed objects of a dataset, newest first."""
    settings = _state(ctx).settings
    try:
        ds = get_dataset(dataset)
    except KeyError as exc:
        typer.echo(f"error: {exc.args[0]}", err=True)
        raise typer.Exit(code=2) from None
    with _writer(settings) as (lock, registry):
        if registry.schema_version() == 0:
            typer.echo("error: registry is not migrated; run `evedw migrate`", err=True)
            raise typer.Exit(code=1)
        lake = open_lake(settings)
        client = EveRefClient(settings.everef_base_url, contact=settings.esi_contact)
        try:
            job = SyncJob(
                dataset=ds,
                client=client,
                importer=importer_for(ds, lake),
                date_from=_day(date_from),
                date_to=_day(date_to),
                force=force,
                sweep=sweep,
                head_days=settings.head_days,
            )
            params = {
                "from": _day(date_from),
                "to": _day(date_to),
                "force": force,
                "sweep": sweep,
            }
            try:
                run = JobRunner(settings, registry, lock).run(
                    f"sync:{ds.name}", job, trigger=Trigger.CLI, params=params
                )
            except Exception as exc:
                typer.echo(f"error: {exc}", err=True)
                raise typer.Exit(code=1) from None
        finally:
            client.close()
    print(
        f"run {run.run_id} {run.status.value}: {run.objects_changed} objects imported, "
        f"{run.rows_written} rows written"
    )


@app.command()
def verify(
    ctx: typer.Context,
    dataset: Annotated[str | None, typer.Option("--dataset", help="Limit to one dataset.")] = None,
    date_from: Annotated[datetime | None, typer.Option("--from", formats=DATE_FORMATS)] = None,
    date_to: Annotated[datetime | None, typer.Option("--to", formats=DATE_FORMATS)] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Machine-readable output.")] = False,
    no_hash: Annotated[
        bool, typer.Option("--no-hash", help="Skip hashing retained raw files.")
    ] = False,
    offline: Annotated[
        bool, typer.Option("--offline", help="Do not fetch totals.json for count checks.")
    ] = False,
) -> None:
    """Check registry, lake and raw files against each other and upstream totals."""
    settings = _state(ctx).settings
    names = [dataset] if dataset else [n for n in DATASETS if DATASETS[n].index_path]
    try:
        datasets = [get_dataset(n) for n in names]
    except KeyError as exc:
        typer.echo(f"error: {exc.args[0]}", err=True)
        raise typer.Exit(code=2) from None
    settings.ensure_dirs()
    registry = open_registry(settings, read_only=True)
    lake = open_lake(settings)
    client = (
        None if offline else EveRefClient(settings.everef_base_url, contact=settings.esi_contact)
    )
    failed = False
    try:
        reports: list[VerifyReport] = []
        for ds in datasets:
            totals = client.totals(ds) if client is not None and ds.totals_path else None
            report = verify_dataset(
                settings,
                registry,
                lake,
                ds,
                date_from=_day(date_from),
                date_to=_day(date_to),
                hash_raw=not no_hash,
                totals=totals,
            )
            reports.append(report)
            failed = failed or not report.ok
        if as_json:
            print("[" + ",\n".join(r.to_json() for r in reports) + "]")
        else:
            for report in reports:
                print(report.to_text())
    finally:
        registry.close()
        if client is not None:
            client.close()
    if failed:
        raise typer.Exit(code=1)


@app.command()
def views(ctx: typer.Context) -> None:
    """Print DuckDB view DDL for consumers that read the lake directly."""
    settings = _state(ctx).settings
    print(open_lake(settings).view_sql(), end="")


# --- entities -----------------------------------------------------------------------------


@entities_app.command("seed")
def entities_seed(
    ctx: typer.Context,
    snapshot: Annotated[
        str,
        typer.Argument(help="Backfill snapshot date (YYYY-MM-DD) or 'latest'."),
    ] = "latest",
    force: Annotated[bool, typer.Option("--force", help="Re-import an imported snapshot.")] = False,
) -> None:
    """Import an EVE Ref character/corporation/alliance backfill into the entity tables."""
    settings = _state(ctx).settings
    ds = ENTITIES_BACKFILL
    with _writer(settings) as (lock, registry):
        _require_migrated(registry)
        lake = open_lake(settings)
        entities = open_entity_store(settings)
        client = EveRefClient(settings.everef_base_url, contact=settings.esi_contact)
        try:
            if snapshot == "latest":
                listed = [o.logical_date for o in client.discover(ds, []) if o.logical_date]
                if not listed:
                    typer.echo("error: no backfill archives listed upstream", err=True)
                    raise typer.Exit(code=1)
                day = max(listed)
            else:
                try:
                    day = date.fromisoformat(snapshot)
                except ValueError:
                    typer.echo(f"error: not a date: {snapshot!r}", err=True)
                    raise typer.Exit(code=2) from None
            job = SyncJob(
                dataset=ds,
                client=client,
                importer=importer_for(ds, lake, entities=entities),
                date_from=day,
                date_to=day,
                force=force,
                head_days=settings.head_days,
            )
            run = _run(
                JobRunner(settings, registry, lock),
                "entities:seed",
                job,
                {"snapshot": day, "force": force},
            )
            counts = {table: entities.count(table) for table in ENTITY_TABLES}
        finally:
            client.close()
            entities.close()
    _print_run(run, objects="archives imported")
    for table, count in counts.items():
        print(f"{table:<30} {count:>12}")


@entities_app.command("refresh")
def entities_refresh(
    ctx: typer.Context,
    budget: Annotated[
        int | None,
        typer.Option(
            "--budget", help="Requests this run may send; the daily budget still applies."
        ),
    ] = None,
    no_populate: Annotated[
        bool,
        typer.Option("--no-populate", help="Do not scan recent killmails for new entity IDs."),
    ] = False,
) -> None:
    """Refresh queued entities from ESI within the request budget."""
    settings = _state(ctx).settings
    with _writer(settings) as (lock, registry):
        _require_migrated(registry)
        lake = open_lake(settings)
        entities = open_entity_store(settings)
        esi = _esi_client(settings)
        try:
            job = RefreshJob(
                esi,
                entities,
                lake,
                recent_days=settings.esi_recent_days,
                refresh_interval=settings.esi_refresh_interval,
                budget=budget,
                populate=not no_populate,
            )
            run = _run(
                JobRunner(settings, registry, lock),
                "entities:refresh",
                job,
                {"budget": budget, "populate": not no_populate},
            )
            policy = esi.policy
        finally:
            esi.close()
            entities.close()
    _print_run(run, objects="entities refreshed")
    print(
        f"esi requests={policy.requests} cache_hits={policy.cache_hits} "
        f"budget_used_today={policy.budget_used}"
    )


@entities_app.command("export")
def entities_export(ctx: typer.Context) -> None:
    """Write the entity tables to Parquet under the lake for consumers."""
    settings = _state(ctx).settings
    with _writer(settings) as (lock, registry):
        _require_migrated(registry)
        entities = open_entity_store(settings)
        try:
            run = _run(
                JobRunner(settings, registry, lock),
                "entities:export",
                ExportJob(entities, settings.entities_dir),
                {"directory": str(settings.entities_dir)},
            )
        finally:
            entities.close()
    _print_run(run, objects="files written")


@entities_app.command("status")
def entities_status(ctx: typer.Context) -> None:
    """Row counts of the entity tables."""
    settings = _state(ctx).settings
    with _writer(settings) as (_, registry):
        _require_migrated(registry)
        entities = open_entity_store(settings)
        try:
            for table in ENTITY_TABLES:
                print(f"{table:<30} {entities.count(table):>12}")
        finally:
            entities.close()


# --- esi ----------------------------------------------------------------------------------


@esi_app.command("status")
def esi_status(ctx: typer.Context) -> None:
    """Show the persisted ESI policy state: stops, cooldowns, budget, counters."""
    settings = _state(ctx).settings
    policy = load_policy(settings.esi_policy_path)
    now = int(datetime.now(UTC).timestamp())
    print(f"stopped        {policy.stopped or 'no'}")
    blocked = policy.blocked_until - now
    print(f"blocked        {f'{blocked}s' if blocked > 0 else 'no'}")
    print(
        f"budget         {policy.budget_used} of {settings.esi_daily_budget} used "
        f"on {policy.budget_day or 'no day yet'}"
    )
    print(
        f"counters       requests={policy.requests} cache_hits={policy.cache_hits} "
        f"retries={policy.retries} pauses={policy.pauses}"
    )
    for route, group in sorted(policy.routes.items()):
        bucket = policy.buckets.get(group)
        detail = (
            f"{bucket.remaining}/{bucket.limit} per {bucket.window}s" if bucket else "no bucket"
        )
        print(f"  {route:<45} {group:<12} {detail}")


@esi_app.command("resume")
def esi_resume(ctx: typer.Context) -> None:
    """Clear a 403/420 stop after reviewing why it happened."""
    settings = _state(ctx).settings
    policy = load_policy(settings.esi_policy_path)
    if not policy.stopped:
        print("ESI is not stopped")
        return
    with _writer(settings):
        esi = _esi_client(settings)
        try:
            esi.resume()
        finally:
            esi.close()
    print(f"cleared stop: {policy.stopped}")
