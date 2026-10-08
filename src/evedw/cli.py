"""The ``evedw`` command line. Commands trigger jobs or report; import logic lives in jobs.

A job command first looks for a running service (the writer lock names it). If there is
one, the job is queued over HTTP and the command follows the run until it finishes. If
there is none, the command takes the writer lock and runs the same job in-process.
"""

import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer

from evedw import __version__
from evedw.config import Settings
from evedw.domain.datasets import DATASETS, get_dataset
from evedw.domain.registry import DatasetSummary, ImportRun, RunStatus, Trigger
from evedw.domain.schemas import ENTITY_TABLES
from evedw.domain.speed import Sample, describe
from evedw.jobs.catalog import JobCatalog, UnknownJobError
from evedw.jobs.refresh import refresh_status
from evedw.jobs.runner import JobCancelled, LockHeldError, WriterLock
from evedw.jobs.verify import VerifyReport, verify_dataset
from evedw.logs import setup_logging
from evedw.service.client import ServiceClient, ServiceError, find_service
from evedw.sources.esi import load_policy, save_policy
from evedw.sources.everef import EveRefClient
from evedw.store import open_entity_store, open_lake, open_registry
from evedw.store.base import Registry

app = typer.Typer(no_args_is_help=True, help="EVE data warehouse.")
entities_app = typer.Typer(no_args_is_help=True, help="Character, corporation and alliance tables.")
esi_app = typer.Typer(no_args_is_help=True, help="ESI policy state.")
app.add_typer(entities_app, name="entities")
app.add_typer(esi_app, name="esi")

DATE_FORMATS = ["%Y-%m-%d"]

NoWait = Annotated[
    bool,
    typer.Option(
        "--no-wait",
        help="When a service runs the job, return after queueing instead of following it.",
    ),
]


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


def _fail(message: str, *, code: int = 1) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=code)


@contextmanager
def _writer(settings: Settings) -> Generator[tuple[WriterLock, Registry]]:
    settings.ensure_dirs()
    try:
        lock = WriterLock(settings.lock_path)
        lock.acquire()
    except LockHeldError as exc:
        raise _fail(str(exc)) from None
    registry = open_registry(settings)
    try:
        yield lock, registry
    finally:
        registry.close()
        lock.release()


def _require_migrated(registry: Registry) -> None:
    if registry.schema_version() == 0:
        raise _fail("registry is not migrated; run `evedw migrate`")


@contextmanager
def _service(settings: Settings) -> Generator[ServiceClient | None]:
    client = find_service(settings)
    try:
        yield client
    except ServiceError as exc:
        raise _fail(str(exc)) from None
    finally:
        if client is not None:
            client.close()


def _dispatch(
    settings: Settings, job: str, params: dict[str, Any], *, wait: bool = True
) -> ImportRun | None:
    """Queue ``job`` on the running service, or run it here under the writer lock.

    Returns the finished run, or ``None`` when it was queued and not awaited. Exits
    non-zero on a failed run, a bad parameter or an unreachable service.
    """
    with _service(settings) as service:
        if service is not None:
            run_id = service.trigger(job, params)
            print(f"queued run {run_id} for {job} on {service.base_url}")
            if not wait:
                return None
            try:
                run = service.wait(run_id)
            except KeyboardInterrupt:
                print(f"\nrun {run_id} continues in the service; `evedw status` shows it")
                raise typer.Exit(code=130) from None
            if run.status is not RunStatus.SUCCEEDED:
                raise _fail(f"run {run_id} {run.status.value}: {run.error}")
            return run
    with _writer(settings) as (lock, registry):
        _require_migrated(registry)
        entities = open_entity_store(settings)
        try:
            catalog = JobCatalog(settings, registry, open_lake(settings), entities)
            try:
                return catalog.run(job, params, trigger=Trigger.CLI, lock=lock)
            except (UnknownJobError, ValueError) as exc:
                raise _fail(str(exc.args[0]), code=2) from None
            except JobCancelled:
                raise typer.Exit(code=130) from None
            except Exception as exc:
                raise _fail(str(exc)) from None
        finally:
            entities.close()


def _print_run(run: ImportRun | None, *, objects: str = "objects imported") -> None:
    if run is None:
        return
    print(
        f"run {run.run_id} {run.status.value}: {run.objects_changed} {objects}, "
        f"{run.rows_written} rows written"
    )


@app.callback()
def main(
    ctx: typer.Context,
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir", help="Overrides EVEDW_DATA_DIR. Relative to the current directory."
        ),
    ] = None,
    log_level: Annotated[
        str | None, typer.Option("--log-level", help="Overrides EVEDW_LOG_LEVEL.")
    ] = None,
) -> None:
    overrides: dict[str, object] = {}
    if data_dir is not None:
        overrides["data_dir"] = data_dir.resolve()  # relative to where it was typed
    if log_level is not None:
        overrides["log_level"] = log_level
    settings = Settings.load(**overrides)
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
def serve(
    ctx: typer.Context,
    bind: Annotated[
        str | None, typer.Option("--bind", help="host:port, overrides EVEDW_BIND.")
    ] = None,
) -> None:
    """Run the warehouse service: HTTP API plus the job scheduler. Holds the writer lock."""
    import uvicorn

    from evedw.service.app import create_app

    settings = _state(ctx).settings
    if bind is not None:
        settings.bind = bind
    # Fail with a plain message before uvicorn starts; its own startup failure exits 3.
    settings.ensure_dirs()
    holder = WriterLock(settings.lock_path).holder()
    if holder is not None:
        raise _fail(str(LockHeldError(settings.lock_path, holder)))
    if not settings.warehouse_path.exists():
        raise _fail("registry is not migrated; run `evedw migrate`")
    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_config=None,
        access_log=False,
    )


def _print_status(
    settings: Settings,
    version: int | None,
    summaries: list[DatasetSummary],
    runs: list[ImportRun],
    *,
    service_url: str | None,
) -> None:
    print(f"data dir      {settings.data_dir.resolve()}")
    if version is not None:
        print(f"schema        version {version}")
    print(f"service       {service_url or 'not running'}")
    by_name = {s.dataset: s for s in summaries}
    print()
    print(f"{'dataset':<20} {'parser':>6} {'imported':>9} {'pending':>8} {'other':>6}  range")
    for name, dataset in sorted(DATASETS.items()):
        summary = by_name.get(name)
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
            f"{name:<20} {dataset.parser_version:>6} {imported:>9} {pending:>8} {other:>6}  {span}"
        )
    print()
    if not runs:
        print("no runs recorded")
        return
    print(f"{'started (UTC)':<20} {'job':<24} {'trigger':<8} {'status':<10} detail")
    for run in runs:
        started = run.started_at.strftime("%Y-%m-%d %H:%M:%S")
        detail = run.error or f"objects={run.objects_changed} rows={run.rows_written}"
        print(f"{started:<20} {run.job:<24} {run.trigger.value:<8} {run.status.value:<10} {detail}")


@app.command()
def status(ctx: typer.Context) -> None:
    """Show datasets, object counts by status and recent runs."""
    settings = _state(ctx).settings
    with _service(settings) as service:
        if service is not None:
            summaries, _ = service.datasets()
            _print_status(
                settings, None, summaries, service.runs(limit=10), service_url=service.base_url
            )
            return
    if not settings.warehouse_path.exists():
        print(f"no warehouse at {settings.warehouse_path}; run `evedw migrate` first")
        raise typer.Exit(code=1)
    registry = open_registry(settings, read_only=True)
    try:
        version = registry.schema_version()
        if version == 0:
            print("registry is not migrated; run `evedw migrate`")
            raise typer.Exit(code=1)
        _print_status(
            settings,
            version,
            registry.dataset_summaries(),
            registry.runs(limit=10),
            service_url=None,
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
    offline: Annotated[
        bool,
        typer.Option(
            "--offline",
            help="No network: import only from retained raw files. "
            "With --force, rebuilds the lake from raw/.",
        ),
    ] = False,
    no_wait: NoWait = False,
) -> None:
    """Discover, fetch and import changed objects of a dataset, newest first."""
    settings = _state(ctx).settings
    try:
        ds = get_dataset(dataset)
    except KeyError as exc:
        raise _fail(exc.args[0], code=2) from None
    if ds.index_path is None:
        raise _fail(f"{ds.name} is not synced by date; use `evedw entities seed`", code=2)
    run = _dispatch(
        settings,
        f"sync:{ds.name}",
        {
            "from": _day(date_from),
            "to": _day(date_to),
            "force": force,
            "sweep": sweep,
            "offline": offline,
        },
        wait=not no_wait,
    )
    _print_run(run)


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
    no_wait: NoWait = False,
) -> None:
    """Check registry, lake and raw files against each other and upstream totals.

    With a service running, the check runs there as a `verify` job and this command
    reports the run; the issue list is in the service log."""
    settings = _state(ctx).settings
    names = [dataset] if dataset else [n for n in DATASETS if DATASETS[n].index_path]
    try:
        datasets = [get_dataset(n) for n in names]
    except KeyError as exc:
        raise _fail(exc.args[0], code=2) from None
    with _service(settings) as service:
        if service is not None:
            run_id = service.trigger(
                "verify",
                {
                    "dataset": dataset,
                    "from": _day(date_from),
                    "to": _day(date_to),
                    "hash": not no_hash,
                    "offline": offline,
                },
            )
            print(f"queued run {run_id} for verify on {service.base_url}")
            if no_wait:
                return
            run = service.wait(run_id)
            if run.status is RunStatus.SUCCEEDED:
                print(f"run {run_id} succeeded: {run.objects_changed} objects checked, no issues")
                return
            raise _fail(f"run {run_id} {run.status.value}: {run.error}")
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
        typer.Argument(
            help="Backfill snapshot date (YYYY-MM-DD), 'latest', or 'all' for every "
            "archive not yet imported."
        ),
    ] = "latest",
    force: Annotated[bool, typer.Option("--force", help="Re-import an imported snapshot.")] = False,
    no_wait: NoWait = False,
) -> None:
    """Import an EVE Ref character/corporation/alliance backfill into the entity tables."""
    settings = _state(ctx).settings
    if snapshot not in ("latest", "all"):
        try:
            date.fromisoformat(snapshot)
        except ValueError:
            raise _fail(f"not a date: {snapshot!r}", code=2) from None
    run = _dispatch(
        settings, "entities:seed", {"snapshot": snapshot, "force": force}, wait=not no_wait
    )
    _print_run(run, objects="archives imported")
    if run is not None:
        _print_entity_counts(settings)


def _print_entity_counts(settings: Settings) -> None:
    with _service(settings) as service:
        if service is not None:
            _, counts = service.datasets()
            queue, sweeps = service.entity_refresh()
        else:
            with _writer(settings) as (_, registry):
                _require_migrated(registry)
                entities = open_entity_store(settings)
                try:
                    counts = {table: entities.count(table) for table in ENTITY_TABLES}
                finally:
                    entities.close()
                queue, sweeps = refresh_status(registry, now=datetime.now(UTC))
    for table, count in counts.items():
        print(f"{table:<30} {count:>12}")
    print()
    print(f"{'refresh queue':<30} {'entries':>12} {'due':>12}")
    for cls, numbers in queue.items():
        print(f"  {cls:<28} {numbers['entries']:>12} {numbers['due']:>12}")
    print()
    for name, value in sweeps.items():
        detail = ", ".join(f"{k}={v}" for k, v in sorted(value.items())) or "not started"
        print(f"{name:<22} {detail}")


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
        typer.Option("--no-populate", help="Only drain the queue: no sweeps, no new work."),
    ] = False,
    no_wait: NoWait = False,
) -> None:
    """Refresh queued entities from ESI within the request budget."""
    settings = _state(ctx).settings
    run = _dispatch(
        settings,
        "entities:refresh",
        {"budget": budget, "populate": not no_populate},
        wait=not no_wait,
    )
    _print_run(run, objects="entities refreshed")
    if run is not None:
        policy = load_policy(settings.esi_policy_path)
        print(
            f"esi requests={policy.requests} cache_hits={policy.cache_hits} "
            f"budget_used_today={policy.budget_used}"
        )


@entities_app.command("export")
def entities_export(ctx: typer.Context, no_wait: NoWait = False) -> None:
    """Write the entity tables to Parquet under the lake for consumers."""
    settings = _state(ctx).settings
    run = _dispatch(settings, "entities:export", {}, wait=not no_wait)
    _print_run(run, objects="files written")


@entities_app.command("status")
def entities_status(ctx: typer.Context) -> None:
    """Row counts of the entity tables."""
    _print_entity_counts(_state(ctx).settings)


def _speed_sample(settings: Settings) -> Sample:
    policy = load_policy(settings.esi_policy_path)
    characters: int | None = None
    with _service(settings) as service:
        if service is not None:
            _, counts = service.datasets()
            queue, sweeps = service.entity_refresh()
            characters = counts.get("characters")
        else:
            if not settings.warehouse_path.exists():
                raise _fail(f"no warehouse at {settings.warehouse_path}; run `evedw migrate`")
            registry = open_registry(settings, read_only=True)
            try:
                _require_migrated(registry)
                queue, sweeps = refresh_status(registry, now=datetime.now(UTC))
            finally:
                registry.close()
    return Sample(
        at=datetime.now(UTC),
        esi_requests=policy.requests,
        budget_used=settings.esi_daily_budget
        - policy.budget_remaining(settings.esi_daily_budget, datetime.now(UTC).date()),
        pace=max(policy.spacing, settings.esi_spacing),
        cycle=dict(sweeps.get("affiliation_cycle", {})),
        queue={name: numbers["entries"] for name, numbers in queue.items()},
        characters=characters,
    )


@app.command()
def speed(
    ctx: typer.Context,
    seconds: Annotated[
        int, typer.Option("--seconds", min=1, help="Length of the measuring window.")
    ] = 60,
    watch: Annotated[
        bool, typer.Option("--watch", help="Keep measuring, one report per window, until Ctrl-C.")
    ] = False,
) -> None:
    """Measure ESI requests, affiliation progress and queue growth over a window.

    Reads the counters twice, ``--seconds`` apart; sends no ESI requests."""
    settings = _state(ctx).settings
    before = _speed_sample(settings)
    try:
        while True:
            time.sleep(seconds)
            after = _speed_sample(settings)
            print("\n".join(describe(before, after, budget=settings.esi_daily_budget)))
            if not watch:
                return
            print()
            before = after
    except KeyboardInterrupt:
        return


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
    if policy.downtime_since:
        since = datetime.fromtimestamp(policy.downtime_since, tz=UTC)
        print(f"downtime       paused since {since:%H:%M:%S} UTC ({policy.downtime_reason})")
    elif policy.resumed_at:
        resumed = datetime.fromtimestamp(policy.resumed_at, tz=UTC)
        print(f"downtime       no; last resumed {resumed:%Y-%m-%d %H:%M} UTC")
    else:
        print("downtime       no")
    print(
        f"pace           {max(policy.spacing, settings.esi_spacing):.2f}s between requests "
        f"(calm {settings.esi_spacing:.2f}s), {policy.slowdowns} slowdowns"
    )
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
    """Clear a 403/420 stop after reviewing why it happened.

    Only the policy file changes. A running service reads it afresh for every refresh
    job, and nothing talks to ESI while the stop is set, so no lock is needed."""
    settings = _state(ctx).settings
    policy = load_policy(settings.esi_policy_path)
    if not policy.stopped:
        print("ESI is not stopped")
        return
    reason = policy.stopped
    policy.stopped = None
    save_policy(settings.esi_policy_path, policy)
    print(f"cleared stop: {reason}")
