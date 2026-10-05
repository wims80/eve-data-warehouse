"""The ``evedw`` command line. Commands trigger jobs or report; import logic lives in jobs."""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from evedw import __version__
from evedw.config import Settings
from evedw.domain.datasets import DATASETS
from evedw.jobs.runner import LockHeldError, WriterLock
from evedw.logs import setup_logging
from evedw.store import open_registry

app = typer.Typer(no_args_is_help=True, add_completion=False, help="EVE data warehouse.")


@dataclass(slots=True)
class CliState:
    settings: Settings


def _state(ctx: typer.Context) -> CliState:
    state = ctx.obj
    if not isinstance(state, CliState):
        raise RuntimeError("CLI state missing")
    return state


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
    settings.ensure_dirs()
    try:
        with WriterLock(settings.lock_path):
            registry = open_registry(settings)
            try:
                before = registry.schema_version()
                after = registry.migrate()
            finally:
                registry.close()
    except LockHeldError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
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
