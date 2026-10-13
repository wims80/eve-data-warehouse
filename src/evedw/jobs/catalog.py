"""The named jobs a trigger can start, built from a job name and plain parameters.

The CLI, the HTTP trigger and the scheduler all come through ``JobCatalog.run`` so that
manual and scheduled runs execute identical code (design §3). Parameters arrive as JSON
values or CLI values; ``normalise_params`` validates them and produces the dict that is
recorded in the run log.
"""

import logging
import threading
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

from evedw.config import Settings
from evedw.domain.datasets import ENTITIES_BACKFILL, SYNC_DATASETS, Dataset, get_dataset
from evedw.domain.ids import RunId
from evedw.domain.registry import ImportRun, Trigger
from evedw.jobs.discovery import DiscoveryJob
from evedw.jobs.entities import ExportJob
from evedw.jobs.focus import KINDS as FOCUS_KINDS
from evedw.jobs.focus import FocusJob
from evedw.jobs.importers import importer_for
from evedw.jobs.killmail_merge import MergeJob
from evedw.jobs.refresh import RefreshJob
from evedw.jobs.runner import JobFn, JobOutcome, JobRunner, RunContext, WriterLock
from evedw.jobs.sync import SyncJob
from evedw.jobs.verify import VerifyJob
from evedw.sources.esi import EsiClient
from evedw.sources.evekill import PROCESS_GATE as EVEKILL_GATE
from evedw.sources.evekill import EveKillClient
from evedw.sources.everef import EveRefClient
from evedw.sources.zkillboard import PROCESS_GATE, ZKillboardClient
from evedw.store import open_response_cache
from evedw.store.base import EntityStore, Lake, Registry

log = logging.getLogger(__name__)

SYNC_JOBS = tuple(f"sync:{name}" for name in SYNC_DATASETS)
JOB_NAMES: tuple[str, ...] = (
    *SYNC_JOBS,
    "killmails:merge",
    "entities:seed",
    "entities:refresh",
    "entities:export",
    "entities:add",
    "entities:discover",
    "verify",
)

_PARAMS: dict[str, dict[str, str]] = {
    "sync": {
        "from": "date",
        "to": "date",
        "force": "bool",
        "sweep": "bool",
        "offline": "bool",
        "max_objects": "int",
    },
    "killmails:merge": {"from": "date", "to": "date"},
    "entities:seed": {"snapshot": "snapshot", "force": "bool"},
    "entities:refresh": {"budget": "int", "populate": "bool"},
    "entities:export": {},
    "entities:add": {"kind": "entity_kind", "ids": "ids", "names": "names", "members": "bool"},
    "entities:discover": {"full_pages": "int"},
    "verify": {
        "dataset": "dataset",
        "from": "date",
        "to": "date",
        "hash": "bool",
        "offline": "bool",
    },
}
"""Accepted parameter names and kinds per job. ``sync`` covers every ``sync:*`` job."""

_DEFAULTS: dict[str, dict[str, Any]] = {
    "sync": {
        "from": None,
        "to": None,
        "force": False,
        "sweep": False,
        "offline": False,
        "max_objects": None,
    },
    "killmails:merge": {"from": None, "to": None},
    "entities:seed": {"snapshot": "latest", "force": False},
    "entities:refresh": {"budget": None, "populate": True},
    "entities:export": {},
    "entities:add": {"kind": None, "ids": None, "names": None, "members": False},
    "entities:discover": {"full_pages": None},
    "verify": {"dataset": None, "from": None, "to": None, "hash": True, "offline": False},
}


class UnknownJobError(KeyError):
    pass


def _spec_key(name: str) -> str:
    if name in SYNC_JOBS:
        return "sync"
    if name in JOB_NAMES:
        return name
    raise UnknownJobError(f"unknown job {name!r}; known: {', '.join(JOB_NAMES)}")


def _convert(name: str, kind: str, value: Any) -> Any:
    if value is None:
        return None
    if kind == "date":
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                raise ValueError(f"{name}: not a date: {value!r}") from None
    elif kind == "bool":
        if isinstance(value, bool):
            return value
    elif kind == "int":
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    elif kind == "snapshot":
        if value in ("latest", "all"):
            return value
        return _convert(name, "date", value)
    elif kind == "entity_kind":
        if value in FOCUS_KINDS:
            return value
        raise ValueError(f"{name}: expected one of {', '.join(FOCUS_KINDS)}, got {value!r}")
    elif kind == "ids":
        return _ids(name, value)
    elif kind == "names":
        return _names(name, value)
    elif kind == "dataset":
        if isinstance(value, str):
            try:
                get_dataset(value)
            except KeyError as exc:
                raise ValueError(f"{name}: {exc.args[0]}") from None
            return value
    raise ValueError(f"{name}: expected {kind}, got {value!r}")


def _names(name: str, value: Any) -> list[str]:
    """One or more names, as a list (names may contain commas, so no string form)."""
    if not isinstance(value, list | tuple):
        raise ValueError(f"{name}: expected a list of names")
    names = sorted({str(v).strip() for v in value if str(v).strip()})  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    if not names or any(len(n) > 100 for n in names):
        raise ValueError(f"{name}: expected one or more names of at most 100 characters")
    return names


def _ids(name: str, value: Any) -> list[int]:
    """One or more positive ids, as a list or a comma-separated string."""
    parts: list[Any] = []
    if isinstance(value, str):
        parts = value.split(",")
    elif isinstance(value, list | tuple):
        parts = list(value)  # pyright: ignore[reportUnknownArgumentType]
    ids: set[int] = set()
    for part in parts:
        number = _convert(name, "int", part.strip() if isinstance(part, str) else part)
        if number is None or number <= 0:
            raise ValueError(f"{name}: expected positive ids, got {value!r}")
        ids.add(number)
    if not ids:
        raise ValueError(f"{name}: expected one or more ids")
    return sorted(ids)


def normalise_params(name: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate ``raw`` against the job's parameter set and fill in defaults. Raises
    ``UnknownJobError`` for an unknown job and ``ValueError`` for bad parameters."""
    key = _spec_key(name)
    spec = _PARAMS[key]
    unknown = sorted(set(raw) - set(spec))
    if unknown:
        raise ValueError(f"{name}: unknown parameters {unknown}; accepted: {sorted(spec)}")
    params = dict(_DEFAULTS[key])
    for field, kind in spec.items():
        if field in raw:
            params[field] = _convert(field, kind, raw[field])
    if key == "sync" and params["from"] and params["to"] and params["from"] > params["to"]:
        raise ValueError(f"{name}: from {params['from']} is after to {params['to']}")
    if key == "entities:add":
        if params["kind"] is None or (params["ids"] is None and params["names"] is None):
            raise ValueError(f"{name}: kind and ids or names are required")
        if params["members"] and params["kind"] == "character":
            raise ValueError(f"{name}: members applies to a corporation or an alliance")
    return params


@dataclass(slots=True)
class JobCatalog:
    """Builds and runs jobs against long-lived store handles."""

    settings: Settings
    registry: Registry
    lake: Lake
    entities: EntityStore

    def run(
        self,
        name: str,
        params: Mapping[str, Any],
        *,
        trigger: Trigger,
        lock: WriterLock,
        run_id: RunId | None = None,
        cancel: threading.Event | None = None,
    ) -> ImportRun:
        """Run a job to completion under the run log. Re-raises the job's failure after
        the run is recorded, like ``JobRunner.run``."""
        normalised = normalise_params(name, params)
        with self._build(name, normalised) as fn:
            return JobRunner(self.settings, self.registry, lock).run(
                name, fn, trigger=trigger, params=normalised, run_id=run_id, cancel=cancel
            )

    def _client_for(self, dataset: Dataset) -> EveRefClient:
        return self.zkillboard_client() if dataset.source == "zkillboard" else self._everef()

    def zkillboard_client(self) -> ZKillboardClient:
        """Under the zKillboard policy of design §11a, sharing the process's gate with
        every other zKillboard client, the live follower's included."""
        return ZKillboardClient(
            self.settings.zkillboard_base_url,
            policy_path=self.settings.zkillboard_policy_path,
            contact=self.settings.esi_contact,
            cache=open_response_cache(self.settings),
            spacing=self.settings.zkillboard_spacing,
            download_spacing=self.settings.zkillboard_download_spacing,
            gate=PROCESS_GATE,
        )

    def evekill_client(self) -> EveKillClient:
        """Under the eve-kill policy of design §11b, sharing the process's gate."""
        return EveKillClient(
            self.settings.evekill_base_url,
            contact=self.settings.esi_contact,
            spacing=self.settings.evekill_spacing,
            gate=EVEKILL_GATE,
        )

    def _everef(self) -> EveRefClient:
        """Listings are revalidated against the store's response cache (design §6)."""
        return EveRefClient(
            self.settings.everef_base_url,
            contact=self.settings.esi_contact,
            cache=open_response_cache(self.settings),
            spacing=self.settings.everef_spacing,
        )

    def _esi(self) -> EsiClient:
        settings = self.settings
        return EsiClient(
            settings.esi_base_url,
            compatibility_date=settings.esi_compatibility_date,
            contact=settings.esi_contact,
            cache=open_response_cache(settings),
            policy_path=settings.esi_policy_path,
            daily_budget=settings.esi_daily_budget,
            spacing=settings.esi_spacing,
        )

    @contextmanager
    def _build(self, name: str, params: Mapping[str, Any]) -> Generator[JobFn]:
        if name in SYNC_JOBS:
            dataset = get_dataset(name.removeprefix("sync:"))
            client = self._client_for(dataset)
            try:
                yield SyncJob(
                    dataset=dataset,
                    client=client,
                    importer=importer_for(dataset, self.lake),
                    date_from=params["from"],
                    date_to=params["to"],
                    force=params["force"],
                    sweep=params["sweep"],
                    offline=params["offline"],
                    max_objects=params["max_objects"],
                    head_days=self.settings.head_days,
                )
            finally:
                client.close()
        elif name == "killmails:merge":
            yield MergeJob(self.lake, date_from=params["from"], date_to=params["to"])
        elif name == "entities:seed":
            client = self._everef()
            try:
                yield self._seed(client, params)
            finally:
                client.close()
        elif name == "entities:refresh":
            esi = self._esi()
            try:
                yield RefreshJob(
                    esi,
                    self.entities,
                    self.lake,
                    recent_days=self.settings.esi_recent_days,
                    refresh_interval=self.settings.esi_refresh_interval,
                    alliance_sweep_interval=self.settings.alliance_sweep_interval,
                    affiliation_cycle=self.settings.affiliation_cycle,
                    budget=params["budget"],
                    populate=params["populate"],
                )
            finally:
                esi.close()
        elif name == "entities:export":
            yield ExportJob(self.entities, self.settings.entities_dir)
        elif name == "entities:add":
            esi = self._esi()
            evekill = self.evekill_client()
            try:
                yield FocusJob(
                    esi,
                    self.entities,
                    params["kind"],
                    params["ids"] or [],
                    members=params["members"],
                    names=params["names"] or [],
                    evekill=evekill,
                    lake=self.lake,
                    refresh=True,
                )
            finally:
                esi.close()
                evekill.close()
        elif name == "entities:discover":
            evekill = self.evekill_client()
            try:
                yield DiscoveryJob(evekill, self.entities, full_pages=params["full_pages"])
            finally:
                evekill.close()
        elif name == "verify":
            names = [params["dataset"]] if params["dataset"] else list(SYNC_JOBS)
            datasets = [get_dataset(n.removeprefix("sync:")) for n in names]
            clients = {} if params["offline"] else {d.name: self._client_for(d) for d in datasets}
            try:
                yield VerifyJob(
                    self.lake,
                    datasets,
                    clients,
                    date_from=params["from"],
                    date_to=params["to"],
                    hash_raw=params["hash"],
                )
            finally:
                for client in clients.values():
                    client.close()
        else:
            raise UnknownJobError(f"unknown job {name!r}")

    def _seed(self, client: EveRefClient, params: Mapping[str, Any]) -> JobFn:
        """The seed resolves ``latest`` against the upstream listing when it runs, so the
        run log records what was asked for and the log line what it resolved to. ``all``
        is an unranged sync of the backfill dataset: every listed archive that is not
        imported yet, newest first. Older snapshots only fill gaps, because an entity
        upsert never replaces a newer observation."""

        def job(ctx: RunContext) -> JobOutcome:
            snapshot = params["snapshot"]
            if snapshot == "all":
                return SyncJob(
                    dataset=ENTITIES_BACKFILL,
                    client=client,
                    importer=importer_for(ENTITIES_BACKFILL, self.lake, entities=self.entities),
                    force=params["force"],
                    head_days=self.settings.head_days,
                )(ctx)
            if snapshot == "latest":
                listed = [
                    o.logical_date for o in client.discover(ENTITIES_BACKFILL, []) if o.logical_date
                ]
                if not listed:
                    raise RuntimeError("no backfill archives listed upstream")
                snapshot = max(listed)
                log.info("latest backfill snapshot is %s", snapshot)
            return SyncJob(
                dataset=ENTITIES_BACKFILL,
                client=client,
                importer=importer_for(ENTITIES_BACKFILL, self.lake, entities=self.entities),
                date_from=snapshot,
                date_to=snapshot,
                force=params["force"],
                head_days=self.settings.head_days,
            )(ctx)

        return job
