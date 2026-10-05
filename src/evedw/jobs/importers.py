"""Importer lookup by dataset."""

from evedw.domain.datasets import Dataset
from evedw.jobs.sync import Importer
from evedw.store.base import EntityStore, Lake


def importer_for(dataset: Dataset, lake: Lake, *, entities: EntityStore | None = None) -> Importer:
    if dataset.name == "market_history":
        from evedw.jobs.market import MarketHistoryImporter

        return MarketHistoryImporter(lake)
    if dataset.name == "killmails":
        from evedw.jobs.killmails import KillmailImporter

        return KillmailImporter(lake)
    if dataset.name == "entities_backfill":
        from evedw.jobs.entities import BackfillImporter

        if entities is None:
            raise ValueError("the entities_backfill importer needs an entity store")
        return BackfillImporter(entities)
    raise NotImplementedError(f"no importer for dataset {dataset.name!r} yet")
