"""Importer lookup by dataset."""

from evedw.domain.datasets import Dataset
from evedw.jobs.sync import Importer
from evedw.store.base import Lake


def importer_for(dataset: Dataset, lake: Lake) -> Importer:
    if dataset.name == "market_history":
        from evedw.jobs.market import MarketHistoryImporter

        return MarketHistoryImporter(lake)
    if dataset.name == "killmails":
        from evedw.jobs.killmails import KillmailImporter

        return KillmailImporter(lake)
    raise NotImplementedError(f"no importer for dataset {dataset.name!r} yet")
