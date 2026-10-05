"""Importer lookup by dataset."""

from evedw.domain.datasets import Dataset
from evedw.jobs.sync import Importer
from evedw.store.base import Lake


def importer_for(dataset: Dataset, lake: Lake) -> Importer:
    if dataset.name == "market_history":
        from evedw.jobs.market import MarketHistoryImporter

        return MarketHistoryImporter(lake)
    raise NotImplementedError(f"no importer for dataset {dataset.name!r} yet")
