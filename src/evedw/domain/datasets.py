"""Dataset definitions.

A dataset is a family of upstream source objects that share discovery rules and a
normaliser. ``parser_version`` is bumped whenever a normaliser's output changes; the
registry then treats every object of that dataset as changed, which triggers a
re-import from the retained raw files.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class Dataset:
    name: str
    parser_version: int
    first_date: date
    index_path: str | None
    """Path under the EVE Ref base URL of the per-year index, with a ``{year}`` placeholder.
    ``None`` when the dataset is not discovered through index.json."""
    totals_path: str | None
    totals_key_format: str | None
    """``strftime`` format of the keys in totals.json."""
    object_name_pattern: re.Pattern[str]
    """Matches an object's file name and captures the logical date in group ``date``."""

    def years(self, today: date) -> range:
        return range(self.first_date.year, today.year + 1)

    def index_url(self, base_url: str, year: int) -> str:
        if self.index_path is None:
            raise ValueError(f"dataset {self.name} has no index")
        return f"{base_url.rstrip('/')}/{self.index_path.format(year=year)}"

    def totals_url(self, base_url: str) -> str:
        if self.totals_path is None:
            raise ValueError(f"dataset {self.name} has no totals")
        return f"{base_url.rstrip('/')}/{self.totals_path}"

    def logical_date(self, name: str) -> date | None:
        match = self.object_name_pattern.match(name)
        if match is None:
            return None
        return datetime.strptime(match.group("date"), "%Y-%m-%d").date()  # noqa: DTZ007

    def object_key(self, year: int, name: str) -> str:
        return f"{year}/{name}"

    def totals_key(self, day: date) -> str:
        if self.totals_key_format is None:
            raise ValueError(f"dataset {self.name} has no totals")
        return day.strftime(self.totals_key_format)


KILLMAILS = Dataset(
    name="killmails",
    parser_version=1,
    first_date=date(2007, 12, 5),
    index_path="killmails/{year}/index.json",
    totals_path="killmails/totals.json",
    totals_key_format="%Y%m%d",
    object_name_pattern=re.compile(r"^killmails-(?P<date>\d{4}-\d{2}-\d{2})\.tar\.bz2$"),
)

MARKET_HISTORY = Dataset(
    name="market_history",
    parser_version=1,
    first_date=date(2003, 1, 1),
    index_path="market-history/{year}/index.json",
    totals_path="market-history/totals.json",
    totals_key_format="%Y-%m-%d",
    object_name_pattern=re.compile(r"^market-history-(?P<date>\d{4}-\d{2}-\d{2})\.csv\.bz2$"),
)

ENTITIES_BACKFILL = Dataset(
    name="entities_backfill",
    parser_version=1,
    first_date=date(2024, 5, 31),
    index_path=None,
    totals_path=None,
    totals_key_format=None,
    object_name_pattern=re.compile(
        r"^eve-kill-com-karbowiak-(?P<date>\d{4}-\d{2}-\d{2})\.tar\.bz2$"
    ),
)

DATASETS: dict[str, Dataset] = {d.name: d for d in (KILLMAILS, MARKET_HISTORY, ENTITIES_BACKFILL)}


def get_dataset(name: str) -> Dataset:
    try:
        return DATASETS[name]
    except KeyError:
        known = ", ".join(sorted(DATASETS))
        raise KeyError(f"unknown dataset {name!r}; known: {known}") from None
