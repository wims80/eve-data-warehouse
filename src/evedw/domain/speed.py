"""Throughput between two samples of the warehouse counters, for ``evedw speed``.

A sample is what an operator would otherwise read from ``evedw esi status`` and
``evedw entities status`` twice and subtract by hand.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class Sample:
    at: datetime
    esi_requests: int
    """Cumulative requests sent; unlike the budget it does not reset at midnight."""
    budget_used: int
    pace: float
    cycle: Mapping[str, object] = field(default_factory=dict[str, object])
    """The affiliation cycle's sweep state: ``checked``, ``changed``, ``finished_at``."""
    queue: Mapping[str, int] = field(default_factory=dict[str, int])
    """Refresh queue entries per class name."""
    characters: int | None = None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _duration(seconds: float) -> str:
    minutes = round(seconds / 60)
    return f"{minutes} min" if minutes < 120 else f"{minutes / 60:.1f} h"


def describe(before: Sample, after: Sample, *, budget: int) -> list[str]:
    """Report lines for the interval between two samples."""
    elapsed = (after.at - before.at).total_seconds()
    if elapsed <= 0:
        return ["window         0 s; nothing to measure"]
    per_minute = 60 / elapsed
    lines = [f"window         {elapsed:.0f} s ending {after.at:%H:%M:%S} UTC"]

    sent = max(after.esi_requests - before.esi_requests, 0)
    rate = sent / elapsed
    esi = (
        f"esi            {sent * per_minute:,.0f} requests/min ({rate:.2f}/s), "
        f"pace {after.pace:.2f}s, about {rate * 86_400:,.0f}/day at this rate"
    )
    lines.append(esi)
    left = budget - after.budget_used
    midnight = datetime.combine(after.at.date() + timedelta(days=1), datetime.min.time(), UTC)
    if rate > 0 and left / rate < (midnight - after.at).total_seconds():
        spent = after.at + timedelta(seconds=left / rate)
        lines.append(
            f"budget         {after.budget_used:,} of {budget:,} used, spent at {spent:%H:%M} UTC"
        )
    else:
        lines.append(f"budget         {after.budget_used:,} of {budget:,} used, lasts the day")

    lines.append(_cycle(before.cycle, after.cycle, after.characters, elapsed))

    deltas = [
        f"{name} {(after.queue.get(name, 0) - before.queue.get(name, 0)) * per_minute:+,.0f}"
        for name in after.queue
    ]
    lines.append("queue          " + "  ".join(deltas) + " entries/min")
    return lines


def _cycle(
    before: Mapping[str, object],
    after: Mapping[str, object],
    characters: int | None,
    elapsed: float,
) -> str:
    label = "affiliation    "
    if after.get("finished_at"):
        return f"{label}cycle finished at {after['finished_at']}"
    checked, changed = _int(after.get("checked")), _int(after.get("changed"))
    if checked is None or changed is None:
        return f"{label}cycle not started"
    done = checked - (_int(before.get("checked")) or 0)
    if before.get("started_at") != after.get("started_at") or done < 0:
        return f"{label}{checked:,} checked; a new cycle started in this window"
    found = changed - (_int(before.get("changed")) or 0)
    line = (
        f"{label}{done * 60 / elapsed:,.0f} characters/min, {found * 60 / elapsed:,.0f} "
        f"changes/min; {checked:,} checked"
    )
    if characters is not None and done > 0:
        # The character table includes deleted ones the cycle skips, so this is an upper bound.
        remaining = max(characters - checked, 0)
        line += f", at most {_duration(remaining / (done / elapsed))} left"
    return line
