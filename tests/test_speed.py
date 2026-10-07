from datetime import UTC, datetime, timedelta

from evedw.domain.speed import Sample, describe

T0 = datetime(2026, 10, 7, 21, 0, tzinfo=UTC)
CYCLE = {"started_at": "2026-10-07T16:31:44+00:00"}


def sample(seconds: int, requests: int, *, budget_used: int = 40_000, **cycle: object) -> Sample:
    return Sample(
        at=T0 + timedelta(seconds=seconds),
        esi_requests=requests,
        budget_used=budget_used,
        pace=0.2,
        cycle={**CYCLE, **cycle},
        queue={"change": 1000 + seconds, "idle": 50},
        characters=20_000_000,
    )


def test_rates_over_the_window() -> None:
    lines = describe(
        sample(0, 1000, checked=12_000_000, changed=100),
        sample(60, 1120, checked=12_120_000, changed=1300),
        budget=300_000,
    )
    assert lines[0] == "window         60 s ending 21:01:00 UTC"
    assert lines[1] == (
        "esi            120 requests/min (2.00/s), pace 0.20s, about 172,800/day at this rate"
    )
    assert lines[2] == "budget         40,000 of 300,000 used, lasts the day"
    assert lines[3] == (
        "affiliation    120,000 characters/min, 1,200 changes/min; 12,120,000 checked, "
        "at most 66 min left"
    )
    assert lines[4] == "queue          change +60  idle +0 entries/min"


def test_budget_spent_before_midnight() -> None:
    lines = describe(sample(0, 0), sample(60, 600, budget_used=290_000), budget=300_000)
    assert lines[2] == "budget         290,000 of 300,000 used, spent at 21:17 UTC"


def test_cycle_states() -> None:
    assert describe(sample(0, 0), sample(60, 0), budget=1)[3] == (
        "affiliation    cycle not started"
    )
    finished = sample(60, 0, finished_at="2026-10-07T23:00:00+00:00")
    assert describe(sample(0, 0), finished, budget=1)[3].endswith(
        "finished at 2026-10-07T23:00:00+00:00"
    )
    restarted = Sample(
        at=T0 + timedelta(seconds=60),
        esi_requests=0,
        budget_used=0,
        pace=0.2,
        cycle={"started_at": "later", "checked": 1000, "changed": 0},
    )
    assert (
        "new cycle started" in describe(sample(0, 0, checked=5, changed=0), restarted, budget=1)[3]
    )


def test_empty_window() -> None:
    assert describe(sample(0, 0), sample(0, 5), budget=1) == [
        "window         0 s; nothing to measure"
    ]
