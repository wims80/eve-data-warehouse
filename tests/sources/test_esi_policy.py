"""Policy arithmetic ported from kat's tests: every malformed or extreme header fails closed."""

from datetime import UTC, datetime

from evedw.sources.esi.policy import (
    MAX_SPACING,
    DowntimeWindow,
    Policy,
    parse_limit,
    policy_route,
    server_up,
)


def test_extreme_and_malformed_legacy_headers_do_not_bypass_cooldowns() -> None:
    policy = Policy()
    headers = {"x-esi-error-limit-remain": "0", "x-esi-error-limit-reset": str(2**62)}
    policy.observe("route", 420, headers, 100)
    assert policy.ready_at("route", 100) == 2**62 + 101

    policy = Policy()
    policy.observe("route", 200, {"x-esi-error-limit-remain": "invalid"}, 100)
    assert policy.ready_at("route", 100) == 161


def test_bucket_and_error_cooldowns_survive_serialisation() -> None:
    policy = Policy()
    headers = {
        "x-ratelimit-group": "character",
        "x-ratelimit-limit": "150/15m",
        "x-ratelimit-remaining": "20",
    }
    policy.observe("/characters/affiliation/", 200, headers, 1000)
    restored = Policy.from_dict(policy.to_dict())
    assert restored.ready_at("/characters/affiliation/", 1001) == 1901

    policy = Policy()
    headers = {
        "x-esi-error-limit-remain": "19",
        "x-esi-error-limit-reset": "45",
        "retry-after": "90",
    }
    policy.observe("/universe/names/", 420, headers, 100)
    assert policy.ready_at("/characters/affiliation/", 100) == 191


def test_malformed_and_extreme_rate_headers_fail_conservatively() -> None:
    policy = Policy()
    policy.observe("/test/", 429, {"retry-after": "not a date"}, 100)
    assert policy.ready_at("/test/", 100) >= 160
    policy.observe("/test/", 429, {"retry-after": str(2**62)}, 100)
    assert policy.ready_at("/test/", 100) == 2**62 + 101

    policy = Policy()
    policy.observe("/test/", 200, {"x-ratelimit-group": "g", "x-ratelimit-limit": "invalid"}, 100)
    assert policy.ready_at("/test/", 100) >= 3701


def test_reserve_spaces_requests_and_drains_bucket() -> None:
    policy = Policy()
    policy.observe("/r", 200, {"x-ratelimit-group": "g", "x-ratelimit-limit": "10/1m"}, 100)
    # remaining defaulted to 0 and 0 <= max(5, 2): blocked a full window.
    assert policy.ready_at("/r", 101) == 161
    policy.observe(
        "/r",
        200,
        {"x-ratelimit-group": "g", "x-ratelimit-limit": "100/1m", "x-ratelimit-remaining": "90"},
        200,
    )
    assert policy.ready_at("/r", 200) == 201
    policy.reserve("/r", 201)
    assert policy.buckets["g"].remaining == 85
    assert policy.next_request == 202
    # Past the reset the bucket refills before reserving.
    policy.reserve("/r", 400)
    assert policy.buckets["g"].remaining == 95


def test_budget_is_per_day() -> None:
    from datetime import date

    policy = Policy()
    assert policy.budget_remaining(3, date(2026, 10, 5)) == 3
    policy.charge(date(2026, 10, 5))
    policy.charge(date(2026, 10, 5))
    assert policy.budget_remaining(3, date(2026, 10, 5)) == 1
    assert policy.budget_remaining(3, date(2026, 10, 6)) == 3
    policy.charge(date(2026, 10, 6))
    assert policy.budget_used == 1


def test_helpers() -> None:
    assert parse_limit("150/15m") == (150, 900)
    assert parse_limit("10/1h") == (10, 3600)
    assert parse_limit("0/1m") is None
    assert parse_limit("abc") is None
    assert parse_limit("5/1d") is None
    assert policy_route("/characters/123/corporationhistory") == (
        "/characters/{id}/corporationhistory"
    )
    assert policy_route("/universe/names") == "/universe/names"


def test_warning_signs_double_the_spacing_once_a_minute_up_to_the_ceiling() -> None:
    policy = Policy(floor=0.2, spacing=0.2)
    assert policy.observe("/r", 200, {"x-esi-error-limit-remain": "95"}, 1000.0) is None
    assert policy.next_request == 1000.2
    assert policy.observe("/r", 502, {}, 1001.0) == "HTTP 502"
    assert policy.spacing == 0.4
    assert policy.observe("/r", 200, {"x-esi-error-limit-remain": "85"}, 1030.0) is not None
    assert policy.spacing == 0.4  # same minute
    for minute in range(1, 6):
        policy.warn(1001.0 + 60 * minute)
    assert policy.spacing == MAX_SPACING
    assert policy.slowdowns == 4
    warning = policy.observe(
        "/r", 200, {"x-ratelimit-limit": "150/15m", "x-ratelimit-remaining": "70"}, 1400.0
    )
    assert warning == "rate-limit bucket at 70 of 150"


def test_calm_halves_the_spacing_back_to_the_floor() -> None:
    policy = Policy(floor=0.2, spacing=0.8, last_warning=1000.0, paced_at=1000.0)
    policy.observe("/r", 200, {}, 1200.0)
    assert policy.spacing == 0.8
    policy.observe("/r", 200, {}, 1300.0)
    assert policy.spacing == 0.4
    policy.observe("/r", 200, {}, 1400.0)
    assert policy.spacing == 0.4
    policy.observe("/r", 200, {}, 1600.0)
    assert policy.spacing == 0.2
    policy.observe("/r", 200, {}, 9000.0)
    assert policy.spacing == 0.2
    restored = Policy.from_dict(policy.to_dict())
    assert (restored.spacing, restored.paced_at) == (0.2, 1600.0)
    # A raised floor wins over a persisted faster spacing.
    restored.floor = 1.0
    assert restored.pace() == 1.0


# --- daily downtime -------------------------------------------------------------------------

ELEVEN = datetime(2026, 10, 5, 11, tzinfo=UTC).timestamp()


def status(start_time: str, **extra: object) -> dict[str, object]:
    return {"players": 21000, "server_version": "3000000", "start_time": start_time, **extra}


def test_downtime_window_is_anchored_to_eleven_utc() -> None:
    window = DowntimeWindow.on(ELEVEN + 3 * 3600)
    assert window.watch_from == ELEVEN - 15 * 60
    assert window.pause_from == ELEVEN - 2 * 60
    assert window.restart_by == ELEVEN + 30 * 60
    assert not window.watching(ELEVEN - 15 * 60 - 1) and window.watching(ELEVEN - 15 * 60)
    assert not window.pausing(ELEVEN - 121) and window.pausing(ELEVEN - 120)
    assert window.watching(ELEVEN + 30 * 60 - 1) and not window.watching(ELEVEN + 30 * 60)


def test_pause_is_due_from_two_minutes_before_and_once_per_window() -> None:
    policy = Policy()
    assert not policy.downtime_due(ELEVEN - 121)
    assert policy.downtime_due(ELEVEN - 120)
    policy.pause_for_downtime(ELEVEN - 120, "daily downtime")
    assert not policy.downtime_due(ELEVEN - 60)  # already paused
    policy.resume_after_downtime(ELEVEN + 600)
    assert not policy.downtime_due(ELEVEN + 660)  # resumed in this window
    assert policy.downtime_due(ELEVEN + 86_400 - 60)  # tomorrow's window


def test_server_errors_are_expected_only_in_the_window_or_while_paused() -> None:
    policy = Policy()
    assert not policy.outage_expected(ELEVEN - 15 * 60 - 1)
    assert policy.outage_expected(ELEVEN - 15 * 60)
    assert not policy.outage_expected(ELEVEN + 30 * 60)
    policy.pause_for_downtime(ELEVEN, "HTTP 502")
    assert policy.outage_expected(ELEVEN + 2 * 3600)  # a long downtime stays expected


def test_server_up_needs_a_restart_inside_the_window_and_no_vip() -> None:
    before = status("2026-10-04T11:05:00Z")
    restarted = status("2026-10-05T11:06:12Z")
    assert not server_up(before, ELEVEN + 600)
    assert server_up(restarted, ELEVEN + 600)
    assert not server_up(status("2026-10-05T11:06:12Z", vip=True), ELEVEN + 600)
    assert server_up(before, ELEVEN + 30 * 60)  # past the deadline a healthy status will do
    assert not server_up(status("not a date"), ELEVEN + 600)
    assert not server_up(["not", "an", "object"], ELEVEN + 600)


def test_expected_server_errors_leave_the_pace_alone_and_downtime_survives_a_restart() -> None:
    policy = Policy(floor=0.05, spacing=0.05)
    policy.observe("/characters/{id}", 502, {}, ELEVEN, expected=True)
    assert policy.spacing == 0.05 and policy.slowdowns == 0
    policy.observe("/characters/{id}", 502, {}, ELEVEN)
    assert policy.spacing == 0.1 and policy.slowdowns == 1

    policy.pause_for_downtime(ELEVEN - 120, "daily downtime at 11:00 UTC")
    policy.status_checked_at = ELEVEN - 60
    restored = Policy.from_dict(policy.to_dict())
    assert restored.downtime_since == ELEVEN - 120
    assert restored.downtime_reason == "daily downtime at 11:00 UTC"
    assert restored.status_checked_at == ELEVEN - 60
