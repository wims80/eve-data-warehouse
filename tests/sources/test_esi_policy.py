"""Policy arithmetic ported from kat's tests: every malformed or extreme header fails closed."""

from evedw.sources.esi.policy import Policy, parse_limit, policy_route


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
