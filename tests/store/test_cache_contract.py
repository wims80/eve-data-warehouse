"""Contract tests every ResponseCache backend must pass."""

from datetime import UTC, datetime, timedelta

import pytest

from evedw.store.base import CachedResponse, ResponseCache

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def entry(key: str = "k", **overrides: object) -> CachedResponse:
    base: dict[str, object] = {
        "key": key,
        "status": 200,
        "body": '{"name": "A"}',
        "etag": '"one"',
        "last_modified": None,
        "cache_control": "public, max-age=3600",
        "observed_at": NOW,
        "expires_at": NOW + timedelta(hours=1),
    }
    base.update(overrides)
    return CachedResponse(**base)  # type: ignore[arg-type]


def test_put_get_replace_delete(response_cache: ResponseCache) -> None:
    assert response_cache.get("k") is None
    response_cache.put(entry())
    assert response_cache.get("k") == entry()
    response_cache.put(entry(body='{"name": "B"}', etag='"two"'))
    got = response_cache.get("k")
    assert got is not None and got.body == '{"name": "B"}' and got.etag == '"two"'
    assert got.expires_at.tzinfo is not None
    response_cache.delete("k")
    assert response_cache.get("k") is None
    response_cache.delete("k")  # idempotent


def test_naive_datetimes_are_rejected(response_cache: ResponseCache) -> None:
    with pytest.raises(ValueError):
        response_cache.put(entry(observed_at=datetime(2026, 10, 5)))  # noqa: DTZ001
