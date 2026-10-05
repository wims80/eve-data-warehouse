"""The HTTP service against an in-process ASGI app with its lifespan running."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa
import pytest
import respx

from evedw.config import Settings
from evedw.domain.datasets import MARKET_HISTORY
from evedw.domain.registry import RunStatus, Trigger
from evedw.jobs.runner import LockHeldError, WriterLock
from evedw.jobs.scheduler import ScheduledJob
from evedw.service.app import NotMigratedError, create_app
from evedw.service.routes.query import ARROW_STREAM
from evedw.service.state import ServiceState
from evedw.store import open_registry
from evedw.store.base import Registry
from tests.helpers import BASE_URL, D1, D2, D3, FakeEveRef, market_csv_bz2, publish_three_days


@pytest.fixture
def fake(settings: Settings) -> Iterator[FakeEveRef]:
    settings.everef_base_url = BASE_URL
    with respx.mock(assert_all_called=False) as router:
        yield FakeEveRef(router, MARKET_HISTORY)


@pytest.fixture
async def api(
    settings: Settings, registry: Registry, fake: FakeEveRef
) -> AsyncIterator[httpx.AsyncClient]:
    """A migrated warehouse served by the app with the scheduler idle."""
    registry.close()
    app = create_app(settings, schedule=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
            client.app = app  # type: ignore[attr-defined]
            yield client


def state_of(client: httpx.AsyncClient) -> ServiceState:
    return client.app.state.warehouse  # type: ignore[attr-defined, no-any-return]


async def wait_for(client: httpx.AsyncClient, run_id: str) -> dict[str, Any]:
    for _ in range(200):
        run = (await client.get(f"/runs/{run_id}")).json()
        if run["status"] in ("succeeded", "failed", "cancelled"):
            return run  # type: ignore[no-any-return]
        await asyncio.sleep(0.02)
    raise AssertionError(f"run {run_id} did not finish")


async def test_health_and_empty_listings(api: httpx.AsyncClient, settings: Settings) -> None:
    health = (await api.get("/health")).json()
    assert health["lock_held"] is True and health["service_url"] == settings.service_url
    assert health["current_run"] is None and health["queued"] == [] and health["schedule"] == []
    holder = WriterLock(settings.lock_path).holder()
    assert holder is not None and holder.service_url == settings.service_url

    datasets = (await api.get("/datasets")).json()
    assert [d["name"] for d in datasets["datasets"]] == [
        "entities_backfill",
        "killmails",
        "market_history",
    ]
    assert datasets["entities"]["characters"] == 0
    assert (await api.get("/runs")).json() == {"runs": []}
    assert (await api.get("/lake")).json()["partitions"] == []
    assert (await api.get("/jobs")).json()["jobs"][0] == "sync:killmails"
    listed = (await api.get("/query")).json()
    assert listed["arrow_media_type"] == ARROW_STREAM
    assert {q["name"] for q in listed["queries"]} >= {"killmails_by_date", "alliances_by_id"}


async def test_trigger_runs_sync_and_exposes_results(
    api: httpx.AsyncClient, fake: FakeEveRef
) -> None:
    publish_three_days(fake)
    before = datetime.now(UTC)
    response = await api.post("/jobs/sync:market_history", json={"from": "2026-10-02"})
    assert response.status_code == 202, response.text
    queued = response.json()
    assert queued["status"] == "queued" and queued["params"]["from"] == "2026-10-02"
    run = await wait_for(api, queued["run_id"])
    assert run["status"] == "succeeded", run
    assert run["trigger"] == "manual" and run["objects_changed"] == 2 and run["rows_written"] == 300

    runs = (await api.get("/runs", params={"limit": 5, "job": "sync:market_history"})).json()
    assert [r["run_id"] for r in runs["runs"]] == [queued["run_id"]]
    assert (await api.get("/runs/nope")).status_code == 404

    datasets = (await api.get("/datasets")).json()
    market = next(d for d in datasets["datasets"] if d["name"] == "market_history")
    assert market["objects_by_status"] == {"imported": 2, "new": 1}
    assert market["oldest_imported"] == "2026-10-02"

    objects = (await api.get("/datasets/market_history/objects")).json()
    assert [o["logical_date"] for o in objects["objects"]] == [str(D1), str(D2), str(D3)]
    changed = (
        await api.get(
            "/datasets/market_history/objects",
            params={"changed_since": before.isoformat()},
        )
    ).json()
    assert [o["logical_date"] for o in changed["objects"]] == [str(D3), str(D2)]
    assert all(o["current_revision"] == 1 for o in changed["objects"])
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    assert (
        await api.get("/datasets/market_history/objects", params={"changed_since": later})
    ).json()["objects"] == []
    naive = await api.get(
        "/datasets/market_history/objects", params={"changed_since": "2026-10-05T00:00:00"}
    )
    assert naive.status_code == 422 and "timezone" in naive.json()["detail"]
    assert (await api.get("/datasets/stocks/objects")).status_code == 404

    key = f"2026/market-history-{D3}.csv.bz2"
    detail = (await api.get(f"/datasets/market_history/objects/{key}")).json()
    assert detail["object"]["object_key"] == key and detail["object"]["status"] == "imported"
    assert [r["revision"] for r in detail["revisions"]] == [1]
    assert detail["revisions"][0]["observed_count"] == 100
    assert (
        await api.get("/datasets/market_history/objects/2026/missing.csv.bz2")
    ).status_code == 404

    lake = (await api.get("/lake", params={"table": "market_history"})).json()
    assert [p["partition"] for p in lake["partitions"]] == [f"date={D2}", f"date={D3}"]
    assert lake["partitions"][0]["revision"] == 1 and len(lake["partitions"][0]["sha256"]) == 64
    assert lake["partitions"][0]["written_at"] is not None
    assert (await api.get("/lake", params={"table": "stocks"})).status_code == 404

    health = (await api.get("/health")).json()
    assert health["last_runs"]["sync:market_history"]["run_id"] == queued["run_id"]


async def test_query_json_and_arrow(api: httpx.AsyncClient, fake: FakeEveRef) -> None:
    publish_three_days(fake)
    run_id = (await api.post("/jobs/sync:market_history", json={})).json()["run_id"]
    assert (await wait_for(api, run_id))["status"] == "succeeded"

    params = {"date_from": str(D1), "date_to": str(D2)}
    as_json = (await api.get("/query/market_history_by_date", params=params)).json()
    assert as_json["row_count"] == 500 and as_json["columns"][0] == "date"
    assert as_json["rows"][0]["date"] == str(D1)

    arrow = await api.get(
        "/query/market_history_by_date", params=params, headers={"Accept": ARROW_STREAM}
    )
    assert arrow.status_code == 200 and arrow.headers["content-type"] == ARROW_STREAM
    assert arrow.headers["x-row-count"] == "500"
    table = pa.ipc.open_stream(arrow.content).read_all()
    assert table.num_rows == 500 and table.column("date").to_pylist()[-1] == D2

    assert (await api.get("/query/nope")).status_code == 404
    missing = await api.get("/query/market_history_by_date", params={"date_from": str(D1)})
    assert missing.status_code == 422 and "date_to" in missing.json()["detail"]
    empty = await api.get(
        "/query/characters_by_id", params={"ids": "1,2"}, headers={"Accept": ARROW_STREAM}
    )
    assert pa.ipc.open_stream(empty.content).read_all().num_rows == 0


async def test_bad_triggers(api: httpx.AsyncClient) -> None:
    unknown = await api.post("/jobs/sync:stocks", json={})
    assert unknown.status_code == 404 and "unknown job" in unknown.json()["detail"]
    bad = await api.post("/jobs/sync:killmails", json={"from": "tomorrow"})
    assert bad.status_code == 422 and "not a date" in bad.json()["detail"]
    extra = await api.post("/jobs/entities:export", json={"force": True})
    assert extra.status_code == 422 and "unknown parameters" in extra.json()["detail"]
    assert (await api.get("/runs")).json() == {"runs": []}


async def test_jobs_run_one_at_a_time_and_failures_are_recorded(
    api: httpx.AsyncClient, fake: FakeEveRef
) -> None:
    publish_three_days(fake)
    fake.serve_instead[D3] = b"not the file"  # download fails on size/etag mismatch
    first = (await api.post("/jobs/sync:market_history", json={"from": str(D3)})).json()
    second = (await api.post("/jobs/entities:export", json={})).json()
    health = (await api.get("/health")).json()
    assert {r["run_id"] for r in health["queued"]} | (
        {health["current_run"]["run_id"]} if health["current_run"] else set()
    ) >= {second["run_id"]}
    failed = await wait_for(api, first["run_id"])
    assert failed["status"] == "failed" and "1 of 1 objects failed" in failed["error"]
    exported = await wait_for(api, second["run_id"])
    assert exported["status"] == "succeeded" and exported["objects_changed"] == 5
    assert exported["started_at"] >= failed["finished_at"]


async def test_scheduled_submit_waits_for_completion(
    api: httpx.AsyncClient, fake: FakeEveRef
) -> None:
    publish_three_days(fake)
    state = state_of(api)
    job = ScheduledJob("sync:market_history", timedelta(hours=6), {"sweep": False})
    run = await state.worker.run_scheduled(job)
    assert run is not None and run.status is RunStatus.SUCCEEDED and run.objects_changed == 3
    assert run.trigger is Trigger.SCHEDULE
    assert state.worker.current is None and state.worker.queued == []


async def test_shutdown_cancels_queued_and_running_jobs(
    settings: Settings, registry: Registry, fake: FakeEveRef
) -> None:
    registry.close()
    for day in (D1, D2, D3):
        fake.put(day, market_csv_bz2(day, rows=50), expected=50)
    app = create_app(settings, schedule=False)
    async with app.router.lifespan_context(app):
        state: ServiceState = app.state.warehouse
        running = state.worker.submit("sync:market_history", {}, trigger=Trigger.MANUAL)
        queued = state.worker.submit("entities:export", {}, trigger=Trigger.MANUAL)
        await asyncio.sleep(0.05)
    reopened = open_registry(settings)
    try:
        runs = {r.run_id: r for r in reopened.runs()}
        assert runs[queued.run_id].status is RunStatus.CANCELLED
        assert "before the run started" in (runs[queued.run_id].error or "")
        assert runs[running.run_id].status in (RunStatus.CANCELLED, RunStatus.SUCCEEDED)
        assert runs[running.run_id].finished_at is not None
    finally:
        reopened.close()
    assert WriterLock(settings.lock_path).holder() is None


async def test_service_refuses_held_lock_and_unmigrated_registry(settings: Settings) -> None:
    settings.ensure_dirs()
    with WriterLock(settings.lock_path):
        app = create_app(settings)
        with pytest.raises(LockHeldError):
            async with app.router.lifespan_context(app):
                pass
    app = create_app(settings)
    with pytest.raises(NotMigratedError):
        async with app.router.lifespan_context(app):
            pass
    assert WriterLock(settings.lock_path).holder() is None
