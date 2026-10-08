"""Job catalog: parameter validation and building each job by name."""

from datetime import date

import pytest

from evedw.domain.registry import RunStatus, Trigger
from evedw.jobs.catalog import JOB_NAMES, JobCatalog, UnknownJobError, normalise_params
from evedw.store import open_entity_store
from tests.helpers import BASE_URL, D1, D3, SyncEnv, publish_three_days

# The catalog opens its stores through the factories, so it needs a real backend.
pytestmark = pytest.mark.duckdb_only


def test_normalise_params_fills_defaults_and_parses() -> None:
    assert normalise_params("sync:killmails", {}) == {
        "from": None,
        "to": None,
        "force": False,
        "sweep": False,
        "offline": False,
    }
    parsed = normalise_params(
        "sync:market_history", {"from": "2026-10-01", "to": date(2026, 10, 2)}
    )
    assert parsed["from"] == date(2026, 10, 1) and parsed["to"] == date(2026, 10, 2)
    assert normalise_params("entities:seed", {"snapshot": "all"})["snapshot"] == "all"
    assert normalise_params("entities:seed", {"snapshot": "2026-05-10"})["snapshot"] == date(
        2026, 5, 10
    )
    assert normalise_params("entities:refresh", {"budget": "40"})["budget"] == 40
    assert normalise_params("verify", {"dataset": "killmails", "hash": False})["hash"] is False
    assert normalise_params("entities:add", {"kind": "alliance", "ids": "3, 1,3"}) == {
        "kind": "alliance",
        "ids": [1, 3],
        "members": False,
    }
    assert (
        normalise_params(
            "entities:add", {"kind": "corporation", "ids": [98000001], "members": True}
        )["members"]
        is True
    )


@pytest.mark.parametrize(
    ("job", "params", "message"),
    [
        ("sync:killmails", {"from": "soon"}, "not a date"),
        ("sync:killmails", {"force": "yes"}, "expected bool"),
        ("sync:killmails", {"from": "2026-10-02", "to": "2026-10-01"}, "is after"),
        ("sync:killmails", {"budget": 1}, "unknown parameters"),
        ("entities:refresh", {"budget": True}, "expected int"),
        ("entities:seed", {"snapshot": "newest"}, "not a date"),
        ("verify", {"dataset": "stocks"}, "unknown dataset"),
        ("entities:add", {"kind": "alliance"}, "kind and ids are required"),
        ("entities:add", {"kind": "planet", "ids": [1]}, "expected one of"),
        ("entities:add", {"kind": "alliance", "ids": "1,-2"}, "ids: expected"),
        ("entities:add", {"kind": "alliance", "ids": []}, "one or more ids"),
        ("entities:add", {"kind": "character", "ids": [1], "members": True}, "members applies"),
    ],
)
def test_normalise_params_rejects_bad_values(
    job: str, params: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        normalise_params(job, params)


def test_unknown_job() -> None:
    with pytest.raises(UnknownJobError, match="unknown job 'sync:stocks'"):
        normalise_params("sync:stocks", {})
    assert "sync:entities_backfill" not in JOB_NAMES


def test_catalog_runs_sync_and_export(env: SyncEnv) -> None:
    publish_three_days(env.fake)
    env.settings.everef_base_url = BASE_URL  # the catalog builds its own EVE Ref client
    entities = open_entity_store(env.settings)
    try:
        catalog = JobCatalog(env.settings, env.registry, env.lake, entities)
        run = catalog.run(
            "sync:market_history",
            {"from": "2026-10-02"},
            trigger=Trigger.MANUAL,
            lock=env.lock,
        )
        assert run.status is RunStatus.SUCCEEDED
        assert run.objects_changed == 2 and run.params == {
            "from": "2026-10-02",
            "to": None,
            "force": False,
            "sweep": False,
            "offline": False,
        }
        assert not env.partition(D1).exists() and env.partition(D3).exists()

        export = catalog.run("entities:export", {}, trigger=Trigger.SCHEDULE, lock=env.lock)
        assert export.objects_changed == 5
        assert (env.settings.entities_dir / "alliances.parquet").is_file()

        verify = catalog.run("verify", {"offline": True}, trigger=Trigger.CLI, lock=env.lock)
        assert verify.status is RunStatus.SUCCEEDED and verify.objects_changed == 2

        env.partition(D3).unlink()
        with pytest.raises(Exception, match="1 issues \\(missing_partition\\)"):
            catalog.run("verify", {"offline": True}, trigger=Trigger.CLI, lock=env.lock)
        assert env.registry.runs(limit=1)[0].status is RunStatus.FAILED
    finally:
        entities.close()
