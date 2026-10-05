# Implementation plan

Status: M0, M1 and M2 complete (2026-10-05). Next: M3.

Milestones are ordered so that each one leaves a working, testable system.
Do not start a milestone before the previous one's acceptance checks pass.
Design decisions are in [design.md](design.md); do not re-decide them here.
If a milestone reveals the design is wrong, change design.md first, then the
code.

Where kat code is a useful reference, the path under `../kat/src` is given.
Port the behaviour, not the structure.

## M0. Scaffold and registry

Deliverables:

- `pyproject.toml` with the dependency set from design §14, `uv.lock`,
  `ruff` and `pyright` configuration, a `[project.scripts]` entry `evedw`.
- `src/evedw/config.py` with every key from design §13 and a `.env.example`.
- `src/evedw/domain/datasets.py` describing `killmails` and `market_history`
  (index URL pattern per year, totals URL, object key pattern, logical date
  parser, parser_version).
- `src/evedw/domain/schemas.py` with pyarrow schemas for every lake table and
  registry table in design §5 and §7.
- `src/evedw/store/base.py` Protocols from design §10.
- `src/evedw/store/duckdb_lake/registry.py` plus `migrations/001_registry.sql`
  and a migration runner keyed by `schema_version`.
- `src/evedw/jobs/runner.py`: writer lock, `import_run` rows, structured
  logging context.
- `evedw migrate`, `evedw status` (prints datasets, object counts by status,
  last runs).
- `tests/store/test_registry_contract.py`: contract tests written against the
  Protocol, parametrised over backends, with only DuckDB today.

Acceptance: `uv run evedw migrate && uv run evedw status` works on an empty
data dir; a second process attempting a job while the lock is held fails
fast with a clear message; `ruff`, `pyright` and `pytest` are clean.

## M1. Market history end to end

The simplest dataset proves the whole lifecycle.

Deliverables:

- `sources/everef.py`: per-year index discovery, totals, download with
  Content-Length and ETag verification against the index entry, sha256 while
  streaming, retention into `raw/`. Reference: `infrastructure/everef.rs`.
- `sources/archives.py`: bz2 CSV to scratch.
- `jobs/sync.py` generic discover, fetch, import loop over a dataset, newest
  first, with `--from`, `--to`, `--force`.
- `jobs/market.py`: DuckDB CSV read with explicit columns, Parquet write via
  `Lake.write_partition`, atomic replace, Parquet key-value metadata.
- `store/duckdb_lake/lake.py` and the view DDL; `evedw views` prints it.
- `jobs/verify.py` for this dataset.
- `evedw sync market_history --from 2026-09-01` and `evedw verify`.
- Fixture: a trimmed `market-history-2026-10-01.csv.bz2` plus a matching
  fake `index.json` and `totals.json`, served by a `respx` mock in tests.

Acceptance: importing a range, then editing the mock index's etag for one
day and syncing again, produces revision 2 for that day only, the old
partition file is replaced atomically, and `verify` is clean. Killing the
process mid-import leaves no `data.parquet` damage and the next sync
finishes the object. Run it for real against a recent month.

## M2. Killmails

Deliverables:

- `sources/archives.py`: tar.bz2 member streaming into one NDJSON file per
  day, asserting single-line members.
- `jobs/killmails.py`: the normaliser from `docs/prototypes/killmail_normalise.py`
  with explicit schema, three tables, nested items with `parent_ordinal`, a
  hard error on nesting deeper than one level, and the key-set drift check.
  Reference for edge cases: `domain/killmail.rs` and its tests, including
  negative damage.
- `killmails_unique` view.
- Fixture: about 50 real killmails including one with a container, one with
  negative damage, one with `moon_id` and `war_id`.

Acceptance: counts per table on the fixture match hand-computed values;
a full year imports without manual intervention; `verify` reports the
expected-count gaps for days where totals.json disagrees, without failing.
Record the measured per-day import time in design §7.1 if it differs
materially from the prototype.

Outcome: the first live run failed 31 of 33 days because the killmail
index lags the files (design §2.1). Discovery now refreshes file headers
with HEAD requests for a bounded set of objects (design §6), and `sync`
gained `--sweep` for the weekly full re-check.

## M3. Entities

Deliverables:

- `sources/everef.py`: backfill listing via HTML parse with the index.json
  path tried first.
- `jobs/entities.py` seed: extract, inspect and pin the exact field sets
  (write them into design §7.3), DuckDB `read_json(format='array')`,
  upsert into entity tables, registry object of dataset `entities_backfill`.
  Reference: `ingestion/history_archive/rows.rs` for the diagnostics it
  records on malformed events.
- `sources/esi/`: client, policy and cache per design §11. Reference:
  `infrastructure/esi/` in kat is a complete, audited implementation; port
  its policy decisions and its tests for 304, throttling, retry and 420.
- `jobs/entities.py` refresh: queue population from recent killmail
  partitions, budgeted drain, upserts with `source='esi'`.
- `jobs/entities.py` export to `lake/entities/*.parquet`.
- `evedw entities seed <archive-or-url>`, `evedw entities refresh --budget`.

Acceptance: seed of the 2026-05-10 backfill completes and reports counts per
table; refresh with a mocked ESI respects pacing and budget in tests and
stops on 420; export files are readable with plain `read_parquet`.

## M4. Service

Deliverables:

- `service/app.py` and routes from design §9, Arrow IPC streaming on
  `Accept`.
- `jobs/scheduler.py` with the default intervals.
- `evedw serve`; CLI commands detect a running service and trigger over HTTP.
- `GET /datasets/{name}/objects?changed_since` backed by a registry query.

Acceptance: service runs for 24 hours unattended, run log shows the scheduled
syncs, a CLI trigger while it runs returns a run id and shows up in
`/runs`. A sample consumer script in `docs/consumers.md` pulls a date range
as Arrow and reads the lake directly with DuckDB.

## M5. Full backfill and hardening

- Full killmail and market history backfill on the real machine. Record
  wall time, lake size and raw size in design §4.
- `verify` end to end on the full lake.
- Second contract-test run with an in-memory stub backend to prove the
  Protocols are complete.
- Operations notes in `docs/operations.md`: starting the service, forcing a
  range, raising `parser_version`, recovering from a failed object, rebuilding
  the lake from `raw/` offline.

## M6. Consumers migrate

Not part of this repository. Valuation, membership reconstruction and reports
move out of kat into their own applications reading the lake. Kat is retired
once those run against the warehouse.
