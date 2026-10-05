# EVE Data Warehouse: design

Status: accepted design, 2026-10-05. Supersedes the `../kat` architecture.
Implementation milestones live in [plan.md](plan.md). Working rules for
sessions live in [../CLAUDE.md](../CLAUDE.md).

## 1. Purpose and scope

A local warehouse service that keeps three datasets current on a developer
machine and delivers them to a handful of local applications.

In scope:

- Killmail history, full EVE Ref history from 2007-12-05 to the latest closed
  UTC day.
- Character, corporation and alliance history: identity plus employment and
  alliance membership events.
- Market price history, EVE Ref daily market history for all regions.
- Self-updating on a schedule, manual runs of the same jobs, a registry that
  records every source object imported and every revision of it, including
  upstream backfills.
- A store boundary that lets the database be swapped by writing one new store
  package.

Out of scope, by decision. These become consumer applications that read the
warehouse:

- ISK valuation of killmails, blueprint and manufacturing cost fallbacks.
- Membership reconstruction, throw weight, fight reconstruction, dashboards.
- EveWho, DOTLAN and zKillboard scraping. A zKillboard metadata dataset can be
  added later as a fourth dataset if its flags are wanted.
- Disk budgeting beyond a free-space floor. Nothing is deleted automatically.

## 2. Sources and how they change

Facts below were measured on 2026-10-05 against live EVE Ref data.

### 2.1 EVE Ref killmails

- URL pattern: `https://data.everef.net/killmails/<year>/killmails-<YYYY-MM-DD>.tar.bz2`.
- Per-year index: `https://data.everef.net/killmails/<year>/index.json`, an
  array of `{name, size, etag, last_modified, file_time, type, url}`. This is
  the change feed. Twenty requests cover all years.
- Expected counts: `https://data.everef.net/killmails/totals.json`, keys are
  `YYYYMMDD`, values are killmail counts per day.
- Archive layout: `killmails/<killmail_id>.json`, one compact single-line JSON
  document per killmail, the ESI body plus two EVE Ref additions,
  `killmail_hash` and `http_last_modified`. Top-level keys observed:
  `attackers, http_last_modified, killmail_hash, killmail_id, killmail_time,
  moon_id, solar_system_id, victim, war_id`.
- Items nest one level: a container item carries an `items` array whose
  members have no further `items`. Zero deeper nesting was observed in a full
  day; the normaliser must still reject deeper nesting loudly rather than
  silently drop it.
- Days are rewritten in place when late killmails arrive. The `last_modified`
  of a day can be months after the day itself. Sync therefore diffs every
  year's index, not a recent tail.
- Size: a 2026 day is around 3 MB compressed and 15k killmails. 2007 days are
  a few thousand killmails. Full history is in the order of 140M killmails.
- The same killmail ID can appear in two daily archives (kat found about
  14k such IDs). The lake keys facts by `(source_date, killmail_id)` and
  provides a deduplicating view for consumers that need one row per ID.

### 2.2 EVE Ref market history

- URL pattern: `https://data.everef.net/market-history/<year>/market-history-<YYYY-MM-DD>.csv.bz2`.
- Same per-year `index.json` shape. `totals.json` keys are `YYYY-MM-DD`.
- CSV header: `average,date,highest,lowest,order_count,volume,http_last_modified,region_id,type_id`.
  All regions are included, about 48k rows per day.
- Days are rewritten in place, and old days are rewritten long after the fact:
  the 2026-01-01 file was last modified 2026-10-03. Full-index diff is
  mandatory here.

### 2.3 Character, corporation and alliance history

- Bulk seed: `https://data.everef.net/characters-corporations-alliances/backfills/eve-kill-com-karbowiak-<YYYY-MM-DD>.tar.bz2`.
  Four exist (2024-05-31, 2024-09-27, 2025-03-13, 2026-05-10, the last is
  about 820 MB). The directory's `index.json` returns 404, so discovery
  parses the HTML listing. Members are `characters.json`, `corporations.json`
  and `alliances.json`, each a large JSON array. Character records carry a
  `history` array of `{corporation_id, record_id, start_date}`; corporation
  records carry `{alliance_id, record_id, start_date, is_deleted}`. The exact
  field set is confirmed during milestone M3 by inspecting the archive.
- Currency: ESI, public endpoints only. `GET /characters/{id}/`,
  `GET /characters/{id}/corporationhistory/`, `GET /corporations/{id}/`,
  `GET /corporations/{id}/alliancehistory/`, `GET /alliances/{id}/`,
  `POST /universe/names/`. Refresh is driven by the IDs that appear in
  recently imported killmails, under a daily request budget.
- A snapshot date is an observation horizon, not an event date. Every entity
  row records `observed_at` and `source`.

## 3. Architecture

Three layers, dependencies point downward only.

```
service   FastAPI app, asyncio scheduler, job runner, writer lock
jobs      discover, fetch, import, verify, entity seed/refresh/export
sources   EVE Ref client, ESI client with policy and cache, archive readers
store     Protocols (base.py) and one backend package per database
domain    dataset definitions, Arrow schemas, value types; no I/O
```

Rules:

- `domain` imports nothing from the other layers and performs no I/O.
- `store/base.py` defines Protocols only. A backend package such as
  `store/duckdb_lake/` implements them and is the only place that imports a
  database driver.
- `jobs` talk to the store only through the Protocols. `sources` return Arrow
  tables or file paths, never database handles.
- Arrow is the interchange type across every boundary: sources produce it,
  the store consumes and returns it, the API streams it.
- The CLI never imports data itself. It calls the running service, or, when
  no service is running, starts the job runner in-process under the same
  writer lock. Manual and scheduled runs execute identical code.

Package layout:

```
pyproject.toml
CLAUDE.md
docs/design.md, docs/plan.md, docs/prototypes/
src/evedw/
  config.py            pydantic-settings, EVEDW_ prefix, .env support
  domain/
    datasets.py        Dataset definitions: name, index URLs, totals URL, parser_version
    schemas.py         pyarrow schemas for every lake table and registry table
    ids.py             small value types (ObjectKey, Revision, RunId)
  sources/
    everef.py          index.json discovery, totals, download with etag/size checks
    archives.py        tar.bz2 and csv.bz2 readers producing NDJSON/CSV scratch files
    esi/
      client.py        httpx client, headers, compatibility date, conditional requests
      policy.py        pacing, error limits, Retry-After, cooldown persistence
      cache.py         persistent response cache keyed by URL
  store/
    base.py            Protocols: Registry, Lake, EntityStore, Queries
    duckdb_lake/
      registry.py      registry tables in warehouse.duckdb
      lake.py          Parquet writer with atomic replace, view DDL
      entities.py      entity tables, upserts, export
      queries/*.sql    named read queries
      migrations/*.sql registry schema versions
  jobs/
    runner.py          writer lock, run log, cancellation, trigger source
    scheduler.py       asyncio interval scheduler
    sync.py            discover + fetch + import per dataset, newest first
    killmails.py       NDJSON -> normalised tables
    market.py          CSV -> table
    entities.py        seed from backfill, ESI refresh, Parquet export
    verify.py          counts, file integrity, registry consistency
  service/
    app.py             FastAPI factory
    routes/            datasets, jobs, runs, query, lake
  cli.py               typer entry point `evedw`
tests/
  fixtures/            one small real day of each dataset, trimmed
```

## 4. Storage layout

Everything lives under `EVEDW_DATA_DIR` (default `./data`).

```
data/
  warehouse.duckdb                 registry, run log, entity tables, views. Private to the service.
  writer.lock                      flock held by the single writer
  raw/
    killmails/2026/killmails-2026-10-01/<sha256>.tar.bz2
    market_history/2026/market-history-2026-10-01/<sha256>.csv.bz2
    entities_backfill/<year>/<name>/<sha256>.tar.bz2
  lake/
    killmails/source_date=2026-10-01/data.parquet
    attackers/source_date=2026-10-01/data.parquet
    items/source_date=2026-10-01/data.parquet
    market_history/date=2026-10-01/data.parquet
    entities/characters.parquet, corporations.parquet, alliances.parquet,
             character_employment.parquet, corporation_alliance_history.parquet
    esi/      ESI response cache (sqlite or duckdb, backend's choice)
  scratch/                          per-run extraction dirs, deleted after the run
```

- Raw archives are retained per content hash, every revision. They are the
  evidence and the regeneration source; the lake can always be rebuilt from
  them with no network.
- The lake is the delivery contract. Any process may read it at any time.
  The partition column is stored inside each file as well as in the
  directory name, so a single file is self-describing. DuckDB reads the
  layout as is with or without `hive_partitioning`. pyarrow's and Polars'
  hive mode infers the directory value as a string and conflicts with the
  date column in the file, so those readers should use plain directory
  scans and rely on Parquet statistics for date pruning, or declare the
  partition schema as date explicitly. `evedw views` prints DuckDB view DDL
  for consumers.
- `warehouse.duckdb` is opened only by the service. DuckDB permits one
  writer or many readers on a file, not both, so consumers must never open
  it. Entity tables are exported to Parquet for them.
- Parquet files carry key-value metadata: `dataset`, `object_key`,
  `revision`, `source_sha256`, `parser_version`, `written_at`.

Size expectations, full history, ZSTD Parquet, from the measured day:

| Table | Per 2026 day | Full history estimate |
| --- | --- | --- |
| killmails | 0.9 MB | 4 GB |
| attackers | 2 MB | 10 GB |
| items | 0.9 MB | 4 GB |
| market_history | 1 MB | 8 GB |
| raw archives | 4 MB | 15 GB |

Everything fits in well under 100 GB. The disk guard is a free-space floor
(`EVEDW_MIN_FREE_GB`, default 20) checked before each download and import.

## 5. Registry

Registry tables live in the store backend. The Protocol exposes them as
operations, not SQL. Columns below are the logical model.

`source_object`, one row per upstream file:

| column | notes |
| --- | --- |
| dataset | `killmails`, `market_history`, `entities_backfill` |
| object_key | `2026/killmails-2026-10-01.tar.bz2` |
| url | |
| logical_date | the day the object covers; the snapshot date for backfills |
| upstream_etag, upstream_size, upstream_last_modified | from index.json |
| expected_count | from totals.json, null if absent |
| discovered_at, last_seen_at | |
| current_revision | revision number whose data is live in the lake, null if none |
| status | `new`, `changed`, `fetching`, `fetched`, `importing`, `imported`, `failed`, `gone` |
| last_error | |

`source_revision`, one row per fetched content hash of an object:

| column | notes |
| --- | --- |
| dataset, object_key, revision | revision is 1, 2, 3 per object |
| sha256, size, raw_path | |
| upstream_etag, upstream_last_modified | as seen at fetch time |
| fetched_at, imported_at | |
| parser_version | |
| observed_count | rows produced by the normaliser |
| verified | `expected_count` matched, or null when no expectation |
| status | `fetched`, `imported`, `superseded`, `failed` |

`import_run`, append-only:

| column | notes |
| --- | --- |
| run_id, job, trigger (`schedule`, `manual`, `cli`) | |
| started_at, finished_at, status | |
| params_json | |
| objects_changed, rows_written | |
| error | |

`entity_refresh` queue: `kind, entity_id, priority, last_refreshed_at,
next_due_at, etag, failures, last_error`.

`schema_version` for registry migrations.

## 6. Object lifecycle

```
discover   fetch per-year index.json; for each entry compare (etag, size,
           last_modified) with source_object. New or different -> status
           `changed`. Entries that vanished -> `gone` (data stays). Refresh
           expected_count from totals.json.
fetch      download to scratch, verify Content-Length against index size and
           response ETag against index etag. Mismatch means the upstream file
           moved under us: re-discover that object instead of failing the
           run. Hash to sha256, move into raw/, insert source_revision.
import     extract to scratch, normalise with DuckDB into temp tables, write
           Parquet to `<partition>/data.parquet.tmp`, compare observed_count
           with expected_count, then os.replace() over data.parquet. Update
           source_revision and source_object.current_revision in one
           registry transaction. Previous revision -> `superseded`.
verify     recount Parquet partitions against registry and totals, confirm
           every current revision's raw file exists and hashes correctly,
           report drift. Read-only.
```

Properties this gives:

- A backfilled day is just revision N+1 of an object. The registry keeps the
  full chain, so "what changed since my last pull" is a query.
- Interrupted runs leave at most a `.tmp` file and an object in `fetching`
  or `importing`; the next run redoes that object. Readers never see a
  partial day because the replace is atomic.
- Raising `parser_version` for a dataset marks every object `changed`
  without refetching: import reuses the retained raw file.
- Count mismatches do not block import. The revision is marked unverified
  and reported, because totals.json and the archive are not updated at the
  same instant upstream.
- Sync order is newest first, so the most useful data arrives first on a
  cold start and the common incremental case finishes in seconds.

## 7. Dataset specifications

### 7.1 Killmails

Normalisation runs inside DuckDB with an explicit column schema. Schema
inference across days is not used because it drifts. The reference query is
`docs/prototypes/killmail_normalise.py`, measured on 2026-10-01:

| step | result |
| --- | --- |
| concatenate 14,807 member files into one NDJSON, no parsing | 0.09 s |
| read NDJSON with explicit schema | 0.05 s |
| same data read as 14,807 separate files | 3.2 s |
| normalise to three tables and write Parquet | 0.9 s |

The archive reader therefore writes one NDJSON file per day; it never hands
DuckDB a glob of small files.

Tables, all with leading `source_date DATE`:

- `killmails`: `killmail_id, killmail_hash, killmail_time, http_last_modified,
  solar_system_id, moon_id, war_id, victim_character_id,
  victim_corporation_id, victim_alliance_id, victim_faction_id,
  victim_ship_type_id, damage_taken, pos_x, pos_y, pos_z, attacker_count`.
- `attackers`: `killmail_id, ordinal, character_id, corporation_id,
  alliance_id, faction_id, ship_type_id, weapon_type_id, damage_done,
  final_blow, security_status`.
- `items`: `killmail_id, parent_ordinal, ordinal, flag, item_type_id,
  quantity_destroyed, quantity_dropped, singleton`. `parent_ordinal` is null
  for top-level items and the container's ordinal for nested ones.

Damage fields are `BIGINT` and keep the sign; historical killmails contain
negative damage and the source value is preserved.

Schema drift check: each import collects the distinct key sets at the
top level, `victim`, `attackers[*]` and `victim.items[*]` with `json_keys`
and compares them with the expected sets. Unknown keys are logged to the run
and surfaced by `verify`; they do not fail the import.

Consumer views, generated by the backend and printable with `evedw views`:
`killmails`, `attackers`, `items` over the partition globs, and
`killmails_unique` which keeps the newest `source_date` per `killmail_id`.

### 7.2 Market history

One table `market_history` partitioned by `date`: `date, region_id, type_id,
average, highest, lowest, order_count, volume, http_last_modified`. Prices
are `DOUBLE`, counts `BIGINT`. DuckDB reads the decompressed CSV directly with
an explicit column list. Verification compares row count with totals.json.

Measured 2026-10-05 against live EVE Ref: 34 days (2026-09-01 to 2026-10-04)
synced in 13 s wall time, about 49k rows per day, 0.56 MB raw and 0.8 MB
Parquet per day. A second sync with nothing changed took under two seconds.

### 7.3 Entities

Native tables in the store backend, exported to Parquet after each refresh
job:

- `characters`: `character_id, name, corporation_id, alliance_id, faction_id,
  birthday, security_status, deleted, observed_at, source`.
- `corporations`: `corporation_id, name, ticker, alliance_id, ceo_id,
  member_count, date_founded, deleted, observed_at, source`.
- `alliances`: `alliance_id, name, ticker, executor_corporation_id,
  date_founded, deleted, observed_at, source`.
- `character_employment`: `character_id, record_id, corporation_id,
  start_date, observed_at, source`. Primary key `(character_id, record_id)`.
- `corporation_alliance_history`: `corporation_id, record_id, alliance_id,
  start_date, is_deleted, observed_at, source`. Primary key
  `(corporation_id, record_id)`.

`source` is `everef_backfill:<sha256>` or `esi`. History rows are upserted by
record ID; ESI wins over the backfill on conflict because it is newer.

Seed job: extract the three members to scratch, read each with DuckDB
`read_json(format='array')` and an explicit schema, keeping `history` as a
JSON column, then unnest into the history tables. The archive is imported
once per sha256 and recorded in the registry like any other object.

Refresh job: pops due entries from `entity_refresh` ordered by priority and
`next_due_at`. Priority sources: IDs seen in killmails imported in the last
seven days get priority 1, everything else ages in by `last_refreshed_at`.
Each entity costs one or two requests. Stops when the daily budget
(`EVEDW_ESI_DAILY_BUDGET`, default 20,000) is spent or the queue is empty.
Conditional requests with stored ETags make unchanged entities cheap.

## 8. Jobs and scheduling

| job | default interval | what it does |
| --- | --- | --- |
| `sync:killmails` | every 6 h | discover, fetch, import changed objects, newest first |
| `sync:market_history` | every 6 h | same |
| `entities:refresh` | continuous, paced by ESI policy | drain due refresh queue within budget |
| `entities:export` | daily | write entity Parquet snapshots |
| `verify` | weekly | read-only consistency report |

The scheduler is a small asyncio loop owned by the service: each job has an
interval and a next-due time, jobs never overlap with each other because the
runner serialises writers, and a failed run reschedules at the normal
interval with the error in the run log. No third-party scheduler.

The writer lock is an `fcntl.flock` on `data/writer.lock`. The service holds
it for its lifetime. A CLI invoked while the service runs sends an HTTP
trigger instead of running in-process. A CLI invoked with no service takes
the lock and runs the job runner itself.

Every job accepts a date range and a `--force` flag that treats objects in
range as `changed`. That is the manual backfill path.

## 9. Service API

Bound to `127.0.0.1:8470` by default. No authentication, local only.

| method and path | purpose |
| --- | --- |
| `GET /datasets` | names, parser versions, object counts, newest and oldest logical date |
| `GET /datasets/{name}/objects?changed_since=<ts>` | objects whose current revision was imported after a timestamp. The incremental pull primitive for consumers. |
| `GET /datasets/{name}/objects/{key}` | full revision chain |
| `POST /jobs/{name}` with `{from, to, force}` | trigger; returns run id |
| `GET /runs?limit=` and `GET /runs/{id}` | run log |
| `GET /lake` | partition manifest: table, partition, path, revision, sha256 |
| `GET /query/{named}?params` | runs a named read query from `store/.../queries/`, returns Arrow IPC stream when `Accept: application/vnd.apache.arrow.stream`, else JSON |
| `GET /health` | lock held, last run per job, free space |

Named queries are deliberately few: by-date-range reads of each table and
entity lookups by ID. Analytical queries belong in consumers reading Parquet.

## 10. Store boundary and swapping

`store/base.py` Protocols:

- `Registry`: `upsert_objects`, `mark`, `objects(status, dataset)`,
  `add_revision`, `promote(object, revision)`, `start_run`, `finish_run`,
  `runs`, `refresh_queue_pop`, `refresh_queue_push`.
- `Lake`: `write_partition(table, partition, arrow_table, metadata)`,
  `partitions(table)`, `read(table, date_from, date_to)`.
- `EntityStore`: `upsert_characters`, `upsert_corporations`,
  `upsert_alliances`, `upsert_employment`, `upsert_alliance_history`,
  `export_parquet(dir)`, `lookup(kind, ids)`.
- `Queries`: `run(name, params) -> pyarrow.Table`.

The DuckDB backend is the first implementation. Facts live in Parquet
regardless of backend, so a second backend (ClickHouse, Postgres with ADBC)
only has to implement registry, entities and named queries, and can leave
`Lake` as the shared Parquet implementation or replace it. The boundary is
exercised by a contract test suite that any backend must pass.

What is not abstracted: the SQL inside named queries and inside the killmail
normaliser. These are per-backend files by design.

## 11. ESI policy

Binding for every ESI request made by this project, including ad hoc scripts.

- One request in flight, at least one second between requests, five attempts
  for transient failures with exponential backoff and jitter.
- Send `User-Agent` with project name, version, repository URL and
  `EVEDW_ESI_CONTACT`. Send the `X-Compatibility-Date` header required by
  current ESI and pin its value in `config.py`. Check the live endpoint
  specification before adding an endpoint.
- Honour `Cache-Control`, `Expires`, `ETag` and `Last-Modified`. Store
  bodies and validators; a 304 keeps the cached body. Never request an
  entity before its cached expiry.
- Read both the per-bucket rate-limit headers and the legacy
  `X-ESI-Error-Limit-Remain` and `-Reset` headers. Pause early when
  allowances run low. Honour `Retry-After`. Persist cooldowns so a restart
  does not reset them.
- A 420 or a 403 stops all ESI work until an operator clears it. Permanent
  errors (404, 410, 422) are recorded on the entity and not retried.
- `POST /universe/names/` batches at most 1,000 unique IDs. A rejected batch
  is recorded, not split and retried.
- Tests mock ESI. Live calls happen only in explicitly marked manual tests.

## 12. Verification

`evedw verify [--dataset] [--from --to]`, also scheduled weekly:

- Every `source_object` with a current revision has a readable Parquet
  partition whose row count equals the revision's `observed_count`.
- Every current revision's raw file exists and its sha256 matches.
- `observed_count` versus `expected_count` from a fresh totals.json;
  mismatches listed with the gap.
- Partitions on disk with no registry row, and the reverse.
- Unknown JSON keys recorded by imports.

Output is a text table or JSON. It never writes.

## 13. Configuration

`pydantic-settings`, prefix `EVEDW_`, loaded from environment and `.env`.
Keys: `DATA_DIR`, `BIND`, `MIN_FREE_GB`, `ESI_CONTACT`, `ESI_DAILY_BUDGET`,
`ESI_COMPATIBILITY_DATE`, `EVEREF_BASE_URL`, `ESI_BASE_URL`,
`SYNC_INTERVAL_KILLMAILS`, `SYNC_INTERVAL_MARKET`, `LOG_LEVEL`. Base URLs
are overridable so tests can point at a local fixture server.

## 14. Tooling

- Python 3.13 or newer. `uv` for environments and locking, `ruff` for lint
  and format, `pyright` strict for types, `pytest` with `respx` for HTTP
  mocks.
- Runtime dependencies: `duckdb`, `pyarrow`, `httpx`, `pydantic`,
  `pydantic-settings`, `fastapi`, `uvicorn`, `typer`.
- Measured on this machine: duckdb 1.5.6 and pyarrow 25 install on Python
  3.14.
- Logging through the standard library with a key-value formatter. Every log
  line from a job carries `run_id`, `dataset` and `object_key` where known.
- Fixtures: one real day of each dataset, trimmed to about 50 killmails and
  a few hundred market rows, committed under `tests/fixtures/`.

## 15. Deferred decisions

- zKillboard metadata dataset. Add as `zkb` with its own object type when
  someone needs `fitted_value`, `solo` or `npc` flags.
- Second store backend. Not started until a consumer needs server-side SQL;
  the contract tests exist from M1 so it stays possible.
- Reference data (types, groups, systems) is not a warehouse dataset.
  Consumers load the SDE themselves; revisit if three consumers duplicate it.
