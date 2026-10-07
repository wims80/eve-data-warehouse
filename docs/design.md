# EVE Data Warehouse: design

Status: accepted design, 2026-10-05. Supersedes the `../kat` architecture.
Implementation milestones live in [plan.md](plan.md). Working rules for
sessions live in [../CLAUDE.md](../CLAUDE.md).

## 1. Purpose and scope

A local warehouse service that keeps a broad EVE Online dataset complete and
current on a developer machine and delivers it to local applications. Raw
EVE data is in scope; analysis built on it is not. Coverage aims at the
whole population of a dataset, not at what happens to appear on killmails.

In scope:

- Killmail history, full EVE Ref history from 2007-12-05 to the latest closed
  UTC day.
- Character, corporation and alliance history: identity plus employment and
  alliance membership events, for every entity the warehouse knows of.
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
- Authenticated ESI (EVE SSO login), and with it full corporation member
  lists. Only public endpoints are used (decided 2026-10-07).

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
  of a day can be months after the day itself.
- The per-year index lags the files it lists. Measured 2026-10-05: the 2026
  index was regenerated at 07:23 UTC, the files it lists were rewritten at
  11:35 the same day, and 57 of 275 entries carried an ETag the file no
  longer had. The 2025 index was last regenerated on 2026-06-22 while two of
  forty sampled 2025 files had changed since. Indexes for 2020 and 2012
  matched. The index is therefore the listing, never the change feed; see
  section 6.
- Size: a 2026 day is around 3 MB compressed and 15k killmails. 2007 days are
  a few thousand killmails. Full history measured 95.6M killmails on
  2026-10-05 (section 4).
- The same killmail ID can appear in two daily archives (kat found about
  14k such IDs). The lake keys facts by `(source_date, killmail_id)` and
  provides a deduplicating view for consumers that need one row per ID.

### 2.2 EVE Ref market history

- URL pattern: `https://data.everef.net/market-history/<year>/market-history-<YYYY-MM-DD>.csv.bz2`.
- Same per-year `index.json` shape. `totals.json` keys are `YYYY-MM-DD`.
- CSV header: `average,date,highest,lowest,order_count,volume,http_last_modified,region_id,type_id`.
  All regions are included, about 48k rows per day.
- The header changed over time. Measured over all 8,403 files on 2026-10-05:
  up to 2018 `date,region_id,type_id,average,highest,lowest,volume,order_count`;
  then `lowest` and `highest` swap places; from 2020-06-18 the current
  columns plus `http_last_modified`, in two orders. Columns are read by name,
  only these two column sets are accepted, and `http_last_modified` is NULL
  before 2020-06-18.
- Days are rewritten in place, and old days are rewritten long after the fact:
  the 2026-01-01 file was last modified 2026-10-03. The market history index
  was fresh when measured, but the same header-refresh policy applies to it
  as to killmails.

### 2.3 Character, corporation and alliance history

- Bulk seed: `https://data.everef.net/characters-corporations-alliances/backfills/eve-kill-com-karbowiak-<YYYY-MM-DD>.tar.bz2`.
  Four exist (2024-05-31, 2024-09-27, 2025-03-13, 2026-05-10; 278 MB,
  573 MB, 651 MB, 859 MB). The directory's `index.json` returns 404, so
  discovery parses the HTML listing (`<tr class="data-file">` rows with the
  link, a byte-size cell and a `<time datetime>`); the listing has size and
  last-modified but no ETag, so the HEAD refresh of section 6 supplies it.
- Archive layout, measured on the 2026-05-10 file: one directory containing
  `characters.json` (10.3 GB, 20,826,709 records, 1,409,845 with a non-empty
  `history`, 13,067,089 history events, longest history 1,120),
  `corporations.json` (562 MB, 978,601 records, 20,329 with history, 59,014
  events) and `alliances.json` (6.6 MB, 18,201 records). Each member is one
  JSON array with one record per line. The exact key sets are pinned in
  `jobs/entities.py` and summarised in section 7.3. Top-level timestamps look
  like `2003-03-12 20:04:00+00` with optional fractions; history
  `start_date` values are ISO `2010-11-02T20:05:00.000Z`. Corporation
  history events have no `is_deleted`; that column is only filled from ESI.
  `deleted` is true for 2,857,776 characters, 2,485,188 of which sit in
  Doomheim (corporation 1000001); no corporation or alliance in the file is
  marked deleted.
- DuckDB reads the 10 GB array directly with `read_json(format='array')` in
  about 15 seconds per pass as long as the query streams; a `list()`
  aggregate over all records exhausts memory.
- Currency: ESI, public endpoints only, pinned to compatibility date
  2026-08-18 (the newest listed by `/meta/compatibility-dates` on
  2026-10-05). Routes have no version prefix and no trailing slash:
  `GET /characters/{id}`, `GET /characters/{id}/corporationhistory`,
  `GET /corporations/{id}`, `GET /corporations/{id}/alliancehistory`,
  `GET /alliances/{id}`, `POST /universe/names`. All return `Cache-Control`,
  `ETag` and `Last-Modified`. Refresh is driven by the IDs that appear in
  recently imported killmails, under a daily request budget.
- A snapshot date is an observation horizon, not an event date. Every entity
  row records `observed_at` and `source`.
- The archives are eve-kill.com database exports and differ by generation,
  measured 2026-10-07. 2024-05-31 is a MongoDB export (newline-delimited,
  `$oid`/`$date` wrappers, other field names) and is marked unsupported
  (section 6). 2025-03-13 carries Mongo `_id`/`__v`. 2024-09-27 embeds whole
  ESI responses (`error`, `body`, `headers`). Error placeholder records (an
  `error` key or no name) are skipped. eve-kill fetched history only for
  entities it cared about: after seeding 2024-09-27, 2025-03-13 and
  2026-05-10, 1,433,725 of 17,970,116 live characters have employment
  history and 97,738 of 978,705 corporations have alliance history. Older
  snapshots added 261k employment and 166k alliance-history events.
- ESI facts for the refresh, checked 2026-10-07 against the specification
  and a two-request probe: `POST /characters/affiliation` takes 1 to 1,000
  character ids and returns `character_id, corporation_id, alliance_id,
  faction_id`; a deleted character comes back in Doomheim; one nonexistent
  id fails the batch with `400 Invalid character ID`. `GET /alliances` lists
  live alliance ids and `GET /alliances/{id}/corporations` an alliance's
  members. Character history does not record the move to Doomheim. None of
  these routes sends rate-limit bucket headers, so only the error limit and
  our own pacing (at most 86,400 requests a day) bound them.

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
      policy.py        pacing, error limits, Retry-After, budget; persisted as JSON
  store/
    base.py            Protocols: Registry, Lake, EntityStore, ResponseCache, Queries
    duckdb_lake/
      registry.py      registry tables in warehouse.duckdb
      lake.py          Parquet writer with atomic replace, view DDL
      entities.py      entity tables, upserts, export
      cache.py         ESI response cache table
      queries.py       named query runner: parses the query headers, binds parameters
      queries/*.sql    named read queries
      migrations/*.sql registry schema versions
  jobs/
    runner.py          writer lock, run log, cancellation, trigger source
    catalog.py         job names, parameter validation, builds a job from name + params
    scheduler.py       asyncio interval scheduler
    sync.py            discover + fetch + import per dataset, newest first
    killmails.py       NDJSON -> normalised tables
    market.py          CSV -> table
    entities.py        seed from backfill, ESI refresh, Parquet export
    verify.py          counts, file integrity, registry consistency
  service/
    app.py             FastAPI factory and lifespan
    state.py           what the service owns; routes reach it through the request
    worker.py          single job worker: queued runs, one at a time, cancel on stop
    models.py          response bodies, shared with the client
    client.py          HTTP client the CLI uses when a service holds the lock
    routes/            health, datasets, jobs, runs, lake, query
  cli.py               typer entry point `evedw`
tests/
  fixtures/            one small real day of each dataset, trimmed
```

## 4. Storage layout

Everything lives under `EVEDW_DATA_DIR` (default `./data`).

```
data/
  warehouse.duckdb                 registry, run log, entity tables, ESI response cache. Private to the service.
  writer.lock                      flock held by the single writer
  esi/policy.json                  ESI pacing state: cooldowns, stop flag, daily budget use
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

Sizes measured 2026-10-05 after the full backfill (killmails 2007-12-05 to
2026-10-03, 6,877 days; market history 2003-10-01 to 2026-10-04, 8,403 days),
ZSTD Parquet:

| Table | Per recent day | Full history, measured |
| --- | --- | --- |
| killmails | 1.2 MB | 5.6 GB, 95,579,144 rows (95,565,101 distinct ids) |
| attackers | 0.8 MB | 4.4 GB, 441,519,636 rows |
| items | 0.9 MB | 4.6 GB, 1,403,479,129 rows |
| market_history | 0.8 MB | 7.1 GB, 425,033,763 rows |
| raw killmail archives | 3 MB | 14.8 GB |
| raw market archives | 0.6 MB | 4.5 GB |
| entity tables in warehouse.duckdb (2026-05-10 seed) | | 2.1 GB |
| entity Parquet export | | 0.5 GB |
| raw backfill archive | | 0.82 GB each |

The whole data dir is about 45 GB. Backfill wall time on this machine, one
object at a time, newest first: market history 6,104 days in 15.5 min when
the raw files were already retained (the first pass downloaded at about
2.7 days a second, roughly 50 min for all of history); killmails 6,408 days
in 2 h 52 min including downloads. No day of either dataset disagreed with
totals.json. The disk guard is a free-space floor
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
| status | `new`, `changed`, `fetching`, `fetched`, `importing`, `imported`, `failed`, `gone`, `skipped` |
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
| run_id, job, trigger (`schedule`, `manual`, `cli`) | `manual` is an HTTP trigger, `cli` an in-process run |
| started_at, finished_at, status | `queued`, `running`, `succeeded`, `failed`, `cancelled`. The service inserts a run as `queued` so a trigger can return its id before the writer is free; `started_at` is reset when it starts. |
| params_json | |
| objects_changed, rows_written | |
| error | |

`entity_refresh` queue: `kind, entity_id, priority, last_refreshed_at,
next_due_at, etag, failures, last_error`.

`sweep_state`: `key, value, updated_at`. Small JSON documents holding the
refresh job's progress (alliance sweep position, affiliation cursor, crawl
cursors), so a restart resumes a sweep instead of starting it again.

`schema_version` for registry migrations.

## 6. Object lifecycle

```
discover   fetch per-year index.json for the object listing, then HEAD a
           bounded set of files and take etag, size and last_modified from
           the response headers, because the index lags the files (section
           2.1). The set is: every object inside an explicit date range,
           every object not yet in the registry, every object still pending
           (it is about to be downloaded), every object whose index
           entry differs from the registry, every object younger than
           `EVEDW_HEAD_DAYS` (default 120), and on a sweep every object.
           HEADs run two at a time, as EVE Ref's download guide does
           (`rclone --checkers 2`); a full killmail sweep is about 7,000
           requests. A 429 waits for `Retry-After`, at least 30 seconds,
           doubling per attempt. A file whose HEAD still fails keeps its
           index metadata for this run, so one file cannot fail discovery;
           measured 2026-10-07, eight concurrent HEADs drew 429s from EVE
           Ref and aborted a killmail sync. Then compare with source_object: new or different ->
           status `changed`. `last_modified` compares at whole seconds,
           because the index carries milliseconds and HTTP dates do not.
           Entries that vanished -> `gone` (data stays). Refresh
           expected_count from totals.json.
fetch      skip an object whose live revision already has its etag under
           the current parser_version (an identical rewrite upstream only
           moves Last-Modified); mark it `imported` again, unless forced.
           Otherwise download to scratch, verify Content-Length against index size and
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

A dataset can name upstream objects it does not support, with the reason
(`Dataset.unsupported`). Sync marks them `skipped` instead of fetching them;
they stay listed in the registry and are reported, never retried.

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

Measured 2026-10-05 against live EVE Ref: one year (330 days to import,
5.53M killmails) synced in 10 min 14 s end to end, about 1.9 s per day
including download, header checks and the three Parquet writes. Every day's
row count matched totals.json. `verify` without hashing covers a year in
under a second; hashing the raw archives is what makes it slow.

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
and compares them with the expected sets. Unknown keys are logged, written
into the partition metadata as `evedw.unknown_keys`, and reported by
`verify` as an `unknown_keys` issue. They do not fail the import. Items
nested deeper than one level do fail it, with a `NestingError`, because the
schema would silently drop them otherwise.

The three partitions of a day are written attackers, items, killmails, and
the registry promotes the revision only after all three. A reader that
scans two tables during that window can see a new killmails partition next
to an old attackers partition for that day. Consumers that join tables and
need a consistent day should filter by the registry's current revision
through the partition metadata, or read through the service.

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

`source` is `everef_backfill:<sha256>` or `esi`. Every upsert is "newer
observation wins": a stored row is replaced only when the incoming
`observed_at` is not older. Backfill rows carry the record's own `updatedAt`
(falling back to the snapshot date), ESI rows carry the request time, so ESI
always wins over a backfill and an older backfill never overwrites a newer
one. Primary keys are the entity id, or `(entity id, record_id)` for history.

Field mapping, pinned 2026-10-05 from the 2026-05-10 archive (full key sets
in `jobs/entities.py`; an unlisted upstream key is logged by the seed and
reported in its counts, never absorbed):

| table | backfill source | ESI source |
| --- | --- | --- |
| characters.deleted | `deleted` flag | `corporation_id == 1000001` (Doomheim), or a 404/410 on refresh |
| corporations.deleted | `deleted` flag (never true in the file) | `state == "closed"`, or a 404/410 |
| alliances.deleted | `deleted` flag | 404/410 on refresh |
| character_employment | `history[]` of `{record_id, corporation_id, start_date}` | `/corporationhistory` |
| corporation_alliance_history | `history[]` of `{record_id, alliance_id, start_date}`; `is_deleted` NULL | `/alliancehistory` incl. `is_deleted` |

Dropped on purpose: descriptions, genders, races, bloodlines, home stations,
shares, tax rates, URLs, creator ids, `last_active`, `createdAt`,
`achievement_score`, titles. They are not history and consumers that want
them can call ESI.

Seed job (`evedw entities seed [date|latest|all]`): the archive is a registry
object of dataset `entities_backfill`, discovered from the HTML listing,
fetched and retained like any other object, and imported by streaming the
three members out of the tar into scratch, then reading each with DuckDB
`read_json(format='array')` and explicit columns. Entities and history are
two passes over the same file, streamed into the store in batches of 250,000
rows so the 10 GB character file never has to fit in memory. The object's
`observed_count` is the number of entity records (characters, corporations
and alliances). Re-running the seed on an imported snapshot does nothing;
`--force` re-imports it, which is a no-op for the data because of the upsert
rule. Error placeholder records are skipped (parser_version 2). `all`
seeds every listed archive that is not imported yet, newest first; an older snapshot only adds rows the newer ones lack (history events,
entities gone since), because the upsert never replaces a newer
observation. The scheduler runs `entities:seed all` daily, so a new upstream
backfill is imported without an operator.

Refresh job (`evedw entities refresh [--budget N]`). Change is detected in
bulk, history is fetched only for what changed, and the rest of the budget
completes history that was never fetched. One run is a slice that works
through these steps in order until its budget is spent; progress lives in
the registry's `sweep_state`, so a restart resumes where the slice stopped.

1. Alliance sweep, every `EVEDW_ALLIANCE_SWEEP_INTERVAL` (daily).
   `GET /alliances`, then `GET /alliances/{id}/corporations` per live
   alliance. A corporation newly listed under an alliance gets that
   `alliance_id` written at once and is queued as a change; a stored member
   no longer listed is queued as a change without a write (it may have
   switched to an alliance processed later). An alliance absent from the
   list is marked deleted. Unknown alliances and corporations are queued as
   changes. Alliances and member corporations whose details are older than
   `EVEDW_ESI_REFRESH_INTERVAL` are queued as active.
2. Affiliation, recent: once a day the characters on killmails of the last
   `EVEDW_ESI_RECENT_DAYS` days, 1,000 per `POST /characters/affiliation`.
3. Affiliation cycle: every live character in id order, 1,000 per request,
   one pass per `EVEDW_AFFILIATION_CYCLE` (weekly), about 18,000 requests
   swept as fast as the budget allows, then idle until the next cycle. Per
   character: Doomheim marks it deleted; a different corporation writes the
   new affiliation at once and queues a change; a different alliance or
   faction only writes; an unknown character or corporation is queued as a
   change. A `400` batch is retried in tenths until the invalid ids are
   isolated, about four errors per invalid id in a batch of 1,000 (halving
   cost about ten); an invalid known character is marked deleted, so it is
   never sent again. Measured 2026-10-07: invalid ids cluster among the
   newest character ids (above 2.1 billion).
4. Active: entities on killmails of the last `EVEDW_ESI_RECENT_DAYS` days
   whose details are older than `EVEDW_ESI_REFRESH_INTERVAL` (30 days).
5. Crawl feed: when fewer than a day of crawl entries is due, more are
   queued. First every character and corporation that appears on any
   killmail and has no queue entry, then every other live character and
   corporation by id, highest first.
6. Drain the queue. Priorities are classes: 0 change, 1 active, 3 crawl;
   2 is a settled entry, idle until something queues it again. Within a
   class kinds take turns, so corporations never wait behind characters.
   A change or active entry fetches details and history (an alliance only
   details); a crawl entry fetches history only, one request. Success sets
   `last_refreshed_at`, priority 2 and a next-due time ten years out. A
   404/410 marks the stored row deleted with `source = 'esi'`; other 4xx
   park the entry for a year with the status recorded; transient failures
   back off by hours, doubling per failure up to a day. A 403 or 420 raises
   out of the job, which records the run as failed; nothing else talks to
   ESI until `evedw esi resume`.

The daily budget (`EVEDW_ESI_DAILY_BUDGET`, default 800,000) bounds all of
it. Steps 1 to 4 need a few thousand requests a day plus the change rate;
the crawl gets the rest. At the calm pace (section 11) the pace, not the
budget, is the limit. Measured 2026-10-07 and 08 with one request in
flight: at 0.2 s spacing affiliation batches went out at about 120 a minute
(ESI takes about 0.3 s to answer one) and the drain's detail and history
GETs at about 220 a minute; at 0.1 s at 325 a minute, and at 0.05 s at
377 a minute, about 540,000 a day. Each GET cycle is now mostly ESI's
response time (about 0.1 s), so a lower floor gains little: even none would
give at most about 550 a minute. At 540,000 a day character history (16.5M
characters, one request each) completes in about a month. The budget was 80,000 (about eight months) until
the adaptive pace replaced the fixed one-second spacing on 2026-10-07, then
300,000, which the drain slightly exceeded; it was raised to 800,000 with
the 0.05 s floor on 2026-10-08 after a clean first cycle (0 errors, 0
slowdowns), so it stays a safety cap above what the pace can send.
`evedw entities status` shows the sweep state and the queue per class.

Export job (`evedw entities export`): `COPY` each table to
`lake/entities/<table>.parquet.tmp`, fsync, replace. `evedw views` adds a
view per exported file.

## 8. Jobs and scheduling

| job | default interval | what it does |
| --- | --- | --- |
| `sync:killmails` | every 6 h | discover, fetch, import changed objects, newest first |
| `sync:market_history` | every 6 h | same |
| `sync:*` with sweep | weekly (`EVEDW_SWEEP_INTERVAL`) | same, but every file's headers are re-checked |
| `entities:refresh` | continuous, paced by ESI policy | sweeps, change queue, active refresh, history crawl (section 7.3) |
| `entities:seed` all | daily (`EVEDW_SEED_INTERVAL`) | import any listed backfill archive not yet seeded |
| `entities:export` | daily | write entity Parquet snapshots |
| `verify` | weekly | read-only consistency report |

The scheduler is a small asyncio loop owned by the service: each entry has a
job name, parameters, an interval and a next-due time. Entries never overlap
because every run, scheduled or triggered, goes through the service's single
job worker, which executes one run at a time in a thread. An entry's next due
time is computed when its run finishes, so the cadence is measured from the
end of the previous run and a slow run never queues itself twice. A failed
run reschedules at the normal interval with the error in the run log. At
startup the next-due times are seeded from the run log: the last run of the
same job with the same parameters counts whatever its trigger, so a manual
sync pushes the next scheduled one back a full interval, and a restart does
not reset a weekly job. A cancelled run does not count: it stopped before
finishing, so after a restart its job is due again. An entry with no recorded run is due immediately,
except the sweeps and `verify`, which wait one interval. No third-party
scheduler.

`entities:refresh` is "continuous" as slices: every `EVEDW_REFRESH_INTERVAL`
(1 min) a refresh run may send at most `EVEDW_REFRESH_SLICE` (5,000) requests,
about 20 to 30 minutes at the calm pace, then yields so a sync is never
blocked for hours by a long drain. A queued scheduled slice also lets any
other queued run start before it, so a triggered backfill does not wait
behind refresh work; a manually triggered refresh keeps its place. It is not scheduled at all while
`EVEDW_ESI_CONTACT` is unset, because ESI asks for contact details in the
User-Agent; the service logs a warning instead. `entities:export` runs every
`EVEDW_EXPORT_INTERVAL` (daily) and `verify` every `EVEDW_VERIFY_INTERVAL`
(weekly) as a job: issues are logged and the run is recorded as failed with
their count and kinds, so drift shows up in the run log.

The scheduled syncs carry no date range, so the first service run on a data
dir that was only partially synced from the CLI performs the full backfill,
newest first.

The writer lock is an `fcntl.flock` on `data/writer.lock`. The service holds
it for its lifetime and writes `service <url>` into the file next to its pid.
A CLI command reads the holder: when it names a service, the command sends
an HTTP trigger and follows the run; otherwise it takes the lock and runs the
job in-process. `evedw status` and `evedw entities status` read through the
service for the same reason, because DuckDB does not let a second process
open `warehouse.duckdb` while the service has it open.

`evedw speed [--seconds N] [--watch]` (added 2026-10-08) reads the ESI
counters in `policy.json` and the refresh state the same way, twice, `N`
seconds apart (default 60), and prints requests per minute with a daily
projection, when the budget would run out, affiliation cycle progress with
an upper bound on the time left, and queue growth per class. It sends no ESI
requests; `--watch` repeats until Ctrl-C.

On shutdown the service sets a cancel flag that jobs check between objects
(sync) or entities (refresh), waits for the current run to notice it, records
it `cancelled`, and marks queued runs cancelled without starting them.

Every job accepts a date range and a `--force` flag that treats objects in
range as `changed`. That is the manual backfill path. `sync --offline` sends
no requests: it skips discovery and works only on objects whose current
etag has a retained raw file, so `sync --offline --force` rebuilds the lake
from `raw/` (section 4). It needs the registry; `raw/` alone does not record
etags or expected counts.

## 9. Service API

Bound to `127.0.0.1:8470` by default. No authentication, local only.

| method and path | purpose |
| --- | --- |
| `GET /datasets` | names, parser versions, object counts by status, newest and oldest logical date, plus row counts of the entity tables |
| `GET /datasets/{name}/objects?changed_since=<ts>` | objects whose current revision was imported after a timestamp (timezone required). The incremental pull primitive for consumers. Without the parameter: every object, oldest first. |
| `GET /datasets/{name}/objects/{key}` | the object and its full revision chain |
| `GET /jobs` | job names |
| `POST /jobs/{name}` with the parameters as a JSON object | queues the job and returns `202` with the run id. Unknown job `404`, bad parameter `422`. Parameters per job: sync `{from, to, force, sweep, offline}`, `entities:seed` `{snapshot, force}`, `entities:refresh` `{budget, populate}`, `verify` `{dataset, from, to, hash, offline}`. |
| `GET /runs?limit=&job=` and `GET /runs/{id}` | run log |
| `GET /lake?table=` | partition manifest: table, partition, path, row count, revision, source sha256, parser version, written_at |
| `GET /query` | the named queries and their parameters |
| `GET /query/{named}?params` | runs a named read query from `store/.../queries/`, returns an Arrow IPC stream when `Accept: application/vnd.apache.arrow.stream` (row count in `X-Row-Count`), else JSON `{name, row_count, columns, rows}` with rows as objects |
| `GET /health` | lock held, free space, the current and queued runs, the schedule with next-due times, last run per job |

Named queries are deliberately few: by-date-range reads of each table and
entity lookups by ID. Analytical queries belong in consumers reading Parquet.
A query file declares its parameters in leading comment lines
(`-- param date_from: date required`); kinds are `date`, `int`, `ids` (a
comma-separated list) and `str`. Entity queries read the live tables, so
they are fresher than the daily Parquet export. The whole result is built in
memory before it is streamed; a consumer that wants a year of killmails
should read the lake directly (see `docs/consumers.md`).

## 10. Store boundary and swapping

`store/base.py` Protocols:

- `Registry`: `upsert_objects`, `mark`, `objects(status, dataset)`,
  `add_revision`, `promote(object, revision)`, `start_run`, `finish_run`,
  `runs`, `refresh_push`, `refresh_pop`, `refresh_update`,
  `refresh_filter`, `refresh_counts`, `state_get`, `state_put`.
- `Lake`: `write_partition(table, partition, arrow_table, metadata)`,
  `partitions(table)`, `read(table, date_from, date_to)`, `view_sql()`.
- `EntityStore`: `upsert(table, arrow_table)` with the newer-observation
  rule of section 7.3, `lookup(table, ids)`, `ids(table, ...)` for
  cursors over live entities, `count(table)`, `export_parquet(dir)`.
- `ResponseCache`: `get(key)`, `put(entry)`, `delete(key)`; the ESI client's
  body and validator store. In DuckDB it is the `esi_cache` table.
- `Queries`: `names()`, `describe(name)`, `run(name, params) -> pyarrow.Table`
  with `params` as the raw strings of a query string, converted by the
  declared kinds.

The DuckDB backend is the first implementation. Facts live in Parquet
regardless of backend, so a second backend (ClickHouse, Postgres with ADBC)
only has to implement registry, entities, response cache and named queries,
and can leave `Lake` as the shared Parquet implementation or replace it. The
boundary is exercised by a contract test suite that any backend must pass
(`tests/store/test_*_contract.py`).

What is not abstracted: the SQL inside named queries and inside the killmail
normaliser. These are per-backend files by design.

## 11. ESI policy

Binding for every ESI request made by this project, including ad hoc scripts.

- One request in flight, five attempts for transient failures (transport
  errors, 408, 429, 5xx) with exponential backoff and jitter. Every request
  goes through `EsiClient._request`.
- The pace adapts (decided 2026-10-07). CCP publishes no request-rate cap
  for the routes we use (none sends `X-RateLimit-Group`); what gets an
  application banned is ignoring the error limit or getting around the cache
  (developers.eveonline.com, checked 2026-10-07). So the next request waits
  `EVEDW_ESI_SPACING` (0.05 s; 0.2 s until 2026-10-08, then briefly 0.1 s)
  after the previous response when calm, which with one request in flight
  is at most 20 a second, in practice about 6 for small GETs because ESI's
  response time (about 0.1 s, measured 2026-10-08 with the service at 10%
  CPU) adds to the gap. CCP's
  pages (rate limiting and best practices, checked 2026-10-08) set no
  request-rate cap for routes without a bucket and ask only that the shared
  API is not abused and that clients do not operate at a limit; a steady
  single stream fits that, back-to-back requests at about 14 a second
  around the clock would push it, so the floor stays above zero. They also
  warn of an undocumented limiter that can answer 429 without headers,
  which the pace treats as a warning sign like any other 429. A warning sign doubles
  the spacing, at most once a minute, up to 2 s: a 429, 420 or 5xx, a
  transport error, fewer than 90 of the 100 legacy errors left in the
  window, or a rate-limit bucket below half. After 5 quiet minutes the
  spacing halves back towards the floor. Every change is logged (`ESI pace
  slowed to ...` at warning level), counted in `slowdowns` and shown by
  `evedw esi status`; spacing and timers live in `policy.json`, so a
  restart keeps a slowed pace. A fresh policy starts at 1 s and ramps
  down.
- Send `User-Agent` with project name, version and `EVEDW_ESI_CONTACT`.
  Send `X-Compatibility-Date`, pinned in `config.py` (2026-08-18, verified
  2026-10-05 against `/meta/openapi.json?compatibility_date=`). Check the
  live specification before adding an endpoint or moving the date.
- Honour `Cache-Control`, `Expires`, `Age`, `Date`, `ETag` and
  `Last-Modified`. Bodies and validators live in the store's
  `ResponseCache`, keyed by base URL, method, route and body. A fresh entry
  is served without a request; an expired one is revalidated with
  `If-None-Match` or `If-Modified-Since`, and a 304 keeps the cached body and
  takes the new expiry. `no-store` responses are never kept. Requests the
  warehouse does not repeat within their cache lifetime are sent with
  `store=False` and keep nothing: affiliation batches, and entity details
  and history fetched by the refresh, whose queue entry is then not due
  again for at least a day. Otherwise the crawl alone would add millions
  of cache rows.
- Read both the per-bucket `X-RateLimit-Group/Limit/Remaining` headers and
  the legacy `X-ESI-Error-Limit-Remain/Reset` headers. Reserve bucket units
  before sending, pause a full window when a bucket is near exhaustion,
  pause a minute on a malformed or negative legacy header, an hour on an
  unparseable limit. Honour `Retry-After` as seconds or a date. The whole
  state is written to `data/esi/policy.json` after every change, so a
  restart does not reset a cooldown.
- A 420 or a 403 sets `stopped` with the reason; every later request raises
  until an operator runs `evedw esi resume`. Other 4xx are permanent for
  that request: raised to the caller, recorded on the entity, not retried.
- A daily request budget (`EVEDW_ESI_DAILY_BUDGET`, default 800,000; 80,000
  until the adaptive pace on 2026-10-07, then 300,000, and 800,000 with the
  0.05 s floor on 2026-10-08) counts requests sent,
  including revalidations and retries, not cache hits; it is kept in the
  policy file and resets by UTC day.
- `POST /universe/names` and `POST /characters/affiliation` batch at most
  1,000 distinct positive IDs. The client raises a rejected batch to the
  caller. The affiliation sweep alone retries a `400` batch in tenths to
  isolate invalid ids; every split request counts against the budget and
  the error limit.
- ESI's error limit is 100 non-2xx/3xx responses a minute on routes
  without bucket limits; past it every route answers 420, and CCP warns
  that ignoring it can get an application banned (checked 2026-10-07).
  At 20 requests a second the limit could be reached in 5 seconds of
  errors, so three guards stack: the pace slows from the tenth error in a
  window, every request pauses until the window resets when 20 or fewer
  remain, and a refresh slice counts
  its error responses, logs them with its batch splits, and ends early at
  100. It checks the cap only after saving progress, so a failing request
  is never sent again because a slice stopped halfway.
- Tests mock ESI (`tests/fake_esi.py`). Live calls happen only in explicitly
  marked manual tests or operator-run commands.

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
Both are anchored to a home directory (decided 2026-10-08), so `evedw` works
from any directory once installed with `uv tool install --editable .`: the
home is `EVEDW_HOME` if set, else the source checkout the package runs from,
else the current directory. `.env` is read from the home, and a relative
`DATA_DIR` from it or the environment resolves against the home; `--data-dir`
resolves against the current directory, where it was typed.
Keys: `DATA_DIR`, `BIND`, `MIN_FREE_GB`, `ESI_CONTACT`, `ESI_DAILY_BUDGET`, `ESI_SPACING`,
`ESI_COMPATIBILITY_DATE`, `ESI_REFRESH_INTERVAL`, `ESI_RECENT_DAYS`,
`EVEREF_BASE_URL`, `ESI_BASE_URL`, `SYNC_INTERVAL_KILLMAILS`,
`SYNC_INTERVAL_MARKET`, `SWEEP_INTERVAL`, `REFRESH_INTERVAL`, `REFRESH_SLICE`,
`EXPORT_INTERVAL`, `SEED_INTERVAL`, `ALLIANCE_SWEEP_INTERVAL`,
`AFFILIATION_CYCLE`, `VERIFY_INTERVAL`, `HEAD_DAYS`, `LOG_LEVEL`.
Intervals are seconds in the environment. Base URLs are overridable so tests
can point at a local fixture server.

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
