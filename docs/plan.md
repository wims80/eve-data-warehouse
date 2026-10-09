# Implementation plan

Status: M0 to M5 complete (2026-10-07), M6 (entity coverage) complete
(2026-10-09). M7 (gentler EVE Ref traffic) and M8 (entity discovery and
manual add) not started; each starts once the previous one is accepted.

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

Outcome (2026-10-05): the live seed of the 2026-05-10 archive took 4 min
41 s end to end (77 s download, the rest extraction and import) and loaded
20,826,709 characters, 13,067,089 employment events, 978,601 corporations,
59,014 alliance events and 18,201 alliances, matching an independent scan
of the archive; no unknown keys. `warehouse.duckdb` grew to 2.2 GB and the
Parquet export is 534 MB, written in 2 s. The ESI compatibility date moved
to 2026-08-18 after checking the live specification; routes lost their
trailing slashes. The response cache moved from files into the store
(`ResponseCache` Protocol) so a million cached bodies do not become a
million files. A six-request live refresh behaved: one second between
requests, bodies cached, budget persisted. One week of killmails yields
about 57,000 distinct entities, so the default budget of 20,000 requests a
day cycles the active population roughly every four days; revalidations
cost a request each, so the budget, not the cache, is the limit.

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

Outcome (2026-10-05): every trigger path goes through `jobs/catalog.py`, so
the CLI,`POST /jobs` and the scheduler build identical jobs. The service
queues runs (new `queued` status) and executes them one at a time in a
worker thread; a trigger returns its run id at once. The lock file names the
service, and the CLI reads it to decide between HTTP and in-process. The
first live start seeded the schedule from the CLI runs in the log, skipped
`entities:refresh` because `EVEDW_ESI_CONTACT` is unset, and immediately
started the scheduled `sync:market_history` with no date range, which
HEAD-checked all 8,403 upstream days and marked 8,126 of them `new`: the
scheduled syncs do the full backfill (M5) the moment the service runs. A
`SIGTERM` during that discovery stopped the service in four seconds with the
run recorded `cancelled` and the queued CLI trigger cancelled too. The
24-hour unattended run is left for M5, together with the backfill it
implies.

## M5. Full backfill and hardening

- Full killmail and market history backfill on the real machine. Record
  wall time, lake size and raw size in design §4.
- `verify` end to end on the full lake.
- Second contract-test run with an in-memory stub backend to prove the
  Protocols are complete.
- Operations notes in `docs/operations.md`: starting the service, forcing a
  range, raising `parser_version`, recovering from a failed object, rebuilding
  the lake from `raw/` offline.

Outcome (2026-10-07): the backfill ran inside the
service. Market history (8,403 days, 425M rows) and killmails (6,877 days,
95.6M killmails) are complete with no failed day and no count mismatch at
import; sizes and wall times are in design §4. `verify` with hashing checked
all 15,280 objects in a minute and reported only the newest market day,
which upstream was still extending. The backfill found five defects, all
fixed with tests: index `last_modified` (milliseconds) never matched HEAD
(seconds), so unchanged days looked changed and every sync would re-HEAD all
history; an identical upstream rewrite added a revision; pre-2020-06-18
market files have no `http_last_modified` and two column orders (design
§2.2); pending days registered from a lagging index were downloaded without
a header check; a blank `EVEDW_ESI_CONTACT` counted as set and briefly
enabled ESI refresh. The scheduler no longer counts cancelled runs, and a
scheduled refresh slice yields to queued syncs. `sync --offline` was added
because design §4 promised an offline rebuild that no command provided.
The contract suites, sync, entity, runner and scheduler tests now also run
against `tests/store/memory.py`; doing so moved entity primary keys and the
partition helpers out of the DuckDB package into `domain/schemas.py`. The
24-hour unattended run (2026-10-06 00:17 to 2026-10-07 00:17 UTC) recorded
229 runs and no failure: four syncs of each dataset, the daily export and
221 refresh slices. Syncs picked up rewritten recent days (the 2026-10-04
market day reached revision 6), the ESI daily budget ran out in the evening
and reset at midnight, and the service's memory stayed flat at 1.7 GB.

## M6. Entity coverage

The warehouse is a general EVE dataset (design §1), so entity data must
cover the whole population and stay current without an operator. Design
§7.3 has the refresh design, decided 2026-10-07: 80,000 requests a day, the
2024-05-31 backfill archive unsupported.

Deliverables:

- `Dataset.unsupported` and object status `skipped`; the 2024-05-31
  archive listed.
- Seed skips error placeholder records; `entities_backfill` parser_version
  2, so all archives re-import from `raw/`.
- Registry migration 003: `sweep_state`. Protocol additions
  `state_get/state_put`, `refresh_filter`, `refresh_counts`,
  `EntityStore.ids`; kinds take turns within a priority in `refresh_pop`.
  Contract tests on both backends.
- `EsiClient` requests with `store=False`.
- `jobs/refresh.py`: alliance sweep, affiliation (recent and cycle) with
  batch splitting, active populate, crawl feed, drain by class.
- `evedw entities status` shows sweep state and queue counts per class.
- Adaptive ESI pace (decided 2026-10-07, design §11): 0.2 s spacing (0.05 s from 2026-10-08) when
  calm, doubling to 2 s on warning signs, logged and shown in `evedw esi
  status`; daily budget 300,000, raised to 800,000 on 2026-10-08; refresh slices of 5,000 every minute.
- Daily downtime pause (decided 2026-10-08, design §11): ESI work pauses
  from 10:58 UTC or the first server error from 10:45, `/status` once a
  minute until the restarted server answers; what met the downtime stays
  due. Shown in `evedw esi status`.
- Live run: first affiliation cycle and alliance sweep; record change
  counts, crawl throughput and the first day's request mix in design §7.3.

Acceptance: the first alliance sweep and the first affiliation cycle
complete on the live data dir without anyone running a command; a
corporation that changed alliance is current after the sweep and a character
who changed corporation is queued as a change; the crawl has refreshed its
first entities within the budget; `evedw entities status` shows all of it;
change counts and the request mix are recorded in design §7.3.

Accepted 2026-10-09: sweep 2026-10-07 15:20 to 16:30 UTC and cycle 16:31 to
22:35 UTC unattended (17,969,441 checked, 171,366 changed); spot checks
against live ESI on 2026-10-07; the crawl's first refreshes at about 02:49
UTC on 2026-10-09; numbers in design §7.3.

## M7. Gentler EVE Ref traffic

Not started. Starts once M6 is accepted.

EVE Ref's download guide asks clients to use `etag`, `last-modified` and
`content-length` to avoid downloading the same data twice, and uses two
concurrent requests. On 2026-10-07 eight concurrent HEADs drew 429s from
its Cloudflare front and aborted a killmail sync; two concurrent requests,
`Retry-After` handling and a per-file HEAD fallback were fixed in M6. What
remains is volume: every sync downloads every year's `index.json` (20 for
killmails, 24 for market history, about 117 KB each for a killmail year)
and `totals.json` in full although most never change, and HEADs every file
younger than 120 days, about 1,200 HEADs a day across both datasets.

Deliverables:

- Conditional GETs for `index.json`, `totals.json` and the backfill
  listing: the last body and its `etag`/`last-modified` are kept in the
  store, a `304` reuses the body. EVE Ref serves both validators
  (checked 2026-10-07). Discovery still reads every year's index, as the
  invariant requires; it just stops downloading the unchanged ones.
- `EVEDW_HEAD_DAYS` default 120 -> 30. Rewrites of older days are caught by
  the weekly sweep, which stays.
- Per-sync counts of index requests answered `304`, HEADs and downloads in
  the sync log line, so EVE Ref traffic is visible.
- Tests against the fake EVE Ref; design §6 and §13 amended.

Acceptance: a sync with nothing changed upstream downloads no index body
that was unchanged since the previous sync and sends at most about 30 HEADs
per dataset; a full day of scheduled syncs shows the reduced request count
in the log; no 429 from EVE Ref.

## M8. Entity discovery and manual add

Not started. Starts once M7 is accepted.

ESI cannot enumerate characters, so a character that never appears on a
killmail or in a backfill archive stays unknown. eve-kill.com, whose
database exports are the EVE Ref backfills, has a public read-only JSON API
(`https://eve-kill.com/api`, no auth, behind Cloudflare). Checked
2026-10-07: `GET /characters` lists every character it knows, 21,055,422
against our 20,835,778, with `character_id, name, corporation_id,
alliance_id, faction_id, security_status, last_active`, up to 1,000 per page
by ascending id with an `after` cursor; `GET /corporations/{id}/members` and
`GET /alliances/{id}/members` page members the same way. Corporation history
comes only per character, so it does not replace the ESI crawl. No rate
limit or usage terms are published; we pace ourselves as with ESI. EveWho's
API (500 per page, 10 requests per 30 s) was considered and is not used
unless eve-kill falls short. DOTLAN has no API and stays out.

Deliverables:

- Design §1 amended: documented third-party APIs (eve-kill) are allowed as
  discovery sources; scraping stays out. ESI remains the source of truth:
  anything eve-kill names is refreshed from ESI before it is stored.
- `sources/evekill.py`: client with our User-Agent and contact, one request
  in flight, paced, 429 and `Retry-After` handled like EVE Ref.
- Discovery job, scheduled daily: walk `GET /characters` past the highest
  id seen (kept in `sweep_state`); unknown characters, and unknown
  corporations and alliances they name, go on the refresh queue as changes.
- One-time full walk of the character list (about 21,000 requests) to find
  characters below that id which the backfills missed; resumable.
- `evedw entities add character <id-or-name>...` and the job `entities:add`
  (`POST /jobs/entities:add`). Names resolve through ESI `POST
  /universe/ids`; the characters are refreshed in full at once. Built early
  on 2026-10-08 by id, for the first consumer report: the `focus` queue
  class, `entities add <kind> <id>... [--members]` with members from our own
  tables and, for an alliance, ESI's corporation list. Names and eve-kill
  members remain M8 work.
- `evedw entities add corporation <id-or-name>... [--members]`: details and
  alliance history at once; `--members` takes the member list from
  eve-kill and refreshes every member we did not have, plus the known
  members in our own tables.
- `evedw entities status` shows discovery progress; tests against a fake
  eve-kill; design §7.3, §9 and §13 amended.

Acceptance: the daily discovery run finds characters created since the last
run without anyone running a command; the full walk completes and reports
how many unknown characters it found; adding a character by name that the
warehouse did not have stores its details and employment history in one
run; adding a corporation with `--members` stores members eve-kill lists
that we did not have; everything works from the CLI with the service running
and without it.

## M9. Consumers migrate

Not part of this repository. Valuation, membership reconstruction and reports
move out of kat into their own applications reading the lake. Kat is retired
once those run against the warehouse.
