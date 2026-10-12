# Implementation plan

Status: M0 to M5 complete (2026-10-07), M6 (entity coverage) complete
(2026-10-09), M7 (gentler EVE Ref traffic) complete (2026-10-11), M8 (zKillboard
as a second killmail source, one merged lake) complete (2026-10-12). Next M9
(live killmails, code done, acceptance in progress), then M10 (entity
discovery and manual add).

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
UTC on 2026-10-09; numbers in design §7.3. Follow-ups the same morning,
from the live crawl (design §7.3): crawl entries for deleted or never-stored
entities move to a `deferred` class behind the crawl, still requested; a 404
or other definitive 4xx sets `last_refreshed_at`, so deleted entities on
recent killmails are no longer asked again every slice.

## M7. Gentler EVE Ref traffic

Complete (2026-10-11).

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

Accepted 2026-10-11: 12 scheduled syncs from 2026-10-09 22:12 to 2026-10-11
05:00 UTC drew no 429 (before M7 every scheduled sync drew 4 to 5); listings
answered 304 except the current year and the totals; 28 to 36 HEADs a sync,
more only when a year's listing changed. Numbers in design §6.

## M8. zKillboard as a second killmail source, one merged killmail lake

Accepted 2026-10-12. Code done 2026-10-11. History backfill done
2026-10-12 00:13 UTC (6,879 of 6,885 days, no 403 or 429). Nine days then
stayed deferred because zKillboard's CDN kept serving older copies than its
headers announced; added 2026-10-12: such a copy is imported as
`provisional` after three syncs and replaced when the newer one is served
(design §6), and `verify` hashes gzipped raw files decompressed (it reported
every zKillboard day as `raw_hash` before).

EVE Ref's daily archives are the only killmail source, and they are not
complete on their own. Comparing EVE Ref's per-day counts in the lake with
zKillboard's R2Z2 history totals (checked 2026-10-11, 6,881 days to
2026-10-06): 95,628,630 against 95,624,822 killmails, 6,431 days equal, 446
days where zKillboard has more, 4 where EVE Ref has more. EVE Ref has no
2026-07-13 at all (not listed, 404, totals 0; zKillboard has 15,746) and
trails by 4 to 50 killmails a day on recent days until it republishes them;
zKillboard's history lacks most of 2023-05-06 (1 against 17,240) and part of
2023-05-08. EVE Ref's 2026-01-06 file also carries 14,042 killmails of
2026-01-05 that are in that day's file too: 14,043 killmail ids sit in two
day partitions of today's lake, so counts across days are inflated.

zKillboard's documented R2Z2 API (`https://r2z2.zkillboard.com`, replacing
RedisQ since 2026-05-31) publishes `/history/totals.json` (killmails per
day) and `/history/raw/{YYYYMMDD}.json` (every killmail of a UTC day as a
map from id to ESI killmail, about 37 MB a recent day, every day since
2007-12-05; ETag and Last-Modified served). Rules: at most 15 requests a
second per IP, a non-empty User-Agent. The history holds ESI killmails only,
no `zkb` metadata.

Deliverables:

- Design §1 amended: zKillboard's documented R2Z2 API is a source; scraping
  zKillboard stays out. §2 gains the source's description.
- A zKillboard policy in design, binding for every request to zKillboard
  including one-off scripts, as §11 is for ESI, and named in `CLAUDE.md`.
  It follows zKillboard's API documentation
  (https://zkillboard.com/api/docs/, checked 2026-10-11), which is checked
  again before the client changes:
  - "Send a descriptive `User-Agent` with a maintainer or project URL": the
    project name, version, `EVEDW_ESI_CONTACT` and the repository URL.
  - "Send `Accept-Encoding: gzip`" (or another compression the client
    supports).
  - "Cache responses locally and space repeated requests out": one request
    in flight, at most 5 a second (`EVEDW_ZKILLBOARD_SPACING` 0.2 s), daily
    history files at least `EVEDW_ZKILLBOARD_DOWNLOAD_SPACING` (1 s) apart;
    well under R2Z2's "up to 15 requests per second per IP". The spacing
    values are ours, not zKillboard's (relaxed from 5 s on 2026-10-11).
  - History files have a one-day cache: `totals.json` and every day file
    are conditional GETs against the stored validators, and a day is
    downloaded again only when its `totals.json` count, ETag or
    Last-Modified changed ("identify missing or changed daily files").
    Discovery still reads `totals.json` in full on every sync.
  - The compact `history/{YYYYMMDD}.json` id and hash index is used to see
    which killmails changed before a day's raw file is fetched again
    ("Use this compact index when rebuilding an archive").
  - The live feed (M9): "Increment by one after each successful response",
    about 100 ms between successes, "After a 404, wait at least six seconds
    before trying again", consume promptly since files are kept "at least
    24 hours", "Filter locally after retrieval", and expect "A reprocessed
    killmail may appear again under a new sequence ID".
  - A 403 or 429 stops every zKillboard request for an hour, logged at
    warning level and shown in status; three in a day stop zKillboard work
    until an operator resumes it, so the warehouse cannot keep pressing a
    service that has asked it to stop.
  - No polling of `/cache/bypass/healthcheck/` and no query strings on API
    paths (filters go in the path, with the trailing slash).
- `sources/zkillboard.py`: R2Z2 client implementing that policy, verified
  streaming downloads, raw retained compressed.
- Dataset `zkillboard_killmails`: discovery from `totals.json` plus each
  day file's headers, the generic sync loop (registry objects, revisions by
  sha256, raw retained, compressed), the existing killmail normaliser.
  Every day, all of history, as the other datasets.
- Two lake layers. Per source: `lake/sources/<source>/{killmails,attackers,
  items}/` with one partition per upstream file, never read by consumers.
  Merged: `lake/{killmails,attackers,items}/` with one partition per UTC day
  of `killmail_time`, one row per `killmail_id`, a `sources` column naming
  every source that had it; when both have a killmail the rows are the same
  immutable ESI killmail and EVE Ref's copy is kept. A merge step rebuilds a
  day, and the days either side of it (upstream files spill over, as on
  2026-01-06), whenever a source partition for those days changes, through
  `data.parquet.tmp` and `os.replace`; each merged partition's metadata names
  the source partitions and revisions it was built from.
- Migration: today's EVE Ref partitions move under `lake/sources/everef/`
  and the merged layer is built from them (and from zKillboard once
  imported), from `raw/` without downloads.
- `verify` compares per-day counts across both sources and the merged layer
  and reports days missing from a source, duplicated ids, and days whose
  merged count is below either source.
- Design §4, §5, §6, §7.1 and §12 amended; `docs/consumers.md` describes the
  merged tables and `sources`; tests against a fake R2Z2.

Acceptance: the full zKillboard history is imported and retained without a
403 or 429, at the policy's pace; the
merged layer has no killmail id twice; 2026-07-13 is present; every day's
merged count is at least the larger source count; the 2026-01-06
duplicates are gone; `verify` reports the cross-source differences;
consumers read the merged tables unchanged in shape apart from `sources`.

Result (2026-10-12 19:31 UTC, numbers in design §7.1): all 6,886 zKillboard
days imported and retained, nine of them provisional, no 403 or 429 during
the import; the merged layer holds 95,739,833 killmails, each id once, over
6,886 days; 2026-07-13 has 15,746; no day's merged count is below either
source; the 14,043 ids EVE Ref repeats across 2026-01-05/06 appear once;
`verify` (hashing everything, 22,180 objects) found 0 issues in zKillboard
and in the merged layer, noted the nine provisional days and the two days EVE
Ref lacks (2026-07-13, 2026-10-11), and 26 `expected_count` differences in
EVE Ref's own data (25 killmail days from 2026-07-15 to 2026-09-11 trailing
its totals by 1 to 4 killmails, two by 17 and 20, and the current market
history day); the merged tables
keep their columns, with `sources` added to `killmails`.

## M9. Live killmails from zKillboard

Code done 2026-10-12; live acceptance in progress. Settled while building it
(design §7.1, §8, §11a): the follower is a thread the service starts, not a
queued job, and flushes every 30 s itself; `live` is a third killmail
source, last in precedence, so daily files take over by the existing merge;
`killmail_zkb` lives under `lake/killmail_zkb/`; one process-wide gate keeps
one zKillboard request in flight across the follower and the history syncs.

R2Z2's live feed: `GET /ephemeral/sequence.json` gives the current sequence,
`GET /ephemeral/{sequence}.json` one object, 404 when none is newer yet.
Each object carries the full ESI killmail (`esi`), a `zkb` block
(`totalValue`, `fittedValue`, `droppedValue`, `destroyedValue`,
`totalDroppableValue`, `points`, `npc`, `solo`, `awox`, `attackerCount`,
`locationID`, `labels`, `hash`), `sequence_id` and `uploaded_at` (checked
2026-10-11). Rules: about 100 ms after a success, at least 6 s after a 404,
at most 15 requests a second, objects kept at least 24 hours, reprocessed
killmails reappear under a new sequence. zKillboard metadata exists only in
this feed, so it is collected from the live feed onward.

Deliverables:

- The zKillboard policy of M8 applies to every request of the follower.
- A follower inside the service, an asyncio task beside the scheduler, not a
  queued job: from the stored `sequence_id` in `sweep_state`, or from
  `sequence.json` on first start or after more than 24 hours away (the daily
  history covers any gap).
- Raw: objects appended to `raw/zkillboard/live/<day>/` in hourly NDJSON
  files, compressed when the hour closes, never modified after.
- Live layer: killmails of days that neither source's daily file covers yet
  go into the merged layer's day partition as rows with `sources = ['live']`,
  rewritten every few minutes; when a daily file for that day arrives, the
  merge step rebuilds the day from the daily sources and keeps live rows only
  for killmails they still lack.
- `killmail_zkb` dataset: one row per killmail from the `zkb` block plus
  `sequence_id` and `uploaded_at`, partitioned by day under `lake/zkb/`,
  newest sequence wins, kept for every day.
- Entities: the recent-killmails step reads the live rows too, so new
  characters on today's kills are queued the same day.
- `evedw live status` (sequence, lag, killmails today, last error); `/health`
  shows the follower. Tests against a fake R2Z2.

Acceptance: killmails appear in the merged layer within a minute of
zKillboard publishing them; a restart resumes from the stored sequence with
no gap and no duplicate; a day's live rows give way to its daily files; a
full day of following draws no 403 or 429.

## M10. Entity discovery and manual add

Not started. Starts once M9 is accepted.

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
  members remain M10 work.
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

## Consumers

Not a milestone (decided 2026-10-09). Applications that read the warehouse
live in their own repositories and need no milestone to finish: they read the
lake and the API, and steer the importer with generic jobs such as
`entities:add`. They can start whenever the data they need is there; the
first, a membership-origins report in `../eve-reports`, already runs. Kat's
valuation, membership and report code was a proof of concept for comparing
datasets and is not carried over; kat can be retired independently.
