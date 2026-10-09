# Operations

How to run the warehouse and what to do when something goes wrong. Design
decisions are in [design.md](design.md); reading the data is in
[consumers.md](consumers.md). `.env` and the default `data/` directory live
in the repository root. `uv run evedw` works from inside the repository; an
`evedw` installed as in the README works from any directory and reads the
same `.env` and `data/` (design §13). The two are interchangeable below.

## Starting and stopping the service

The service runs as a systemd user unit (decided 2026-10-08), so it restarts
after a crash, starts with your session, and logs to the journal:

```
systemctl --user status evedw           # running? since when, main PID
systemctl --user restart evedw          # after a code or .env change
journalctl --user -u evedw -f -o cat    # follow the log
journalctl --user -u evedw --since "1 hour ago" -p warning -o cat
```

The service holds `data/writer.lock` for its lifetime and is the only
process that opens `data/warehouse.duckdb`. It logs to stdout, which the
unit sends to the journal; journald rotates it, so there is no log file to
manage. `-o cat` drops journald's own timestamp, since every line carries a
UTC one.

The unit, `~/.config/systemd/user/evedw.service`. Adjust the two paths
(`which uv` for the second) and run `systemctl --user daemon-reload`, then
`systemctl --user enable --now evedw`:

```ini
[Unit]
Description=EVE data warehouse
After=network-online.target

[Service]
WorkingDirectory=/home/you/code/eve-data-warehouse
ExecStart=/home/you/.local/bin/uv run evedw serve
Environment=PYTHONUNBUFFERED=1
KillSignal=SIGTERM
# uv itself exits 143 on SIGTERM after the service has shut down cleanly
SuccessExitStatus=143
TimeoutStopSec=120
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
```

User units stop when you log out. To keep the service running without a
session and start it at boot, enable lingering once:
`loginctl enable-linger $USER`.

For a quick foreground run instead, `uv run evedw serve` (Ctrl-C stops it).
Stop the unit first; the second process fails fast on the writer lock.

### When a change needs a restart

The running service loaded the code and `.env` at startup, so:

- Code the service runs (jobs, sources, store, service routes) or `.env`:
  `systemctl --user restart evedw`.
- A schema migration: `evedw migrate` refuses while the service holds the
  lock, so `systemctl --user stop evedw && evedw migrate && systemctl
  --user start evedw`.
- Tests, CLI-only changes, docs and consumer code need no restart. Each
  `evedw` command is a fresh process and picks up new code at once.

A restart is cheap: the running job is cancelled and resumes from its saved
state (see below). Do not run the service with auto-reload on file changes;
every save would cancel a job.

To try service changes without touching the live one, run a second instance
on its own data dir and port. Leave the ESI contact blank so it schedules no
entity refresh and never adds a second ESI request stream next to the live
one (design §11); its EVE Ref syncs still run.

```
EVEDW_BIND=127.0.0.1:8471 EVEDW_ESI_CONTACT= evedw --data-dir /tmp/evedw-dev serve
```

Stopping (`systemctl --user stop`, `SIGTERM` or Ctrl-C): the current job notices at the next
object boundary, is recorded `cancelled`, and queued runs are cancelled
without starting. Nothing is left half-written: an object that was being
imported is redone by the next sync. A cancelled run does not count for the
schedule, so a restart picks its job up again straight away.

What the scheduler does on its own (design §8): a sync of each dataset every
6 h, a header sweep of every file weekly, a check for new entity backfill
archives daily (all unseeded ones are imported), the entity Parquet export
daily, `verify` weekly. `entities:refresh` is scheduled only when
`EVEDW_ESI_CONTACT` is set to a real contact; leave it empty to keep the
service off ESI entirely.

Health and progress:

```
uv run evedw status                       # datasets, object counts, recent runs
curl -s localhost:8470/health             # current and queued runs, schedule
curl -s 'localhost:8470/runs?limit=5'
```

With the service running, every CLI job command (`sync`, `verify`,
`entities ...`) is sent to it over HTTP and the CLI follows the run;
`--no-wait` returns as soon as the run is queued. Without a service the
command takes the writer lock and runs the job in-process. The two paths run
identical code.

## A fresh machine

```
uv sync
cp .env.example .env        # set EVEDW_DATA_DIR if data/ should live elsewhere
uv run evedw migrate
```

then install the systemd unit above, or `uv run evedw serve` in a terminal.

The first scheduled syncs carry no date range, so they perform the full
backfill of both datasets, newest day first (design §4 has the sizes and
times), and the scheduled `entities:seed all` imports every EVE Ref entity
backfill archive. Nothing needs to be run by hand.

## Forcing a range

Re-import days that are already imported, for example after hand-editing
or losing partitions:

```
uv run evedw sync killmails --from 2026-09-01 --to 2026-09-30 --force
```

Discovery runs for the range only, every file in it gets its headers
re-checked, and every day in it is re-imported. If the upstream file is
unchanged the retained raw file is reused, so nothing is downloaded; each
day gets a new revision row pointing at the same raw file. If the file did
change upstream, it is fetched as a new revision.

To catch upstream rewrites of old days without re-importing anything,
`--sweep` re-checks the headers of every file and imports only what moved.
The weekly scheduled sweep does the same.

## Raising `parser_version`

When a normaliser's output changes (new column, fixed parsing), bump that
dataset's `parser_version` in `src/evedw/domain/datasets.py` in the same
commit, and add the change to design §7.

The next sync of the dataset, scheduled or manual, marks every imported
object whose live revision was produced by an older parser `changed` and
re-imports it from the retained raw file. No downloads happen. To do it
immediately and without any network traffic:

```
uv run evedw sync killmails --offline
```

Each re-imported day gets a new revision (the chain records which parser
produced the live data) and its partitions carry the new
`evedw.parser_version` in their metadata. Consumers polling
`/datasets/{name}/objects?changed_since=` see every re-imported day as
changed, which is correct: its data changed. Re-importing full history takes
about as long as the import part of the original backfill (design §4).

## Recovering from a failed object

A failed object stays `failed` with its error in `last_error`, and the
other objects of the run are unaffected. `failed` is a pending status, so
the next sync retries it without being asked. Find failed objects and their
errors:

```
uv run evedw status
curl -s localhost:8470/datasets/killmails/objects \
  | jq '.objects[] | select(.status == "failed") | {object_key, last_error}'
curl -s localhost:8470/datasets/killmails/objects/2026/killmails-2026-10-01.tar.bz2
```

Then by error:

- `UpstreamChangedError` or `DownloadError` (retries exhausted on 5xx, short
  reads or transport errors): upstream moved or hiccupped mid-download.
  Retry the day:
  `uv run evedw sync killmails --from 2026-10-01 --to 2026-10-01`.
- `DiskSpaceError`: free space fell under `EVEDW_MIN_FREE_GB`. Free space,
  then sync again. Nothing was written.
- `NestingError` or another normaliser error: the archive contains something
  the normaliser does not handle. Fix the normaliser, bump `parser_version`,
  sync. Keep the failing day as a trimmed fixture under `tests/fixtures/`.
- `interrupted`: the process was stopped mid-object. The next sync redoes it.

A count mismatch against `totals.json` is not a failure. The revision is
imported and marked unverified (`verified = false`), because EVE Ref updates
the archive and the totals at different times; `verify` lists the gap. If it
persists after the next sweep, force the day.

### When `verify` reports a raw file problem

`missing_raw` or `raw_hash` means the retained archive of a live revision is
gone or damaged on disk. Raw archives are never deleted, so move a damaged
file aside rather than removing it, then force the day so it is fetched
again:

```
mkdir -p data/quarantine
mv data/raw/killmails/2026/killmails-2026-10-01/<sha256>.tar.bz2 data/quarantine/
uv run evedw sync killmails --from 2026-10-01 --to 2026-10-01 --force
```

If EVE Ref still serves the same content, the new download hashes to the
same sha256 and lands at the same path.

Other `verify` issue kinds: `missing_partition`, `row_count` and
`stale_partition` (the partition on disk does not match the live revision)
are fixed by forcing the day; `orphan_partition` is a partition directory
with no registry object, usually left by hand; `unknown_keys` means
upstream added JSON keys the normaliser does not map yet (data is still
imported).

## Rebuilding the lake from `raw/` offline

The lake is derived data. With the registry (`warehouse.duckdb`) and `raw/`
intact, rebuild it without touching the network:

```
uv run evedw sync killmails --offline --force
uv run evedw sync market_history --offline --force
uv run evedw verify --offline
```

`--offline` skips discovery and works only on objects whose current etag
has a retained raw file; everything else is skipped and left for the next
online sync. `--force` re-imports days that are already imported. Use
`--from` and `--to` to rebuild part of the lake. Partitions are replaced
atomically one day at a time, so consumers can keep reading during the
rebuild. Entity exports are rebuilt with `uv run evedw entities export`.

The registry cannot be rebuilt from `raw/` alone: the file names hold the
content hash but not the upstream etag or the expected counts. Back up
`warehouse.duckdb` by copying it while the service is stopped. If it is lost
anyway, `evedw migrate` and a service start rebuild everything by
downloading again, entity backfills included; ESI-refreshed entity rows
come back as the refresh queue cycles.

## Entity refresh

With `EVEDW_ESI_CONTACT` set, the service keeps characters, corporations and
alliances current by itself (design §7.3): a daily alliance membership
sweep, a weekly affiliation sweep over every live character, a change queue
for what moved, and a crawl that fetches the history nobody fetched before,
within `EVEDW_ESI_DAILY_BUDGET` (800,000 requests a day). Progress:

```
uv run evedw entities status    # row counts, queue per class, sweep state
uv run evedw esi status         # requests and budget used today
uv run evedw speed              # requests/min, budget outlook, cycle time left (60 s window)
uv run evedw speed --seconds 300 --watch   # a report every 5 minutes until Ctrl-C
```

The service schedules both entity jobs, so neither normally needs running
by hand. `evedw entities seed [date|latest|all] [--force]` imports an EVE
Ref backfill archive from `raw/` or upstream; the daily scheduled run is
`all`, which imports any archive not seen yet, and `--force` re-imports one
that was. `evedw entities refresh [--budget N] [--no-populate]` runs one
refresh slice. Every request it sends counts against the daily budget and
the ESI error limit just like a scheduled slice, on top of the schedule, so
reach for it only without a service (a data dir being prepared, or the
service stopped on purpose) or with a small `--budget` to test a change.
`--no-populate` skips the sweeps and the crawl feed and only drains what is
already queued. With a service running, both commands queue their job on it
and follow the run.

To have particular entities refreshed first, for example everyone in an
alliance a report is about:

```
evedw entities add alliance 99010468 --members   # the alliance, its corporations, their known characters
evedw entities add character 2118583008 2118583261
evedw entities status                            # the focus line drains first
evedw entities export                            # then refresh the Parquet files for consumers
```

The job only queues (`focus`, ahead of every other class); the next slices
fetch details and history for each, about two requests an entity. Only
characters the warehouse already knows are found as members.

The queue classes are `focus` (asked for with `entities add`), `change` (a
sweep saw it move), `active` (on recent killmails or in an alliance, details
older than 30 days), `crawl` (history never fetched), `deferred` (crawl entries
for deleted or never-stored characters, requested after the rest of the crawl)
and `idle` (settled). `crawl` shrinking day by day is the
history filling in; `change` should stay short once the first affiliation
cycle has caught up with what changed since the last backfill.

## ESI pace and error rates

Requests go out 0.05 s apart when calm (`EVEDW_ESI_SPACING`). Warning signs
(errors piling up in ESI's error window, 429, 5xx, timeouts) double the
spacing up to 2 s; five quiet minutes halve it again (design §11). To see
whether that is happening:

```
uv run evedw esi status                         # current pace and slowdown count
journalctl --user -u evedw -o cat | grep -E "ESI pace|ESI errors|HTTP [45][0-9][0-9]" | tail
```

An occasional slowdown is the policy doing its job; a single `HTTP 504`
from ESI, retried after a few seconds, is typical (one in about 25,000
requests on 2026-10-08). Repeated slowdowns, or
a refresh slice ending early on `100 ESI errors`, mean something is sending
bad requests: stop the service and find it before raising the pace again.

## ESI downtime

ESI work pauses every day around EVE's downtime at 11:00 UTC (design §11):
from 10:58, or earlier at the first server error from 10:45, until
`GET /status` shows the restarted server. `evedw esi status` shows
`downtime  paused since ...` while it lasts. The journal has one line when
it pauses, one per minute while `/status` says it is still down, and one
with the length of the pause when it resumes:

```
journalctl --user -u evedw --since "10:40 UTC" -o cat | /usr/bin/grep -E "downtime|still down"
```

Refresh slices during the pause end at once, leaving their work due; EVE
Ref syncs carry on. Nothing needs doing by hand. If `/status` keeps
answering after 11:30 while other routes fail, the normal retries and
slowdowns take over.

## ESI stops

A 420 or 403 from ESI stops every ESI request until an operator clears it
(design §11). `uv run evedw esi status` shows the reason and the counters.
Find out why it happened before resuming with `uv run evedw esi resume`.
