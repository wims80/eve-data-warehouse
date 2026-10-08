# Reading the warehouse

Two ways in, both from any process on the machine:

1. **The lake directly.** Parquet under `data/lake/` is the delivery
   contract (design §4). Any DuckDB, pyarrow or Polars session can read it
   while the service writes, because every partition is replaced atomically.
   This is the right path for anything analytical or larger than a few days.
2. **The service API** at `http://127.0.0.1:8470` (design §9). Use it for
   "what changed since my last pull", for small date-range reads as Arrow,
   for entity lookups that must be fresher than the daily Parquet export, and
   to trigger jobs.

Never open `data/warehouse.duckdb` from a consumer. DuckDB allows one
writer or many readers on a file, not both, and the service is the writer.

## Reading the lake with DuckDB

`evedw views` prints view DDL for the populated tables. Run it once per
session and query the views:

```python
import duckdb
import subprocess

con = duckdb.connect()
con.execute(subprocess.run(["uv", "run", "evedw", "views"], capture_output=True, text=True).stdout)

top_systems = con.execute("""
    SELECT solar_system_id, count(*) AS kills
    FROM killmails_unique
    WHERE source_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").fetchall()
```

Without the views, read the partition globs with `hive_partitioning = false`;
the partition column is stored inside each file:

```sql
SELECT * FROM read_parquet('data/lake/market_history/*/data.parquet', hive_partitioning = false)
WHERE date >= DATE '2026-10-01' AND region_id = 10000002;
```

pyarrow and Polars should also scan the directories plainly, or declare the
partition column as a date; their hive mode infers it as a string and
conflicts with the column in the file.

## Incremental pulls over the API

A consumer that keeps its own copy asks which objects changed since its last
pull, then reads those days. The timestamp needs a timezone.

```python
"""Pull the killmail days imported since the last pull, as Arrow, then read one
of them straight from the lake with DuckDB. Run with `uv run python`."""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa

SERVICE = "http://127.0.0.1:8470"
ARROW = "application/vnd.apache.arrow.stream"
STATE = Path("last_pull.txt")

last_pull = STATE.read_text().strip() if STATE.exists() else "2000-01-01T00:00:00Z"
now = datetime.now(UTC)

with httpx.Client(base_url=SERVICE, timeout=120) as client:
    changed = client.get("/datasets/killmails/objects", params={"changed_since": last_pull}).json()[
        "objects"
    ]
    days = sorted(o["logical_date"] for o in changed)
    print(f"{len(days)} killmail days imported since {last_pull}")

    if days:
        response = client.get(
            "/query/killmails_by_date",
            params={"date_from": days[0], "date_to": days[-1]},
            headers={"Accept": ARROW},
        )
        response.raise_for_status()
        table: pa.Table = pa.ipc.open_stream(response.content).read_all()
        print(f"{table.num_rows} killmails as Arrow, {response.headers['x-row-count']} announced")

        # Entity names for the victims, from the live tables.
        victims = table.column("victim_character_id").drop_null().unique().to_pylist()[:1000]
        names = client.get(
            "/query/characters_by_id", params={"ids": ",".join(map(str, victims))}
        ).json()
        print(f"{names['row_count']} of {len(victims)} victims known")

    manifest = client.get("/lake", params={"table": "killmails"}).json()
    lake_dir = manifest["lake_dir"]

# The same days, read directly from Parquet. Faster and streaming for big ranges.
if days:
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{lake_dir}/killmails/*/data.parquet', "
        "hive_partitioning = false) WHERE source_date BETWEEN ? AND ?",
        [days[0], days[-1]],
    ).fetchone()
    print(f"{rows[0] if rows else 0} killmails in the lake for the same range")

STATE.write_text(now.isoformat())
```

The same shape works for `market_history` with `market_history_by_date`
(optional `region_id`), and for the attacker and item tables. `GET /query`
lists every query and its parameters.

The query result is built in memory before it is streamed, so keep API reads
to a few days at a time and use the lake for anything bigger.

## Entities

`data/lake/entities/<table>.parquet` is rewritten daily by `entities:export`
and `evedw views` adds a view per file. The `*_by_id` queries read the live
tables instead and reflect the latest ESI refresh. Rows carry `observed_at`
and `source` (`esi` or `everef_backfill:<sha256>`); `deleted` is true for
characters in Doomheim, closed corporations and anything ESI returned 404 for.

To find the newest employment or alliance record, order by `start_date` and
then `record_id`. `start_date` has minute precision, and a character often
passes through an NPC corporation and into a player corporation in the same
minute (about 300,000 characters have such a pair), so `start_date` alone,
or DuckDB's `arg_max(corporation_id, start_date)`, picks either. `arg_max`
also skips rows whose value is NULL, which hides a corporation's latest
"left the alliance" record; use `arg_max_null` or a window:

```sql
SELECT character_id, corporation_id
FROM (SELECT *, row_number() OVER (PARTITION BY character_id
                                   ORDER BY start_date DESC, record_id DESC) AS n
      FROM character_employment)
WHERE n = 1;
```

The current corporation is also on `characters.corporation_id`, kept up to
date by the weekly affiliation cycle, which is fresher than history when
ESI's history lags.

## Triggering a job

```sh
curl -s -X POST localhost:8470/jobs/sync:market_history \
  -H 'content-type: application/json' -d '{"from": "2026-10-01", "force": true}'
# {"run_id":"...","job":"sync:market_history","status":"queued","params":{...}}
curl -s localhost:8470/runs/<run_id>
```

`evedw sync market_history --from 2026-10-01 --force` does the same and waits
for the run when the service is up. Runs execute one at a time; `GET /health`
shows the current one, the queue and when each scheduled job is next due.

An application can steer the importer the same way, for instance to have an
alliance it reports on refreshed before anything else:

```sh
curl -s -X POST localhost:8470/jobs/entities:add -H 'content-type: application/json' \
  -d '{"kind": "alliance", "ids": [99010468], "members": true}'
```

That only changes what the warehouse fetches first. Queries for a report stay in
the application's own repository; the warehouse offers the lake, the API and
generic jobs, never report-specific code.
