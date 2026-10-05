# Working in this repository

Read `docs/design.md` before changing anything. It holds the decisions;
`docs/plan.md` holds the order of work. If code and design disagree, the
design is right until it is amended, and amendments go into design.md in the
same change as the code.

## Layers

`domain` < `store` < `sources` < `jobs` < `service`. Imports only point
downward.

- `domain` does no I/O and imports no driver, no httpx, no duckdb.
- `store/base.py` is Protocols only. Only a backend package under `store/`
  may import a database driver. Jobs and sources never do.
- Arrow tables cross every boundary. Do not pass dicts, dataframes or
  database cursors between layers.
- SQL lives in `store/<backend>/queries/*.sql` and in the dataset normalisers.
  Nowhere else.
- The CLI triggers jobs; it never contains import logic.

## Invariants

- A day is written to `data.parquet.tmp` and promoted with `os.replace`.
  Readers must never observe a partial partition.
- Registry updates that promote a revision happen in one transaction after
  the file replace succeeds.
- Raw archives are never deleted or modified. Every revision is retained by
  sha256.
- Discovery diffs every year's index. Never assume only recent days change.
- `parser_version` lives in `domain/datasets.py`. Bump it whenever a
  normaliser's output changes; this is what triggers re-import.
- Schema is explicit. Never let DuckDB infer the JSON or CSV schema in a job.
- Keep `observed_at` and `source` on every entity row. An entity upsert
  replaces a row only when the incoming observation is not older, so a
  backfill can never overwrite newer ESI data.
- Entity field sets are pinned in `jobs/entities.py`; the seed reports
  unknown upstream keys instead of absorbing them.

## ESI

Design §11 is binding for every ESI request, including one-off scripts you
run while debugging. One request in flight, one second spacing, conditional
requests, honour every cache and rate header, stop on 420 or 403. Check the
current endpoint specification at
https://developers.eveonline.com/api-explorer before adding an endpoint, and
keep the compatibility date pinned in `config.py`.

## Tests

- `uv run pytest` must pass with no network. HTTP is mocked with `respx`.
  Fixtures under `tests/fixtures/` are trimmed real data; keep them small.
- Store backends are tested through `tests/store/test_*_contract.py`. Add new
  Protocol methods to the contract tests in the same change.
- Live tests are marked `@pytest.mark.live` and are never run by default.
- Never point tests at the real `data/` directory.

## Checks before finishing a change

```
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
```

## Conventions

- Python 3.13+, type hints everywhere, `pyright` strict.
- Logging via the standard library; include `run_id`, `dataset` and
  `object_key` in job log records.
- Timestamps are UTC `datetime` objects; dates are `datetime.date`. No naive
  datetimes.
- File and module names are plain nouns; one responsibility per module;
  prefer a new module over a utilities module.
- Do not add dependencies without noting why in design §14.

## Reference material

`../kat` is the predecessor Rust project. Its EVE Ref client, ESI policy
module and killmail parser tests are worth reading when porting behaviour.
Its snapshot manifest, disk budget and reporting code are deliberately not
carried over.
