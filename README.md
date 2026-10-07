# EVE Data Warehouse

A local warehouse service for EVE Online data: killmail history, character,
corporation and alliance history, and market price history. It keeps itself
current from EVE Ref and ESI, records every source revision it has imported,
and delivers data to local applications as Parquet files and over a small
HTTP API.

See `docs/design.md` for the design and `docs/plan.md` for the milestones.

```sh
uv sync
cp .env.example .env
uv run evedw migrate
uv run evedw status
uv run evedw serve        # API on 127.0.0.1:8470 plus the scheduler
```

To leave it running unattended, install it as a systemd user unit and read
its log with `journalctl --user -u evedw -f -o cat`; `docs/operations.md`
has the unit and when a change needs `systemctl --user restart evedw`.

To type `evedw` instead of `uv run evedw`, from any directory, install it as
an editable tool and, for zsh, add tab completion:

```sh
uv tool install --editable .     # puts evedw in ~/.local/bin; code changes apply at once
evedw --install-completion zsh   # then open a new shell
```

The installed command reads `.env` and `data/` from this checkout wherever it
is run (or from `EVEDW_HOME` if set). Reinstall with `--reinstall` after
dependencies change.

To keep an eye on a running warehouse (all read-only, safe at any time):

```sh
evedw status             # datasets, object counts, recent runs
evedw entities status    # entity row counts, refresh queue, sweep progress
evedw esi status         # ESI pace, slowdowns, budget used today, stops
evedw speed              # requests/min, budget outlook, time left (60 s window)
```

Recovery, rebuilds, manual jobs and what to do when ESI access is stopped are
in `docs/operations.md`.

While `evedw serve` runs, `evedw sync`, `evedw verify` and the `evedw entities`
commands send their job to the service and follow the run. Consumers are
described in `docs/consumers.md`.
