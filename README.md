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
```
