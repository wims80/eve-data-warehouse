-- Current alliance rows for the given ids, from the live entity table.
-- param ids: ids required
SELECT alliance_id, name, ticker, executor_corporation_id,
       date_founded::TIMESTAMPTZ AS date_founded, deleted,
       observed_at::TIMESTAMPTZ AS observed_at, source
FROM alliances
WHERE list_contains($ids, alliance_id)
ORDER BY alliance_id
