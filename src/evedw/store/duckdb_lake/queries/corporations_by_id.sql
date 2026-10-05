-- Current corporation rows for the given ids, from the live entity table.
-- param ids: ids required
SELECT corporation_id, name, ticker, alliance_id, ceo_id, member_count,
       date_founded::TIMESTAMPTZ AS date_founded, deleted,
       observed_at::TIMESTAMPTZ AS observed_at, source
FROM corporations
WHERE list_contains($ids, corporation_id)
ORDER BY corporation_id
