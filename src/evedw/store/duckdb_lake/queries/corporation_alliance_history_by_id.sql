-- Alliance history of the given corporations, from the live entity table.
-- param ids: ids required
SELECT corporation_id, record_id, alliance_id, start_date::TIMESTAMPTZ AS start_date,
       is_deleted, observed_at::TIMESTAMPTZ AS observed_at, source
FROM corporation_alliance_history
WHERE list_contains($ids, corporation_id)
ORDER BY corporation_id, record_id
