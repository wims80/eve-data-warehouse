-- Current character rows for the given ids, from the live entity table.
-- param ids: ids required
SELECT character_id, name, corporation_id, alliance_id, faction_id,
       birthday::TIMESTAMPTZ AS birthday, security_status, deleted,
       observed_at::TIMESTAMPTZ AS observed_at, source
FROM characters
WHERE list_contains($ids, character_id)
ORDER BY character_id
