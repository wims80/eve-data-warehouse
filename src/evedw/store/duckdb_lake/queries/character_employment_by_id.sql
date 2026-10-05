-- Employment history of the given characters, from the live entity table.
-- param ids: ids required
SELECT character_id, record_id, corporation_id, start_date::TIMESTAMPTZ AS start_date,
       observed_at::TIMESTAMPTZ AS observed_at, source
FROM character_employment
WHERE list_contains($ids, character_id)
ORDER BY character_id, record_id
