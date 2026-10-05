-- Attacker rows for every source_date in the range, inclusive.
-- param date_from: date required
-- param date_to: date required
SELECT *
FROM attackers
WHERE source_date BETWEEN $date_from AND $date_to
ORDER BY source_date, killmail_id, ordinal
