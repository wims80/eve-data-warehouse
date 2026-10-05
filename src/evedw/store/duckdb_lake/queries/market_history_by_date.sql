-- Daily market history for every date in the range, inclusive; optionally one region.
-- param date_from: date required
-- param date_to: date required
-- param region_id: int optional
SELECT *
FROM market_history
WHERE date BETWEEN $date_from AND $date_to
  AND ($region_id IS NULL OR region_id = $region_id)
ORDER BY date, region_id, type_id
