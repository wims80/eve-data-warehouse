-- The etag a download kept serving while discovery recorded another one, and for how
-- many consecutive syncs: after three, that older copy is imported as provisional
-- (design section 6).

ALTER TABLE source_object ADD COLUMN stale_etag VARCHAR;
ALTER TABLE source_object ADD COLUMN stale_runs INTEGER DEFAULT 0;
