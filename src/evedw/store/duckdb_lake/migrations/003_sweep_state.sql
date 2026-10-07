-- Refresh job progress (design section 5) and removal of eve-kill error placeholders
-- seeded before parser_version 2 of entities_backfill (design section 2.3).

CREATE TABLE sweep_state (
    key        VARCHAR PRIMARY KEY,
    value      VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

DELETE FROM characters WHERE name IS NULL AND source LIKE 'everef_backfill:%';
DELETE FROM corporations WHERE name IS NULL AND source LIKE 'everef_backfill:%';
DELETE FROM alliances WHERE name IS NULL AND source LIKE 'everef_backfill:%';
