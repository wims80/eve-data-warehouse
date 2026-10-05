-- Entity tables (design section 7.3) and the ESI response cache (design section 11).
-- Timestamps are naive UTC, like the registry tables.

CREATE TABLE characters (
    character_id    BIGINT PRIMARY KEY,
    name            VARCHAR,
    corporation_id  BIGINT,
    alliance_id     BIGINT,
    faction_id      BIGINT,
    birthday        TIMESTAMP,
    security_status DOUBLE,
    deleted         BOOLEAN NOT NULL DEFAULT false,
    observed_at     TIMESTAMP NOT NULL,
    source          VARCHAR NOT NULL
);

CREATE TABLE corporations (
    corporation_id BIGINT PRIMARY KEY,
    name           VARCHAR,
    ticker         VARCHAR,
    alliance_id    BIGINT,
    ceo_id         BIGINT,
    member_count   BIGINT,
    date_founded   TIMESTAMP,
    deleted        BOOLEAN NOT NULL DEFAULT false,
    observed_at    TIMESTAMP NOT NULL,
    source         VARCHAR NOT NULL
);

CREATE TABLE alliances (
    alliance_id             BIGINT PRIMARY KEY,
    name                    VARCHAR,
    ticker                  VARCHAR,
    executor_corporation_id BIGINT,
    date_founded            TIMESTAMP,
    deleted                 BOOLEAN NOT NULL DEFAULT false,
    observed_at             TIMESTAMP NOT NULL,
    source                  VARCHAR NOT NULL
);

CREATE TABLE character_employment (
    character_id   BIGINT NOT NULL,
    record_id      BIGINT NOT NULL,
    corporation_id BIGINT,
    start_date     TIMESTAMP,
    observed_at    TIMESTAMP NOT NULL,
    source         VARCHAR NOT NULL,
    PRIMARY KEY (character_id, record_id)
);

CREATE TABLE corporation_alliance_history (
    corporation_id BIGINT NOT NULL,
    record_id      BIGINT NOT NULL,
    alliance_id    BIGINT,
    start_date     TIMESTAMP,
    is_deleted     BOOLEAN,
    observed_at    TIMESTAMP NOT NULL,
    source         VARCHAR NOT NULL,
    PRIMARY KEY (corporation_id, record_id)
);

CREATE TABLE esi_cache (
    key           VARCHAR PRIMARY KEY,
    status        INTEGER NOT NULL,
    body          VARCHAR NOT NULL,
    etag          VARCHAR,
    last_modified VARCHAR,
    cache_control VARCHAR,
    observed_at   TIMESTAMP NOT NULL,
    expires_at    TIMESTAMP NOT NULL
);
