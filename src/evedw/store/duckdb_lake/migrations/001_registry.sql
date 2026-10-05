CREATE TABLE source_object (
    dataset                VARCHAR NOT NULL,
    object_key             VARCHAR NOT NULL,
    url                    VARCHAR NOT NULL,
    logical_date           DATE,
    upstream_etag          VARCHAR,
    upstream_size          BIGINT,
    upstream_last_modified TIMESTAMP,
    expected_count         BIGINT,
    discovered_at          TIMESTAMP NOT NULL,
    last_seen_at           TIMESTAMP NOT NULL,
    current_revision       INTEGER,
    status                 VARCHAR NOT NULL,
    last_error             VARCHAR,
    PRIMARY KEY (dataset, object_key)
);

CREATE TABLE source_revision (
    dataset                VARCHAR NOT NULL,
    object_key             VARCHAR NOT NULL,
    revision               INTEGER NOT NULL,
    sha256                 VARCHAR NOT NULL,
    size                   BIGINT NOT NULL,
    raw_path               VARCHAR NOT NULL,
    upstream_etag          VARCHAR,
    upstream_last_modified TIMESTAMP,
    fetched_at             TIMESTAMP NOT NULL,
    imported_at            TIMESTAMP,
    parser_version         INTEGER NOT NULL,
    observed_count         BIGINT,
    verified               BOOLEAN,
    status                 VARCHAR NOT NULL,
    PRIMARY KEY (dataset, object_key, revision)
);

CREATE TABLE import_run (
    run_id          VARCHAR NOT NULL PRIMARY KEY,
    job             VARCHAR NOT NULL,
    trigger         VARCHAR NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR NOT NULL,
    params_json     VARCHAR NOT NULL,
    objects_changed BIGINT NOT NULL DEFAULT 0,
    rows_written    BIGINT NOT NULL DEFAULT 0,
    error           VARCHAR
);

CREATE TABLE entity_refresh (
    kind              VARCHAR NOT NULL,
    entity_id         BIGINT NOT NULL,
    priority          INTEGER NOT NULL,
    last_refreshed_at TIMESTAMP,
    next_due_at       TIMESTAMP NOT NULL,
    etag              VARCHAR,
    failures          INTEGER NOT NULL DEFAULT 0,
    last_error        VARCHAR,
    PRIMARY KEY (kind, entity_id)
);
