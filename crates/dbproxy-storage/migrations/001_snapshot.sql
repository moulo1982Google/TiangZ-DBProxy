CREATE TABLE IF NOT EXISTS dbproxy_snapshots (
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, record_key)
);

CREATE TABLE IF NOT EXISTS dbproxy_idempotency (
    request_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    expected_revision BIGINT NULL,
    revision BIGINT NOT NULL
);
