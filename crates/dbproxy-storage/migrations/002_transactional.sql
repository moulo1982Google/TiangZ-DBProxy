CREATE TABLE IF NOT EXISTS dbproxy_transactions (
    operation_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    expected_revision BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    result BYTEA NOT NULL,
    new_revision BIGINT NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL
);
