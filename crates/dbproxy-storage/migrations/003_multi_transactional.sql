CREATE TABLE IF NOT EXISTS dbproxy_multi_transactions (
    operation_id TEXT PRIMARY KEY,
    result BYTEA NOT NULL,
    record_count BIGINT NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbproxy_multi_transaction_records (
    operation_id TEXT NOT NULL REFERENCES dbproxy_multi_transactions(operation_id) ON DELETE CASCADE,
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    expected_revision BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL,
    new_revision BIGINT NOT NULL,
    PRIMARY KEY (operation_id, namespace, record_key)
);

CREATE INDEX IF NOT EXISTS dbproxy_multi_transaction_records_lookup
    ON dbproxy_multi_transaction_records (operation_id);
