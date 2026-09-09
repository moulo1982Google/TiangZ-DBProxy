-- Preserve legacy trade rows and their immutable-content protections.
ALTER TABLE dbproxy_outbox ALTER COLUMN trade_id DROP NOT NULL;
ALTER TABLE dbproxy_outbox DROP CONSTRAINT dbproxy_outbox_operation_id_fkey;
ALTER TABLE dbproxy_outbox ADD CONSTRAINT dbproxy_outbox_operation_id_fkey
    FOREIGN KEY (operation_id) REFERENCES dbproxy_operation_claims(operation_id);

CREATE TABLE dbproxy_multi_transaction_effects (
    operation_id TEXT PRIMARY KEY REFERENCES dbproxy_multi_transactions(operation_id),
    payload BYTEA NOT NULL
);

CREATE TABLE dbproxy_append_records (
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES dbproxy_multi_transactions(operation_id),
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    occurred_at_unix_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, record_key)
);
CREATE INDEX dbproxy_append_records_operation ON dbproxy_append_records(operation_id);

CREATE FUNCTION dbproxy_reject_append_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'dbproxy append records and commit effects are immutable';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dbproxy_append_immutable BEFORE UPDATE OR DELETE ON dbproxy_append_records
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_append_no_truncate BEFORE TRUNCATE ON dbproxy_append_records
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_effects_immutable BEFORE UPDATE OR DELETE ON dbproxy_multi_transaction_effects
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_effects_no_truncate BEFORE TRUNCATE ON dbproxy_multi_transaction_effects
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_append_mutation();
