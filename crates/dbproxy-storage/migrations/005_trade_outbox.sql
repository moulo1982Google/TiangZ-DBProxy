CREATE TABLE IF NOT EXISTS dbproxy_trades (
    trade_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL CHECK (version > 0),
    state SMALLINT NOT NULL CHECK (state BETWEEN 1 AND 4),
    payload BYTEA NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbproxy_trade_operations (
    operation_id TEXT PRIMARY KEY,
    trade_id TEXT NOT NULL REFERENCES dbproxy_trades(trade_id) DEFERRABLE INITIALLY DEFERRED,
    expected_version BIGINT NOT NULL CHECK (expected_version >= 0),
    expected_state SMALLINT NULL CHECK (expected_state BETWEEN 1 AND 4),
    next_state SMALLINT NOT NULL CHECK (next_state BETWEEN 1 AND 4),
    trade_payload BYTEA NOT NULL,
    result BYTEA NOT NULL,
    record_count BIGINT NOT NULL CHECK (record_count > 0),
    ledger_count BIGINT NOT NULL CHECK (ledger_count >= 0),
    outbox_count BIGINT NOT NULL CHECK (outbox_count >= 0),
    updated_at_unix_ms BIGINT NOT NULL,
    new_trade_version BIGINT NOT NULL CHECK (new_trade_version > 0)
);

CREATE INDEX IF NOT EXISTS dbproxy_trade_operations_trade
    ON dbproxy_trade_operations (trade_id, new_trade_version);

CREATE TABLE IF NOT EXISTS dbproxy_trade_operation_records (
    operation_id TEXT NOT NULL REFERENCES dbproxy_trade_operations(operation_id) ON DELETE CASCADE,
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

CREATE TABLE IF NOT EXISTS dbproxy_ledger_postings (
    posting_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES dbproxy_trade_operations(operation_id),
    trade_id TEXT NOT NULL REFERENCES dbproxy_trades(trade_id),
    account_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount <> 0),
    metadata BYTEA NOT NULL,
    created_at_unix_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS dbproxy_ledger_postings_trade
    ON dbproxy_ledger_postings (trade_id, posting_id);

CREATE INDEX IF NOT EXISTS dbproxy_ledger_postings_account
    ON dbproxy_ledger_postings (account_id, asset, posting_id);

CREATE OR REPLACE FUNCTION dbproxy_reject_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'dbproxy ledger postings are immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dbproxy_ledger_postings_immutable ON dbproxy_ledger_postings;
CREATE TRIGGER dbproxy_ledger_postings_immutable
    BEFORE UPDATE OR DELETE ON dbproxy_ledger_postings
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_ledger_mutation();

DROP TRIGGER IF EXISTS dbproxy_ledger_postings_no_truncate ON dbproxy_ledger_postings;
CREATE TRIGGER dbproxy_ledger_postings_no_truncate
    BEFORE TRUNCATE ON dbproxy_ledger_postings
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_ledger_mutation();

CREATE TABLE IF NOT EXISTS dbproxy_outbox (
    event_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES dbproxy_trade_operations(operation_id),
    trade_id TEXT NOT NULL REFERENCES dbproxy_trades(trade_id),
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    occurred_at_unix_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner TEXT NULL,
    lease_until TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    published_at TIMESTAMPTZ NULL,
    dead_lettered_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS dbproxy_outbox_ready
    ON dbproxy_outbox (available_at, occurred_at_unix_ms)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS dbproxy_outbox_dead_lettered
    ON dbproxy_outbox (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;

CREATE OR REPLACE FUNCTION dbproxy_reject_outbox_content_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.event_id IS DISTINCT FROM NEW.event_id
       OR OLD.operation_id IS DISTINCT FROM NEW.operation_id
       OR OLD.trade_id IS DISTINCT FROM NEW.trade_id
       OR OLD.topic IS DISTINCT FROM NEW.topic
       OR OLD.partition_key IS DISTINCT FROM NEW.partition_key
       OR OLD.payload IS DISTINCT FROM NEW.payload
       OR OLD.occurred_at_unix_ms IS DISTINCT FROM NEW.occurred_at_unix_ms
       OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'dbproxy outbox event content is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dbproxy_outbox_content_immutable ON dbproxy_outbox;
CREATE TRIGGER dbproxy_outbox_content_immutable
    BEFORE UPDATE ON dbproxy_outbox
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_outbox_content_mutation();

CREATE OR REPLACE FUNCTION dbproxy_reject_unpublished_outbox_delete()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.published_at IS NULL THEN
        RAISE EXCEPTION 'unpublished dbproxy outbox events cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dbproxy_outbox_unpublished_no_delete ON dbproxy_outbox;
CREATE TRIGGER dbproxy_outbox_unpublished_no_delete
    BEFORE DELETE ON dbproxy_outbox
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_unpublished_outbox_delete();

CREATE OR REPLACE FUNCTION dbproxy_reject_outbox_truncate()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'dbproxy outbox cannot be truncated';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dbproxy_outbox_no_truncate ON dbproxy_outbox;
CREATE TRIGGER dbproxy_outbox_no_truncate
    BEFORE TRUNCATE ON dbproxy_outbox
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_outbox_truncate();
