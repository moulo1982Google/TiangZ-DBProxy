-- Keep the timestamp in new snapshot idempotency receipts so a reused request_id cannot change
-- request content silently. NULL is retained for receipts created by older DBProxy versions.
ALTER TABLE dbproxy_idempotency
    ADD COLUMN IF NOT EXISTS updated_at_unix_ms BIGINT NULL;

ALTER TABLE dbproxy_outbox
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

CREATE INDEX IF NOT EXISTS dbproxy_outbox_partition_order
    ON dbproxy_outbox (topic, partition_key, created_at, event_id)
    WHERE published_at IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'dbproxy_trade_operations'::regclass
          AND conname = 'dbproxy_trade_operations_expected_state_valid'
    ) THEN
        ALTER TABLE dbproxy_trade_operations
            ADD CONSTRAINT dbproxy_trade_operations_expected_state_valid
            CHECK (expected_state IS NULL OR expected_state BETWEEN 1 AND 4);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'dbproxy_trade_operations'::regclass
          AND conname = 'dbproxy_trade_operations_next_state_valid'
    ) THEN
        ALTER TABLE dbproxy_trade_operations
            ADD CONSTRAINT dbproxy_trade_operations_next_state_valid
            CHECK (next_state BETWEEN 1 AND 4);
    END IF;
END;
$$;
