-- Existing receipts receive the migration time, granting a full seven-day grace period.
-- A stable default avoids rewriting the existing table to populate every row.
ALTER TABLE dbproxy_idempotency
    ADD COLUMN recorded_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp();

CREATE INDEX dbproxy_idempotency_retention
    ON dbproxy_idempotency(recorded_at, request_id);
