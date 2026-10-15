-- Multi-record transaction receipts are retired after a configured retention: the header and its
-- records are deleted, while the operation claim stays as a small "retired" marker so that a late
-- lookup or replay is answered "expired" instead of "never executed".
-- Nullable without a default: no table rewrite for existing claims.
ALTER TABLE dbproxy_operation_claims ADD COLUMN retired_at TIMESTAMPTZ;

CREATE INDEX dbproxy_operation_claims_multi_retention
    ON dbproxy_operation_claims(claimed_at, operation_id)
    WHERE retired_at IS NULL AND operation_kind = 'multi';
