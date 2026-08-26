CREATE TABLE IF NOT EXISTS dbproxy_operation_claims (
    operation_id TEXT PRIMARY KEY,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('single', 'multi', 'trade')),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT operation_id FROM dbproxy_transactions
        INTERSECT
        SELECT operation_id FROM dbproxy_multi_transactions
    ) OR EXISTS (
        SELECT operation_id FROM dbproxy_transactions
        INTERSECT
        SELECT operation_id FROM dbproxy_trade_operations
    ) OR EXISTS (
        SELECT operation_id FROM dbproxy_multi_transactions
        INTERSECT
        SELECT operation_id FROM dbproxy_trade_operations
    ) THEN
        RAISE EXCEPTION 'an existing operation_id is reused across DBProxy transaction kinds';
    END IF;
END;
$$;

INSERT INTO dbproxy_operation_claims (operation_id, operation_kind)
SELECT operation_id, 'single' FROM dbproxy_transactions
ON CONFLICT (operation_id) DO NOTHING;

INSERT INTO dbproxy_operation_claims (operation_id, operation_kind)
SELECT operation_id, 'multi' FROM dbproxy_multi_transactions
ON CONFLICT (operation_id) DO NOTHING;

INSERT INTO dbproxy_operation_claims (operation_id, operation_kind)
SELECT operation_id, 'trade' FROM dbproxy_trade_operations
ON CONFLICT (operation_id) DO NOTHING;
