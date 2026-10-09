WITH expired AS MATERIALIZED (
    SELECT ctid
    FROM dbproxy_idempotency
    -- $1 is the configured retention in seconds; strictly older receipts are eligible.
    WHERE recorded_at < statement_timestamp() - make_interval(secs => $1::double precision)
    ORDER BY recorded_at, request_id
    FOR UPDATE SKIP LOCKED
    LIMIT 500
)
-- These tuple locations are consumed in this statement while their row locks are held.
-- An array of TIDs avoids a hash-join DELETE scanning the entire receipt table.
DELETE FROM dbproxy_idempotency AS receipt
WHERE receipt.ctid = ANY(ARRAY(SELECT ctid FROM expired))
