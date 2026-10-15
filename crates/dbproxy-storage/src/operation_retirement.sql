WITH due AS MATERIALIZED (
    SELECT claim.operation_id
    FROM dbproxy_operation_claims AS claim
    WHERE claim.operation_kind = 'multi'
      AND claim.retired_at IS NULL
      -- $1 is the configured retention in seconds, measured from the server-side claim time.
      AND claim.claimed_at < statement_timestamp() - make_interval(secs => $1::double precision)
      -- Commit effects, append records and outbox rows reference these operations and are
      -- immutable business facts; such receipts are never retired.
      AND NOT EXISTS (
          SELECT 1 FROM dbproxy_multi_transaction_effects AS effects
          WHERE effects.operation_id = claim.operation_id
      )
    ORDER BY claim.claimed_at, claim.operation_id
    -- A commit replaying this operation holds FOR SHARE on its claim; skip it and retire later.
    FOR UPDATE SKIP LOCKED
    LIMIT 500
), removed AS (
    -- Records follow the header through ON DELETE CASCADE. An id array keeps both statements on
    -- the primary keys; a join could hash the whole receipt table.
    DELETE FROM dbproxy_multi_transactions AS header
    WHERE header.operation_id = ANY(ARRAY(SELECT operation_id FROM due))
)
UPDATE dbproxy_operation_claims AS claim
SET retired_at = statement_timestamp()
WHERE claim.operation_id = ANY(ARRAY(SELECT operation_id FROM due))
