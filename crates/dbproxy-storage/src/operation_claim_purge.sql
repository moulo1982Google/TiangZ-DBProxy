WITH due AS MATERIALIZED (
    SELECT claim.operation_id
    FROM dbproxy_operation_claims AS claim
    WHERE claim.operation_kind = 'multi'
      AND claim.retired_at IS NOT NULL
      -- $1 is the configured marker retention in seconds, measured from the server-side retirement time.
      AND claim.retired_at < statement_timestamp() - make_interval(secs => $1::double precision)
    ORDER BY claim.retired_at, claim.operation_id
    -- A late replay that already found the marker holds FOR SHARE on it; skip it and delete later.
    FOR UPDATE SKIP LOCKED
    LIMIT 500
)
-- A retired claim has no header, records or effects left, so nothing references it any more.
DELETE FROM dbproxy_operation_claims AS claim
WHERE claim.operation_id = ANY(ARRAY(SELECT operation_id FROM due))
