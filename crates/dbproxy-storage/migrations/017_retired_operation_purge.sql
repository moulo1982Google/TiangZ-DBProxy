-- Retired multi-record operation markers are deleted after a second retention, so the claim table
-- stops growing with every operation ever made. This index finds the oldest retired markers.
CREATE INDEX dbproxy_operation_claims_retired_purge
    ON dbproxy_operation_claims(retired_at, operation_id)
    WHERE retired_at IS NOT NULL AND operation_kind = 'multi';
