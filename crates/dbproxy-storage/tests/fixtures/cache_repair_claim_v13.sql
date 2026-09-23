WITH candidate AS (
    SELECT namespace, record_key
    FROM dbproxy_cache_repairs
    WHERE dead_lettered_at IS NULL
      -- 本条语句开始时已到期的任务才参与领取，使日期索引可用于范围定位。
      -- Use a statement-stable eligibility boundary for indexed range access.
      AND available_at <= statement_timestamp()
      AND (lease_until IS NULL OR lease_until <= clock_timestamp())
    ORDER BY requested_at, namespace, record_key
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE dbproxy_cache_repairs AS repair
SET lease_owner = $1,
    lease_token = nextval('dbproxy_cache_repair_lease_seq'),
    lease_until = clock_timestamp() + ($2::BIGINT * interval '1 millisecond')
FROM candidate
WHERE repair.namespace = candidate.namespace
  AND repair.record_key = candidate.record_key
RETURNING repair.namespace, repair.record_key, repair.target_revision, repair.attempt_count, repair.lease_token
