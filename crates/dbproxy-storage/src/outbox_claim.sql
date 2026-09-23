-- 先尝试前 32 条，再按组查找；32 是快速路径大小，不是整队列扫描上限。
-- Try a small prefix before grouping; 32 bounds the fast path, not total queue I/O.
-- Repeat eligibility on the locked base row so concurrent lease changes are rechecked.
WITH shortlist AS MATERIALIZED (
    SELECT current_event.event_id FROM dbproxy_outbox AS current_event
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      -- 到期筛选使用语句时间；租约检查和新租约起点仍使用实时钟。
      -- Use statement time for eligibility, but wall-clock time for lease checks and grants.
      AND current_event.available_at <= statement_timestamp()
      AND ($3::TEXT IS NULL OR current_event.publisher_id = $3)
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
    ORDER BY current_event.enqueue_order LIMIT 32
), fast AS MATERIALIZED (
    SELECT current_event.event_id FROM shortlist
    JOIN dbproxy_outbox AS current_event USING (event_id)
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      -- 到期筛选使用语句时间；租约检查和新租约起点仍使用实时钟。
      -- Use statement time for eligibility, but wall-clock time for lease checks and grants.
      AND current_event.available_at <= statement_timestamp()
      AND ($3::TEXT IS NULL OR current_event.publisher_id = $3)
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM dbproxy_outbox AS prior_event
          WHERE prior_event.publisher_id = current_event.publisher_id
            AND prior_event.destination = current_event.destination
            AND prior_event.partition_key = current_event.partition_key
            AND prior_event.published_at IS NULL
            AND prior_event.enqueue_order < current_event.enqueue_order
      )
    ORDER BY current_event.enqueue_order
    FOR UPDATE OF current_event SKIP LOCKED LIMIT 1
), fallback AS MATERIALIZED (
    SELECT current_event.event_id FROM dbproxy_outbox AS current_event
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      -- 到期筛选使用语句时间；租约检查和新租约起点仍使用实时钟。
      -- Use statement time for eligibility, but wall-clock time for lease checks and grants.
      AND current_event.available_at <= statement_timestamp()
      AND ($3::TEXT IS NULL OR current_event.publisher_id = $3)
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
      AND NOT EXISTS (SELECT 1 FROM fast)
      AND EXISTS (SELECT 1 FROM shortlist)
      AND NOT EXISTS (
          SELECT 1
          FROM (
              SELECT publisher_id, destination, partition_key, MIN(enqueue_order) AS enqueue_order
              FROM dbproxy_outbox
              WHERE published_at IS NULL
                AND ($3::TEXT IS NULL OR publisher_id = $3)
              GROUP BY publisher_id, destination, partition_key
          ) AS prior_event
          WHERE prior_event.publisher_id = current_event.publisher_id
            AND prior_event.destination = current_event.destination
            AND prior_event.partition_key = current_event.partition_key
            AND prior_event.enqueue_order < current_event.enqueue_order
      )
    ORDER BY current_event.enqueue_order
    FOR UPDATE OF current_event SKIP LOCKED LIMIT 1
), candidate AS (
    SELECT event_id FROM fast UNION ALL SELECT event_id FROM fallback
)
UPDATE dbproxy_outbox AS event
SET lease_owner = $1,
    lease_token = event.lease_token + 1,
    expired_leases = event.expired_leases + CASE WHEN event.lease_until IS NOT NULL THEN 1 ELSE 0 END,
    lease_until = clock_timestamp() + ($2::BIGINT * interval '1 millisecond')
FROM candidate
WHERE event.event_id = candidate.event_id
RETURNING event.event_id, event.operation_id, event.trade_id, event.topic,
          event.partition_key, event.payload, event.occurred_at_unix_ms, event.attempt_count,
          event.producer, event.publisher_id, event.destination, event.lease_token
