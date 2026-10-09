WITH candidate AS (
    SELECT current_event.event_id
    FROM dbproxy_outbox AS current_event
    CROSS JOIN LATERAL (
        SELECT prior_event.enqueue_order
        FROM dbproxy_outbox AS prior_event
        WHERE prior_event.publisher_id = current_event.publisher_id
          AND prior_event.destination = current_event.destination
          AND prior_event.partition_key = current_event.partition_key
          AND prior_event.published_at IS NULL
        ORDER BY prior_event.enqueue_order
        LIMIT 1
    ) AS group_head
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      -- 到期筛选使用语句时间；租约检查和新租约起点仍使用实时钟。
      -- Use statement time for eligibility, but wall-clock time for lease checks and grants.
      AND current_event.available_at <= statement_timestamp()
      AND ($3::TEXT IS NULL OR current_event.publisher_id = $3)
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
      AND current_event.enqueue_order = group_head.enqueue_order
    ORDER BY current_event.enqueue_order
    FOR UPDATE OF current_event SKIP LOCKED
    LIMIT 1
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
