-- 先限定需要计数的集合，避免统计每次都处理所有已发布历史。
-- Filter the input sets so sampling does not process all published history.
SELECT
    COUNT(*) FILTER (WHERE lease_until IS NULL OR lease_until <= clock_timestamp()),
    COUNT(*) FILTER (WHERE lease_until > clock_timestamp()),
    (SELECT COUNT(*) FROM dbproxy_outbox WHERE dead_lettered_at IS NOT NULL),
    (EXTRACT(EPOCH FROM (clock_timestamp() - MIN(created_at))) * 1000)::DOUBLE PRECISION
FROM dbproxy_outbox
WHERE published_at IS NULL AND dead_lettered_at IS NULL
