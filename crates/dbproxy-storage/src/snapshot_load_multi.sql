-- 完整主键最多返回一条；保留逐键查找，避免较大批次被改成全分区扫描。
-- A full primary key has at most one row; retain per-key lookup for larger batches.
SELECT snapshot.*
FROM unnest($1::TEXT[], $2::TEXT[]) AS requested(namespace, record_key)
JOIN LATERAL (
    SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms
    FROM dbproxy_snapshots
    WHERE namespace = requested.namespace AND record_key = requested.record_key
    LIMIT 1
) AS snapshot ON true
