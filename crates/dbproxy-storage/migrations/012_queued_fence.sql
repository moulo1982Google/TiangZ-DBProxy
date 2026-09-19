-- 排队写落库的防护序号：只有序号更大的排队写才能覆盖，迟到的旧值成为空操作。
-- 可为空：升级前的行与非排队写不带序号；分区父表加列会同步到全部分区。
-- Fence sequence for queued flushes: only a larger sequence may overwrite, so a late older value becomes a no-op.
-- Nullable: rows from before the upgrade and non-queued writes carry none; adding it to the partitioned parent
-- propagates to every partition.
ALTER TABLE dbproxy_snapshots ADD COLUMN IF NOT EXISTS queued_sequence BIGINT;
