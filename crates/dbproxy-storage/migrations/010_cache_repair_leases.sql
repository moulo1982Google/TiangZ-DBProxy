-- Queue rows can be deleted and recreated: a per-row counter cannot fence old workers.
-- Do not reset this sequence when clearing repair tasks.
-- These guarantees require every old queue writer/worker/admin process to exit;
-- applying this migration alone does not make mixed-version operation safe.
CREATE SEQUENCE dbproxy_cache_repair_lease_seq AS BIGINT NO CYCLE;
ALTER TABLE dbproxy_cache_repairs ADD COLUMN lease_token BIGINT NOT NULL DEFAULT 0;
