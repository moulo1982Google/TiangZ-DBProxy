-- 009 changed ordering to publisher/destination/partition/enqueue_order.
-- The old topic/timestamp index no longer serves claim or admin queries.
-- Keep dbproxy_outbox_order: unpublished dead letters must still block their group.
-- Do not add an enqueue_order-only index yet: dead-head backlog plans regressed in testing.
DROP INDEX dbproxy_outbox_partition_order;
