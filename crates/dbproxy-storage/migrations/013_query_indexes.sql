-- 回执恢复查询需要覆盖所有历史，不能仅索引未发布的事件。
-- Receipt recovery includes history, so the outbox index must include published events.
CREATE INDEX dbproxy_ledger_postings_operation
    ON dbproxy_ledger_postings (operation_id, posting_id);
CREATE INDEX dbproxy_outbox_operation
    ON dbproxy_outbox (operation_id, event_id);
