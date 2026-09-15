# Outbox claim 索引验证（2026-09-08）

结论：迁移 011 只删除 007 的 `dbproxy_outbox_partition_order(topic, partition_key, created_at, event_id)`。保留现有 ready 索引和 `dbproxy_outbox_order(publisher_id, destination, partition_key, enqueue_order) WHERE published_at IS NULL`。本轮不加入 enqueue_order 单列部分索引，因为它改善正常领取却使死信头阻塞的样本退化。

## 方法

独立本机 PostgreSQL 18.4 容器，无业务 worker。使用原始 `outbox.rs` 的完整 claim SQL（含 UPDATE、FOR UPDATE SKIP LOCKED、NOT EXISTS），替换 worker/lease/publisher 参数后执行 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`；publisher 参数为 NULL。每种分布插入 100,001 条未发布事件，最后一条属于独立可推进组。

同一事务、同一份数据比较三种索引设置：原索引、仅删除旧索引、删除旧索引并增加 `ON dbproxy_outbox(enqueue_order) WHERE published_at IS NULL AND dead_lettered_at IS NULL`。每次 claim 用 savepoint 回滚，避免已领取状态影响下一次；每种设置运行五次，丢弃第一次，报告其余四次执行时间的中位数。每种分布开始前清理此前回滚产生的死元组并更新统计信息，没有修改 PostgreSQL planner 开关。

| 分布 | 原索引 ms | 只删旧索引 ms | 新排序索引 ms |
| --- | ---: | ---: | ---: |
| ready：前 100,000 条分入 1,000 组，均可按序推进 | 494.691 | 513.219 | 0.151 |
| backoff：前 90,000 条延后一小时，每条使用独立组 | 59.064 | 38.688 | 12.403 |
| dead-heads：1,000 组各自首条死信，后续 99,000 条被阻塞 | 518.744 | 517.039 | 717.736 |

新索引在正常分布下可以很快找到第一条合格事件，但遇到大量被死信头阻塞的后序事件，仍需扫描并执行前序排除，样本中比原索引慢约 38%。所有计划都未使用 007 的旧 topic/timestamp 索引。只删除旧索引保持查询语义，减少后续写入必须维护的一个索引；表中小样本时间差不视为吞吐收益保证。

这是一次执行计划验证，不是生产容量承诺。没有覆盖 publisher 专用过滤、多 worker 竞争、完整事件发布往返、冷盘或长期表膨胀。故障积压的领取复杂度仍是后续性能工作，不能宣称通过新增一个索引已经解决。

复现脚本与 45 份完整计划保存在此次本机验收证据 `D:/UGit/TiangZ/.build-tmp/dbproxy-b1-controlled.mjs`、`dbproxy-b1-controlled-plans.json`；脚本使用迁移 010 的独立测试库，索引变更均在回滚事务内完成。迁移 011 需要正常 DDL 锁，应与迁移 010 一起安排受控升级，不在固定版本外网长测中执行。
