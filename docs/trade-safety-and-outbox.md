# 交易安全、托管状态机与 Outbox

本实现把一次交易中必须同生共死的持久化结果放进同一个 PostgreSQL 事务：交易状态、相关快照 Revision、不可变账本、幂等回执、缓存修复任务和 Outbox 事件。Redis 不参与交易提交判定。

## 责任边界

业务服务仍然负责检查玩家在线状态、交易锁、道具所有权、数量、价格、风控和权限，并据此生成完整的 `TradeTransaction`。DBProxy 再做数据库侧的最终校验：

- `operation_id` 全局只能属于 single、multi、trade 中的一种事务类型；
- 交易版本和当前状态必须等于请求中的期望值；
- 每条玩家/领域快照的 Revision 必须等于期望值；
- 同一资产下的账本 Posting 金额总和必须为零；
- Posting ID、Event ID 和 RecordKey 不能在请求内重复；
- 相同 `operation_id` 的重试必须与第一次提交内容完全一致。

所以，业务层检查负责给出“这笔交易是否允许”的答案；PostgreSQL 事务和 Revision 检查负责阻止检查之后出现的并发篡改。外挂绕过客户端规则也不能绕过服务端业务校验和数据库 CAS。

## 托管状态机

允许的状态迁移如下：

| 当前状态 | 下一状态 | 用途 |
| --- | --- | --- |
| 不存在 | `Proposed` | 创建待确认交易单 |
| 不存在 | `Escrowed` | 业务已经完成双方确认，直接进入托管 |
| `Proposed` | `Escrowed` | 锁定卖方物品、买方资金或对应快照 |
| `Proposed` | `Cancelled` | 确认前取消 |
| `Escrowed` | `Settled` | 交付并结算 |
| `Escrowed` | `Cancelled` | 释放托管并补偿回原账户 |

`Settled` 和 `Cancelled` 是终态。新交易要求 `expected_version=0` 且没有 `expected_state`；后续迁移必须同时携带当前版本和状态。交易版本与玩家快照 Revision 独立递增，避免用某个玩家的背包版本冒充整笔交易的生命周期版本。

## 一次提交的顺序

`ApplyTradeTransaction` 在一个 PostgreSQL 事务中执行：

1. 在 `dbproxy_operation_claims` 占用全局 `operation_id` 类型。
2. 写入或读取 `dbproxy_trade_operations` 幂等回执头。
3. 按 `trade_id` 加事务级锁，校验交易版本和状态。
4. 按排序后的 `topic + partition_key` 取得 Outbox 分区事务锁，保证并发交易的提交可见顺序。
5. 按 `(namespace, record_key)` 排序加锁，校验所有快照 Revision。
6. 以带 expected Revision 的原子 SQL 写入再次执行 CAS，覆盖首次创建记录的竞争窗口。
7. 更新 `dbproxy_trades`，写入所有权威快照和每条记录的回执。
8. 追加 `dbproxy_ledger_postings`；数据库触发器禁止 UPDATE、DELETE 和 TRUNCATE。
9. 插入 `dbproxy_outbox` 事件；内容和 `created_at` 不可修改，未发布事件不可删除，表不可 TRUNCATE。
10. 为每条变更快照 upsert `dbproxy_cache_repairs`。
11. 提交后尝试立即刷新 Redis；失败不改变已经提交的交易结果，修复 worker 会继续处理。

任何步骤失败都会回滚上述所有 PostgreSQL 变化。网络在提交后断开时，调用方使用完全相同的 `operation_id` 重试，得到 `Duplicate` 和第一次保存的 `TradeReceipt`；修改 Payload、Posting、Event 或记录集合的重试会得到 `OPERATION_CONFLICT`。

## 不可变账本

每条 Posting 是带正负号的 `i64 amount`。同一 `asset` 的一组 Posting 必须平衡，例如：

```text
buyer:gold          -100
escrow:trade-1001   +100
合计                    0
```

结算时再追加新的 Posting，把托管账户转给卖方；取消时追加反向 Posting。已经存在的 Posting 永远不修改或删除。`posting_id` 是全局幂等键，冲突会使整笔交易回滚。当前账本提供不可变审计事实，不代替业务侧的余额/物品规则，也没有实现按账本实时聚合余额的查询 API。

## PostgreSQL Outbox worker

Outbox 事件与交易在同一事务写入，因此不存在“交易已提交但事件意图丢失”的窗口。worker 使用 `FOR UPDATE SKIP LOCKED` 领取短租约，失败后按指数退避重试，达到 `maxAttempts` 后进入死信。默认参数位于顶层 `outbox` 配置：1 个 worker、30 秒租约、1 秒到 60 秒退避、最多 20 次。

当前发布目标是 Redis Stream `dbproxy:outbox:{topic}`。产生事件的事务先按分区串行化，同一 `topic + partition_key` 又只允许最早的未发布事件被领取；前序进入死信也会阻塞后序，直到定点处理，从而避免未提交窗口或多 worker 越过分区缺口。worker 在 `XADD` 后执行 `WAITAOF 1 0 2000`，只有本地 AOF 确认后才在 PostgreSQL 标记 `published_at`。如果发布成功后 PostgreSQL ACK 丢失，事件会再次发布，所以这是至少一次投递；消费者仍必须按稳定的 `event_id` 去重。Redis Stream 的消费组、业务重试和保留/裁剪策略属于消费者部署责任。

## 故障语义

| 故障点 | 客户端/worker 结果 | 恢复方式 |
| --- | --- | --- |
| Revision 或交易状态冲突 | 整笔回滚 | 业务重新读取并重新决策，使用新的 operation ID |
| PostgreSQL 提交前断开 | 结果未知 | 原 operation ID、原请求重试 |
| PostgreSQL 已提交，响应丢失 | 重试返回 `Duplicate` | 使用保存的 Receipt 恢复内存状态 |
| Redis 缓存不可用 | 交易仍返回已提交 | durable cache repair worker 自动修复 |
| Redis Outbox 发布失败 | PG 事件保持未发布 | 指数退避重试，耗尽后死信 |
| Redis 已 XADD，PG ACK 失败 | 可能重复事件 | 消费者按 event ID 去重 |
| DBProxy worker 崩溃 | 租约到期后重新领取 | 无需人工释放租约 |

## 运维检查

重点指标是：

- `dbproxy_cache_repair_pending/processing/dead_lettered/oldest_age_seconds`
- `dbproxy_cache_repair_worker_polls_total{result=...}`
- `dbproxy_outbox_pending/processing/dead_lettered/oldest_age_seconds`
- `dbproxy_outbox_worker_polls_total{result=...}`

死信不能盲目批量清除。修好 Redis/AOF 或事件消费者后，先检查 `last_error` 和目标内容，再通过 `PostgresCacheRepairQueue::requeue_dead_letter(record)` 或 `PostgresOutboxQueue::requeue_dead_letter(event_id)` 定点重放。数据库表中的死信记录本身也是故障证据，不应直接删除。

交易涉及的权威快照可以位于 `dbproxy_snapshots` 的不同 HASH 叶子分区，因为它们仍属于同一个 PostgreSQL 数据库事务。交易、账本和 Outbox 表本身的分区、历史归档及物理分库继续延期；当前实现不提供跨 database/cluster 原子事务。
