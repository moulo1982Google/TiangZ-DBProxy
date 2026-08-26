# DBProxy 持久化与故障恢复手册

本文覆盖 Redis AOF backlog、PostgreSQL 权威写入、缓存修复队列和 Outbox 的恢复操作。演练只操作本机 `tiangz-dbproxy-postgres` 与 `tiangz-dbproxy-redis` 容器，不删除命名卷，也不启动 Prometheus/Grafana，适合内存有限的笔记本。

## ACK 边界

| API/worker | 成功代表什么 |
| --- | --- |
| `SaveSnapshot` / `ApplyTransaction` / `ApplyMultiTransaction` / `ApplyTradeTransaction` | PostgreSQL 权威事务已经提交；Redis 缓存可以稍后由持久修复队列补齐 |
| `EnqueueSnapshot` / `EnqueueMultiSnapshot` | Redis 已执行入队脚本，并通过 `WAITAOF 1 0 2000` 确认本机 AOF；尚不代表 PostgreSQL 已提交 |
| Outbox worker 的 PostgreSQL ACK | 事件已写入 Redis Stream，且本机 Redis AOF 已确认；仍可能至少一次重复投递 |

如果 Redis 未启用 AOF，`WAITAOF` 会返回 Redis 错误；如果已启用但 2 秒内没有本机确认，则返回 `RedisAofNotDurable`。两种情况都拒绝可靠 ACK，不会把易失内存写入伪装成持久成功。连接管理器的响应窗口是 3 秒，刻意比 2 秒 AOF 判定多留 1 秒网络与调度余量。笔记本轻量模式默认关闭 AOF，因此测试这两个路径时必须使用 `-Aof`。

## 自动恢复机制

### 普通快照 AOF backlog

Redis 保存 Payload、pending 索引和 processing lease。worker 领取后写 PostgreSQL，成功或幂等 Duplicate 才 ACK；失败主动 release，进程崩溃则由 lease 到期回收。同一 `RecordKey` 的新快照会替换旧待处理内容，旧 lease 的 ACK 不能删除新内容。

### 已提交快照的缓存修复

每次 PostgreSQL 权威写入在同一数据库事务内 upsert `dbproxy_cache_repairs`。Redis 快速回写成功后定点 ACK；失败时客户端仍得到 PostgreSQL 已提交结果。worker 按租约领取，读取最新权威快照并做 revision-aware Redis 写入；旧 lease 只能 ACK 自己领取的 target Revision，并发产生的新目标会继续保留。

### PostgreSQL Outbox

交易事务把事件写入 `dbproxy_outbox`。同一 `topic + partition_key` 的交易先取得事务级分区锁，队列只领取该分区最早的未发布事件；前序死信会阻塞后序。worker 使用短租约、`SKIP LOCKED`、指数退避和死信；发布成功但 ACK 丢失会重复投递，消费者必须按 `event_id` 去重。

Redis backlog、缓存和 Outbox publisher 使用 Redis `ConnectionManager`，Redis 重启后会自动重连。PostgreSQL 分片与维护连接也会在发现旧连接关闭后进行最多 2 秒的有界重连；正在执行的请求仍明确失败，调用方必须用原幂等 ID 重试，下一次请求才使用新连接。这样不会在提交结果未知时由底层偷偷重放写操作。

## 本机演练

先确保 `deploy/local/.env` 存在，然后执行：

```powershell
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 up -Aof
powershell -ExecutionPolicy Bypass -File tools/fault_matrix.ps1
```

`fault_matrix.ps1` 强制使用笔记本覆盖文件、只启动 PostgreSQL/Redis，并设置 AOF 为 `yes`。Redis database 15 专用于该破坏性演练，每次开始会清空；正常开发使用的 database 0 不受影响。测试串行执行，过程中会短暂停止并恢复容器，最后保证二者回到健康状态。

演练覆盖：

1. Redis 停机时读取回源 PostgreSQL；事务提交后缓存失败仍由 durable repair 恢复。
2. PostgreSQL 停机时绝不报告成功写入。
3. 进程内 flush queue 在 PostgreSQL 恢复后重试。
4. Redis AOF backlog 在 PostgreSQL 停机期间保留并在恢复后排空。
5. backlog 已入队数据经过 Redis 整机重启后仍存在，并由原连接管理器重连领取。
6. 新快照替换 processing 中的旧快照时，旧 ACK 不会误删新值。

2026-08-27 已在本机以 PostgreSQL 18.4、Redis 8.8.1（AOF `everysec`）完成一次实际演练，结果为 6/6 通过。演练同时验证了 Redis 整体不可用时读路径只等待一次缓存窗口，随后直接回源 PostgreSQL，不会重复叠加缓存复查、分布式锁和回填等待。

演练结束后释放笔记本内存但保留数据卷：

```powershell
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 down
```

## 积压与死信处理

先观察指标，不要直接删表或删 Redis key：

- backlog：`dbproxy_backlog_pending`、`dbproxy_backlog_processing`、`dbproxy_backlog_oldest_pending_age_seconds`
- cache repair：`dbproxy_cache_repair_pending`、`processing`、`dead_lettered`、`oldest_age_seconds`
- outbox：`dbproxy_outbox_pending`、`processing`、`dead_lettered`、`oldest_age_seconds`

处理顺序：

1. 确认 PostgreSQL、Redis、AOF 和磁盘空间恢复，先阻止故障继续扩大。
2. 检查 `last_error`，区分认证/网络错误、AOF 未启用、Payload 问题和消费者错误。
3. 让自动 worker 处理普通 pending；不要同时人工改 lease 字段。
4. 对已经死信的项目逐条核对，再调用 `requeue_dead_letter` 定点重放。
5. 观察 oldest age 回落到零，并确认 dead-letter 告警消失。

缓存修复任务按 `RecordKey` 合并到最新 target Revision，不会因为 Redis 长时间故障而为同一玩家无限追加行。Outbox 不能这样合并：每个事件都是不可变事实，积压容量必须按事件产生速率、允许故障时长和 Redis Stream 保留策略单独规划。

历史归档、PostgreSQL 分区和物理分库不在本次恢复手册范围内。
