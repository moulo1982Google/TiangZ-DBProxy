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

Redis backlog、缓存和 Outbox publisher 使用 Redis `ConnectionManager`，Redis 重启后会自动重连。Backlog 的 AOF 入队、worker lease/ACK 和指标采样使用三条独立连接，某一角色的 `WAITAOF` 或重连不会持有另外两条连接的 mutex。PostgreSQL 分片与维护连接也会在发现旧连接关闭后进行最多 2 秒的有界重连；正在执行的请求仍明确失败，调用方必须用原幂等 ID 重试，下一次请求才使用新连接。这样不会在提交结果未知时由底层偷偷重放写操作。

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

### 100 玩家两小时连续演练

`run_fault_soak.ps1` 经过真实客户端、TCP 协议和正在运行的 DBProxy，不绕过网络层。默认持续 7200 秒，100 个玩家每秒读取权威快照、每 10 秒执行一次带 revision 和稳定幂等 ID 的事务；可回退快照每 5 秒按协议上限拆成 64+36 两批执行 `EnqueueMultiSnapshot`，交易、账本和 Outbox 按每玩家 10 分钟一笔产生。默认 32 条客户端连接按 24 读 + 8 写拆池，隔离慢写队头阻塞；`PoolSize=1` 时退回共享连接。驱动只保存有界计数器，每分钟输出增量，并跳过故障期间错过的周期而不是在恢复后突发补跑。

先按分区模式启动 PostgreSQL/Redis 和 DBProxy，再执行：

```powershell
cargo build --release --bin dbproxy_fault_soak
powershell -ExecutionPolicy Bypass -File tools/run_fault_soak.ps1
```

正式时间表固定如下；传入 `-DurationSeconds 120` 时会同比压缩，供正式演练前验证编排器：

| 时间 | 动作 | 主要断言 |
| --- | --- | --- |
| 0–10 分钟 | 全部健康 | 建立零错误基线 |
| 10–20 分钟 | Redis 停机 | 读回源 PostgreSQL；权威事务继续提交并产生 durable cache repair |
| 20–30 分钟 | Redis 恢复 | cache repair 与 Outbox 自动排空 |
| 30–40 分钟 | PostgreSQL 停机 | 热缓存继续读；事务不报告成功；快照进入 Redis backlog |
| 40–55 分钟 | PostgreSQL 恢复 | 原幂等 ID 重试，backlog 排空 |
| 55–60 分钟 | PostgreSQL 再停机 | 为 AOF 强杀测试形成确定积压 |
| 60–62 分钟 | `SIGKILL` Redis | 模拟非正常退出，而非优雅停机 |
| 62–70 分钟 | 仅恢复 Redis | 断言强杀前后的 backlog 均非空，验证 AOF 恢复 |
| 70–90 分钟 | 恢复 PostgreSQL | 排空 AOF backlog 并继续负载 |
| 90–95 分钟 | Redis/PostgreSQL 同时停机 | 请求明确失败，驱动保留原事务 ID |
| 95–105 分钟 | 仅恢复 Redis | AOF 缓存读与 backlog 恢复，PostgreSQL 写仍不伪成功 |
| 105–120 分钟 | 全部恢复 | 持续负载、排空并逐玩家最终对账 |

结果写入仓库外层的 `.build-tmp/dbproxy-fault-drill/<timestamp>-<duration>s`：`workload.stdout.log` 是每分钟业务计数和最终对账，`events.jsonl` 是故障时间线，`samples.jsonl` 保存 DBProxy 指标与容器资源快照。脚本用 `finally` 恢复两个容器；成功标准是最终逐玩家对账通过、无缺失快照、无低于已确认 revision 的读取、三个队列归零且 cache repair/Outbox 死信均为零。

2026-08-27 已在本机以 PostgreSQL 18.4、Redis 8.8.1（AOF `everysec`）完成 100 玩家、7,200 秒正式演练。逐玩家最终对账通过，Redis 强杀前后 backlog 均为 100，三个队列最终归零，缓存修复与 Outbox 死信均为 0。完整计数、资源峰值和演练发现见[两小时故障演练报告](fault-soak-report-2026-08-27.md)。

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

`dbproxy_snapshots` 的 32 个库内 HASH 分区不改变本手册的恢复语义；DBProxy 始终通过父表读写。历史归档、其他表分区和物理分库不在本次恢复手册范围内。
