# DBProxy 架构说明

2026-09-07：服务端提交后缓存修复清理改由现有维护 worker 有界合并处理，业务提交及持久修复契约不变，见[提交后清理](cache-repair-cleanup.md)。

## 通用提交集成（2026-09-05）

`CommitRecords` 组合非空快照写集合、可选追加事实、可选 Outbox 和业务回执；复用多记录 CAS 提交，不解释 payload。`dbproxy_append_records` 使用独立 namespace/key 唯一键，禁止 UPDATE/DELETE/TRUNCATE；本轮没有新增扫描/索引查询 API，审计查询由后续只读工具或消费端投影承担。

效果规范化后以 bincode standard 编码保存于 `dbproxy_multi_transaction_effects`，重复操作必须比较完整效果。该编码属于持久幂等契约，后续改变结构/编码必须设计兼容读取。普通无效果事务无需新增效果行；带效果的操作用普通多记录 API 重试会被新服务拒绝。

迁移 008 将 Outbox 的 operation_id 外键转为通用操作注册表，trade_id 允许 NULL。旧交易行保留，通用事件的发布字段 trade_id 为空字符串。全部服务和 worker 升级后才启用通用写入；旧二进制不识别新效果和 NULL trade_id，启用后不得直接回滚二进制。

`TradeState`、零和 Posting 和专用 Trade API 是过渡期兼容能力，尚未从内核移除。新领域采用通用提交，状态机与资产规则在游戏服务中实现。其余章节中的“交易原语”描述旧兼容入口，不能作为继续扩展领域规则的依据。详见[通用持久化调整](generic-persistence-plan.md)。

## 定位与边界

DBProxy 是 TiangZ 的独立持久化边界。业务服务提交已经序列化的完整快照和事务计划；DBProxy 负责 Revision/CAS、幂等、同库原子提交、缓存、持久队列和恢复，不负责场景、道具价格、玩家资格、背包容量等玩法规则。

交易 API 是通用的持久化原语：DBProxy 理解有限状态、平衡 Posting 和 Outbox 意图，但不解释交易 Payload、资产含义或所有权。业务层必须先校验规则，数据库 CAS 是防止校验后并发变化的最后一道门。

```text
TiangZ Repository / domain service
          |
          | versioned protobuf + auth token
          v
DBProxy-1 ---------------- DBProxy-2       无状态对等实例
    |                          |
    +----------+---------------+
               |
       +-------+--------+
       |                |
 PostgreSQL          Redis 8
 权威快照/事务       已提交快照缓存
 trade/ledger        普通快照 AOF backlog
 repair/outbox       Outbox Redis Streams
```

多个 DBProxy 实例不复制内存状态、没有 Leader，也不互相转发；它们必须共享同一 PostgreSQL 和 Redis。客户端切换 Endpoint 时复用原 `request_id`/`operation_id`。

## 数据地址与版本

`RecordKey = namespace + key` 只是通用记录地址，不等于 PostgreSQL 表名或物理分片。所有快照都通过逻辑父表 `dbproxy_snapshots` 访问，以 `(namespace, record_key)` 为主键；PostgreSQL 按相同两列把数据路由到 32 个 HASH 叶子分区。分区对协议和 Repository 透明，详细布局见[PostgreSQL 快照分区](postgresql-partitioning.md)。

每次权威修改生成单调 `Revision`。带 `expected_revision` 的写入使用 Compare-And-Swap：不匹配时返回实际版本并回滚，调用方重新读取后由业务决定合并或拒绝。`request_id`/`operation_id` 是调用方生成的稳定幂等键；相同 ID 只能重放完全相同的请求。

## 三类写入 ACK

| 路径 | 适用数据 | 成功 ACK |
| --- | --- | --- |
| `SaveSnapshot` / `SaveMultiSnapshot` | 普通但需要同步落库的完整快照 | PostgreSQL 已提交；缓存可异步修复 |
| `EnqueueSnapshot` / `EnqueueMultiSnapshot` | 允许有限回退、可按 RecordKey 合并的状态 | Redis backlog 已通过本机 AOF 确认，PostgreSQL 尚未提交 |
| `ApplyTransaction` / `ApplyMultiTransaction` / `ApplyTradeTransaction` | 货币、背包、奖励、交易等关键状态 | PostgreSQL 事务及持久回执已提交；缓存可异步修复 |

`SaveMultiSnapshot` 是批量调用，允许部分成功；它不是跨记录业务事务。服务端会先按连接 shard 分组，同一 shard 内的普通快照共享一次 PostgreSQL transaction/commit，以减少每条记录一次 WAL flush/commit 的开销；Revision 或幂等冲突只拒绝对应条目，SQL/连接级错误则回滚该 shard 整批，调用方继续用原 request ID 重试。需要“要么全部成功、要么全部失败”的背包/货币/交易仍必须使用 `ApplyMultiTransaction` 或 `ApplyTradeTransaction`。`Enqueue*` 禁止携带 CAS，不能用于关键经济数据。

## PostgreSQL 权威写入与缓存修复

直接写入路径固定为：

```text
PostgreSQL transaction
  1. claim idempotency ID
  2. validate CAS and persist snapshot/receipt
  3. upsert dbproxy_cache_repairs in the same transaction
  4. commit
Redis revision-aware fast-path refresh
  success -> remove repair target at or below cached revision
  failure -> return committed result; repair worker retries
```

这消除了“数据库已经提交，但 Redis 失败导致客户端收到模糊失败”的旧语义。修复表按 `RecordKey` 合并，只保留最高 `target_revision`；worker 使用 PostgreSQL 时钟、短租约和 `FOR UPDATE SKIP LOCKED`，指数退避后进入死信。旧 lease 只能 ACK 自己领取的目标，不能删除并发产生的新版本。

缓存写入由 Lua 脚本比较 Revision，旧快照不能覆盖新快照。普通批量快照、multi transaction 和 trade 提交后的缓存刷新都使用一次批量 Lua 调用，再用一条 PostgreSQL `unnest` 删除已覆盖的修复目标；如果任一步失败，事务内预先写入的修复行仍然存在。读取失败、编码损坏或 miss 会回源 PostgreSQL；缓存预热失败不影响权威读取结果。

严格 read-after-write 部署必须把快照缓存与 AOF backlog/Outbox 分开。可靠队列 Redis 会从 AOF 恢复；若它同时保存缓存，崩溃前尚未刷入 AOF 的新缓存可能在重启后被旧值和旧 freshness 标记替代，并在 repair worker 赶上前产生短暂旧读。`cacheRedisUrlEnv`因此指向关闭 AOF/RDB 的易失实例：重启后缓存为空，只能回源 PostgreSQL，再由读预热或持久 repair 重建。`redisUrlEnv`仍只负责必须保留的 backlog 和 Outbox。单 Redis 配置保留用于兼容和本地开发，但不能通过这一严格恢复边界。

## 缓存击穿与生命周期

正缓存默认 fresh 5 分钟、稳定抖动最多 30 秒、stale-while-revalidate 30 秒；负缓存默认 5 秒。miss 回源受到以下保护：

- 同一进程按 `RecordKey` singleflight；
- 每个存储分片的回源并发闸门和超时；
- closed/open/half-open 熔断器；
- 多实例 Redis 租约锁及锁内二次缓存检查；
- stale 命中立即返回，后台有界刷新。

Redis 锁不可用或等待超时时，系统仍可回源 PostgreSQL；协调层不能把权威数据变得不可读。所有缓存与回源指标只使用固定低基数标签。

## Redis AOF 普通快照 backlog

`RedisSnapshotBacklog` 的 Lua 脚本原子写 entry 和 pending 索引，随后执行 `WAITAOF 1 0 2000`。AOF 没有确认就不返回可靠 ACK。worker 领取时把记录移到 processing 并设置 lease：

```text
enqueue -> WAITAOF -> pending
claim -> processing lease
PostgreSQL SaveSnapshot -> Applied/Duplicate
ack -> remove
```

PostgreSQL 失败时主动 release；worker 崩溃时 lease 到期回收。worker 每次最多批量领取 64 条，按 shard 合并 PostgreSQL commit，并批量 ACK/release，避免积压恢复时每条快照产生一套 Redis 往返和数据库提交。相同 RecordKey 的新快照替换旧内容，旧 processing ACK 返回 `Superseded`，不能误删新值。详细演练见[持久化与故障恢复手册](durability-recovery-runbook.md)。

## 关键事务层次

### 单记录事务

`ApplyTransaction` 原子保存一条快照和第一次业务 `result`。提交响应丢失后，用同一 `operation_id` 重试会返回原始 Receipt，不会再次递增 Revision。

### 多记录事务

`ApplyMultiTransaction` 最多提交 256 个不重复 RecordKey。DBProxy 排序后获取 advisory lock 和行锁，校验所有 Revision，再以带 expected Revision 的原子 SQL 写入。任何记录冲突都会整组回滚；首次创建记录与单记录 API 并发时也不能绕过 CAS。

### 交易事务

`ApplyTradeTransaction` 在上述多记录 CAS 外，再原子提交交易状态机、不可变平衡账本、Outbox 和完整 Receipt。`dbproxy_operation_claims` 禁止同一个 operation ID 跨 single/multi/trade 类型复用。详细状态、失败矩阵和消费者契约见[交易安全、托管状态机与 Outbox](trade-safety-and-outbox.md)。

这些事务只覆盖同一个 PostgreSQL 数据库实例。不同 schema 仍属于同库事务；不同 database/cluster 不在当前契约中，也没有伪装成两阶段提交。

## Outbox

Outbox 内容和交易在同一 PostgreSQL 事务中写入。交易先取得 `topic + partition_key` 事务锁，数据库触发器禁止修改事件内容和排序时间、删除未发布事件或 TRUNCATE。worker 使用租约、`SKIP LOCKED`、指数退避和死信，把事件至少一次写入 `dbproxy:outbox:{topic}` Redis Stream；`XADD` 后必须获得本机 AOF 确认才标记 `published_at`。同一分区的前序未发布事件会阻塞后序，死信也不会被后序越过。

发布后 PostgreSQL ACK 丢失会产生重复 Stream entry，因此消费者必须按 `event_id` 去重。业务消费组、下游补偿和 Redis Stream 保留策略不在 DBProxy publisher 内隐式处理。

## 连接与并发

服务端按 RecordKey/operation ID 稳定路由到固定 `TieredSnapshotStore` 连接分片，每个分片有独立 PostgreSQL/Redis 连接和共享存储指标。所有连接分片仍指向同一组 PostgreSQL/Redis；它们与 PostgreSQL 的 32 个快照表分区、未来物理分库都不是同一概念。缓存修复与 Outbox 使用单独的 PostgreSQL 维护连接，不占住请求分片锁。

Redis 使用自动重连的 `ConnectionManager`。PostgreSQL 连接发现关闭后进行 2 秒有界重连；当前在途操作仍返回失败，下一次使用原幂等 ID 的调用才走新连接，避免底层擅自重放结果未知的写入。

## 数据库对象

迁移在 PostgreSQL advisory lock 下按顺序执行：

- `000_schema_migrations.sql`：记录已经提交的 schema 版本，避免每个连接重复执行 DDL；
- `001_snapshot.sql`：32 个 HASH 分区的权威快照父表、叶子表与快照幂等回执；
- `002_transactional.sql`：单记录事务回执；
- `003_multi_transactional.sql`：多记录事务头和记录回执；
- `004_cache_repair.sql`：持久缓存修复队列；
- `005_trade_outbox.sql`：交易、不可变账本和 Outbox；
- `006_operation_registry.sql`：跨事务类型的 operation ID 注册表；
- `007_hardening.sql`：旧库兼容字段和交易状态约束；
- `008_generic_commit.sql`：通用提交效果、不可变追加记录以及与交易无关的 Outbox；
- `009_outbox_relay.sql`：持久路由/Publisher 身份、入队序号、租约令牌与管理审计。新配置与兼容限制见 [Outbox Relay](outbox-relay.md)。

## 网络和 SDK

协议 v2 使用大端四字节长度前缀和 Protobuf，默认 frame 上限 8 MiB、单个应用 Payload/Result 上限 1 MiB。握手同时检查版本、proto SHA-256 指纹和共享令牌。Rust 客户端与 TypeScript SDK 都不会生成或替换幂等 ID；TypeScript 协议锁生成器直接读取 Rust `PROTOCOL_VERSION`，避免两处常量漂移。

## 当前安全边界

已经实现：有界 frame/payload、严格配置、共享令牌、低基数指标、CAS/幂等、全局 operation 类型、数据库不可变触发器、AOF ACK、worker 租约/退避/死信和故障演练。

仍属于部署或后续工作：TLS/mTLS、令牌轮换、租户隔离/配额、PostgreSQL/Redis 多副本高可用、Outbox 下游消费组、备份恢复和密钥系统。观测 HTTP 端口没有业务认证，只能绑定本机或运维内网。

当前只实现 `dbproxy_snapshots` 的库内 HASH 分区。历史数据归档、其他表分区和物理分库仍未实施；尤其没有把同库事务伪装成跨库事务。
