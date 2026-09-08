# TiangZ DBProxy

本地集成分支新增 `CommitRecords`：多记录 CAS、不可变追加事实和 Outbox 同一事务提交，不要求交易状态或账本规则。旧接口保持兼容，旧 Trade API 暂留兼容入口。实施状态与发布前验证见[通用持久化计划](docs/generic-persistence-plan.md)；当前远程七天演练不使用这些修改。

TiangZ DBProxy 是独立的 Rust 持久化服务项目。

它负责通用的：

- 玩家、Item、任务等快照的读取与写入
- Revision 和 Compare-And-Swap 版本校验
- 重试幂等与请求去重
- Redis 缓存、PostgreSQL/MySQL/MongoDB 等存储适配
- 监控、故障恢复和部署协议

DBProxy 不依赖 TiangZ Runtime，也不包含任何游戏玩法。TiangZ 只向它提供稳定的快照 Payload、Schema 和 Repository 适配逻辑。

## 当前状态

`v0.6.0` 是当前工作版本。它在已有快照、关键事务、多 Endpoint 和跨记录原子事务之上，增加持久缓存修复队列、AOF 确认与重连、交易托管状态机、不可变账本、PostgreSQL Outbox 和权威快照 HASH 分区：

- `RecordKey`：`namespace + key`
- `Revision`：由 DBProxy 生成的单调版本号
- `SnapshotWrite`：带 `expected_revision` 的条件写入
- `request_id`：重试时必须保持不变的幂等键
- `InMemorySnapshotStore`：只用于测试，不保证重启恢复
- `TransactionalWrite`：带 `operation_id`、期望版本、完整快照和持久化操作结果
- `TransactionReceipt`：按`operation_id + RecordKey`读取第一次提交保存的Revision和业务结果
- `MultiRecordTransactionalWrite`：在一个 PostgreSQL 事务中原子提交多条完整记录，适合跨玩家交易、奖励转移等业务
- `MultiRecordTransactionReceipt`：按`operation_id + 多个RecordKey`恢复整组提交结果；重复提交返回同一结果
- `InMemoryTransactionalStore`：验证事务提交、CAS 冲突和原始结果重试语义
- `PostgresSnapshotStore`：PostgreSQL 权威快照、CAS、幂等写入和关键事务收据
- `dbproxy_snapshots`：按完整 RecordKey 路由到 32 个 PostgreSQL HASH 叶子分区，DBProxy 始终访问逻辑父表
- `RedisSnapshotCache`：只缓存 PostgreSQL 已提交的快照
- `TieredSnapshotStore`：先提交 PostgreSQL 和 durable repair，再尽力刷新 Redis；缓存失败不改变权威提交结果
- `PostgresCacheRepairQueue`：与权威写入同事务提交的缓存修复目标，支持租约、指数退避、死信和定点重放
- `TieredSnapshotStore::repair_cache`：从 PostgreSQL 重建缓存，或删除数据库中已不存在的旧缓存
- `SnapshotFlushQueue`：按 `RecordKey` 合并普通快照，只保留最新值；关键事务不进入该队列
- `SnapshotFlushQueue::flush` 与 `flush_until_empty`：限制每轮写入量和最大轮数，失败保留请求并返回剩余积压
- `RedisSnapshotBacklog`：把尚未落 PostgreSQL 的普通快照保存到独立 Redis backlog，支持 lease、ACK、释放、续租和过期回收
- `TradeTransaction`：原子提交交易状态、多记录CAS、不可变平衡账本、Outbox和完整回执
- `PostgresOutboxQueue`：租约/重试/死信 worker，把交易事件至少一次发布到 AOF 确认的 Redis Stream
- `dbproxy-protocol`：v2 Protobuf、协议指纹、8 MiB frame 和 1 MiB 应用 Payload 默认上限
- `dbproxy-server`：内部令牌握手、按 RecordKey 分片的真实存储连接，以及 backlog/cache-repair/outbox worker
- `MemoryBackend`：保留Revision、CAS、幂等和多记录原子语义的易失后端，用于隔离网络/协议/调度成本；不会连接PostgreSQL或Redis
- `dbproxy-client`：Rust 异步客户端及多连接池；TiangZ 不需要引用存储 crate
- `@tiangz/dbproxy-sdk`：TypeScript稳定类型、参数校验、防御性Payload复制、多记录事务和可插拔Transport；不绑定Node、Deno或TiangZ
- `fault_matrix.ps1`：在笔记本限额容器中显式停止/恢复 Redis/PostgreSQL，验证 AOF、积压、自动重连和缓存修复边界
- `network_smoke.ps1`：验证 Rust SDK -> TCP -> DBProxy -> Redis/PostgreSQL 完整闭环

TiangZ主仓库已经提供首个Player Snapshot Repository和Rust Host Transport适配；这些领域Payload与恢复逻辑不属于本仓库。交易 API 只提供通用状态/CAS/账本/Outbox 原子边界，所有权、价格、余额和风控仍由主工程的领域 Repository 决定。架构、演练、分区和审视结果分别见[架构说明](docs/architecture.md)、[恢复手册](docs/durability-recovery-runbook.md)、[两小时故障演练报告](docs/fault-soak-report-2026-08-27.md)、[PostgreSQL 快照分区](docs/postgresql-partitioning.md)、[交易安全说明](docs/trade-safety-and-outbox.md)和[代码审视记录](docs/dbproxy-code-review.md)。

## 启动配置

缓存与 PG 回源的等待预算现分离：`storage.cacheOperationTimeoutMs` 默认 200 ms，`cacheFallbackTimeoutMs` 仍默认 2,000 ms。升级兼容、正确性边界与测试见[缓存操作预算](docs/cache-operation-budget.md)。可靠 Redis AOF/MQ 确认不使用该缓存预算。

PG 请求分片现使用独立的连接排队预算与重连失败冷却：`storage.postgresConnectionWaitTimeoutMs` 默认 2,000 ms，`storage.postgresReconnectCooldownMs` 默认 500 ms。只限制取得连接前的等待，不缩短已发送 SQL/事务的执行时间；提交后修复 ACK 排队失败保留修复目标。范围、兼容和验证见[PG 请求预算](docs/postgres-request-budget.md)，后续演练安排见[交接记录](docs/handoff-2026-09-07.md)。

通用 Outbox Relay 的 Redis 路由、禁用的未来 MQ 声明、离线检查和审计管理命令见[Outbox Relay](docs/outbox-relay.md)与[配置示例](configs/outbox-relay.example.json)。旧配置及旧 Stream 地址保持兼容；新来源不能在所有 worker 升级前启用。

DBProxy使用带`configVersion: 1`的严格JSON保存普通启动参数，默认读取`configs/local.json`，并由`configs/dbproxy.schema.json`提供编辑器提示。`runtime.workerThreads`可以固定Tokio Runtime工作线程数，省略时沿用Tokio按逻辑CPU选择的行为；它与只负责Redis积压消费的`backlog.workers`不是同一个参数。连接串和认证令牌不能写进JSON；配置文件只记录环境变量名，由部署环境注入实际密钥：

```powershell
cargo run -p tiangz-dbproxy-server -- --config configs/local.json
```

未知字段、零worker、零lease、空密钥变量会在建立网络连接前直接报错。每个 DBProxy 实例只配置一个监听地址；部署两个实例时使用两份 JSON，二者共享同一 PostgreSQL 和 Redis 服务。`redisUrlEnv`承载必须保留 AOF 的 backlog/Outbox；可选的`cacheRedisUrlEnv`把快照缓存放到独立、无持久化的 Redis。省略后者时继续复用`redisUrlEnv`，兼容单 Redis 开发环境。多 Endpoint 写在业务客户端配置中，而不是 DBProxy 服务端配置中：第一个地址是首选，后续地址是故障切换候选。

存储后端必须显式选择。正式和恢复测试使用`postgresRedis`；`memory`只用于本地开发与性能隔离，进程退出后数据全部丢失，并且`EnqueueSnapshot`会直接写入内存权威快照，不模拟Redis AOF与异步刷盘：

```json
{
  "runtime": { "workerThreads": 4 },
  "observability": { "listenAddr": "127.0.0.1:9090" },
  "storage": { "backend": "memory", "shards": 16 }
}
```

配置`observability.listenAddr`后，DBProxy在独立HTTP端口提供`/live`、`/ready`、`/dependencies`和Prometheus格式的`/metrics`。真实存储模式只有在 PostgreSQL 与可靠队列 Redis 都可达时才 Ready；独立快照缓存不可达时安全回源 PostgreSQL，并由缓存读写错误与回源指标告警，不把可降级缓存误判为持久依赖。本地Compose会启动Prometheus与Grafana并自动加载Dashboard；指标、告警和部署边界见[可观测性指南](OBSERVABILITY.md)。观测端口不要求业务认证，因此只能绑定本机或运维内网，禁止经Nginx暴露公网。

仓库提供`configs/perf-memory-4.json`，固定使用4个Runtime工作线程和MemoryStub。该配置只测DBProxy自身的网络、协议、调度、分片锁和事务语义，不把PostgreSQL或Redis性能混入结果。

```text
DBProxy-1: 127.0.0.1:7800 ─┐
                           ├─ 同一 PostgreSQL + 可靠队列 Redis
DBProxy-2: 127.0.0.1:7801 ─┘                    + 可选易失缓存 Redis
客户端: [7800, 7801]
```

两个实例是无状态对等节点，不需要互相同步或 Leader 选举。客户端切换地址时必须复用原 `requestId`/`operationId`，因此“提交成功但响应丢失”不会重复发奖或重复扣物品。

## 开发

当前处于持续开发阶段，`Cargo.toml`、`Cargo.lock`和工作区版本号不作为冻结契约；日常修改依赖时允许Cargo重新解析，CI也不使用`--locked`。准备发布正式Tag时，再统一执行锁文件、版本、协议指纹和完整测试审查。

```powershell
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
npm run test:typescript
```

GitHub Actions 的普通分支和 Pull Request 只运行开发门禁；推送 `v*` Tag 或发布对应的 GitHub Release 时会自动进入发布验收门，使用 `npm ci`、`cargo ... --locked`，并启动 PostgreSQL/Redis 完成真实存储、网络闭环和故障矩阵测试。发布 Tag 只有在这组测试全部通过后才算验收完成。

本机启动 PostgreSQL 和 Redis：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml up -d
$env:DBPROXY_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz"
$env:DBPROXY_REDIS_URL = "redis://:tiangz_dev@127.0.0.1:6379/15"
$env:DBPROXY_TEST_POSTGRES_URL = $env:DBPROXY_POSTGRES_URL
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION = "1"
$env:DBPROXY_CACHE_REDIS_URL = $env:DBPROXY_REDIS_URL
cargo test -p tiangz-dbproxy-storage --test postgres_redis -- --ignored --nocapture --test-threads=1
```

运行这组直接存储集成测试前先停止本机 DBProxy。测试会主动认领 backlog、缓存修复和 outbox 任务；若业务 worker 同时运行，会消费测试刚写入的任务并造成竞争性假失败。database 15 用于隔离测试数据，执行前仍应确认其中没有需要保留的数据。

缓存修复并发回归使用独立、可丢弃且无业务 worker 的 PostgreSQL 数据库，设置 `DBPROXY_TEST_POSTGRES_URL` 和 `DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1` 后执行：

```powershell
cargo test -p tiangz-dbproxy-storage --test cache_repair_concurrency -- --ignored --nocapture --test-threads=1
```

该组验证目标合并、实际修复 revision/缺失记录的 ACK、排队顺序、退避/死信保留、过期租约、同名 worker 和删除后重新入队隔离。它不在默认 `cargo test --workspace` 的通过数量中；真实 Redis 缓存读写还需执行上面的 `postgres_redis` 测试。迁移 010 的受控切换要求见[恢复手册](docs/durability-recovery-runbook.md)。

运行故障矩阵。该命令会短暂停止并恢复本机 PostgreSQL/Redis 容器，但不会删除数据卷：

```powershell
powershell -ExecutionPolicy Bypass -File tools/fault_matrix.ps1
```

DBProxy 已启动后，用 100 个模拟玩家执行两小时真实 TCP 故障演练（会按时间表停止、强杀并恢复本机 PostgreSQL/Redis 容器）：

```powershell
cargo build --release --bin dbproxy_fault_soak
powershell -ExecutionPolicy Bypass -File tools/run_fault_soak.ps1
```

正式运行前可用 `-DurationSeconds 120` 做同比压缩预演。完整阶段、断言和结果文件说明见[持久化与故障恢复手册](docs/durability-recovery-runbook.md)，2026-08-27 的 100 玩家正式结果见[两小时故障演练报告](docs/fault-soak-report-2026-08-27.md)。

启动本机网络服务：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_local.ps1
```

默认监听`127.0.0.1:7800`。运行真实网络闭环：

```powershell
powershell -ExecutionPolicy Bypass -File tools/network_smoke.ps1
```

## 业务持久化性能

当前4-worker MemoryBackend基线中，100并发拾取事务达到约3.53万次/秒，NPC商店事务约3.84万次/秒；30个领域使用`LoadMultiSnapshot`后，玩家恢复吞吐相对逐条读取提高约12.23倍。完整环境、延迟、并发曲线、结果边界和复现命令见[性能基线](PERFORMANCE.md)。

`dbproxy_business_load`直接经过Rust客户端、TCP协议和DBProxy后端，使用Starter当前的持久化形状：

- `playerDataSingle`：每个玩家领域各发一个`LoadSnapshot`，作为批量读取的对照组。
- `playerDataBatch`：用一个`LoadMultiSnapshot`读取全部玩家领域。
- `playerSaveSingle`：按旧周期Flush路径为每个领域依次发送一个`SaveSnapshot`。
- `playerSaveBatch`：用一个`SaveMultiSnapshot`保存全部玩家领域并逐条推进Revision。
- `pickup`：原子提交`inventory + quest + wallet`三条记录及拾取回执。
- `npcShop`：原子提交`inventory + wallet`两条记录及买卖回执。

它刻意不运行战斗、AOI、怪物死亡或NPC距离检查，因此测到的是DBProxy链路，不是整服玩法吞吐。Payload大小会写入结果，所有写操作都持续推进真实Revision，任何冲突都会计为失败并停止对应虚拟玩家。

```powershell
cargo build --release -p tiangz-dbproxy-server --bin tiangz-dbproxy-server
cargo build --release --bin dbproxy_business_load
$env:DBPROXY_AUTH_TOKEN = "local-perf-token-1234"
./target/release/tiangz-dbproxy-server --config configs/perf-memory-4.json

# 在另一个终端运行。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 32 --players 100 --duration 30

# 模拟30个玩家持久化领域，比较逐条Load和LoadMulti。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 64 --players 100 --duration 30 --domain-count 30 --workloads playerDataSingle,playerDataBatch

# 比较5/10/30领域的逐条Save和SaveMulti；正式结论至少运行三轮。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 32 --players 100 --duration 30 --domain-count 30 --workloads playerSaveSingle,playerSaveBatch
```

正式测试必须保持服务端`runtime.workerThreads`、`storage.shards`、客户端连接池、玩家数、时长和机器环境一致。每种工作负载使用全新DBProxy进程，至少运行三轮，报告`ops/s、p50/p95/p99、失败数、DBProxy CPU/RSS`。该结果是DBProxy自身的性能上界，不代表PostgreSQL容量，也不用于评价数据库选型。

本机开发账号只绑定回环地址：PostgreSQL 用户和数据库都是 `tiangz`，密码是 `tiangz_dev`；Redis 密码也是 `tiangz_dev`。这些凭据只适用于本地开发，不能复制到线上。

## 设计原则

1. DBProxy 只理解记录地址、Schema、Revision 和二进制 Payload，不理解游戏业务字段。
2. 快照写入必须支持重试，重试不能导致重复扣物品、重复发奖励或重复保存。
3. Redis 不是最终一致性的替代品。缓存和持久库的责任、故障恢复顺序必须由适配器明确实现。
4. 普通快照、关键事务和至少一次事件投递使用不同 ACK；同库多记录/交易事务不能被普通批量写入替代。
5. TiangZ 的主工程不直接依赖 DBProxy 的内部模块，只依赖版本化协议或客户端 SDK。
6. 普通Entity可以由TiangZ的`.native`生成版本化Codec和通用Repository；DBProxy仍只维护固定通用表。复杂查询、二级索引和跨玩家事务必须使用专门的领域存储设计。

## TypeScript SDK

SDK以`DbProxyTransport`隔离宿主I/O。业务或框架适配层创建`DbProxyClient`后，只调用版本化SDK；SDK不会生成幂等ID，也不会在失败后偷偷换ID重试。玩家由多个持久化领域组成时，应使用`LoadMulti`一次恢复最多64条快照，避免为每个领域单独产生网络往返。

```ts
import { DbProxyClient, type DbProxyTransport } from "@tiangz/dbproxy-sdk";

const transport: DbProxyTransport = createHostTransport();
const client = new DbProxyClient(transport);
const snapshot = await client.Load({ namespace: "player", key: "1001" });
```

协议版本和SHA-256指纹由`tools/generate_typescript_protocol_lock.mjs`从权威`dbproxy.proto`生成。修改协议后必须同时运行Rust测试和`npm run test:typescript`，禁止手工维护两份指纹。

## 网络边界

当前 v2 协议提供十三类 RPC：

```text
LoadSnapshot       读取已提交权威快照
LoadMultiSnapshot  按请求顺序批量读取最多64条权威快照，缺失记录保留空位
SaveSnapshot       同步写 PostgreSQL，再刷新 Redis；成功才表示本次提交完成
SaveMultiSnapshot  最多64条普通快照按shard并行，逐记录返回revision或错误，不提供原子性
EnqueueSnapshot    写入 Redis AOF backlog；成功只表示已可靠接收，不表示 PostgreSQL 已落库
EnqueueMultiSnapshot 最多64条普通快照一次写入Redis backlog，禁止携带expectedRevision
ApplyTransaction   提交单记录关键事务并保存原始业务结果
LoadTransaction    按operationId与RecordKey读取已提交事务回执
ApplyMultiTransaction  在一个 PostgreSQL 事务中原子提交多条记录
LoadMultiTransaction   按operationId和记录集合读取跨记录事务回执
ApplyTradeTransaction  原子提交交易状态、多记录、账本、Outbox和回执
LoadTrade              读取交易当前版本、状态和不透明Payload
LoadTradeTransaction   按operationId和tradeId读取已提交交易回执
```

每条连接先校验`protocol_version + protocol_fingerprint + auth_token`，之后才允许 RPC。帧使用大端四字节长度前缀，默认上限 8 MiB。客户端连接内按顺序执行请求；`DbProxyClientPool::connect`保持读写共享连接的兼容行为，`connect_split`可把读写分到独立连接组，避免慢写和 AOF ACK 阻塞读连接。两种模式都按`RecordKey`或 operation ID 稳定路由。服务端存储连接也按相同原则分片，避免所有玩家共享一个事务锁。

详细错误码、ACK语义、Endpoint故障切换和跨记录限制见[网络协议说明](docs/network-protocol.md)。

`SnapshotFlushQueue`是 DBProxy 进程内的协调器；`RedisSnapshotBacklog`是独立的 Redis AOF 持久积压区。前者随进程消失，后者只有在入队脚本后通过 `WAITAOF` 才返回成功，DBProxy/Redis 重启后可以重新领取。两者都只适合等级、任务进度、角色位置等允许小范围回退的数据；关键经济事务必须走 PostgreSQL 事务。AOF 和本地数据卷不等于 Redis 多副本高可用。

## 许可证

Apache-2.0，Copyright 2025-2026 郑昕。
