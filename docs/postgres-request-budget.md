# PG 请求排队与重连失败冷却

后续修复：服务端提交后的成功缓存 ACK 已移到现有维护 worker 的有界批量清理，详见[清理路径](cache-repair-cleanup.md)。下文请求分片 ACK 等待仍适用于独立同步 Store；服务端不再在该阶段等待请求分片。

日期：2026-09-07。基线提交 `c126b80`；缓存操作预算已经独立，本次继续处理请求分片的 PG 连接排队。

## 复现与实现

旧实现的请求入口先无限等待 `Arc<Mutex<ReconnectingPostgresClient>>`，再在锁内检查并重连。两个精确回归在修改前均失败：固定连接锁时，请求超过外层 900 ms 仍未返回；让 PG 拒绝重连时，4 个并发调用形成 4 次连接尝试。新实现让等待者在自己的排队期限内退出，并在同一连接上共享一次重连失败的冷却。

| 配置 | 默认值 | 覆盖范围 |
|---|---:|---|
| `storage.postgresConnectionWaitTimeoutMs` | 2,000 ms | 请求分片每次等待 PG 连接锁，含该分片的单条/批量缓存修复 ACK 排队 |
| `storage.postgresReconnectCooldownMs` | 500 ms | 同一请求连接重连失败或重连 future 被取消后的冷却 |
| 现有 PG 建连预算 | 2,000 ms | 拿到锁后的一次连接尝试，包括协议握手 |
| `storage.cacheFallbackTimeoutMs` | 2,000 ms | 原 PG 回源调用预算，仍可覆盖其内部排队、重连及读取 |
| `storage.cacheOperationTimeoutMs` | 200 ms | 原缓存单次操作预算 |

初始候选采用 500 ms 排队预算，但新机器单分片 16 个并发写入者的健康基线出现排队超时，失败日志保留。临时放宽到 30 秒仅用于测量该负载：64 次保存的 PG 排队均值 816.503 ms，p99 在 1–2 秒桶，PG 操作均在 25–250 ms 桶。因此最终默认排队预算设为 2 秒，重连失败冷却仍为 500 ms；该诊断不作为默认参数验收。

排队预算不是整个 RPC 的 SLA，也不直接照搬缓存的 200 ms。等待者会被更早拒绝；慢 SQL 的执行时间没有被缩短。新机器健康基线和完整故障演练应分别记录，不能与旧笔记本的 r13 宣称等负载百分比收益。

互斥锁仍保证同一连接至多一个重连。失败和取消通过 Drop 启动冷却；冷却中的请求返回可重试错误，不逐个重连。冷却到期后一个持锁调用尝试恢复；成功清除冷却并复用新连接，之后再次失败仍适用相同策略。没有后台重连任务，也没有额外连接池。

## 发送与取消边界

| 请求所处状态 | 本次行为与结果解释 |
|---|---|
| 等待连接，尚未发送 SQL | 排队超时取消锁等待者。该次 Store 操作没有发送 SQL，计入 `postgres_connection_wait`，不启动 `postgres_operation` |
| 持锁重连，尚未发送业务 SQL | 原 2 秒建连预算不变。重连失败/取消后共享冷却；不将取消解释成数据库写入 |
| SQL/事务已发送 | 排队预算不再参与执行。外层取消仍按驱动原语义处理；事务 Drop 排队回滚，不意味着服务端立刻停止 |
| PG 已提交、回执未收到 | 结果未知，必须使用原 request/operation ID 和原请求恢复；不得生成新 ID 或假定已回滚 |
| PG 已提交、缓存同步或修复 ACK 等待失败 | 保留已提交结果与持久化修复目标，交给已有 Worker 重试；不能提前清除修复记录 |

真实 PG 回归在服务器返回 `CommandComplete(COMMIT)` 后主动断开转发连接，确认回包丢失时客户端收到存储错误；之后查询原回执、原请求重试返回 Duplicate，revision、追加事实及 Outbox 均只产生一次。模拟端点只验证排队和连接次数，不能代替这项真实事务证明。

## 调用范围与兼容

- 请求范围包括单/批量快照、单/多记录事务、CommitRecords、旧 Trade 和对应回执查询。请求分片的修复 ACK 仅限制取得连接前的等待，ACK SQL 本身没有新增短超时。
- `PostgresSnapshotStore::connect` / `connect_existing` 保持原等待与重连行为；需要预算的直接调用方使用 `connect_with_request_config`。`TieredSnapshotStore` 为请求分片显式启用 `PostgresRequestConfig`。
- 服务端的独立维护连接继续使用原构造入口。CacheRepair/Outbox 的 claim、lease、fail、stats、管理查询及发布策略未改变。Backlog 落库和 CacheRepair 回源仍调用请求分片，因此会收到新的排队/冷却错误，并走原 release/retry/backoff 路径。
- 排队和冷却错误仍映射为协议已有的 `StorageUnavailable`；公开提示要求复用原幂等键。一次 RPC 可能包含多个 Store 操作，不能从其中一次未发送推断整个 RPC 从未提交。
- JSON 省略新字段时使用上述默认值；显式零值拒绝。Rust 配置拒绝零、亚毫秒和超出时钟表示范围的预算。Rust 直接构造 `TieredSnapshotStoreConfig` 时补 `postgres` 字段或使用 `..Default::default()`。带新字段的 JSON 必须与新二进制配套。
- Schema、所有 local/external JSON 模板及启动接线同步；网络协议、SQL 迁移、业务状态、可靠 Redis AOF 和 MQ 确认策略未改变。

## 指标与验证

继续使用固定的 `postgres_connection_wait`、`postgres_operation` 和 `cache_repair_ack`。排队取消/超时正常关闭阶段；重连冷却拒绝发生在取得锁后，因此可产生短的 `postgres_operation` 样本，但没有 SQL。修复 ACK 仍使用自己的阶段，不重复加入请求 PG 阶段。直方图计数不是成功数，父子阶段的 p99 不可相加减。

精准测试覆盖所有请求入口未发送即超时、过期等待者无残留、共享失败冷却、重连取消后恢复、SQL 执行不受排队预算影响、独立维护连接仍按原策略等待、配置默认/覆盖/非法值和协议错误映射。

真实 PG 测试位于 `crates/dbproxy-storage/tests/support/postgres_request.rs`：用服务端 `pg_stat_activity` 确认在途 SQL 被锁阻塞后，再使其他调用排队超时；分别验证原事务继续提交、主动取消后回滚及连接复用；另验证 COMMIT 回包丢失。短时健康并发用例使用同一请求分片的 16 个写入者、共 64 次保存，打印排队均值与 p99 桶上界，不作为容量基准。

专用数据库执行入口：

```text
cargo test --workspace --locked -j1
cargo clippy --workspace --all-targets --locked -j1 -- -D warnings
cargo fmt --all -- --check
cargo test -p tiangz-dbproxy-storage --test postgres_redis --locked -j1 -- --ignored --test-threads=1
cargo test -p tiangz-dbproxy-storage --test outbox_concurrency --locked -j1 -- --ignored --test-threads=1
cargo test -p tiangz-dbproxy-storage --test outbox_relay --locked -j1 -- --ignored --test-threads=1
cargo test -p tiangz-dbproxy-storage --test fault_matrix --locked -j1 -- --ignored --test-threads=1
cargo test -p tiangz-dbproxy-server --test postgres_redis_network --locked -j1 -- --ignored --test-threads=1
```

真实测试必须配置专用的 `DBPROXY_POSTGRES_URL`、`DBPROXY_REDIS_URL`、`DBPROXY_CACHE_REDIS_URL`；并发与请求预算测试另要求 `DBPROXY_TEST_POSTGRES_URL` 和 `DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1`。Docker 故障矩阵需显式 `DBPROXY_RUN_DOCKER_FAULTS=1`，仅能指向本机演练容器。默认 ignored 不算通过。

上述真实套件必须逐个执行，并在套件之间使用独立的专用库/Redis 状态，或按既有控制器的允许列表重建已确认可销毁的契约库、清空演练 Redis。复用上轮已注册 Relay 路由的数据库，会使无对应发布器的网络套件正确地拒绝启动。本轮曾因此复跑失败，日志保留；这不是关闭路由校验的理由。新 PG 回归通过 `postgres_redis.rs` 引入，已纳入既有控制器的套件清单；CI 同步了专用库迁移的显式环境开关。

## 实现提交前的本机验证记录

以下为实现提交前的检查；提交后的完整构建与启动失败见[验收记录](pg-queue-acceptance-2026-09-07.md)。

最终默认参数为排队 2,000 ms、重连失败冷却 500 ms。默认 workspace 测试 136 项通过、31 项 ignored；另按套件隔离状态显式运行全部 31 项真实测试：存储 16、Outbox 并发 6、Relay 1、基础设施故障矩阵 7、网络闭环 1，全部通过。全目标严格 Clippy 与格式检查通过。新增真实回归已经合入既有存储套件，未把 ignored 计作通过。

`cargo build --workspace --release --locked -j1` 通过，日志为同目录 `release-final.log`。双仓库 diff 检查、TiangZ 本机痕迹门禁通过；本轮未改变生成输入，未执行 codegen。TiangZ 仅同步三份文档，其 release、正常 Model/Hotfix、探针重建及运行时门禁尚未执行。

最终参数下，同一分片 16 个并发写入者的 64 次保存连续两轮全部成功，PG 排队均值分别为 908.797 ms 和 892.183 ms，p99 均落在 (1,000, 2,000] ms 桶。这是本机短时健康验证，不能推导完整游戏负载的容量或百分比收益。

原始日志保存在工作区 `.build-tmp/pg-queue-20260907/`：`workspace-final.log`、`clippy-final.log`、`fmt-final.log` 及五份 `isolated-*-3.log` 为最终检查；`healthy-baseline-2.log` 为最终参数的另一轮健康验证。保留 `healthy-baseline-1.log` 的 500 ms 候选失败、`healthy-diagnostic-30s-budget.log` 的临时诊断，以及 `real-contracts-2.log` 的套件状态污染失败，不将这些记录归入最终通过结果。数据库故障测试结束后，三个演练容器均恢复健康。

提交后已启动完整 100 游戏玩家 + 100 持久化探针的 30 分钟验收计划，但在启动健康检查失败，未进入计时负载和故障注入；详见[提交后验收失败记录](pg-queue-acceptance-2026-09-07.md)。当前精确回归不替代游戏尾延迟、最终全链路对账或七天长稳验收。
