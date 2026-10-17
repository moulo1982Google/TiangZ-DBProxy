# 提交后缓存修复清理移出请求路径

## 已证实的阻塞与修复

2026-09-07 的受控回归锁住 PG 连接，只让 Redis 缓存更新成功。旧实现仍等待修复行 DELETE，200 ms 的外层观察窗口耗尽。该路径会占住 SDK 串行连接，放大后续请求排队；它证明一个可修复的阻塞点，不证明此前所有启动超时都只有这一个根因。

服务端现在在业务事务提交并完成原有缓存同步尝试后返回，不再等待清理 DELETE。所有请求分片共享最多 1,024 个 key/revision 提示，同一 key 合并为最高已缓存 revision，不保存 payload，不为每个请求创建任务或连接。

现有 CacheRepair worker 每轮使用原维护连接最多清理 128 个提示，再处理一条持久修复，避免持续清理流量饿死恢复。使用同一条版本条件 DELETE，只有 `target_revision <= cached_revision` 才能删除。每批最多一次 PG 提交，减少请求路径和逐条清理的同步写成本。原有连接数、排队预算、SQL 执行语义、RPC 超时及持久化策略不变。

## 正确性和生命周期

- PG 快照、回执和修复目标仍在原事务内提交；成功含义未改成内存接收或缓存接收。
- Redis 写失败或超时不生成清理提示，持久修复目标保留。
- 清理提示满载时放弃新增提示；清理 SQL 失败、取消或进程退出时，不依赖重试内存提示，原有持久修复 worker 重建缓存并确认。
- 新 revision 到来后，迟到的旧提示不能删除新目标；提示不能代替租约，也不改变原 worker 的目标/owner 校验。
- 关闭时不要求排空这些非持久提示；未完成的清理交由持久修复恢复。不会增加未受管理的后台任务。
- 独立 `TieredSnapshotStore` 默认保留同步清理；服务端显式接入共享提示。直接调用 `defer_cache_acknowledgements` 的宿主负责驱动 `flush` 或持久修复。

## 观测与验证

服务端的 `committed_cache_sync` 现在包含缓存尝试和本地提示登记；`cache_repair_ack` 记录后台批量清理的 PG 等待/重连/SQL，不再是前者的子阶段。独立同步 Store 的原指标关系不变。两种模式、旧新版本及前后台窗口不能混算成单请求时延。

回归覆盖受控 PG 阻塞不再延迟已缓存结果、缓存失败不生成提示、有界容量/批量与 revision 合并。真实数据库测试覆盖旧提示保护新目标、丢失提示后恢复及原 ID 重试幂等。完整游戏启动及 30 分钟验收结果待本轮执行后补充；不能将精确回归写成全链路验收通过。

提交前已通过 139 项默认测试（32 项真实测试另行 ignored）、全目标严格 Clippy、fmt，以及新增真实 PG/Redis 恢复测试。第一次并行测试暴露 Windows 时间戳重名导致配置临时文件相互覆盖，命名加入进程 ID 和原子序号后全量复跑通过。失败和成功日志均保留在工作区 `.build-tmp/repair-ack-20260907/`。

## 提交后首轮：启动通过，18 分钟健康阶段失败

候选 `24d66b7` / TiangZ `2597c8f`，证据 `20260907-cleanup-30m-r1`。release、139 项默认测试、32 项显式真实测试及 7 项控制器测试通过。100 玩家启动的探针/业务/移动错误均为 0，业务 p99 约 616/640 ms；100 持久化探针就绪，缓存、PG、可靠 Redis 三次故障均完成两轮原账号恢复及交易复核。

北京时间 18:46:13 开始计划计时，19:04:24 的健康阶段地图 100 出现 1 次探针、2 次移动 RPC 超时，控制器严格判失败并清理。PG 恢复曾有 6 次离线保存等待重试，Redis 故障窗口有一张地图探针失败。末次分钟采样累计读取 84,337 次，旧读/缺失/不变量错误均 0；入队错误 236、事务错误 869、交易错误 49 均保留。没有完成 30 分钟、AOF 注入及最终对账，不能宣称验收通过。

提交后缓存同步已降至约 2 ms，下一项定位到同库批次分片提交放大，见[批次提交](snapshot-batch-commit.md)。本轮没有迁移 Docker 数据或放宽检查。

## 最终验收状态

r6 在 NVMe PG 路径下完整 30 分钟、五次故障及最终对账通过；机械盘候选的失败保留，缓存清理代码的精确回归与整体验收结果分别说明，见[最终报告](storage-path-acceptance-2026-09-07.md)。

## 批量清理与请求事务的死锁（2026-10-17 修复）

### 现象

v0.7.4、`postgresRedis` 后端、2 个请求分片、1000 客户端 16 小时长稳（2026-10-16，下游项目的 220 长稳）中，DBProxy 日志 10 次 `ERROR durable queue worker failed error=postgres error: ERROR 40P01: deadlock detected ... queue="cache repair"`。同一时刻 PostgreSQL 日志的 10 次死锁，受害方都是维护连接上的批量清理 `DELETE FROM dbproxy_cache_repairs ... USING unnest(...)`，另一方都是请求连接上的修复行 upsert `INSERT INTO dbproxy_cache_repairs ... ON CONFLICT ... DO UPDATE`。DBProxy 的连接诊断显示请求连接当时执行的是 2–3 条记录的 `commit_records`，持有约 1 秒（等于 `deadlock_timeout`）。结束时 `dbproxy_cache_repairs` 为 0 行，没有丢失修复；工作线程按 `baseRetryDelayMs` 重试。

### 根因

- 请求一侧：`commit_records`、交易、`save_batch` 在同一个事务里逐条调用 `enqueue_in_transaction`，每条记录一次 `INSERT ... ON CONFLICT DO UPDATE`，修复行的行锁一直持有到事务提交。`commit_records` 与交易按 Rust 字节序排序记录后写；`save_batch` 按调用方给的顺序写，没有统一顺序。
- 清理一侧：`acknowledge_cached_revisions`（服务端后台提示刷新和 `acknowledge_cached_multi` 共用）原来是一条 DELETE，按执行计划的顺序（unnest 数组顺序或表的堆顺序；提示来自 HashMap，顺序随机）逐行删，遇到被锁的行就等待。
- 两边对同一批行的加锁顺序不一致：请求事务已写 A、接着写 B，清理语句已删 B、接着等 A，各持一行再等对方即成环。PostgreSQL 回滚先进入等待的一方，长稳里都是清理语句。
- 只在清理一侧按键排序不够：`save_batch` 没有统一顺序；而且 SQL 的 `ORDER BY` 默认按数据库排序规则，和 Rust 的字节序也不一定一致。

### 修复

- 批量清理改为两步写在一条语句里：先 `SELECT ... FOR UPDATE OF repair SKIP LOCKED` 锁住与提示匹配、`target_revision <= 已缓存版本` 且没有被别人锁住的修复行，再按键删除这些行并再次核对版本条件。
- 清理语句不再等待任何行锁，就不会进入等待环，与请求事务按什么顺序写无关。被请求事务锁住的行被跳过：那条请求往往正在写更新的目标，即便不是，修复行仍是持久目标，原 CacheRepair worker 照常重建缓存后确认，代价最多是一次多余的 Redis 写。提示本来就是尽力而为，语义不变。
- 单条清理 `acknowledge_cached` 一条语句只锁一行，不可能与别人成环，保持原样（仍会等待并在等待后复核版本）。
- 持久队列工作线程（缓存修复与 Outbox 共用）遇到 PostgreSQL 死锁 40P01 或序列化冲突 40001 时改记 WARN（`durable queue worker hit a retryable PostgreSQL conflict; retrying`），其他错误仍记 ERROR。指标仍记一次 `failure`，重试间隔不变。判断依据是新增的公开方法 `StorageError::is_retryable_conflict()`。
- 不改协议、SDK、存储结构与迁移。

### 验证

- 新增真实 PostgreSQL 用例 `batch_cleanup_never_deadlocks_with_a_request_writing_several_repair_rows`：请求事务先写一行并保持事务，清理批次同时包含这一行和请求稍后要写的另一行，两个方向各跑一次。修复前连续 5 次都以 `40P01: deadlock detected` 失败；修复后连续 5 次通过，清理只删除未被持有的那一行，事务提交后两行都保留新目标。
- 新增 `real_deadlock_and_serialization_failures_are_retryable_conflicts`：用两个连接制造真实的 40P01 与 40001，确认判为可重试；除零错误和非数据库错误不判。
- `cache_repair_concurrency` 全部 10 项、`postgres_redis` 22 项、`postgres_redis_network` 2 项真实库用例通过；默认测试、fmt、全目标严格 Clippy 通过。
- 未在 220 长稳上复验；修复后长稳是否完全不再出现 40P01 待下一轮长稳确认。
