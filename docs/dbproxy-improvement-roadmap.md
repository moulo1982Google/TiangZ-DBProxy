# DBProxy 改进路线图与完成状态

## 当前本地工作：通用持久化边界

已实现 CommitRecords、追加事实、通用 Outbox、完整效果幂等校验和 TiangZ 在线交易适配；远程七天演练仍使用原版本。旧 Trade API 为兼容暂留，真实存储验收和正式依赖发布尚待完成。以下历史“交易安全已完成”不代表其领域状态机属于通用内核。新计划与 WoW335 影响见[通用持久化计划](generic-persistence-plan.md)。

本文记录本轮改进的实际状态。顺序仍是先保证写入和恢复语义，再逐步增加容量能力；当前已实现快照表库内分区，历史归档、其他表分区和物理分库继续延期。

## 已完成：输入与缓存护栏

- [x] `server.maxPayloadBytes` 默认 1 MiB，独立于 8 MiB frame 上限。
- [x] 批量数量、文本长度、Payload/Result/metadata 和交易事件数量均有协议入口限制。
- [x] 缓存 TTL + 稳定抖动、负缓存、stale-while-revalidate。
- [x] miss singleflight、回源并发闸门/超时/熔断、跨实例 Redis 租约锁和二次检查。
- [x] revision-aware Redis 写入，旧快照不能覆盖新版本。

## 已完成：可观测、可重试的持久缓存修复

- [x] 每次 PostgreSQL 权威写入在同一事务 upsert `dbproxy_cache_repairs`。
- [x] PostgreSQL 提交后 Redis 快速回写失败不再制造“数据库已提交但客户端收到模糊失败”。
- [x] `FOR UPDATE SKIP LOCKED` 领取、PostgreSQL 时钟租约、指数退避、最大次数和死信。
- [x] 按 RecordKey/最高 target Revision 合并；旧 lease 不能 ACK 新目标。
- [x] pending/processing/dead-letter/oldest-age 和固定结果指标、Grafana panel 与告警。
- [x] 定点 `requeue_dead_letter(record)`，避免未经检查的批量死信清除。

## 已完成：AOF/backlog 故障恢复

- [x] `EnqueueSnapshot`/批量入队后执行 `WAITAOF`，AOF 未确认时拒绝可靠 ACK。
- [x] Redis backlog、缓存和 Outbox 连接自动重连。
- [x] Redis 命令响应窗口覆盖 `WAITAOF` 判定窗口，避免客户端先于服务端 AOF 超时。
- [x] PostgreSQL 断线后有界重连；结果未知的当前请求不自动重放。
- [x] 笔记本故障矩阵只启动限额 PostgreSQL/Redis，并强制 AOF。
- [x] 演练覆盖 Redis 重启后 AOF 数据恢复、PostgreSQL 停机积压与恢复排空、lease/release、旧 ACK 不删除新快照、缓存故障后的 durable repair。
- [x] 严格恢复部署把 AOF backlog/Outbox 与易失快照缓存拆到两个 Redis；缓存重启为空并回源 PostgreSQL，避免 AOF 恢复旧 freshness 产生短暂旧读。
- [x] 正确性 soak 与最终日志审计均将任何低于已确认 Revision 的读取作为失败，而不只检查最终收敛。

演练命令和故障处理见[持久化与故障恢复手册](durability-recovery-runbook.md)。

## 已完成：交易安全

- [x] `Proposed -> Escrowed -> Settled` / `Cancelled` 有限状态机和独立交易版本。
- [x] 交易状态、多玩家/领域快照 CAS、账本、Outbox、缓存修复目标和 Receipt 同一 PostgreSQL 事务提交。
- [x] 不可变账本 Posting，按 asset 校验零和，数据库触发器拒绝 UPDATE/DELETE/TRUNCATE。
- [x] 全局 operation registry，禁止同一 ID 跨 single/multi/trade 复用。
- [x] 完整幂等内容比较；篡改记录、状态、Posting、Event 或 result 的重试均拒绝。
- [x] 多记录最终写入再次携带 expected Revision，封闭首次创建记录的跨 API 并发窗口。
- [x] Rust Core/客户端、协议 v2、服务端、MemoryBackend 和 TypeScript SDK 全链路支持。

业务层仍负责所有权、余额、价格、资格和玩家交易锁；详见[交易安全、托管状态机与 Outbox](trade-safety-and-outbox.md)。

## 已完成：PostgreSQL Outbox worker

- [x] 交易事件与权威状态同事务写入 PostgreSQL。
- [x] 事件内容和数据库产生时间不可变；未发布事件不可删除，投递状态可更新。
- [x] worker 租约、`SKIP LOCKED`、指数退避、死信和定点重放。
- [x] 写入事务持有分区锁，领取查询阻塞前序未发布/死信，保持 `topic + partition_key` 顺序。
- [x] 发布到按 topic 划分的 Redis Stream，并在 PostgreSQL ACK 前等待 Redis AOF。
- [x] 明确至少一次语义，消费者必须按 `event_id` 去重。
- [x] pending/processing/dead-letter/oldest-age、worker result 指标和告警。

## 已完成：代码审视和收敛

- [x] 六层 `connect_with_cache_...` 构造函数收敛为 `TieredSnapshotStoreConfig` 和 `StorageBackendConfig`。
- [x] 缓存修复、Outbox 和交易分别拆为存储模块，避免继续膨胀主文件。
- [x] 修复 snapshot 幂等回执遗漏 `updated_at_unix_ms` 的内容比较；旧回执保持兼容。
- [x] TypeScript 协议锁从 Rust 权威版本自动生成，消除手工双写常量。
- [x] 协议指纹先规范化 CRLF/LF；server 为已部署 v2 旧指纹提供唯一、可测试的滚动兼容别名，不放宽其他握手校验。
- [x] 修复跨 single/multi/trade 首次创建记录时的 CAS 竞争。
- [x] 后台维护使用独立 PostgreSQL 连接，避免阻塞请求 shard。
- [x] 删除 PostgreSQL 已提交后再返回 `CacheSync` 错误的旧分支。
- [x] Redis 整体不可用时跳过重复缓存复查/锁/回填，只等待一次后直接回源 PostgreSQL。
- [x] 修复 MemoryBackend 时间戳幂等指纹和跨 shard Posting/Event ID 唯一性。
- [x] advisory lock 使用 scope + 长度前缀，消除合法冒号键的确定性别名。
- [x] 补齐旧事务查询/提交 RPC 的 operation ID 网络长度上限。

完整审视记录见[代码审视记录](dbproxy-code-review.md)。

2026-08-27 的[100 玩家两小时故障演练](fault-soak-report-2026-08-27.md)已通过数据安全和自动恢复验收。演练后继续完成：

- [x] 真实存储 readiness 依赖 PostgreSQL 与 Redis 健康，并增加 `/dependencies`、`dbproxy_dependency_up`、告警和 Dashboard；
- [x] Redis backlog 的 AOF 入队、worker lease/ACK、stats 使用独立连接，消除跨职责连接锁等待；
- [x] Rust 客户端增加兼容的 `connect_split(read_size, write_size)`，故障演练默认把 32 条连接拆为 24 读 + 8 写；
- [x] 故障驱动跳过错过的周期，不在恢复后补跑并制造人工尖峰；
- [x] 100 玩家 120 秒压缩回归再次通过，AOF 强杀前后 backlog 为 93/93，最终队列与死信归零。
- [x] `SaveMultiSnapshot` 按连接 shard 合并 PostgreSQL commit，批量执行 revision-aware Redis 回写与 cache-repair ACK；逻辑冲突仍逐条返回，数据库错误整批回滚。
- [x] 普通快照用条件 UPDATE CTE 与受限 INSERT 同时执行 expected Revision；缺失记录只接受无条件写或 expected Revision 0，已有记录的非零匹配 revision 仍能正常推进，并由真实 PostgreSQL 回归锁定两条边界。
- [x] backlog worker 每轮最多批量 claim/save/ACK/release 64 条，降低 PostgreSQL 恢复后的 commit 与 Redis 往返放大。
- [x] multi transaction/trade 首次提交后直接构造已提交缓存快照；幂等重复批量读取当前版本，既移除逐条回读又不允许旧 Payload 覆盖新 Revision。

仍需在正式部署环境按目标 QPS 决定 PostgreSQL shard/连接池大小、读写池比例、worker 数量和告警阈值；没有容量证据时不引入协议多路复用或通用批处理框架。

## 待读验证：读写连接池分配 / Pending validation: read/write pool allocation

`connect_split(read_size, write_size)` 将客户端物理连接分为独立的 read pool 和 write pool。每条连接可有多个在途请求（2026-09-19起，同一记录/操作/交易仍按发送顺序执行）；记录操作按 `RecordKey` 稳定路由，操作按 `operation_id`（交易使用对应交易 ID）稳定路由。同一个键会固定在同一池内的一条连接上，不同键才会形成并行度。连接池拆分可以隔离慢写造成的队头阻塞，但不替代 revision/CAS、业务锁或权威读取契约。

当前故障演练使用的 **24 读 + 8 写** 只是测试基线，不是已经证明的生产最优比例。该比例的合理性必须用相同总连接数、相同负载和重复测量来判断；仅凭玩家数、CPU 核数或 PostgreSQL `max_connections` 推导比例不够。

### 测试矩阵 / Test matrix

- 固定总客户端连接数为 32，固定 DBProxy 版本、server 配置、PostgreSQL/Redis 配置、机器资源、玩家数和测试数据；候选至少包括共享池 `connect(32)`、`28/4`、`24/8`、`20/12` 和 `16/16`。
- 每个候选先运行 2–5 分钟 smoke，再预热 5 分钟并测量 15–20 分钟；每个场景至少重复 3 轮，报告中位数、P95 和 P99，不用单轮峰值下结论。
- 场景一为当前 100 玩家混合负载；场景二提高 `load/load_multi` 比例验证读池；场景三提高 transaction、trade、enqueue 和 AOF ACK 比例验证写池；故障窗口和长稳验收只在健康矩阵完成后对候选或胜出配置执行。
- 增加一个多键场景（至少 10,000 个键）检查稳定哈希在各连接槽的分布；再增加少量热点键场景，明确单键固定在一条连接上时，扩大池不能消除该键的串行瓶颈。
- 让一个 endpoint 短暂不可用，验证 read/write 两个池的物理连接都能独立切换到备用 endpoint，恢复后无连接风暴、旧响应串线或数据正确性错误。

### 观测和数据 / Observability and data

- 客户端必须安装 `ClientObserver`，按 read/write 分类记录 `queue_wait`（等待连接互斥锁）和 `exchange`（编码、网络、DBProxy 服务端处理及返回）；当前 `dbproxy_fault_soak` 未安装该 observer，因此它能证明正确性，不能单独证明 24/8 最优。
- 观测标签只保留池类型和连接槽等有界维度，不把 `RecordKey`、玩家 ID 或 `operation_id` 放进 Prometheus 标签。若需要槽位公平性，单独输出每槽请求数/等待时间汇总。
- 同时保存 DBProxy 的 `dbproxy_requests_in_flight`、RPC 请求/失败/错误/耗时、连接 active/limit/rejected、cache-repair/outbox backlog 与 oldest-age，以及 PostgreSQL/Redis 的 CPU、内存、I/O、活动连接和等待；记录测试版本、配置哈希和时间窗口。
- 每轮都必须检查 `SOAK_FINAL.validation.passed=true`、`readsBehindAcknowledgedRevision=0`、缺失/旧读/不变量错误为零，故障后的 backlog、processing 和 dead-letter 按场景预期收敛。性能提升不能抵消正确性失败。

### 选择规则 / Decision rules

- 健康基线中，以 `queue_wait` 的 P95/P99 作为连接数不足的直接信号；先用“排队等待不超过总 `exchange` 的 10%”作为临时门槛，若业务 SLA 更严格则以 SLA 为准，并在首轮基线上校准。
- 读池等待高而写池低，且 DBProxy/PG/Redis 仍有余量时，增加 read pool；写池等待高而读池低时，增加 write pool。两池等待都低但 `exchange` 高，优先归因于 DBProxy、PostgreSQL、Redis 或网络，而不是继续调整比例。
- 连接槽请求数明显不均衡时，先检查稳定哈希分布和热点键；不能用总吞吐掩盖单槽队头阻塞。
- 候选必须在不引入错误、重连异常、旧读或队列不收敛的前提下，改善目标池的 P95/P99；如果读 P99 出现预先约定的明显回退（默认以基线相对回退 10% 为临时拒绝门槛），即使总吞吐上升也不接受。

矩阵结果进入正式部署参数前，应附上原始采样、三轮汇总和归因结论。没有上述证据时，继续使用 24/8 作为可回退的演练基线，不把它写成容量承诺。

## 已完成：权威快照 HASH 分区

- [x] `dbproxy_snapshots` 使用 `(namespace, record_key)` 建立 32 个原生 HASH 分区。
- [x] DBProxy SQL 始终访问逻辑父表，不拼接物理子表名。
- [x] 启动时验证父表类型、分区键以及全部 modulus/remainder 边界。
- [x] 旧普通表明确拒绝启动，不静默假装分区已经生效。
- [x] 真实 PostgreSQL 测试验证 catalog 布局和实际行路由。
- [x] 分区数量属于 schema migration，不与 `storage.shards` 连接并发参数绑定。

布局、开发库重建和未来扩容规则见[PostgreSQL 快照分区](postgresql-partitioning.md)。

## 明确延期

以下项目本轮不做，也没有预埋复杂抽象：

- [ ] 历史幂等回执、账本和 Outbox 的归档/保留期；
- [ ] 交易、账本、Outbox、幂等回执等其他表的时间或哈希分区；
- [ ] 物理分库、跨库事务或两阶段提交；
- [ ] 多数据库方言 Adapter。

以后只有在真实 PostgreSQL p95/p99、WAL、索引/VACUUM、备份窗口和恢复时间证明需要时，才继续分区其他表或进入物理分库评估。`storage.shards` 仍只是同一数据库地址上的连接/并发分片。

## 尚需部署侧完成

- TLS/mTLS、共享令牌轮换和密钥系统；
- 租户级配额/限流；
- PostgreSQL/Redis 多副本高可用、备份和跨机恢复演练；
- Redis Stream 消费组、消费者幂等表和保留/裁剪策略；
- 正式环境基于真实写入率调整 worker 数量、告警阈值和死信策略。
