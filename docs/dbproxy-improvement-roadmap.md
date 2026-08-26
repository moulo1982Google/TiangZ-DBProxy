# DBProxy 改进路线图与完成状态

本文记录本轮改进的实际状态。顺序仍是先保证写入和恢复语义，再考虑容量扩展；历史归档、分区和物理分库已明确延期。

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
- [x] 修复跨 single/multi/trade 首次创建记录时的 CAS 竞争。
- [x] 后台维护使用独立 PostgreSQL 连接，避免阻塞请求 shard。
- [x] 删除 PostgreSQL 已提交后再返回 `CacheSync` 错误的旧分支。
- [x] Redis 整体不可用时跳过重复缓存复查/锁/回填，只等待一次后直接回源 PostgreSQL。
- [x] 修复 MemoryBackend 时间戳幂等指纹和跨 shard Posting/Event ID 唯一性。
- [x] advisory lock 使用 scope + 长度前缀，消除合法冒号键的确定性别名。
- [x] 补齐旧事务查询/提交 RPC 的 operation ID 网络长度上限。

完整审视记录见[代码审视记录](dbproxy-code-review.md)。

## 明确延期

以下项目本轮不做，也没有预埋复杂抽象：

- [ ] 历史幂等回执、账本和 Outbox 的归档/保留期；
- [ ] PostgreSQL 时间或哈希分区；
- [ ] 物理分库、跨库事务或两阶段提交；
- [ ] 多数据库方言 Adapter。

以后只有在真实 PostgreSQL p95/p99、WAL、索引/VACUUM、备份窗口和恢复时间证明需要时，才进入单库分区或物理分库评估。`storage.shards` 仍只是同一数据库地址上的连接/并发分片。

## 尚需部署侧完成

- TLS/mTLS、共享令牌轮换和密钥系统；
- 租户级配额/限流；
- PostgreSQL/Redis 多副本高可用、备份和跨机恢复演练；
- Redis Stream 消费组、消费者幂等表和保留/裁剪策略；
- 正式环境基于真实写入率调整 worker 数量、告警阈值和死信策略。
