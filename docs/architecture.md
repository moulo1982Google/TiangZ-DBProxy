# DBProxy 架构说明

## 开发与发布锁定

当前是持续开发阶段。Cargo依赖、工作区版本和实现细节允许迭代，开发命令不强制`--locked`；只有准备发布正式Tag时，才统一审查`Cargo.lock`、版本、协议指纹，并使用`cargo test --workspace --locked`和`cargo clippy --workspace --all-targets --locked`完成发布验证。Protobuf版本与协议指纹属于运行时兼容性契约，即使开发阶段也不能让客户端和服务端静默使用不匹配的协议。

## 世界观

DBProxy 不是游戏逻辑服务器，也不是把所有业务对象搬进数据库的 ORM。
它是一个独立的持久化边界：业务服务提交已经序列化好的快照，DBProxy 负责版本、幂等、缓存和最终存储。

普通Entity不需要开发者为每一种类型设计PostgreSQL表。TiangZ在`.native`中声明版本化结构并生成Codec/Repository，DBProxy统一写入`dbproxy_snapshots`等固定通用表。这个便利只覆盖“按稳定Key读写完整记录”；需要按业务字段检索、排行榜、拍卖行、跨玩家交易或多记录原子提交时，仍要建立专门的领域表、索引和事务边界。

```text
TiangZ Map/Login/其他业务服务
        |
        | [DBProxy-1, DBProxy-2]：同一套客户端候选地址
        v
  两个无状态 DBProxy 对等实例
        |
        +-- Redis：已提交快照缓存、普通快照持久 backlog
        +-- PostgreSQL：权威快照、Revision、幂等记录和跨记录事务
```

DBProxy 实例之间不复制内存状态，也不互相转发请求。任意实例都可以处理任意请求；共享状态在 PostgreSQL 和 Redis 中。这样故障切换不需要 Leader 选举，客户端只要保持同一幂等 ID 即可安全重放。

## 第一阶段冻结的语义

### RecordKey

`namespace` 和 `key` 共同标识一条记录。例如：

```text
namespace = player
key       = 1001
```

Item、Quest、Buff 可以作为玩家快照的一部分，也可以在以后使用独立 namespace。第一版不强迫业务采用某一种拆分方式。

### Revision

DBProxy 返回并递增 Revision。业务服务更新时可以携带上次读取到的 Revision：

```text
读取 Revision=7
业务修改
写入 expected_revision=7
成功后得到 Revision=8
```

如果当前版本已经不是 7，DBProxy 返回冲突，业务层必须重新读取并决定合并或拒绝。不能静默覆盖其他进程的更新。

### 幂等写入

同一个 `request_id` 的重试必须返回第一次写入的结果，不得再次递增 Revision。
例如网络超时后，业务服务可以安全重试保存请求，而不会重复发奖励。

第一版内存实现只用于验证语义。真正部署时，幂等记录必须和快照写入处于同一个可靠的持久化边界，不能只放在进程内存中。

## 当前真实适配器

当前第一套真实适配器位于`crates/dbproxy-storage`：

```text
SnapshotWrite
    -> PostgreSQL transaction
       1. claim request_id
       2. CAS upsert snapshot
       3. update idempotency receipt
       4. commit
    -> Redis SET committed SnapshotEnvelope
```

PostgreSQL 是唯一权威写入端。Redis写入失败时，PostgreSQL事务不会回滚；调用方收到缓存同步错误后，可以使用原`request_id`重试，DBProxy会返回Duplicate并再次修复缓存。读取优先读Redis；Redis读取失败或缓存编码损坏时，自动回源PostgreSQL，缓存故障不会扩大成数据不可用。缓存预热失败只记录告警，不影响这次数据库读取。

## 关键事务写入

关键经济操作不使用普通`SnapshotWrite`覆盖快照，而使用`TransactionalWrite`：

```text
Wallet/Inventory/Reward
    -> operation_id + expected_revision + new snapshot + business result
    -> PostgreSQL transaction
       1. claim operation_id
       2. lock and compare current revision
       3. write snapshot with new revision
       4. save the exact business result
       5. commit
    -> Redis refresh
```

`operation_id`只在快照和操作结果同时提交时才生效。CAS失败会回滚操作收据，业务可以读取新版本后重新生成请求；数据库提交后如果网络超时，使用原`operation_id`重试会得到`Duplicate`和第一次的原始结果，不会再次发放奖励或递增Revision。

Redis仍然不是权威写入端。PostgreSQL提交成功而Redis刷新失败时，调用方会收到缓存同步错误；用同一个`operation_id`重试会命中已提交收据，并再次把最新快照写入Redis。

运维恢复也可以直接调用`TieredSnapshotStore::repair_cache(record)`：有权威记录就按Revision覆盖缓存，没有权威记录就删除对应缓存键。这个方法不产生新Revision、不执行游戏业务操作，适合启动修复、定时扫描和故障恢复队列。

## 普通快照积压与优雅停机

普通快照可以进入`SnapshotFlushQueue`。队列按`RecordKey`合并，同一玩家或同一业务记录在短时间内多次变更时只保留最后一份Payload；`SnapshotWrite.expected_revision`必须为空，因为被合并的旧请求不能再作为CAS顺序提交。货币、背包、交易和奖励等关键操作必须直接使用`AsyncTransactionalStore`，不能为了排队而丢掉`operation_id`或业务结果。

排空分两层：

```text
SnapshotFlushQueue::flush(store, max_items)
    -> 本轮最多写 max_items 条
    -> 成功计入 Applied/Duplicate
    -> 失败请求放回队首，返回 error + remaining

SnapshotFlushQueue::flush_until_empty(store, per_round, max_rounds)
    -> 停机窗口内重复执行有限轮
    -> remaining == 0 才表示本次队列已排空
    -> remaining > 0 必须记录告警并保留恢复信息
```

`SnapshotFlushQueue`只在当前DBProxy进程内存中存在。它能覆盖“PostgreSQL短暂不可用、进程仍然存活、恢复后重试”的情况，但不能覆盖“DBProxy自己已经重启”的情况。

### Redis AOF 持久积压

`RedisSnapshotBacklog`是独立于快照缓存的持久队列。入队使用一个Redis脚本同时写入Payload和pending索引；消费者领取时把记录移动到processing并设置lease。成功写入PostgreSQL后才调用ACK：

```text
RedisSnapshotBacklog::enqueue(snapshot)
    -> Redis SET entry + ZADD pending

claim(lease)
    -> 回收过期 processing
    -> 原子领取一条记录并设置 lease

PostgreSQL SnapshotWrite(request_id)
    -> 成功或 Duplicate
    -> ack(lease)
```

ACK前如果同一`RecordKey`又入队了新快照，旧ACK只会移除旧processing并把新记录留在pending，不会删除新Payload。消费者进程崩溃后，lease过期会自动回收；数据库写入失败时可以主动release，或者等待lease过期。数据库提交成功但ACK前崩溃时，重试仍复用原`request_id`，由PostgreSQL幂等记录返回Duplicate。

这个backlog依赖Redis AOF和持久数据卷，Redis本身不是PostgreSQL的权威业务库。Redis数据卷损坏、AOF未持久化、单Redis节点故障和跨机复制仍不在本版本保证范围；后续网络服务阶段再增加backlog指标、死信处理、Redis高可用和多消费者容量控制。

当前表为`dbproxy_snapshots`、`dbproxy_idempotency`、`dbproxy_transactions`和`dbproxy_multi_transactions/dbproxy_multi_transaction_records`，迁移脚本位于`crates/dbproxy-storage/migrations/001_snapshot.sql`、`002_transactional.sql`与`003_multi_transactional.sql`。启动迁移使用PostgreSQL事务级advisory lock，多个DBProxy进程可以并发启动而不会竞争DDL。这是独立适配器契约，不等于TiangZ已经完成网络化DBProxy接入。

### 跨记录原子事务

`MultiRecordTransactionalWrite` 用一个 `operation_id` 描述一组记录更新：

```text
业务 Repository
    -> 校验所有玩家/记录，生成完整的新 Payload
    -> ApplyMultiTransaction(operationId, writes[], result)
    -> DBProxy 按 RecordKey 排序加锁
    -> 一次 PostgreSQL transaction 完成全部 CAS 和全部快照写入
    -> 保存整组回执
```

其中任何一条记录的 Revision 不匹配，整个事务回滚，前面的记录也不会改变。重复 `operation_id` 必须携带完全相同的记录集合、版本、Payload 和 result，否则返回 `OPERATION_CONFLICT`。跨玩家交易、玩家转账、共享奖励转移可以使用它；DBProxy 不负责判断“玩家是否有钱”或“交易是否合法”，这些规则必须在业务侧先生成纯数据计划。

该事务要求所有记录共享同一个 PostgreSQL 权威存储。它不是跨数据库的两阶段提交；如果未来不同领域必须落到不同数据库，需要单独设计 Outbox/补偿，不能把这个 API 当成万能分布式事务。

本机依赖使用`deploy/local/docker-compose.yml`，固定为PostgreSQL 18.4 Bookworm和Redis 8.8.1 Trixie，数据使用Docker命名卷保存。

## 后续阶段

### Phase 1：核心契约

- Snapshot、Revision、CAS、幂等
- 内存参考实现
- 协议和错误码

### Phase 2：单记录持久化

- [x] Redis缓存与PostgreSQL权威快照适配器
- [x] 读缓存、写入顺序、Revision/CAS和幂等重试
- [x] 本地Docker Compose和外部依赖集成测试
- [x] 单记录关键事务：operation_id、Revision/CAS、原始结果和Redis修复
- [x] Redis读取故障回源、PostgreSQL写入故障拒绝成功、缓存修复和原操作ID重试
- [x] 进程内普通快照积压合并、有界Flush和PostgreSQL恢复后重试
- [x] Redis AOF 持久积压、lease/ACK、DBProxy重启后的重新领取和新快照替代旧快照
- [ ] 长时间故障、死信/积压指标和多消费者容量控制；Redis/PostgreSQL高可用由云厂商提供，不在本项目实现
- [ ] 其他数据库Adapter；先不同时实现MongoDB、MySQL和PostgreSQL多套方言

### Phase 3：DBProxy 服务

- [x] Rust TCP 网络服务、版本化 Protobuf 和协议指纹
- [x] 内部共享令牌鉴权；租户级配额与隔离尚未实现
- [x] Rust 异步客户端与按 RecordKey 分片的连接池
- [x] 运行时无关TypeScript SDK、协议指纹锁和可插拔Transport
- [x] Redis backlog 后台消费者和有限停机窗口
- [x] 多Endpoint客户端：首选地址、备用地址、连接失效后的顺序切换，并保留原幂等ID重放
- [x] 两个对等DBProxy实例共享云Redis/PostgreSQL；网络测试覆盖请求中断、同ID重放和全候选失败
- [x] 跨记录原子事务：固定排序加锁、整组CAS、整组回执和重复提交恢复
- [x] 批量读取和批量写入：按shard并行、逐记录结果，普通快照不冒充跨记录事务
- Prometheus 指标
- [ ] 生产级优雅停机指标、死信处理和连接自动恢复

### Phase 4：TiangZ 集成

- [x] 首个玩家快照 Repository与Rust Host Transport
- [x] Numeric、Item、Buff、Skill冷却和Quest快照策略
- [x] 登录恢复、正常下线保存和服务重启恢复冒烟
- [ ] 批量登录恢复与周期快照
- [ ] 关键经济事务、崩溃窗口和节点接管验收

Outbox、跨数据库补偿和生产节点接管仍是后续阶段；普通批量写入允许部分成功，跨记录事务只覆盖同一 PostgreSQL 权威库内的整组快照提交。

## 网络服务边界

网络协议位于`crates/dbproxy-protocol/proto/dbproxy.proto`。服务端不会复用TiangZ Runtime的Actor帧，也不允许业务消息穿过DBProxy；两边只共享“大端四字节长度前缀 + 有界帧”的传输习惯。

```text
TiangZ Repository
    -> DbProxyClientPool
    -> ClientHello(version + fingerprint + token)
    -> Load / Save / Enqueue / batch snapshot operations
    -> ApplyTransaction / LoadTransaction
    -> ApplyMultiTransaction / LoadMultiTransaction
    -> StorageBackend(record or operation shard)
    -> PostgreSQL / Redis
```

`SaveSnapshot`和`ApplyTransaction`的响应代表PostgreSQL已经提交；`EnqueueSnapshot`响应只代表Redis AOF backlog接收。调用方必须根据数据等级选择接口，不能把`EnqueueSnapshot`用于货币、背包、交易或奖励确认。

一个SDK连接内有且只有一个在途请求，避免超时后响应错位。需要并发时使用`DbProxyClientPool`；同一RecordKey稳定落在同一连接，不同记录可以并行。服务端同样按RecordKey选择独立`TieredSnapshotStore`分片，数据库仍负责跨连接的Revision、唯一键和事务一致性。

TypeScript SDK不直接假定Node或Deno网络API，而是定义`DbProxyTransport`。宿主Transport负责真实TCP、连接池、超时和重连，SDK负责参数校验、Payload所有权与各类RPC的ACK语义。这样TiangZ嵌入式V8、Node工具和未来其他TS宿主可以共用同一业务接口，而不把某个运行时能力带进DBProxy核心。
