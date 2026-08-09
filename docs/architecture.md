# DBProxy 架构说明

## 世界观

DBProxy 不是游戏逻辑服务器，也不是把所有业务对象搬进数据库的 ORM。
它是一个独立的持久化边界：业务服务提交已经序列化好的快照，DBProxy 负责版本、幂等、缓存和最终存储。

```text
TiangZ Map/Login/其他业务服务
        |
        | Snapshot Payload + Schema + Revision + RequestId
        v
     DBProxy
        |
        +-- Redis：已提交快照缓存
        +-- PostgreSQL：权威快照、Revision 和幂等记录
        +-- 其他数据库：后续适配
        +-- MongoDB：文档快照
        +-- 文件/对象存储：备份与迁移
```

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

这套队列只在当前DBProxy进程内存中存在。它能覆盖“PostgreSQL短暂不可用、进程仍然存活、恢复后重试”的情况，但不能覆盖“DBProxy自己已经重启”的情况；后者需要Redis-backed durable backlog、队列版本和接管协议，不能把内存队列包装成可靠消息队列。

当前表为`dbproxy_snapshots`、`dbproxy_idempotency`和`dbproxy_transactions`，迁移脚本位于`crates/dbproxy-storage/migrations/001_snapshot.sql`与`002_transactional.sql`。启动迁移使用PostgreSQL事务级advisory lock，多个DBProxy进程可以并发启动而不会竞争DDL。这是独立适配器契约，不等于TiangZ已经完成网络化DBProxy接入。

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
- [ ] Redis-backed durable backlog、DBProxy重启后的积压恢复和长时间故障矩阵
- [ ] 其他数据库Adapter；先不同时实现MongoDB、MySQL和PostgreSQL多套方言

### Phase 3：DBProxy 服务

- Rust 网络服务
- 鉴权与租户隔离
- 批量读取和批量写入
- Prometheus 指标
- 优雅停机和未完成写入排空

### Phase 4：TiangZ 集成

- 玩家快照 Repository
- Item/Quest/Buff 持久化策略
- 登录恢复
- 正常下线保存
- 进程崩溃后的恢复验收

多记录一致性、跨域事务和 Outbox 在单记录 Snapshot/Transactional 语义通过真实故障测试后再进入设计。
