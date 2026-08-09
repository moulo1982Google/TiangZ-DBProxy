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

PostgreSQL 是唯一权威写入端。Redis写入失败时，PostgreSQL事务不会回滚；调用方收到缓存同步错误后，可以使用原`request_id`重试，DBProxy会返回Duplicate并再次修复缓存。读取优先读Redis，未命中再读PostgreSQL并回填缓存；缓存预热失败不影响这次数据库读取。

当前表为`dbproxy_snapshots`和`dbproxy_idempotency`，迁移脚本位于`crates/dbproxy-storage/migrations/001_snapshot.sql`。这是独立适配器契约，不等于TiangZ已经完成网络化DBProxy接入。

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
- [ ] Redis故障、PostgreSQL故障、缓存修复和积压恢复矩阵
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

事务、多记录一致性和 Outbox 在单记录 Snapshot 语义通过真实故障测试后再进入设计。
