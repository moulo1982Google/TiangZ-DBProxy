# TiangZ DBProxy 能力清单

本文面向架构评审、业务接入和技术交流，按当前工作版本 `v0.6.0` 的代码与协议整理 DBProxy 已具备的能力。

> 口径说明：本文中的“支持”表示协议、服务端、客户端和核心契约已经实现；凡是仍需真实 PostgreSQL/Redis、领域恢复或生产环境验收的部分，会单独标注，不把内存测试等同于生产承诺。

## 一句话定位

DBProxy 是一个独立的 Rust 持久化服务：它把业务服务提供的**带 Schema 的二进制快照**，以 `RecordKey` 为地址保存到权威存储，并提供版本校验、幂等重试、跨记录原子提交、缓存降级和事件投递能力。

它不理解“玩家有多少金币”“这个道具能不能交易”等游戏规则。业务服务负责领域决策，DBProxy 负责把已经决策好的结果可靠地提交、恢复和投递出去。

## 能力总览

| 能力 | DBProxy 能做什么 | 适合解决的问题 |
| --- | --- | --- |
| 通用记录模型 | 用 `namespace + key` 定位记录，保存 `schema`、`schemaVersion`、`revision`、二进制 `payload` 和更新时间 | 玩家、背包、任务、配置、审计文档等不同领域共用一套持久化边界 |
| 权威快照读写 | 读取/保存完整快照；PostgreSQL 是权威写入端，Redis 只作为已提交快照缓存 | 角色登录恢复、周期保存、领域状态持久化 |
| Revision/CAS | 使用期望版本做 Compare-And-Swap；版本只由 DBProxy 递增 | 防止并发保存覆盖、旧进程回写新状态 |
| 幂等与可恢复回执 | 通过 `requestId`/`operationId` 去重；重复请求返回第一次提交的版本和业务结果 | 网络超时、客户端重连、服务进程在提交后崩溃 |
| 批量访问 | 最多 64 条快照批量读取/普通写入，保留请求顺序；普通批量写逐条返回结果 | 一次恢复多个玩家领域，减少网络往返 |
| 持久积压写入 | 普通快照可先可靠写入 Redis AOF backlog，由后台 worker 异步落 PostgreSQL | 位置、任务进度、等级等允许小范围延迟/回退的数据 |
| 单记录关键事务 | 一条记录的 CAS、完整快照和业务结果在一次事务中提交 | 拾取、发奖、单领域状态转移等不能重复执行的操作 |
| 多记录原子事务 | 最多 256 条记录整组 CAS；全部成功或全部回滚 | 跨玩家交易、奖励转移、多个领域同时变更 |
| 通用提交效果 | `CommitRecords` 可把多记录快照、只追加事实和 Outbox 事件放进同一个 PostgreSQL 事务 | 审计事实与状态变更必须同生共死的通用领域场景 |
| 交易兼容原语 | 交易状态迁移、多记录 CAS、零和账本、Outbox 和回执原子提交 | 兼容现有 Trade API；新领域优先使用通用提交 |
| 分层缓存 | 缓存命中、负缓存、过期后 stale-while-revalidate、受限回源和缓存修复 | 缓存故障时继续从 PostgreSQL 读取，不把缓存当最终真相 |
| Outbox Relay | 事务内记录事件意图，后台按租约、顺序、重试和死信投递到 Redis Stream | 状态提交后发布领域事件，避免“状态成功但事件意图丢失” |
| 网络与客户端 | Protobuf/TCP v2、协议指纹握手、Rust 异步客户端、连接池、Endpoint 故障切换、TypeScript SDK | Rust、Node/Deno/浏览器宿主或其他业务框架接入 |
| 多租户入口 v1 | 凭据绑定独立后端、租户连接预算及监控；不同 PostgreSQL database / Redis 逻辑 DB，真实存储隔离与恢复仍待验收 | 多游戏共享入口和物理实例；不承诺 CPU/内存或故障硬隔离，见[部署边界](multitenancy.md) |
| 运行观测 | `/live`、`/ready`、`/dependencies`、Prometheus `/metrics`，覆盖 RPC、缓存、PG、队列和 Outbox | 健康检查、延迟归因、积压/死信告警和故障演练 |

## 整体数据流

```text
业务服务 / Repository
        │  稳定的 requestId 或 operationId + 完整 Payload
        ▼
Rust Client / TypeScript SDK
        │  Protobuf v2 + TCP 长度帧 + 版本/指纹/令牌握手
        ▼
DBProxy Server
   ┌────┼─────────────────────────────────────────────┐
   │    │                                             │
   │    ├─ 默认读快照：PostgreSQL 主库；显式旧读：Redis cache + PG回源
   │    │
   │    ├─ 直接写：PostgreSQL 事务 ──► durable cache repair ──► Redis cache
   │    │
   │    ├─ 排队写：Redis AOF backlog ──► backlog worker ──► PostgreSQL
   │    │
   │    └─ 事务事件：PostgreSQL Outbox ──► relay worker ──► Redis Stream
   └─────────────────────────────────────────────────┘
```

正式的 `postgresRedis` 模式下，PostgreSQL 保存权威数据；Redis 可以分别配置为可靠队列 Redis 和易失快照缓存 Redis。`memory` 模式只用于本地开发、协议测试和性能隔离，进程退出后数据全部丢失。

默认Load/LoadMulti不依赖缓存新鲜度；批量一次SQL快照，PG失败不降级缓存。缓存只在显式允许旧数据的读取中使用，版本下限不能代替登录恢复的默认权威读取。详见[默认读取契约](default-read-contract.md)。

## 核心能力详解

### 1. 通用、与业务解耦的快照模型

DBProxy 只认识以下稳定字段：

```text
RecordKey       = namespace + key
Snapshot        = schema + schemaVersion + revision + payload + updatedAt
Payload         = opaque bytes，由业务负责编码和解码
```

因此可以同时保存 `player/1001`、`inventory/1001`、`quest/1001` 和 `guild/7`，而 DBProxy 不需要知道这些数据的内部字段。业务服务负责生成完整快照；DBProxy 不提供字段级 Patch，也不在存储层追加游戏规则。

### 2. 快照读取、保存和批量访问

当前协议提供：

| RPC | 语义 |
| --- | --- |
| `LoadSnapshot` | 读取一条已提交快照；缓存缺失或缓存不可用时有界回源 PostgreSQL |
| `LoadMultiSnapshot` | 一次读取 1–64 条唯一记录，返回顺序与请求一致，缺失记录保留空位 |
| `SaveSnapshot` | 保存一条普通快照；可带 `expectedRevision`，成功表示 PostgreSQL 权威事务已经提交 |
| `SaveMultiSnapshot` | 一次提交最多 64 条普通快照，逐条返回 `Applied`/`Duplicate` 或错误；不提供跨记录原子性 |
| `EnqueueSnapshot` | 把普通快照写入 Redis AOF backlog；成功只表示可靠入队，不表示 PostgreSQL 已落库 |
| `EnqueueMultiSnapshot` | 最多 64 条普通快照批量进入 backlog；不允许携带 `expectedRevision` |

普通保存的大致语义如下：

```pseudo
function saveSnapshot(request):
    oldReceipt = postgres.idempotencyReceipt(request.requestId)
    if oldReceipt exists:
        if oldReceipt.fingerprint != fingerprint(request):
            return IdempotencyConflict
        return Duplicate(oldReceipt.revision)

    begin postgres transaction
    current = select revision from snapshots(request.record) for update
    actual = current.revision or 0

    if request.expectedRevision exists
       and request.expectedRevision != actual:
        rollback
        return RevisionConflict(actual)

    next = actual + 1
    upsert full snapshot(request.payload, next)
    insert idempotency receipt(request.requestId, fingerprint(request), next)
    enqueue cache-repair target(request.record, next)  // same transaction
    commit

    try refresh Redis cache with revision-aware write
    // cache failure does not undo the committed PostgreSQL result
    return Applied(next)
```

`SaveMultiSnapshot` 的返回结果必须逐条处理：某一条失败不会把已经成功的兄弟条目变回未提交。需要“要么全成功、要么全回滚”时，应使用 `ApplyMultiTransaction` 或 `CommitRecords`。

### 3. Revision/CAS 与幂等重试

DBProxy 把并发控制和网络重试拆成两个维度：

- `expectedRevision` 防止业务基于旧快照覆盖更新；`0` 可表达“只允许首次创建”。
- `requestId` 用于普通快照写入；`operationId` 用于关键事务，并且同一个操作号在重试、重连和 Endpoint 切换时必须保持不变。
- 首次成功返回 `Applied`，重复提交返回 `Duplicate`；重复提交不会再次递增 Revision、扣除资产或追加同一条事实。
- 已使用的 ID 不能换记录、换 Payload、换结果或换事务类型。篡改重试会得到冲突，而不是被当成新操作执行。

调用方在“结果未知”时应遵循这个模式：

```pseudo
operationId = createOnceForBusinessOperation()
request = buildRequest(operationId)

try:
    return ApplyTransaction(request)
catch timeout_or_connection_lost:
    receipt = LoadTransaction(operationId, request.record)
    if receipt exists:
        return receipt                         // 已提交，恢复第一次结果
    return ApplyTransaction(request)           // 仍使用原 operationId 和原请求
```

多记录事务使用 `LoadMultiTransaction(operationId, completeRecordSet)` 恢复；旧 Trade API 使用 `LoadTradeTransaction(operationId, tradeId)` 恢复。查询回执时必须提供原始的完整记录集合，不能用另一组记录“试探”同一个操作号。

### 4. 单记录关键事务

`ApplyTransaction` 将以下内容绑定在一起：

1. 一条记录的期望 Revision；
2. 提交后的完整快照；
3. 调用方希望拿到的原始业务结果，例如实际发放数量、新余额或生成的物品 ID。

业务结果也进入持久回执，因此调用方即使在提交后丢失响应，也能恢复当时的结果。DBProxy 只保存和返回结果字节，不解释其中的业务字段。

### 5. 多记录原子事务

`ApplyMultiTransaction` 在同一个 PostgreSQL 事务中处理最多 256 条完整记录：

```pseudo
function applyMulti(request):
    writes = sortByRecordKey(request.writes)
    reject duplicate RecordKey

    begin postgres transaction
    claim operationId as kind = "multi"

    if operationId already has a receipt:
        compare complete request and result
        if different: rollback; return OperationConflict
        return Duplicate(original receipt)

    for write in writes:
        lock(write.record) in deterministic order
        actual = read current revision
        if actual != write.expectedRevision:
            rollback all writes
            return RevisionConflict(write.record, actual)

    for write in writes:
        write full snapshot with write.expectedRevision + 1
        record newRevision in receipt
        enqueue cache repair target

    persist operation receipt and result
    commit
    best-effort refresh committed snapshots in Redis
    return Applied(all newRevisions, result)
```

锁顺序、Revision 校验和最终写入都在数据库事务内完成，所以不会出现“买方已经扣款、卖方物品却没有转移”的半提交结果。跨记录事务仍然只能发生在同一个 PostgreSQL 数据库内，不能跨两个数据库集群做分布式事务。

### 6. `CommitRecords`：快照、不可变事实和事件一次提交

`CommitRecords` 是面向通用领域的组合式提交接口，包含：

- 最多 256 条带 CAS 的完整快照写入；
- 可选的 `AppendRecord` 集合：每个事实以独立 `namespace/key` 唯一标识，只允许追加，不能更新、删除或清空；
- 可选的 Outbox 事件集合，事件意图与快照共用同一个 PostgreSQL 事务；
- 一个不透明的业务结果和稳定 `operationId`。

它适合“更新两个账户文档，同时追加审计事实并发布变更事件”这类场景：任一 Revision 冲突、事实 ID 冲突、事件 ID 冲突或路由不合法，整组回滚。

```pseudo
function commitRecords(operationId, writes, appends, events, result):
    normalizeAndSort(writes, appends, events)
    validateUniqueRecordKeysAndIds()

    begin postgres transaction
    claim operationId as kind = "multi"

    if operation already committed:
        compare writes + result + appends + events byte-for-byte
        if different: rollback; return OperationConflict
        return Duplicate(original record receipt, original result)

    lock all record keys and event partitions in stable order
    check every expectedRevision
    write all new snapshots
    append immutable facts
    insert Outbox rows
    enqueue cache-repair targets
    persist normalized effects for retry verification
    commit

    return Applied(record revisions, result)
```

追加事实和事件目前是写入边界，不是通用查询系统：审计读取、投影和消费由领域服务或后续只读工具负责。`CommitRecords` 的通用 Relay 路由需要服务端和宿主确认新版协议能力；旧宿主会明确拒绝，不会静默丢弃事件效果。

### 7. Trade 交易兼容能力

当前仍提供旧的 `ApplyTradeTransaction`、`LoadTrade` 和 `LoadTradeTransaction` 入口。它能在同一个 PostgreSQL 事务中原子处理：

- 交易状态和独立的交易版本；
- 多条玩家/领域快照及各自 Revision；
- 不可变 Ledger Posting；
- Outbox 事件；
- 交易操作回执和业务结果；
- 提交后的缓存修复目标。

DBProxy 内置的状态迁移约束是：

```text
不存在 ──► Proposed / Escrowed
Proposed ──► Escrowed / Cancelled
Escrowed ──► Settled / Cancelled
Settled、Cancelled 为终态
```

同一种资产的 Posting 必须零和，例如：

```text
buyer:gold          -100
escrow:trade-1001   +100
                    ────
                       0
```

业务服务仍必须负责玩家在线状态、锁、道具所有权、数量、价格、权限和风控。账本是不可变审计事实，不提供按账本实时聚合余额的业务查询，也不替代领域层的资产规则。新领域推荐使用 `CommitRecords`，旧 Trade API 作为兼容入口保留。

### 8. 分层缓存与故障降级

快照读取采用“缓存优先、权威库兜底”的路径：

```pseudo
function loadSnapshot(record):
    cached = redisCache.get(record)

    if cached is fresh:
        return cached
    if cached is stale and inside staleWhileRevalidate window:
        schedule refresh(record)
        return cached
    if cached is negative:
        return NotFound

    acquire bounded fallback slot
    acquire per-record fallback lock when possible
    recheck cache after lock
    snapshot = postgres.load(record) with timeout/circuit breaker

    if snapshot exists:
        revisionAwarePut(cache, snapshot)  // 旧 Revision 不能覆盖新 Revision
    else:
        putNegativeCache(record)
    release lock
    return snapshot
```

已经提交的快照会把缓存修复目标与权威写入放在同一个事务中。Redis 写入失败、缓存超时或缓存重启不会把 PostgreSQL 已提交的结果改成失败；修复 worker 会租约领取目标，按重试退避补齐缓存，超过次数进入死信，运维修复后可以定点重放。缓存支持正值 TTL、确定性 jitter、负缓存和 stale-while-revalidate，具体等待预算可通过配置调整。

### 9. 普通快照 backlog

对于允许短暂延迟或小范围回退的普通数据，`EnqueueSnapshot` 提供异步路径：

```pseudo
enqueueSnapshot(write without expectedRevision):
    redis Lua enqueue(write)
    wait for local Redis AOF acknowledgement
    return accepted

backlogWorker:
    lease item from Redis backlog
    save item to PostgreSQL using original requestId
    if save succeeded:
        ACK backlog item
    else:
        release item; retry after lease/backoff
```

数据库保存成功但 ACK 丢失时，租约到期后会再次领取；原 `requestId` 会把重复落库变成幂等 Duplicate。该路径的成功语义是“可靠入队”，不是“PostgreSQL 已提交”，因此不适合货币、背包、奖励和交易等关键经济状态。

### 10. Outbox 与通用 Relay

Outbox 的关键点是先把“要发什么”写入 PostgreSQL，再由 worker 发布：

```pseudo
outboxWorker:
    event = claim earliest unpublished event
            with lease + leaseToken
            and preserve topic/partition order

    try:
        streamId = redis.XADD(destination, event)
        redis.WAITAOF(local = 1)
        postgres.ackPublished(event, leaseToken)
    catch error:
        postgres.fail(event, exponentialBackoff(error))
        if attempts exhausted:
            mark deadLetter
```

当前已实现的 Publisher 是 Redis Stream。发布前会等待 Redis 本地 AOF 确认，之后才把 PostgreSQL Outbox 行标记为已发布；如果 Redis 已收到而 PostgreSQL ACK 丢失，事件可能再次发布，因此整体语义是**至少一次**，消费者必须按稳定的 `eventId` 去重。

通用 Relay 使用版本化 `EventEnvelope`，包含 producer、eventType、aggregate、partitionKey、schemaVersion、contentType、payload、发生时间和 routeVersion。Publisher ID、路由目标和路由版本持久绑定，避免重启或凭据轮换时把旧积压发到错误地址。Kafka/RabbitMQ 当前只能作为 `enabled:false` 的未来声明，尚未实现驱动。

### 11. 网络协议与安全边界

网络层当前具备：

- Protobuf 协议 v2 和 SHA-256 protocol fingerprint；
- 第一帧必须完成 `protocolVersion + fingerprint + authToken + clientName` 握手；
- 4 字节大端长度前缀帧，默认 frame 上限 8 MiB；
- 默认单个应用 Payload/Result 上限 1 MiB，所有文本字段和批量数量有界校验；
- 错误码区分参数错误、协议错误、Revision/Operation/Trade 冲突、存储不可用和内部错误；
- 服务端认证令牌使用恒时比较，配置和调试输出不记录真实令牌；
- 严格 JSON 配置，连接串和密钥通过环境变量注入，支持联网前的 `--check-config`。

这套认证是内部服务令牌边界；当前协议文档明确未覆盖 TLS/mTLS、租户隔离和配额，生产网络仍应通过内网、网络策略或外部安全层保护。

### 12. Rust 客户端、连接池和 Endpoint 故障切换

`dbproxy-client` 提供异步 Rust API：

- 单个连接内按顺序处理请求；连接池让不同记录并行；
- 按 `RecordKey` 或 `operationId` 稳定路由，减少同一业务键的乱序；
- `connect_split(readSize, writeSize)` 可将读连接和写/事务连接分开，避免慢写或 AOF ACK 阻塞读连接；
- 支持首选 Endpoint 和有序故障切换候选；连接超时、断开或响应不完整时废弃旧连接，再进行握手重连；
- 重连本身不替调用方生成新幂等号，也不自动把一次结果未知的写入改成另一个业务操作；
- `ClientObserver` 提供有界的连接、故障切换、排队等待和交换耗时观测。

TypeScript SDK `@tiangz/dbproxy-sdk` 采用运行时无关的 Transport：

- 暴露稳定类型和 `bigint` 版本/时间字段；
- 在异步边界前做参数校验和 `Uint8Array` 防御性复制；
- 覆盖快照、批量、单/多记录事务、`CommitRecords` 和 Trade 兼容 API；
- 不绑定 Node、Deno、浏览器或 TiangZ，也不偷偷生成新的幂等 ID；
- 对 Relay 能力进行显式握手确认，旧 Transport 不能静默降级为普通多记录提交。

### 13. 分片、扩展和部署

- PostgreSQL 权威快照表按完整 `RecordKey` 做 32 个 HASH 叶子分区，业务访问逻辑父表；
- DBProxy 进程内部按 `RecordKey` 建立稳定存储连接分片，按 `operationId` 路由事务，减少单连接锁竞争；
- 多个 DBProxy 实例是无状态对等节点，可以共享同一个 PostgreSQL 和可靠队列 Redis，不需要 Leader 选举；
- backlog、cache-repair、Outbox 都使用租约和过期回收，实例退出后未完成任务可被其他 worker 重新领取；
- 启动配置可以分别设置 Tokio worker、存储 shard、缓存预算、PG 连接排队预算、backlog/repair/outbox worker 和重试策略。

这里的“分片”是同一个 PostgreSQL 内的表分区和连接分片，不是跨数据库的自动分库，也不提供跨集群原子事务。

### 14. 可观测性与运维恢复

配置观测端口后，DBProxy 提供：

| Endpoint/指标 | 用途 |
| --- | --- |
| `/live` | 进程是否仍存活 |
| `/ready` | 业务服务是否可以接流量；真实存储模式要求 PostgreSQL 和可靠 Redis 可达 |
| `/dependencies` | 查看持久化依赖健康状态 |
| `/metrics` | Prometheus 格式的 RPC、连接、错误、缓存、回源、延迟、backlog、cache repair 和 Outbox 指标 |
| queue depth / oldest age / dead-letter | 判断积压是否持续、是否需要人工介入 |
| stage latency | 区分客户端排队、网络交换、缓存、PG 操作、修复 ACK 和 Outbox 发布阶段 |

队列死信不会被后台静默删除。修复 Redis/AOF 或外部发布目标后，可以先检查 `last_error`、目标和事件内容，再按记录或事件 ID 定点 requeue，保留故障证据。

## API 选择建议

| 业务需求 | 推荐 API | 需要牢记的成功语义 |
| --- | --- | --- |
| 读取一个领域快照 | `LoadSnapshot` | 返回的是权威快照或缓存安全回源结果 |
| 登录一次恢复多个领域 | `LoadMultiSnapshot` | 最多 64 条，返回顺序与请求一致 |
| 直接保存允许覆盖的普通状态 | `SaveSnapshot` | `Applied` 才代表 PostgreSQL 已提交；缓存失败由修复队列处理 |
| 普通状态先入队再落库 | `EnqueueSnapshot` | `accepted` 只代表 Redis AOF backlog 已可靠接收 |
| 多条普通写但可接受部分成功 | `SaveMultiSnapshot` | 逐条处理结果，不具备整组回滚 |
| 一条记录的关键操作 | `ApplyTransaction` | 用 operation ID 恢复原始业务结果 |
| 多条记录必须同生共死 | `ApplyMultiTransaction` | PostgreSQL 内整组 CAS、整组提交/回滚 |
| 记录 + 审计事实 + 事件一起提交 | `CommitRecords` | 效果也属于幂等指纹，修改效果的重试会冲突 |
| 兼容现有交易状态/账本接口 | `ApplyTradeTransaction` | DBProxy 做存储侧安全校验，业务规则仍在领域层 |

## 对外介绍时必须同时说明的边界

| 不应误解为 | 实际语义 |
| --- | --- |
| “Redis 是最终数据库” | PostgreSQL 才是权威写入端；Redis 是缓存、可靠 backlog 或 Outbox 发布介质 |
| “所有写入都是 exactly-once” | 权威提交按 ID 幂等；Outbox 是至少一次，消费者必须按 `eventId` 去重 |
| “SaveMulti 是批量事务” | `SaveMultiSnapshot` 可部分成功；原子需求使用多记录事务 |
| “Enqueue 成功就是数据库保存成功” | Enqueue 成功只是 Redis AOF backlog 已接收 |
| “DBProxy 会校验游戏规则” | DBProxy 只校验 Revision、状态迁移、账本零和、ID 唯一性等通用/存储侧约束 |
| “可以直接执行任意 SQL 或复杂查询” | 当前 API 以 RecordKey 快照和事务原语为主，没有任意 SQL、通用扫描或二级索引查询 API |
| “内存后端可以用于正式数据” | MemoryBackend 是易失测试后端，重启即丢数据 |
| “已经支持 Kafka/RabbitMQ” | 配置可以离线声明未来能力，但当前已实现 Publisher 是 Redis Stream |
| “已经完成完整持久托管迁移” | 旧 Trade API 可用；通用 CommitRecords 已实现，但新的真实存储验收、领域恢复闭环和生产发布门槛仍需单独完成 |

## 代码与验证入口

- 协议定义：[dbproxy.proto](../crates/dbproxy-protocol/proto/dbproxy.proto)
- Core 持久化契约：[dbproxy-core/src/lib.rs](../crates/dbproxy-core/src/lib.rs)
- PostgreSQL/Redis 适配器：[dbproxy-storage/src/lib.rs](../crates/dbproxy-storage/src/lib.rs)
- TCP 服务端：[dbproxy-server/src/lib.rs](../crates/dbproxy-server/src/lib.rs)
- Rust 异步客户端：[dbproxy-client/src/lib.rs](../crates/dbproxy-client/src/lib.rs)
- TypeScript SDK：[sdk/typescript/src/index.ts](../sdk/typescript/src/index.ts)
- 架构说明：[architecture.md](architecture.md)
- 网络协议与错误语义：[network-protocol.md](network-protocol.md)
- 交易安全与 Outbox：[trade-safety-and-outbox.md](trade-safety-and-outbox.md)
- Outbox Relay：[outbox-relay.md](outbox-relay.md)
- 持久化与故障恢复：[durability-recovery-runbook.md](durability-recovery-runbook.md)
- 性能基线：[性能基线](../PERFORMANCE.md)

常规代码门禁：

```powershell
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
npm run test:typescript
```

仓库还提供真实 TCP 闭环、Redis/PostgreSQL 故障矩阵、持久化积压恢复和业务负载工具。性能数字只代表指定机器、指定后端和指定 Payload 下的 DBProxy 链路参考，不是 PostgreSQL 容量或线上 SLA 承诺。
