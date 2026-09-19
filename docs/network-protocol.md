# DBProxy 网络协议 v2

## 边界

多租户启动模式 --tenants 不改变线协议：凭据在服务端绑定租户后端，client_name 不参与授权，连接内不得切换。原 --config 仍是单租户。隔离及部署约束见 [多租户说明](multitenancy.md)。

TiangZ 只依赖版本化协议和 SDK，不依赖 Redis、PostgreSQL 或 storage crate。DBProxy 识别通用 RecordKey、Schema、Payload、Revision，以及交易状态/Posting/Outbox 持久化原语，不解释 Scene、Entity、道具或价格。

网络失败不会改变幂等规则：重试必须携带原 `request_id` 或 `operation_id` 和完全相同的请求内容。SDK/Transport 禁止在重连或 Endpoint 切换时替换 ID。

## 帧、大小与握手

```text
[4-byte big-endian protobuf length][protobuf payload]
```

- 默认 frame 上限：8 MiB，在按声明长度分配内存前检查；
- 默认单个应用 Payload/Result 上限：1 MiB，可由 `server.maxPayloadBytes` 下调；
- 批量普通快照最多 64 条，多记录/交易快照最多 256 条；
- 交易 Posting 最多 512 条，Outbox 事件最多 64 条；
- 所有 ID、Schema、topic 等文本都有 UTF-8 字节上限，topic 仅允许字母、数字、`.`、`_`、`-`。

第一帧必须是 `ClientHello(protocol_version, protocol_fingerprint, auth_token, client_name)`。当前握手版本是 2，fingerprint 是权威 proto 先统一为 LF 后的 SHA-256。server 精确接受当前指纹及 `LEGACY_PROTOCOL_FINGERPRINT_V2`、`PRE_COMMIT_PROTOCOL_FINGERPRINT_V2`、`PRE_RELAY_PROTOCOL_FINGERPRINT_V2` 及 `PRE_AUTHORITATIVE_PROTOCOL_FINGERPRINT_V2` 四个已知旧指纹，并向连接回显客户端原值；具体清单以 `dbproxy-protocol/src/lib.rs` 为准。未列出的指纹或不匹配的版本不进入 RPC 调度。proto 的 package 名保留 `tiangz.dbproxy.v1` 只是生成代码命名空间；兼容性由握手版本和指纹共同决定。

升级方向是服务端先行：先完成服务端及其数据库迁移的受控切换，再更新客户端。新客户端要求精确的版本/指纹和 `supports_outbox_relay`，不会主动降级到旧服务端。A3 的候选跳过只保证继续寻找兼容服务端，不代表旧服务端可以处理新请求，也不保证混版本的存储/队列语义。所有候选均不兼容时应保留明确拒绝原因并修正部署版本，不能放宽握手检查来掩盖问题。

## 十四类 RPC

| RPC | 语义 |
| --- | --- |
| `LoadSnapshot` | 默认读取PG主库已提交状态；失败返回错误，缺失返回None |
| `LoadMultiSnapshot` | 1..64 个唯一 RecordKey，保持顺序和缺失位置；默认整批一次PG查询，使用同一数据库快照 |
| `SaveSnapshot` | 幂等 PostgreSQL 快照提交；缓存失败由 durable repair 修复 |
| `SaveMultiSnapshot` | 1..64 条独立普通写，逐条结果、允许部分成功，不提供跨记录原子性 |
| `EnqueueSnapshot` | 普通快照写入 Redis backlog 并取得本机 AOF ACK；尚未落 PostgreSQL |
| `EnqueueMultiSnapshot` | 1..64 条无 CAS 快照通过一个 Lua 脚本原子入 backlog，再等待 AOF |
| `ApplyTransaction` | 单记录关键事务，原子保存快照和第一次业务 result |
| `LoadTransaction` | 按 operation ID + RecordKey 读取单记录事务回执 |
| `ApplyMultiTransaction` | 同一 PostgreSQL 内最多 256 条记录整组 CAS/提交/回滚 |
| `LoadMultiTransaction` | 按 operation ID + 完整 RecordKey 集合读取多记录回执 |
| `CommitRecords` | 同一 PostgreSQL 内原子提交多记录快照、只追加事实、Outbox 和业务回执 |
| `ApplyTradeTransaction` | 原子提交交易状态、多记录 CAS、平衡账本、Outbox、修复目标和 Receipt |
| `LoadTrade` | 按 trade ID 读取当前交易版本、状态和 Payload |
| `LoadTradeTransaction` | 按 operation ID + trade ID 读取第一次交易 Receipt |

### 直接写入和缓存

`LoadSnapshotRequest`新增`allow_stale=false`及可选`min_revision`；`LoadMultiSnapshotRequest`新增`allow_stale=false`及`min_revisions`（空或与records等长，0为无下限）。默认不访问缓存。显式允许旧读时使用缓存；任一版本下限不满足则整批回源PG，不混合缓存与权威快照。PG仍低于下限或缺失时返回`STORAGE_UNAVAILABLE`，不无限等待。`storage.authoritativeReadNamespaces`额外禁止匹配namespace使用缓存，批量任一匹配则整批权威读取。完整边界及升级顺序见[默认读取契约](default-read-contract.md)。

`Save*` 与 `Apply*` 成功表示 PostgreSQL 权威事务已提交。Redis 快速回写失败不会改写这个事实，因为 `dbproxy_cache_repairs` 已在同一事务保存目标；worker 稍后补齐。客户端不需要仅为缓存失败重放权威业务操作。

### Enqueue ACK

`Enqueue*` 在 Lua 入队后执行 `WAITAOF 1 0 2000`（部署配置 `backlog.enqueueAck: "memory"` 时跳过，写入 Redis 内存即成功，协议不变）。AOF 未启用、超时或 Redis 不可用会返回 `STORAGE_UNAVAILABLE`。成功仍只代表 Redis 本地 AOF 接收；货币、背包、奖励、交易禁止走该路径。

### 多记录与交易

多记录请求按 `(namespace, key)` 规范化排序并拒绝重复项。所有锁、Revision 校验和最终带条件 mutation 在一个 PostgreSQL 事务中；任一冲突整组回滚。

交易状态允许新建为 Proposed/Escrowed，Proposed 转 Escrowed/Cancelled，Escrowed 转 Settled/Cancelled；终态不能继续迁移。每种 asset 的 Posting 金额必须零和。相同 operation ID 重试会逐项比较状态、记录、账本、Outbox 和 result，任何篡改均拒绝。

## 错误码

| 错误码 | 含义 | 调用方处理 |
| --- | --- | --- |
| `INVALID_REQUEST` (1001) | 缺字段、越界、重复记录或非法状态 | 修代码，不盲目重试 |
| `UNAUTHORIZED` (1002) | 内部令牌不匹配 | 修部署密钥 |
| `PROTOCOL_MISMATCH` (1003) | 版本或 fingerprint 不匹配 | 部署匹配 SDK/服务端 |
| `REVISION_CONFLICT` (2001) | Record CAS 失败 | 读取实际版本，业务重新决策并使用新操作 ID |
| `IDEMPOTENCY_CONFLICT` (2002) | request ID 被用于不同快照请求 | 修 ID/重试逻辑 |
| `OPERATION_CONFLICT` (2003) | operation ID 内容或事务类型不同 | 修 ID/重试逻辑 |
| `TRADE_CONFLICT` (2004) | 交易版本或状态冲突 | 重新加载交易并由业务决策 |
| `LEDGER_CONFLICT` (2005) | Posting ID 已被其他已提交操作占用 | 修交易计划；非法/不平衡请求返回 `INVALID_REQUEST` |
| `OUTBOX_CONFLICT` (2006) | Event ID 已被其他已提交操作占用 | 修事件计划；非法事件返回 `INVALID_REQUEST` |
| `STORAGE_UNAVAILABLE` (3001) | PostgreSQL/Redis/AOF 当前无法完成路径 | 保留原幂等 ID，退避重试 |
| `INTERNAL` (9000) | 服务端持久化不变量损坏 | 告警并人工排查 |

## 连接、并发和 Endpoint

`server.maxConnections` 默认 256，必须为正数；每个实例独立限制 TCP 连接总数，包含尚未认证的握手。accept 后先尝试取得名额，满额直接关闭连接，不读取帧、不创建连接任务，也不发送握手成功。正常断开、握手失败/超时、任务 panic 或取消均释放名额。`dbproxy_connections_rejected_total` 记录容量拒绝，`dbproxy_connections_limit` 给出上限；认证失败仍使用握手拒绝指标。根据客户端池总连接数及 `maxFrameBytes` 的内存预算设置上限，不能把玩家数直接当作连接数。观测 HTTP 端口不占业务连接名额。

一条连接可同时有多个在途请求（2026-09-19）。服务端 `server.maxInFlightPerConnection`（默认64，1..=4096）限制每条连接同时处理的请求数，满额时暂停读取该连接；客户端 `ClientConfig::max_in_flight`（默认64）限制发出未回的请求数，超出在客户端排队。响应按完成顺序返回、以 `rpc_id` 对应，协议格式不变。**顺序保证只针对同一连接上共享键的请求**：涉及同一 RecordKey、operation ID 或 trade ID 的请求按到达顺序执行，前一个执行完后一个才开始；不相关的请求并发执行。连接池把同一记录稳定路由到同一连接，所以业务按调用顺序发出的同一记录写入仍按序落库。不同连接之间没有顺序保证，与此前相同。

单个请求超时不再废弃整条连接：发出后该连接若收到过任何其他响应，只算这个请求慢，迟到的响应被丢弃；发出后整条连接一帧未收到才判定失效并换连接，旧连接上已发出的请求仍可收到结果。写入中途失败或被取消会关闭写端，服务端据此结束连接。服务端某个请求 panic 只让该请求收到 `INTERNAL`（结果未知，按原幂等 ID 重试），同连接其他请求不受影响。连接关闭或停机时，服务端先执行完已接收的请求并尽量写回响应。旧客户端（一次一个请求）与新服务端、新客户端与旧服务端（逐个处理）都兼容。`DbProxyClientPool::connect` 按 RecordKey 在一组共享读写连接中稳定路由，保持原有连接数和顺序语义；`connect_split(read_size, write_size)` 使用两组物理连接，读查询进入 read pool，写入、事务和 enqueue 进入 write pool，避免慢写造成跨用途队头阻塞。它不替代业务锁或 revision/CAS。服务端再按记录或 operation ID 路由到独立存储 shard。

Rust 客户端接收有序 Endpoint 列表。初次连接和故障重连都跳过不可达候选，以及握手阶段的认证拒绝、协议拒绝、指纹或 Relay 能力不匹配；仍严格验证每个候选，不降低协议或认证要求。候选握手拒绝继续记录为 `Rejected`，不会改记为 `Unavailable`；全部候选失败时优先返回拒绝原因，避免被后续网络错误覆盖。端点无关的本地配置错误立即失败。业务 RPC 阶段的 Remote 错误（包括 Unauthorized、Revision/Operation/Trade 冲突）不触发切换。所有候选都必须共享同一 PostgreSQL/Redis，否则幂等和 Revision 契约不成立。

PostgreSQL 已断连接在下一次操作前做 2 秒有界重连；当前失败写不在底层自动重放。Redis 使用 connection manager 自动重连。调用方仍是唯一有权根据业务语义决定是否以原 ID 重试的一方。

## 当前未覆盖

- TLS/mTLS、令牌轮换、同表租户隔离和请求速率/CPU/内存配额；多后端租户绑定及连接额度已提供，真实存储多租户验收待补；
- 协议双版本滚动窗口；
- 跨 PostgreSQL database/cluster 的分布式事务；
- Outbox 下游消费组和消费者实现；
- 历史归档、其他表分区和物理分库。`dbproxy_snapshots` 的库内 HASH 分区对协议透明。
