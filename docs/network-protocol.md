# DBProxy 网络协议 v2

## 边界

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

第一帧必须是 `ClientHello(protocol_version, protocol_fingerprint, auth_token, client_name)`。当前握手版本是 2，fingerprint 是权威 proto 先统一为 LF 后的 SHA-256。server 精确接受当前指纹及 `LEGACY_PROTOCOL_FINGERPRINT_V2`、`PRE_COMMIT_PROTOCOL_FINGERPRINT_V2`、`PRE_RELAY_PROTOCOL_FINGERPRINT_V2` 三个已知旧指纹，并向连接回显客户端原值；具体清单以 `dbproxy-protocol/src/lib.rs` 为准。未列出的指纹或不匹配的版本不进入 RPC 调度。proto 的 package 名保留 `tiangz.dbproxy.v1` 只是生成代码命名空间；兼容性由握手版本和指纹共同决定。

升级方向是服务端先行：先完成服务端及其数据库迁移的受控切换，再更新客户端。新客户端要求精确的版本/指纹和 `supports_outbox_relay`，不会主动降级到旧服务端。A3 的候选跳过只保证继续寻找兼容服务端，不代表旧服务端可以处理新请求，也不保证混版本的存储/队列语义。所有候选均不兼容时应保留明确拒绝原因并修正部署版本，不能放宽握手检查来掩盖问题。

## 十三类 RPC

| RPC | 语义 |
| --- | --- |
| `LoadSnapshot` | Redis 优先，失败/miss 回源 PostgreSQL；缺失返回 None |
| `LoadMultiSnapshot` | 1..64 个唯一 RecordKey，保持顺序和缺失位置；按 shard 并行、每 shard 批量访问后端 |
| `SaveSnapshot` | 幂等 PostgreSQL 快照提交；缓存失败由 durable repair 修复 |
| `SaveMultiSnapshot` | 1..64 条独立普通写，逐条结果、允许部分成功，不提供跨记录原子性 |
| `EnqueueSnapshot` | 普通快照写入 Redis backlog 并取得本机 AOF ACK；尚未落 PostgreSQL |
| `EnqueueMultiSnapshot` | 1..64 条无 CAS 快照通过一个 Lua 脚本原子入 backlog，再等待 AOF |
| `ApplyTransaction` | 单记录关键事务，原子保存快照和第一次业务 result |
| `LoadTransaction` | 按 operation ID + RecordKey 读取单记录事务回执 |
| `ApplyMultiTransaction` | 同一 PostgreSQL 内最多 256 条记录整组 CAS/提交/回滚 |
| `LoadMultiTransaction` | 按 operation ID + 完整 RecordKey 集合读取多记录回执 |
| `ApplyTradeTransaction` | 原子提交交易状态、多记录 CAS、平衡账本、Outbox、修复目标和 Receipt |
| `LoadTrade` | 按 trade ID 读取当前交易版本、状态和 Payload |
| `LoadTradeTransaction` | 按 operation ID + trade ID 读取第一次交易 Receipt |

### 直接写入和缓存

`Save*` 与 `Apply*` 成功表示 PostgreSQL 权威事务已提交。Redis 快速回写失败不会改写这个事实，因为 `dbproxy_cache_repairs` 已在同一事务保存目标；worker 稍后补齐。客户端不需要仅为缓存失败重放权威业务操作。

### Enqueue ACK

`Enqueue*` 在 Lua 入队后执行 `WAITAOF 1 0 2000`。AOF 未启用、超时或 Redis 不可用会返回 `STORAGE_UNAVAILABLE`。成功仍只代表 Redis 本地 AOF 接收；货币、背包、奖励、交易禁止走该路径。

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

一个 `DbProxyClient` 连接内只有一个在途请求。请求写出后超时会废弃连接，防止后续 RPC 读取旧响应。`DbProxyClientPool::connect` 按 RecordKey 在一组共享读写连接中稳定路由，保持原有连接数和顺序语义；`connect_split(read_size, write_size)` 使用两组物理连接，读查询进入 read pool，写入、事务和 enqueue 进入 write pool，避免慢写造成跨用途队头阻塞。它不替代业务锁或 revision/CAS。服务端再按记录或 operation ID 路由到独立存储 shard。

Rust 客户端接收有序 Endpoint 列表。初次连接和故障重连都跳过不可达候选，以及握手阶段的认证拒绝、协议拒绝、指纹或 Relay 能力不匹配；仍严格验证每个候选，不降低协议或认证要求。候选握手拒绝继续记录为 `Rejected`，不会改记为 `Unavailable`；全部候选失败时优先返回拒绝原因，避免被后续网络错误覆盖。端点无关的本地配置错误立即失败。业务 RPC 阶段的 Remote 错误（包括 Unauthorized、Revision/Operation/Trade 冲突）不触发切换。所有候选都必须共享同一 PostgreSQL/Redis，否则幂等和 Revision 契约不成立。

PostgreSQL 已断连接在下一次操作前做 2 秒有界重连；当前失败写不在底层自动重放。Redis 使用 connection manager 自动重连。调用方仍是唯一有权根据业务语义决定是否以原 ID 重试的一方。

## 当前未覆盖

- TLS/mTLS、令牌轮换、租户隔离和配额；
- 协议双版本滚动窗口；
- 跨 PostgreSQL database/cluster 的分布式事务；
- Outbox 下游消费组和消费者实现；
- 历史归档、其他表分区和物理分库。`dbproxy_snapshots` 的库内 HASH 分区对协议透明。
