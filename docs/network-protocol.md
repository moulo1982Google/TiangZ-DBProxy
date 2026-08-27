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

第一帧必须是 `ClientHello(protocol_version, protocol_fingerprint, auth_token, client_name)`。当前握手版本是 2，fingerprint 是权威 proto 文件的 SHA-256。任一不匹配都不会进入 RPC 调度。proto 的 package 名保留 `tiangz.dbproxy.v1` 只是生成代码命名空间；兼容性由握手版本和指纹共同决定。

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

一个 `DbProxyClient` 连接内只有一个在途请求。请求写出后超时会废弃连接，防止后续 RPC 读取旧响应。`DbProxyClientPool::connect` 按 RecordKey 在一组共享读写连接中稳定路由，保持原有连接数和顺序语义；`connect_split(read_size, write_size)` 使用两组物理连接，读查询进入 read pool，写入、事务和 enqueue 进入 write pool，避免慢写造成跨用途队头阻塞。它不替代业务锁或 revision/CAS。服务端再按记录或 operation ID 路由到独立存储 shard。

Rust 客户端接收有序 Endpoint 列表。连接建立失败、超时或断开才切换；Revision/Operation/Trade 等确定性远程错误不会触发切换。所有候选都必须共享同一 PostgreSQL/Redis，否则幂等和 Revision 契约不成立。

PostgreSQL 已断连接在下一次操作前做 2 秒有界重连；当前失败写不在底层自动重放。Redis 使用 connection manager 自动重连。调用方仍是唯一有权根据业务语义决定是否以原 ID 重试的一方。

## 当前未覆盖

- TLS/mTLS、令牌轮换、租户隔离和配额；
- 协议双版本滚动窗口；
- 跨 PostgreSQL database/cluster 的分布式事务；
- Outbox 下游消费组和消费者实现；
- 历史归档、其他表分区和物理分库。`dbproxy_snapshots` 的库内 HASH 分区对协议透明。
