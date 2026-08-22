# DBProxy 网络协议

## 目标

网络层把 DBProxy 从“可引用的 Rust 存储库”变成独立进程，同时保持三个边界：

1. TiangZ 只依赖 SDK 和版本化协议，不依赖 Redis、PostgreSQL 或`dbproxy-storage`。
2. DBProxy 只识别`RecordKey / Schema / Payload / Revision`，不识别 Scene、Entity、Buff 或任务。
3. 网络失败不会改变幂等语义；重试必须携带原`request_id`或`operation_id`。

## 帧与握手

TCP 帧格式：

```text
[4-byte big-endian payload length][protobuf payload]
```

默认单帧上限是 8 MiB。服务端在分配Payload前校验对端声明的长度；超限连接直接关闭。快照长期超过上限时应拆分业务领域记录，不能持续放大网络缓冲。

每条连接的第一帧必须是`ClientHello`：

```text
protocol_version
protocol_fingerprint  // dbproxy.proto 的 SHA-256
auth_token
client_name           // 只用于日志，不参与授权
```

版本或指纹不一致返回`PROTOCOL_MISMATCH`；令牌不一致返回`UNAUTHORIZED`。认证成功后再接受RPC。当前共享令牌只解决内部服务最小鉴权，尚不包含租户配额、证书轮换或mTLS。

## 八类 RPC

### LoadSnapshot

读取Redis缓存，失败或未命中时回源PostgreSQL。返回`None`表示权威库中没有记录，不是网络错误。

### LoadMultiSnapshot

一次读取1至64条不重复的`RecordKey`，用于恢复由多个持久化领域组成的玩家或普通Entity。响应条目数量和顺序与请求严格一致；权威库中不存在的记录保留空条目，不能被压缩掉。客户端必须校验响应数量和每个快照的身份，避免错位应用领域数据。

真实存储后端会按存储shard分组并行读取，不同shard互不阻塞；每个shard使用一次Redis `MGET`，并把全部缓存未命中记录合并为一次PostgreSQL多键查询。这样玩家拥有二三十个持久化领域时，网络、缓存和权威库都不会退化为逐领域串行往返。

### SaveSnapshot

用于需要调用方等待落库的普通快照：

```text
PostgreSQL事务提交
-> Redis缓存刷新
-> 返回Applied或Duplicate
```

缓存刷新失败会返回`STORAGE_UNAVAILABLE`。此时PostgreSQL可能已经提交，调用方必须用原`request_id`重试；DBProxy命中幂等收据后修复缓存。

### EnqueueSnapshot

用于允许小范围回退的合并快照。成功响应只表示Redis AOF backlog已经接收：

```text
EnqueueSnapshot ACK
-> 后台worker领取lease
-> SaveSnapshot到PostgreSQL
-> ACK backlog
```

货币、背包、奖励和交易禁止使用该接口。

### ApplyTransaction

用于单记录关键事务。PostgreSQL在一个事务中保存：

```text
operation_id
expected_revision
提交后的完整快照
第一次提交的业务result
```

网络超时后用原`operation_id`重试，返回第一次保存的Revision和result，不会重复执行。

### LoadTransaction

按`operation_id + RecordKey`读取已经提交的事务回执，返回第一次提交保存的`new_revision/result`。它只用于恢复“PostgreSQL已提交，但调用方在收到响应或应用内存状态前崩溃”的窄窗口；记录不匹配返回`OPERATION_CONFLICT`，不存在返回`None`。DBProxy不会解释result，也不会替业务判断是否应该继续执行操作。

### ApplyMultiTransaction

用于同一 PostgreSQL 权威库内的跨记录关键事务。请求携带一个 `operation_id`、最多 256 条不重复的 `TransactionalRecordWrite` 和不透明的业务 `result`：

```text
1. 按 namespace + key 排序
2. 按固定顺序获取 PostgreSQL transaction advisory lock
3. 锁定并校验全部记录的 expected_revision
4. 任意一条冲突则整组回滚
5. 全部通过后一次性写入全部快照和回执
```

客户端在第一个 Endpoint 断开后会切到下一个 Endpoint，并复用同一个 `operation_id`。如果第一次已经提交，备用实例会返回 `Duplicate` 和原始 result；如果第一次尚未提交，备用实例会正常完成提交。两个实例不需要互相同步，必须共享同一个 PostgreSQL 和 Redis。

这个接口适合跨玩家交易、玩家转账、共享奖励转移等“同库多记录”场景；业务层仍负责余额、背包、权限和任务条件校验。DBProxy 只负责整组 CAS 与持久化，不负责业务补偿。

### LoadMultiTransaction

按 `operation_id + 记录集合`读取跨记录事务回执。记录集合必须与原提交完全一致，顺序不影响比较；集合不同或回执损坏返回 `OPERATION_CONFLICT`。不存在时返回 `None`。它用于应用在提交后崩溃、只留下 operationId 的恢复流程。

## 错误码

| 错误码 | 含义 | 调用方处理 |
|---|---|---|
| `INVALID_REQUEST` | 缺字段、空ID、错误的积压请求 | 修代码，不重试 |
| `UNAUTHORIZED` | 内部令牌不匹配 | 修部署密钥 |
| `PROTOCOL_MISMATCH` | 版本或协议指纹不一致 | 部署匹配SDK |
| `REVISION_CONFLICT` | CAS版本落后 | 读取`actual_revision`，重新加载并由业务决定 |
| `IDEMPOTENCY_CONFLICT` | 同一request_id被用于不同请求 | 调用方ID生成或重试逻辑错误 |
| `OPERATION_CONFLICT` | 同一operation_id被用于不同事务 | 调用方事务ID使用错误 |
| `STORAGE_UNAVAILABLE` | PostgreSQL/Redis当前不可完成请求 | 保持原幂等ID，退避重试 |
| `INTERNAL` | 服务端不变量被破坏 | 告警并人工排查 |

## 并发与超时

一个`DbProxyClient`连接内请求串行执行。超时发生在请求已经写出、响应尚未完整读回时，该连接会被标记为不可复用，防止下一次RPC错误消费旧响应。调用方重新连接后按原幂等ID重试。

`DbProxyClientPool`创建多条真实连接，并按`RecordKey`稳定路由。同一记录自然串行，不同玩家或领域记录可以并行。服务端`DBPROXY_STORAGE_SHARDS`控制独立PostgreSQL/Redis连接分片数；它是启动配置，不支持热更。

### Endpoint 故障切换

Rust 客户端 `ClientConfig::with_endpoints` 接收有序地址列表，主地址由 `endpoint` 指定，备用地址放在 `failover_endpoints`：

```rust
let config = ClientConfig::new("127.0.0.1:7800", token, "map-1")
    .with_endpoints(["127.0.0.1:7801".to_string()]);
```

连接建立失败、读取超时或连接关闭时，客户端按顺序尝试下一个地址；远程 `REVISION_CONFLICT`、`OPERATION_CONFLICT` 等业务错误不会触发切换。写操作切换时必须使用原 `request_id`/`operation_id`，不能在 Transport 层生成新 ID。所有候选地址都不可用时，才把最后一个连接错误返回给业务。

## 当前未完成

- 批量Load/Save，减少大量登录恢复时的RPC开销
- Prometheus延迟、错误码、连接、存储分片和backlog指标
- 健康检查、熔断窗口和恢复探测指标（当前已有连接失效后的顺序切换）
- 生产TLS/mTLS、令牌轮换、租户隔离和限流
- 生产Docker镜像、滚动升级和协议双版本窗口

运行时无关 TypeScript SDK 已支持多记录事务，TiangZ首个Player Snapshot Repository仍通过单记录通用 Repository 接入；跨玩家交易等领域 Repository 应显式调用多记录接口，不应把它偷偷塞进普通Entity Repository。
