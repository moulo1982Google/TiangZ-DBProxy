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

## 四类 RPC

### LoadSnapshot

读取Redis缓存，失败或未命中时回源PostgreSQL。返回`None`表示权威库中没有记录，不是网络错误。

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

## 当前未完成

- 批量Load/Save，减少大量登录恢复时的RPC开销
- Prometheus延迟、错误码、连接、存储分片和backlog指标
- 客户端自动重连、健康检查和连接池熔断
- 生产TLS/mTLS、令牌轮换、租户隔离和限流
- 生产Docker镜像、滚动升级和协议双版本窗口

运行时无关TypeScript SDK和TiangZ首个Player Snapshot Repository已经完成。后者位于TiangZ主仓库，只通过本协议与SDK接入，不让DBProxy反向依赖游戏领域代码；关键经济事务尚未接入。
