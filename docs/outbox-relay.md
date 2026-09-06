# 通用 Outbox Relay 与 Publisher

日期：2026-09-06。当前为本地开发修改，未部署到远程七天演练。真实 PostgreSQL/Redis 验收尚待独立环境执行，不能把编译和内存测试当作真实存储验收。

## 本轮范围与归属

DBProxy 负责版本化记录与事件的原子提交、固定投递目标、租约、发送确认、退避、死信、诊断与审计。事件内容和生产者决策属于游戏领域；新增消费者不需要修改 DBProxy，也不新增消费者专用 Outbox。

本轮使用同一张 `dbproxy_outbox`，逻辑生产者通过 `producer` 区分。Redis 是唯一已实现的 MQ 后端；Kafka/RabbitMQ 仅能以 `enabled:false` 声明，启用会在联网前报“不支持”。不实现物理多表、任意外部表扫描、Kafka/RabbitMQ 驱动或批量历史广播重放。没有动态插件装载。

## 配置

完整示例：[outbox-relay.example.json](../configs/outbox-relay.example.json)。配置仍为严格 camelCase JSON，编辑器 schema 为 `configs/dbproxy.schema.json`。

```json
{
  "outbox": { "workers": 1, "leaseMs": 30000, "maxAttempts": 20 },
  "outboxRelay": {
    "publishTimeoutMs": 5000,
    "defaultPublisher": "events-redis-v1",
    "publishers": [
      { "id": "events-redis-v1", "backend": "redisStream", "connectionEnv": "DBPROXY_EVENTS_REDIS_URL" },
      { "id": "future-kafka-v1", "backend": "kafka", "connectionEnv": "DBPROXY_KAFKA_CONFIG", "enabled": false }
    ],
    "sources": [
      { "producer": "game", "version": 1, "destination": "game.events" },
      { "producer": "achievement", "version": 1, "publisher": "events-redis-v1", "destination": "achievement.events" }
    ]
  }
}
```

- `defaultPublisher` 提供全局默认；每个来源可以显式覆盖。启用来源不能引用缺失或禁用的 Publisher。未来来源可先配置 `enabled:false` 指向已声明的禁用 Kafka/RabbitMQ Publisher，离线校验其结构，但不注册到数据库、不投递消息。
- `connectionEnv` 只保存环境变量名。禁用 Publisher 不读取密钥，也不打开连接。未来 Kafka 的具体认证/确认等参数在驱动实现时再扩展，不接受任意透传配置。
- Publisher ID/producer 为 1–64 个 ASCII 字母、数字、`_` 或 `-`；`legacy` 保留。最多 32 个 Publisher 声明、64 个来源版本。
- 投递目标是完整 Stream 名称，不再临时拼接前缀。`dbproxy:outbox:` 保留给旧事件，不能用于新来源。
- `publishTimeoutMs` 为 3000–60000，并至少比租约短 1000ms；它包含连接等待、发送与 AOF 确认。期限耗尽表示结果可能未知，不表示 MQ 一定没收到。
- Memory 后端不能激活 Publisher/来源；它只验证易失记录契约，不投递到 MQ。
- 不开放 `table` 字段，避免把未实现的多表功能伪装为可用配置。

离线检查不会联网、迁移数据库或读取环境密钥：

```powershell
cargo run -p tiangz-dbproxy-server -- --check-config configs/outbox-relay.example.json
```

离线检查通过只说明结构、引用和能力约束正确。正式启动仍检查环境变量、连通性以及数据库中的不可变路由绑定。

## 事件格式与 SDK

Rust `EventEnvelope::into_outbox()` 和 TypeScript `CreateOutboxEvent()` 生成相同语义的信封：

```json
{
  "event_id": "evt-123",
  "producer": "game",
  "event_type": "DocumentChanged",
  "aggregate_type": "document",
  "aggregate_id": "document-1001",
  "partition_key": "document-1001",
  "schema_version": 1,
  "content_type": "application/json",
  "payload": [123, 125],
  "occurred_at_unix_ms": "1788652800000",
  "route_version": 1
}
```

`payload` 是字节数组，上例表示 UTF-8 `{}`；也可携带 Protobuf 等二进制，DBProxy 不解释内容。64 位时间使用十进制字符串，避免 JavaScript 精度损失。数据库 `created_at` 独立记录入队时间。现有 payload 上限约束完整编码后的信封，不仅约束内层业务字节。

为复用已经发布的 `OutboxEvent` 字节契约，SDK 将信封放入原 payload，并生成保留逻辑 topic `dbproxy.relay.v1.{producer}.{route_version}`。这是版本化路由键，不是最终 MQ 地址，字符集遵循现有 topic 约束。调用方使用 helper，不手工拼接；保留 topic 的格式、ID、分区键、时间不符时拒绝，绝不降级为旧 Stream。旧 Trade API 不接受新信封，应使用 `CommitRecords`。

`CommitRecords` 把记录 CAS、效果幂等回执、不可变事实和事件一起提交。新路由键必须已经注册，未知路由导致整笔回滚。相同 operation ID 重试仍比较完整原始效果字节；不能重新生成 ID、时间或重新编码不同的请求。跨语言 JSON 编码允许语义相同但字节不同，恢复时应保留原请求，而非跨语言重建幂等请求。

Redis 的新事件保留原兼容字段，并额外使用 `event` 字段携带完整信封。未来其他 Publisher 应发送相同信封。事件信封一致并不意味着不同 MQ 的分区、顺序、确认或保留语义相同。

握手新增 `supports_outbox_relay`，协议指纹随契约变化更新；新 Rust SDK 不接受缺少该能力的服务。服务端仍接受三份准确旧指纹并回显，以兼容旧快照/交易客户端。TypeScript Transport 必须显式声明经过验证的 `supportsOutboxRelay:true` 才能提交新信封；旧 TiangZ Host 没有此能力声明，会明确拒绝新信封，但旧交易审计/事件仍可用。不能为了通过检查而给未经验证的旧 Host 随意加 `true`。

## 持久路由与升级约束

迁移 009 新增 Publisher/路由注册表、投递目标、入队序号、租约令牌、过期计数和管理审计。它不删除旧交易表、不重建快照分区。

Publisher ID 固定连接协议、地址和 Redis DB 编号的摘要，不保存连接密码；同地址的凭据轮换不会改变摘要。同 ID 换地址/DB 会拒绝启动。更换集群使用新 Publisher ID；同 producer/version 换 Publisher 或 destination 也会拒绝，必须增加路由版本。DNS 名称背后的集群身份与数据连续性仍是部署责任，地址摘要不是远端集群身份证明。

插入 Outbox 的数据库触发器在业务事务内从不可变注册表填充最终 Publisher 和目标，并禁止后续改写。旧事件固定保留 `legacy` Publisher 和 `dbproxy:outbox:{topic}`。重复操作不重新解释新配置。worker 只使用事件行的持久目标。

已注册的旧路由仍可被旧调用方使用，所以启动时要求保留其 Publisher 配置，即使眼下没有积压；不能通过删配置悄悄改投。路由退役/元数据回收是后续单独管理流程。

来源的 `enabled:false` 仅供提前声明尚未注册的未来路由；对已经注册的相同来源/版本改为 false 会拒绝启动，不能把它当作已有队列的停用/取消开关。

先升级所有 DBProxy 节点与 worker，再启用新 SDK/来源。混跑旧 worker 不安全；新数据写入后不能直接回滚旧二进制。迁移时为存量事件按原 `created_at,event_id` 排序初始化序号，迁移耗时与队列规模需在部署前评估。当前远程演练不执行这些操作。

## 可靠性和顺序

Relay 核心负责领取/失败/死信；`Publisher` 只负责发送与确认。Redis 实现使用独立的受控连接：`XADD` 后在同一连接执行 `WAITAOF 1 0 2000`；只有本地 AOF 确认成功才尝试 PostgreSQL ACK。发送错误或取消会丢弃该连接，再次投递重新建立连接，避免在自动重连后的另一连接上确认旧写入。

这是至少一次，不是恰好一次：MQ 已收到而 PG ACK 丢失时会重复投递。本地 AOF 确认不是 Redis 多副本容灾保证，也不是消费者处理完成。

每次领取递增 `lease_token`；ACK/fail 同时检查 worker、token 和未过期租约，旧任务不能确认新租约。发送采用有界期限，不增加自动续租；超时或 PG 不可用后由租约回收。失败使用有上限的指数退避和按事件/次数确定的抖动，达到上限进入死信；永久格式错误直接死信。

同一 Publisher、destination、partition_key 按入队序号领取；写入端按同一目标取得事务级锁。前序死信继续阻塞后序，不能静默越过。至少一次和失效在途请求仍可能带来迟到的重复消息；需要严格业务序列的消费者仍应检查领域 revision/sequence，不宣称任意故障下的全局严格顺序。

允许多个 producer 或路由版本共用同一 Publisher/destination（扇入）。新版事件先解析持久路由，再按实际目标与 partition_key 加锁，不按原始 topic 各自加锁；同组前序死信也会阻塞其他来源的后续事件。不同 Publisher ID 是不同排序组，即使它们实际连接同一 Redis Stream，也不保证跨 ID 的组内顺序。

消费者必须把 inbox 去重记录与业务变更放进自己的本地事务，提交后再 ACK。生产者 Outbox 不能替消费者完成这件事。多个不同消费组分别收到消息，同组实例分担处理。

## 管理与可观测性

管理通过独立 CLI，不启动 DBProxy worker、不迁移 schema，也不开放新 HTTP 端口。权限边界是运维机器访问权与 PostgreSQL 账号；生产部署应使用单独受限角色。`operator` 是审计说明，不冒充经过认证的用户身份，审计同时记录数据库 `current_user`。

```powershell
cargo run -p tiangz-dbproxy-server --bin dbproxy_outbox_admin -- --postgres-url-env DBPROXY_ADMIN_POSTGRES_URL list
cargo run -p tiangz-dbproxy-server --bin dbproxy_outbox_admin -- --postgres-url-env DBPROXY_ADMIN_POSTGRES_URL inspect evt-123
cargo run -p tiangz-dbproxy-server --bin dbproxy_outbox_admin -- --postgres-url-env DBPROXY_ADMIN_POSTGRES_URL retry evt-123 operator-name "已修复 AOF 配置"
```

列表最多 100 个死信 ID；inspect 不输出游戏 payload。retry 只处理未发布、已死信且无有效租约的原事件；必须提供操作人和原因，保留原目标。审计与重入队同事务，重复执行不会再重置处理中事件。兼容的 `requeue_dead_letter` 也经过审计。没有按时间范围或业务类型批量广播已发布事件的入口。

保留已有全局指标，新增 `dbproxy_outbox_relay_*`：按 producer/publisher 的成功、失败、超时、发送耗时累计、重试、死信、租约丢失、积压、最老年龄和未发布记录中的租约过期数。标签不使用 event_id、玩家 ID、partition_key、目标地址或密钥；未知历史来源汇总到 other。过期数/深度是当前保留记录的 gauge，不冒充永不回退的累计 counter。

数据库中已发布 Outbox 和 Redis Stream 均不是永久事件档案。本轮不自动删除/裁剪任何历史记录；上线前必须确定保留窗口、消费者落后告警与磁盘预算。历史归档和批量重放仍属暂缓项，不能无限留存后宣称七天不会占满磁盘。

## 验证与未验收项

2026-09-06 本地验证结果：

- `cargo test --workspace --locked -j1`：89 项通过，20 项外部环境测试保持 ignored。
- `cargo clippy --workspace --all-targets --locked -j1 -- -D warnings`：通过。
- `npm run test:typescript`：12 项通过；示例配置 `--check-config` 离线预检通过。
- WoW335 的 `npm test`、`npm run test:tiangz-module` 通过；固定旧 SDK 的 `dbproxy_network` 测试连接本轮临时 Memory 服务，重连、幂等与 CAS 验证通过。临时服务已关闭，WoW335 工作区未修改。这不等于真实 PostgreSQL/Redis 或完整游戏链路验收。
- 本轮未新增 TiangZ 修改，未启动 Docker，未部署、重启或修改远程七天演练，未提交或推送。

- Core/TypeScript 使用共享事件 fixture，检查二进制 payload、Unicode、64 位时间与裸 V8 编码；未验证 Relay 的 Host 必须拒绝新信封。
- 网络回归检查旧协议指纹和旧快照调用；Publisher 的本地 RESP fixture 检查 AOF 确认失败/发送取消后重新连接与重复发送。这不是对真实 Redis 持久性的替代验证。
- `crates/dbproxy-storage/tests/outbox_relay.rs` 提供独立环境验收：原子回滚、路由防漂移、租约令牌、发布后 ACK 丢失、多消费组/inbox 幂等、死信阻塞及管理审计。只显式执行：

```powershell
cargo test -p tiangz-dbproxy-storage --test outbox_relay -- --ignored --test-threads=1
```

需要专用 PostgreSQL 和开启 AOF 的 Redis >=7.2，测试使用唯一来源/Publisher 限定领取，不消费其他来源。测试保留故障证据，由独立测试环境统一销毁。本机原生 Redis 5.0 不满足要求，本轮没有启动 Docker，也不借用远程七天演练。该真实存储验收未执行，不作为已通过项目记录。
