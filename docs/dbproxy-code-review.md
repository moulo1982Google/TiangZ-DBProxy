# DBProxy 代码审视记录

本轮审视范围包括 Core 契约、PostgreSQL/Redis 适配器、网络协议、服务端 worker、Rust 客户端、MemoryBackend、TypeScript SDK、配置、监控和本地故障工具。目标是查找不安全、重复、过度抽象或难以运维的实现。后续增加的快照 HASH 分区也沿用同一原则；历史归档、其他表分区和物理分库不在本轮扩张。

## 已修复的高风险问题

### 跨 API 的首次创建 CAS 窗口

多记录/交易事务原先先 `SELECT ... FOR UPDATE`，再使用无条件 `ON CONFLICT DO UPDATE`。对于还不存在的 RecordKey，行锁并不存在；如果单记录 API 在两步之间先插入 revision 1，多记录事务可能覆盖该行并同样报告 revision 1。

现在多记录和交易的最终 mutation 再次携带 expected Revision 条件，并检查 `RETURNING revision`。无论竞争者是否参与 advisory lock，只有一个 expected-revision-zero 写入能提交，另一个整笔回滚。真实 PostgreSQL 集成测试并发混合 single/multi API 验证“恰好一个成功”。

批量快照集成测试随后又覆盖了普通 `SaveSnapshot` 的缺失行分支：旧 `INSERT ... ON CONFLICT DO UPDATE ... WHERE expected` 只把 CAS 谓词应用于冲突更新，`expected_revision=9` 写一个不存在的 key 仍会直接 INSERT。第一版修复把条件放到 `INSERT ... SELECT` 的源行，却因此让合法的非零 expected revision 根本到不了 `ON CONFLICT`；500 玩家启动门用真实周期快照发现了该问题。最终 SQL 使用同一语句内的条件 `UPDATE` CTE 和仅允许 expected 为空/0 的 `INSERT ... ON CONFLICT DO NOTHING`，并为无条件创建竞争保留 blind-upsert 收尾。真实 PostgreSQL 集成测试同时锁定“缺记录+非零 expected 必须冲突”和“已有 revision 1+匹配 expected 必须更新”两条边界。

### PostgreSQL 重启后连接永久失效

原连接断开后没有恢复路径，故障测试只能新建 Store。现在每个 PostgreSQL 分片和维护连接保存连接地址，发现 `Client::is_closed()` 后进行 2 秒有界重连。正在执行且结果未知的调用仍失败；系统不会在底层自动重放写入，调用方用原幂等 ID 重试。

### 数据库提交后的缓存模糊错误

原逻辑可能在 PostgreSQL 已提交、Redis 更新失败后返回 `CacheSync`，调用方难以判断权威结果。现在缓存修复目标与权威数据同事务提交；Redis 失败只延迟缓存同步，客户端仍收到明确的 PostgreSQL 提交结果。worker 自动重试并死信告警。

### 快照幂等内容比较不完整

`SnapshotWrite.updated_at_unix_ms` 原先没有进入 PostgreSQL 幂等回执，同一 request ID 可以改变时间戳而仍被视为相同请求。迁移 007 增加可空兼容字段；新回执保存并比较时间戳，旧回执继续按旧语义读取。

### operation ID 跨事务类型碰撞

single、multi、trade 原先分别拥有主键表，同一 operation ID 可以被不同 API 重用。`dbproxy_operation_claims` 现在先占用事务类型；类型不同立即返回 `OPERATION_CONFLICT`，失败事务会一起回滚 claim。

### AOF ACK 与自动重连

仅执行 Redis 写命令不能证明 AOF 已落本机。backlog 入队和 Outbox 发布现在调用 `WAITAOF`，AOF 未启用或超时会明确失败。Redis 连接统一使用 `ConnectionManager`，重启后 worker 可以继续重试。

### AOF 恢复旧缓存造成短暂旧读

外网故障演练复现了 31 次低于客户端已确认 Revision 的读取。PostgreSQL 权威值和持久 cache-repair 都没有丢；问题是缓存与可靠队列共用 AOF Redis：Redis 停机期间的新写入只进入 PostgreSQL，重启后 AOF 又恢复了旧缓存值和仍处于 fresh 的时间标记，repair worker 修复前的读取会把它当成新值返回。revision-aware 写只能阻止在线旧写覆盖新写，无法识别一次进程重启恢复出的旧内存。

配置现在允许用`cacheRedisUrlEnv`指定独立快照缓存，`redisUrlEnv`继续承载 AOF backlog 与 Outbox。外网部署的缓存实例关闭 AOF、RDB并使用易失`tmpfs`；每次重启都是空缓存，读取安全回源 PostgreSQL。单 Redis 配置继续向后兼容，但不再作为严格 read-after-write 的故障恢复部署。故障驱动分别强杀两个 Redis，soak 自身和最终审计均要求`readsBehindAcknowledgedRevision=0`，不能再用“最终对账恢复”掩盖运行期间的旧读。

### Outbox 年龄与分区乱序

原队列使用业务传入的 `occurred_at` 计算积压年龄，错误时钟会制造虚假告警；多个 worker 也可能同时越过同一分区的前序事件。现在年龄基于 PostgreSQL `created_at`；写入前按 `topic + partition_key` 取得事务级 advisory lock，claim 只允许该分区最早的未发布事件。这样也封住了“较早事务尚未提交、较晚事务先被 worker 看见”的窗口。前序死信会阻塞后序，必须先定点修复，不能静默产生事件缺口。

### 缓存故障等待被串行放大

Redis 断线时，读路径原先可能依次等待初次读取、锁内复查、分布式锁和回填；每一步单独有界，但总延迟会叠加。现在一旦确认是 Redis 连接错误或缓存命令超时，本次请求就跳过后续 Redis 协调步骤，直接执行有界 PostgreSQL 回源。真实停容器演练验证了该路径。

### 正确性驱动截止前忙循环

玩家循环原先只在“完整下一周期仍早于 deadline”时 sleep，却没有在最后不足一个周期时退出，于是会在截止前忙循环，制造与正式频率无关的末秒请求尖峰。现在下一周期越过 deadline 就直接结束；最终对账仍在所有玩家退出后执行。

### 内存后端与 PostgreSQL 语义漂移

MemoryBackend 的快照幂等指纹曾遗漏 `updated_at_unix_ms`，账本 Posting ID 和 Outbox Event ID 也只在单个 trade shard 内唯一。现在时间戳进入指纹，交易标识符使用全局集合，跨 shard 冲突与 PostgreSQL 全局主键保持一致。新增测试覆盖两种漂移。

### 锁键边界碰撞和网络标识符上限

记录 advisory lock 曾使用简单的 `namespace:key` 拼接，包含冒号的合法键可能产生确定性别名，并与交易锁命名空间重叠。现在所有事务锁键都包含 scope 和每段字节长度。网络分发层也补齐了旧 `LoadTransaction`、`ApplyMultiTransaction`、`LoadMultiTransaction` 的 operation ID 长度检查，避免把一个接近 frame 上限的文本直接送入数据库索引查询。

### 数据库不可变边界

账本原先用行级触发器拒绝 UPDATE/DELETE，但 `TRUNCATE` 不触发行级触发器；Outbox 排序依赖的 `created_at` 也未包含在不可变字段中。现在账本同时拒绝 TRUNCATE；Outbox 禁止修改 `created_at`、删除未发布事件或 TRUNCATE。已发布事件仍可由以后明确设计的保留策略按行删除。

### 协议版本双写漂移

Rust 已升级协议 v2，但 TypeScript 生成脚本仍硬编码 v1，测试实际捕获了漂移。生成器现在从 Rust `PROTOCOL_VERSION` 提取权威值，并继续从 proto 计算 SHA-256 指纹。

Windows 工作树中的 proto 曾混有 CRLF/LF，导致相同 schema 在不同 checkout 产生不同指纹。Rust build script 与 TypeScript generator 现在都先规范化为 LF。已部署的 v2 TiangZ 二进制仍携带旧`d20f…e4f1f`，所以新 server 只为协议版本 2 精确接受这一个旧别名并原样回显，支持先升级 server 的滚动窗口；任意其他指纹仍拒绝。新客户端不会对旧 server 降级，必须等所有 server 升级后再发布。

### 分区配置静默漂移

只修改 `CREATE TABLE IF NOT EXISTS` 会让已有普通 `dbproxy_snapshots` 被静默保留，服务虽然启动成功，实际却没有分区。现在迁移先拒绝旧普通表，Rust 启动检查再验证父表分区键以及 32 个子表的名称和边界。分区数量没有加入实例 JSON，也没有在请求路径动态建表，避免多个实例配置不一致或运行时 DDL。

真实并行集成测试还发现：仅用 advisory lock 串行“整套幂等 DDL”并不够。先完成迁移的连接可以立即开始业务写入，而排队的下一个连接随后重跑 `CREATE TABLE`/触发器 DDL，仍可能与业务行锁形成锁环。现在 `dbproxy_schema_migrations` 在同一事务记录 001 到 007；后续连接只读取版本并验证快照布局，不再重复执行已提交 DDL。

### 普通快照逐条提交与恢复积压放大

`SaveMultiSnapshot` 原先只是协议层批量，服务端仍逐条开启 PostgreSQL transaction、逐条更新 Redis、逐条确认缓存修复；backlog 恢复也一次只领取一条。很多玩家同时周期保存时，主要浪费在 commit/WAL flush 和网络往返，而不是 Payload 编码。

现在普通快照按连接 shard 共享一次 PostgreSQL commit，保留逐条 Revision/幂等结果；成功项再通过一次 revision-aware Redis Lua 和一条修复队列 ACK 同步缓存。backlog worker 每轮最多领取、提交并 ACK/release 64 条。Revision/幂等冲突只影响对应条目，SQL/连接错误回滚所在 shard 的整批并依靠原 request ID 重试。关键交易仍走原来的原子事务 API，没有把独立周期快照批处理伪装成业务原子性。

交易和 multi transaction 的 PostgreSQL 原子边界未改变，但提交成功后不再逐记录回读 PostgreSQL再逐条刷新缓存；首次应用可直接从已提交请求和返回 Revision 构造缓存快照。幂等重复仍批量读取当前权威版本，避免旧请求 Payload 覆盖已经推进的新 Revision。

## 已收敛的冗长/过度设计

### 构造函数爆炸

`TieredSnapshotStore` 曾出现六层 `connect_with_cache_config_and_metrics_and_circuit_and_lock_and_cache_policy` 链，服务端又重复一套。现在只有默认 `connect` 和显式 `connect_with_config`：

- `TieredSnapshotStoreConfig` 聚合 fallback、circuit、lock、cache；
- `StorageBackendConfig` 聚合 shard 数和 tiered 配置。

新增缓存参数不再要求增加一层构造函数，也减少测试和 main 中的位置参数错误。

### 功能继续堆进大文件

持久修复队列、Outbox 和交易实现分别位于 `cache_repair.rs`、`outbox.rs` 和 `trade.rs`。公共的最终 CAS mutation 提取为一个 helper，避免 multi 与 trade 复制易出错 SQL。没有为了未来数据库方言引入暂时无用的 Repository/Factory 层。

### 后台任务阻塞请求连接

队列 claim/stats 原先可能复用请求 shard。现在缓存修复和 Outbox 共享一条专用维护连接；耗时 Redis/快照处理发生在 PostgreSQL mutex 外。请求 shard 不被监控轮询或队列领取占用。

### 重复的指标输出参数

缓存修复与 Outbox 指标曾通过八个位置参数传给同一输出函数，字段顺序容易错。现在用一个只保存引用的 `DurableQueueMetricRefs` 聚合传递，不新增运行时所有权或通用指标框架。

### 死信批量操作风险

没有提供“清空全部死信”的便利接口。重放要求指定 RecordKey 或 event ID，迫使运维先检查 `last_error` 和内容，降低故障期间误操作范围。

## 保留的设计及理由

- PostgreSQL 仍是唯一权威数据源；Redis 提供快照缓存与 AOF backlog/Stream 两种职责，但严格恢复部署使用独立实例隔离持久性。它们不被抽象成虚假的通用消息总线。
- Outbox 是至少一次而不是恰好一次。跨 Redis 与 PostgreSQL 声称恰好一次需要分布式事务，复杂且不真实；稳定 event ID + 消费者去重是明确契约。
- Worker 使用数据库租约和 `SKIP LOCKED`，不引入单独协调服务。租约丢失只产生安全的重复工作。
- 账本允许空 Posting 列表，以支持不涉及资产的交易状态变化；只要存在 Posting，就按每种 asset 强制零和。
- MemoryBackend 保留 Revision/CAS/幂等、全局交易标识符和多记录原子语义用于协议/网络测试，但明确是易失实现，不模拟 PostgreSQL 触发器、Outbox worker 或 Redis AOF。

## 已知边界，不在本轮扩张

- PostgreSQL 当前每个 shard 是一条串行 Client，不是动态连接池；容量应先通过真实指标决定是否更换池实现。
- 当前没有只读副本路由。Revision/CAS、事务回执、交易、账本、Outbox 和 read-after-write 都必须读主库；以后即使增加 replica，也只能给明确允许陈旧的查询单独建 API，并依据 replay lag 自动回主，不能把现有 `LoadSnapshot` 静默改成读从库。
- 真实存储的 `/ready` 要求 PostgreSQL 与可靠队列 Redis 最近一次采样都健康，`/dependencies` 和 `dbproxy_dependency_up` 可定位这两个持久依赖；易失快照缓存失败通过缓存错误/回源指标观测并安全降级，不令实例退出 Ready。状态转换存在最长约一个 5 秒采样周期，不承诺瞬时故障检测。
- Rust 客户端仍保留共享连接的 `connect(size)` 兼容入口；故障敏感调用方应显式使用 `connect_split(read_size, write_size)`。两种模式的单条连接仍串行，没有在协议中引入多路复用。
- Redis backlog 已把 enqueue、worker lease/ACK 和 stats 拆为三条连接；每个角色内部仍使用 mutex 保证脚本与 `WAITAOF` 的顺序。高吞吐路径继续优先使用批量 API，是否增加 enqueue 连接分片由正式延迟数据决定。
- Redis Stream 没有在 publisher 中强制裁剪，因为盲目 `MAXLEN` 可能在消费者落后时丢事件；消费组和保留策略必须由部署明确配置。
- 账本没有余额聚合 API，也没有替业务校验透支/物品所有权。
- 共享令牌不是零信任方案；TLS/mTLS、轮换、租户配额属于生产安全建设。
- 当前仅 `dbproxy_snapshots` 使用固定 32 个库内 HASH 分区；历史表保留、归档、其他表分区和物理分库仍按要求延期。
- PostgreSQL 方言是当前明确边界：advisory lock、`SKIP LOCKED`、`unnest`、原生 HASH 分区、触发器和 `ON CONFLICT ... RETURNING` 均参与正确性。MySQL 8.4 不是替换连接串即可支持；若以后确认需要，应实现并独立验收一套 storage backend/migration/故障矩阵，而不是在现有 SQL 中堆方言分支。

上述故障期边界、随后改进及实测数据见[100 玩家两小时故障演练报告](fault-soak-report-2026-08-27.md)。
