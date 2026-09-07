# DBProxy 测试补强与工作区审查（2026-09-06）

## 09-07 尾延迟诊断补强

第二步增加固定 10 阶段的存储耗时与 in_flight：缓存读写、回源配额/同键/分布式租约及释放、PostgreSQL 连接等待/操作、已提交缓存同步和修复 ACK。SQL、超时、锁顺序和提交语义不变；Store 的作用域退出（包括失败和取消）保留耗时，计数不能当作成功提交数。PG operation 包含重连与网络，不是纯 SQL 时间；committed_cache_sync 包含缓存写入与 ACK，父子阶段不能相加。

计数共享于请求分片，启动迁移和专用队列维护连接不混入。沿用存储 poller 冷路径导出，固定 180 条新序列，不添加请求日志/数据标签/后端查询。PG/RESP 私有模拟端点通过真实 Store 方法验证 PG 排队取消、查询取消、缓存失败跳过 ACK、缓存成功后 ACK 卡住；另检查配额超时、同键等待、原子聚合和 Prometheus 导出。它们不访问 Docker 数据或远程服务，也不能替代真实故障验收。完整边界见 TiangZ `docs/testing/latency-attribution.md` 的第二步。

第二步指标为 `dbproxy_storage_stage_seconds`（累计 histogram，单位秒）与 `dbproxy_storage_stage_in_flight`（上次存储 poll 时的活动阶段数）。固定 stage：`cache_lookup`、`cache_write`、`fallback_capacity_wait`、`fallback_key_wait`、`fallback_distributed_lease`、`fallback_lease_release`、`postgres_connection_wait`、`postgres_operation`、`committed_cache_sync`、`cache_repair_ack`。取消样本只反映取消前耗时，不能推断服务器 SQL 已结束。重复采集替换累计快照，不重复加总；进程重启会重置计数，应使用 rate/increase 分析时间窗口。

第二步最终验证：workspace 默认 **124 通过、0 失败、27 ignored**（storage 库 21、server 库 26）；三个真实 Store 调用路径的模拟用例另重复 10 轮全部通过。workspace 全目标严格 Clippy、fmt/diff 通过。未跑真实数据库故障和发布/长稳验收；未提交、未部署。以下 SDK 9 / server 25 与 TiangZ quick 23/23 是第一步结果，不能替代第二步数据库故障验收。

SDK 现通过兼容的默认 `request_attempt_timed` 回调，区分共享连接锁等待和持锁处理；旧 `request_attempt` 仍恰好回调一次。两段总和保持原 attempt 耗时，不改协议、超时、重试或数据库语义。已结束的失败也记录，外部取消未完成的 future 没有完成样本；exchange 包含网络及服务端工作，不是 SQL 时间。

新增 loopback 测试通过实际持锁、手动 poll 和服务端信号控制，分别验证排队/服务端等待的归属，另测请求超时后的未发送失败及旧观察者兼容。RPC 直方图保留原桶，新增 2/5/15/30 秒桶及精确边界/溢出回归，避免故障恢复全部挤入 +Inf。TiangZ 使用固定端点/阶段直方图接收结果，说明见其 `docs/testing/latency-attribution.md`。本轮不触碰真实数据库或远程服务；这些测试不证明旧夜间延迟已修复。

实际验证：workspace 默认测试通过（真实数据库/故障 27 项仍 ignored）；SDK 9 项、服务端库 25 项通过；新增 SDK 3 项计时测试重复 10 轮均通过；client/server 全目标严格 Clippy 及 fmt/diff 检查通过。TiangZ 接入后的 quick 门禁 23/23（含 TypeScript 100 项和 Rust 全目标）通过。本轮未跑故障长稳或无补丁发布门禁，未提交、未部署。

先只读等待 15 分钟（北京时间 07:32:35–07:47:35），再审查用户正在编辑的路由约束；首次测试补强保留用户的 6 个补丁文件。用户随后确认误判并建议撤回，现已定向撤回唯一约束，保留测试，增加扇入回归。未部署远程七天演练、未启动 Docker、未改 TiangZ/WoW335。

## 已补充的独立用例

| 文件 | 新增数量 | 检查内容 |
| --- | ---: | --- |
| `crates/dbproxy-core/tests/commit_contract.rs` | 10 | 规范化顺序无关和幂等、重复追加/事件身份、命名空间隔离、内外信封身份一致、非法 JSON/文本/版本/时间、二进制边界 |
| `crates/dbproxy-server/tests/commit_concurrency.rs` | 4 | 8 条独立 TCP 连接竞争：同操作仅应用一次、同 ID 不同效果拒绝、CAS 失败完整回滚且 ID 可重用、事件 ID 冲突不残留记录和事实 |
| `crates/dbproxy-storage/tests/publisher_contract.rs` | 4 | 同连接 XADD/WAITAOF 顺序、成功连接复用、新旧字段完整性、发送错误脱敏、确认丢失无内部重试、非法信封不发送 |
| `sdk/typescript/test/relay-boundaries.test.mjs` | 6 | 数值类型/范围、UTF-8 长度、所有字节值、能力声明失败关闭、回执身份/版本校验及防御性复制 |
| `crates/dbproxy-storage/tests/outbox_concurrency.rs` | 6（需 PostgreSQL） | 独立数据库连接并发提交、多 Worker 唯一租约、死信分区隔离、过期/旧 token 拒绝 ACK/fail、并发管理重试单次审计、事务后段完整回滚、跨来源目标锁与跨路由版本投递顺序 |
| `crates/dbproxy-server/src/relay_config.rs` | 1 | 多个来源、同来源多个版本、未启用来源声明允许共享同一 Publisher/destination |

Rust 测试数量按独立 `#[test]`/`#[tokio::test]` 计，不把参数矩阵内每个值另算一项。网络测试使用生产 MemoryBackend，但不证明 PostgreSQL 锁、持久性或真实 Redis AOF。RESP fixture 只验证 DBProxy 自身发送行为，不替代真实 Redis。

## 本地验证

- 撤回约束并补扇入回归后，`cargo test --workspace --locked -j1`：108 通过、0 失败、26 ignored（首次补强为 107 通过、25 ignored）。
- `npm run test:typescript`：18 通过、0 失败。
- `cargo clippy --workspace --all-targets --locked -j1 -- -D warnings`：撤回后复验通过；`cargo fmt --all -- --check` 与 `git diff --check` 通过。
- 新增 TCP 并发与 Publisher 测试连续重复 10 轮全部通过（每轮 8 项，不计入独立用例数量）。
- 新增 PostgreSQL 用例均已编译，未连接数据库执行。默认 ignored 不得解释成通过。
- 相对已提交版本，生产实现仅将误导性的循环变量 `topic` 改名 `lock_scope` 并补注释；不改变锁键算法、公开 API、协议锁或依赖。

单独运行本地用例：

```powershell
cargo test -p tiangz-dbproxy-core --test commit_contract --locked -j1
cargo test -p tiangz-dbproxy-server --test commit_concurrency --locked -j1
cargo test -p tiangz-dbproxy-storage --test publisher_contract --locked -j1
npm run test:typescript
```

## 独立 PostgreSQL 验收

新增数据库测试故意不读取部署使用的 `DBPROXY_POSTGRES_URL`，必须提供专用 `DBPROXY_TEST_POSTGRES_URL`，并显式设置 `DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1`。此额外确认不能自动判断 URL 是否安全，执行者仍必须核对目标确为可销毁测试库。

```powershell
# 先通过环境安全注入 DBPROXY_TEST_POSTGRES_URL；不要将口令写进仓库。
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION = '1'
cargo test -p tiangz-dbproxy-storage --test outbox_concurrency --locked -j1 -- --ignored --test-threads=1
```

测试会迁移 schema，以每次唯一的来源/Publisher/记录 ID 隔离状态；领取只限定本用例 Publisher。并发使用独立数据库连接，不能用共享单连接 Mutex 冒充数据库竞争。租约过期由仅针对当前事件的 SQL 注入，不依赖毫秒级 sleep。每项设置总期限，避免锁回归导致永久挂起。

测试数据保留作证据，不执行全表删除或 Redis 裁剪。需要由独立环境统一销毁；不能反复运行后假设没有磁盘累积。测试 Publisher 不连接 MQ，其假指纹只供专用数据库队列用例，不可混入部署库。

## 路由唯一性补丁的审查与处理（已确认撤回）

审查对象：`relay_config.rs`、存储 `lib.rs`/`relay.rs`、`outbox_relay.rs`、`010_outbox_route_destination.sql`、`outbox-relay.md` 的未提交修改。以下记录原补丁的问题；用户确认后已经撤回，不是当前代码仍保留的限制。

1. **作为“修复 topic 锁与投递目标锁不一致”的理由不成立。** `commit::lock_partitions` 对新版信封先查询不可变路由，按实际 `(publisher_id,destination)` 生成 `relay-destination` 键，再结合 `partition_key` 加事务锁；worker 同样按这三个维度阻塞前序。不同 topic 共用相同目标时，写入端已经共用锁。
2. **唯一约束可以是一项主动的配置限制，但不是上述排序机制必需条件。** 它同时禁止多个生产者或同一生产者多个路由版本共享 Stream，需要明确确认是否确实需要这种能力收缩。对已有合法共享目标的路由，010 会因重复值无法迁移；它不提供退役或迁移方案。
3. **“换一个 Publisher ID 来复用同一目标”不是跨来源有序性的替代方案。** 如果两个 ID 实际指向同一 Redis/Stream，DBProxy 会将它们视为不同排序组，不能据此宣称同一物理 Stream 的跨来源顺序得到保证。
4. **集成测试迁移版本预期漏更新。** `postgres_snapshot_table_uses_32_hash_partitions` 的迁移清单目前只列到 009；若保留 010，执行该测试会因实际多出版本 10 而失败。它本轮保持 ignored，所以默认全绿不会暴露此问题。

处理结果：移除配置中的目标唯一检查、未提交的迁移 010 及注册入口、拒绝共享目标的测试，并恢复路由注册的 `query_one` 回读。迁移清单仍到 009，预期无需增加版本 10。保留所有此前新增测试，文档明确支持扇入。

新增 `fan_in_sources_share_the_writer_lock_and_order_with_route_versions`：用独立 SQL 事务阻塞第一个来源的记录锁，通过 `pg_locks` 确认第一个写入已到达记录锁、第二个来源正在等待共同的投递目标锁；释放后验证两个来源与第三条路由版本事件按序领取。轮询观察真实锁状态，不用“睡过一段时间没有完成”作为串行性的证据。此测试尚未连接真实 PostgreSQL 执行。

本次只撤回代码，未执行数据库 DDL。如果有人在其他环境手工执行过 010，删除脚本不会自动撤销已存在的约束；需要先核对该环境再单独处理，不能直接改生产迁移登记。

## 2026-09-06 本机真实数据库补验

已在独立 Docker 测试库串行执行此前 ignored 的 26 项：postgres_redis 12、outbox_concurrency 6、outbox_relay 1、fault_matrix 6、postgres_redis_network 1，全部通过，包括跨 producer/route version 共享目标的真实锁测试。原七天演练没有替换版本。

补验发现并修复 Redis Publisher 的响应超时配置遗漏：WAITAOF 服务端允许等待 2 秒，但新 MultiplexedConnection 默认仅等待 500 毫秒。首次连接及失败/取消后的重连均显式采用已有的 3 秒连接/响应超时，保持 XADD 与 WAITAOF 在同一连接上完成。新增延迟 750 毫秒 AOF 确认用例，且重连 fixture 同样延迟 750 毫秒；不通过改 Redis 落盘配置掩盖缺陷。

交易综合测试改为独立 schema，不再批量更改其他测试遗留事件。外层本机执行器为每个集成套件重建专用测试库及清空专用 Redis，避免不可变路由跨套件污染启动验证。真实数据库及跨来源验收现已补齐，持续进程故障长跑仍需以独立 final.json 为完成证据。默认 Rust 测试现为 110 项（含新 Publisher 用例及持续探针重试分类），TypeScript SDK 18 项通过，严格 clippy/fmt 通过。

## 仍需后续完成

### 夜间探针错误分类收紧

持续持久化探针此前对所有 ClientError/批量条目错误仅累计调用失败，会把独占测试记录的 RevisionConflict、IdempotencyConflict、错误响应类型甚至 Internal 混为故障窗口普通重试。现仅容忍连接中断/超时类 I/O 和 StorageUnavailable；其他错误增加 invariantErrors 并输出有界 SOAK_CONTRACT_ERROR。TiangZ 本机执行器收到该标记立即失败，最终一致性检查也不能通过。暂时错误保留最多 16 条 SOAK_OPERATION_ERROR 样本；永久错误另保留最多 16 条，不因前面的网络错误耗尽采样额度而失去关键证据。

正反向用例覆盖暂时错误不误报、五类远程永久错误、UnexpectedResponse，以及 InvalidData/InvalidInput/PermissionDenied 不得被泛化成连接重试。探针 4 项测试和严格 Clippy 已通过，发布二进制已重建；真实长跑仍按独立轮次对账，见 TiangZ 的 `docs/testing/night-validation-20260906.md`。本次只改变验证工具，不改变服务端协议、业务事务或 WoW335 接口。

### 并发重连重复替换健康连接（09-07 夜间）

换序演练 r7 在游戏健康窗口出现 Scene RPC 超时；同一时间地图进程的 DBProxy 客户端多次连续切换端点。检查发现 SDK 的 `reconnect_next` 在获取连接锁后没有检查前一个等待者是否已经修复连接，多个失败调用会依次重新握手、替换已经可用的连接。

新增本地模拟端点并发用例：8 个调用共同恢复一条失效连接。旧代码稳定建立 9 条连接（含初始连接），预期应为 2；修复后只由首个等待者重连，其他调用复用健康连接。第二次真正失效仍能建立一条新连接，恢复后的实际 Load 请求成功。SDK 6 项测试和严格 Clippy 通过；只在共享连接锁内检查 usable，不改公网/协议、请求超时、重试次数、认证和调用方幂等键。

该缺陷已由精确用例证实，但不能据此断言它解释 r7 的全部高 CPU 或游戏队列等待；需重建所有使用该 SDK 的演练二进制后再次验证。TiangZ 的公共接口及 WoW335 协议未变，WoW335 未修改、未运行实机客户端回归。r7 部分对账通过不等于整轮通过，完整证据和后续轮次见 TiangZ 夜间验证文档。

重建后的补验结果：r8/r9 曾再次出现游戏超时，不能把并发重连缺陷当成所有超时的唯一根因。TiangZ 随后用独立原生工具替代高开销 PowerShell 现场采集，修复版 r10（100 人 / 30 分钟）和 r11（100 人 / 60 分钟）最终通过，覆盖 32 次故障恢复、603,839 次持久化读取，旧读/缺失/一致性错误均为 0，2,120 条通用 Outbox 事件与实际 Stream 最终对账一致。r11 PostgreSQL 恢复仍记录一次业务 RPC 超时重试和两次离线保存中登录重试；健康窗口、最终状态及日志审计通过。秒级尾延迟和宿主机 CPU 归因仍未完全解决，不能据此宣称生产性能稳定或完成 7 天认证。详细失败/成功记录、资源与验证版本见 TiangZ 夜间文档。

### 本机缓存旧读与 AOF 定向补验

本机 3 小时首轮在 19 分钟时提前停止，缓存重启窗口有 703 次旧读，不能作为通过证据。实际缓存 CONFIG 显示 appendonly=no 但 save 仍为默认周期；Redis 日志证实加载了旧 RDB。新增 `ephemeral_cache_restart_cannot_restore_an_acknowledged_old_revision`，主动保存旧缓存镜像、强杀缓存、停机期间提交 revision 2，重启后立即读取，不先修复缓存。旧部署稳定失败（读到 revision 1），关闭 RDB 并挂载 tmpfs 后同一测试通过。

该修正属于部署与验证缺口，不改变 PostgreSQL 事务或 DBProxy 协议。不要据此把所有网络分区、缓存部分写失败下的普通缓存读取宣称为线性一致；本用例证明的是易失缓存重启边界。TiangZ 本机执行器在启动/清理前验证实际 Redis 配置及 tmpfs，发现漂移直接拒绝。

新增 `dbproxy_aof_probe` 经 RPC 入队 64 条独立快照并输出成功确认的 request ID；PG 停机期间强杀/重启可靠 Redis，观察 pending+processing，恢复 PG 后逐条核对权威 payload/schema/revision。只看 pending 指标为零不能区分在途租约、过期采样和真正缺失，不再作为 AOF 丢失结论。持续故障探针对旧读保留最多 16 条详情及完整计数，便于精确定位且控制日志量。

`20260906-recovery-smoke5` 在北京时间 16:53 完成一次真实 AOF 定向验证：64 条 RPC 确认后 pending=0、processing=64；强杀可靠 Redis 并重启后仍为 0/64；恢复 PG 后 64 条权威快照逐条核对通过。现场证实 pending=0 不代表无积压。探针必须使用无 expected_revision 的排队快照（该接口禁止 CAS），不能因测试需要放宽生产接口；此前 smoke4 被该协议检查拒绝，失败证据保留。

- 原 `outbox_relay.rs` 综合验收已恢复到提交时状态；新增并发/租约/管理用例已独立，原多消费组和 ACK 丢失综合用例的进一步拆分仍待完成。
- TiangZ 与 DBProxy 当前版本的持续进程故障、恢复正确性和日志保留验收正在使用本机独立环境推进；启动不等于通过，以本次运行的最终证据为准。
