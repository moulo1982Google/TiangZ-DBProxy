> 本轮发布：`v0.7.0` 正式版（六仓库套件 TiangZ 0.7.0）。本仓库相对 v0.7.0-rc2 无代码或协议改动，只把版本号定为 0.7.0；说明见 [RELEASE-v0.7.0.md](RELEASE-v0.7.0.md)。

# TiangZ DBProxy

2026-10-07 11:12 R7新完整30/960/1440m全部通过，24h已于11:11:59完成120故障、300s空载、全量SQL/Redis及内存/容量独审；281179唯一事件/170一致重复，14954正常区间零错误。实际512MiB控制预算无OOM/swap，原回收压力保留；本轮进程/容器、guard及probe已停止，保护业务正常。详见[24h终验](docs/cloud-fault-soak-acceptance-2026-10-07.md)，旧失败/旧套件不自动补资格，未push/发布，下文为历史记录。

2026-10-06 14:37 旧24h内存夹具漏扩progress等数组已纠正，原实际SQL/Redis/960m资格保持；完整增长数组/40.89MiB报告发布、Python持有与Node解析/双编号缓冲在独立512MiB缓存压限组64s通过，reserve采样峰241.57MiB/448门禁、OOM/swap零。仅内存证明、不保证实际24h收尾，现场预算未改，详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，未push。

2026-10-06 13:02 R7新960m已于11:03:21.549完整通过80故障/300s空载/全量SQL-Redis及独审，187212唯一事件/186一致重复，新30m/960m资格成立。新1440m11:03:23.820开始，约2h/10故障恢复、1227正常区间零错误。控制512MiB总约501.5MiB，主要file缓存396.2MiB、anon96.2MiB，max回收压力143持续记录、OOM/swap零，保持原预算/现场。预计10月7日11:03:24结束24h负载，另需收尾，详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，未push，下文为历史记录。

2026-10-06 08:50 R7新960m约13小时54分、70/80故障恢复，8642正常原始区间错误零；正常窗口有一次PG提交约630ms抖动/6条慢占用WARN，无正常请求失败或排队超时，原日志保留、底层原因未证。目标约0.96GiB/控制约185MiB，连续node2 PSS低位增85KiB、OOM/swap/max零，保护业务正常。预计10:56:13结束负载，再空载及独审后才计资格和接1440m，详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，未改产品/现场或push，下文为历史记录。

2026-10-05 19:28 R7新完整30m已通过5故障/300s空载/SQL-Redis全内容及独审，新960m18:56:13.457开始；约32分钟、3/80故障恢复，326个正常原始区间错误零，PG停机窗口的请求失败保留、数据不变量零。目标约450MiB/控制144MiB、OOM/swap/max零，保护业务及探测正常，960m资格待完整结束。详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，只读核验器本机换行基准误报已留档修正，云端冻结文件未变，未push，下文为历史记录。

2026-10-05 18:25 新R7完整故障长稳已在18:21:01.992开始30m，独审通过后自动960/1440m。v2收尾正式修复engine31a8a5b/记录3fbc982，真实原产品ELF及2C4G/100玩家/2与5s限额保持；52文件/资源/保护核验通过、首次主节点强杀恢复，当前错误暂零。新30分钟探测首轮0及云端guard正常；旧失败资格零，尚无新完整资格，详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，无codegen/产品重编/push，下文为历史记录。

2026-10-05 18:17 v2收尾的目标规模/内存门禁已通过：24h对应实际SQL/准备Redis全280959唯一事件、86内容一致重复匹配，扩大报告与真实计数/集合/全内容收尾155.94s、控制侧无OOM/swap。全部为隔离预检资格零；副本已停止、保护业务正常，准备提交和封存新完整30/960/1440m候选。详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，原失败不改，无产品生成/重编或push。

2026-10-05 18:13 收尾工具已实现全量分项审计v2：同一可重复读只读会话/快照，每条SQL5s、计数会话60s，独立Node审查仍600s；明确区别于旧整条5s，产品PG/AOF2s和SDK5s不变。quick35/35、9项针对性检查、PG9反例和并发快照验证通过；24h对应SQL规模两次冷查询及实际16h完整SQL/Redis内容收尾通过。扩大24h报告的512MiB内存/全内容门禁正在执行，新长稳未启动，旧失败资格零。详见[云上记录](docs/cloud-fault-soak-2026-10-02.md)，无产品重编/生成/提交/push，下文为历史记录。

2026-10-05 15:53 最新收尾状态：账本物化的正式审计最小改法已通过quick35/35及真实16h数据完整冷SQL/Redis内容预检；24h对应1.5倍数据首次查询仍超时，事务内存调参在新夹具上也未解决，已恢复原参数。新长稳尚未启动，旧960m资格零，诊断已停止、保护业务正常；继续设计完整分项/分块审计及一致快照/期限契约。详见[云上故障记录](docs/cloud-fault-soak-2026-10-02.md)，未提交/push，下文为历史记录。

2026-10-05 R5C完整960m/80故障和空载完成，但收尾SQL五秒超时，阶段failed、24h未启动。已在实际数据隔离副本复现冷缓存超时并定位全量账本索引回表；仅审计SQL的临时物化输入原型保持整条5s，冷查询3.461/3.429s、全部原计数一致。正式工具尚未改，完整冷热/SQL-Stream收尾预检和新长稳仍待执行；诊断资源已停止、保护业务正常。详见[云上故障记录](docs/cloud-fault-soak-2026-10-02.md)，旧失败资格零，未push。下文为各自时间的历史快照。

2026-10-04 已完成[960m收尾工具修复的真实数据预检](docs/cloud-fault-soak-2026-10-02.md)：对账事务关闭JIT并合并扫描，保持5s；完整SQL/Stream内容及新固定时间窗容量检查通过。新 `tzfault20261004auditr5c` 在16:47:19开始完整30m，全部审查通过才自动接960/1440m；当前新资格尚未完成。原失败960m资格零，产品ELF和2C4G/100人预算保持，未push。

0.7 PostgreSQL 排队与长稳错误观测见 [连接诊断](docs/postgres-queue-diagnostics.md)：分片/持有者/实际 backend PID、完整持有时长、修正排队超时计数及固定容量的晚期错误记录。

当前版本为 `0.7.0`（正式版），服务与 Rust/TypeScript SDK 使用相同版本。此前候选（0.7.0-rc.2、v0.7.0-rc1、v0.7.0-rc2）的标签与验收历史保留在下文。0.7 的预算、租户与容量/恢复改进见现有专题，协议和数据库迁移不因版本编号额外改变。

0.7 开发中的 Rust SDK 已补齐排队、重连和重试共享的[请求总预算](docs/client-request-budget.md)；新增“确定未发送”超时分类，默认值仍为 5 秒。TS SDK 的 [WithRequestBudget 作用域](docs/typescript-sdk.md)让 Repository 读取/重试共用期限，旧 Transport 不会被默默当作支持超时。宿主候选与发布验证状态见上述记录。

0.7 开发分支的[可靠 Redis 确认预算](docs/redis-durability-budget.md)将入队与后台 Outbox 的 AOF/I/O 等待分别配置，并让排队、重连、写入、确认消费原总预算。默认 AOF 仍为 2 秒，可靠 ACK 不降级；新连接不能确认旧连接的写入。新增阶段耗时、超时与队列等待观测，修改版需要重新构建、重启及独立长稳。

云上 [2C4G/100 玩家基线与复测](docs/remote-capacity-2c4g-100-2026-10-01.md)发现默认 Outbox 发布落后事件输入；开发分支增加[有界批量发布](docs/outbox-batch-publication.md)，最多 16 个独立排序组共用一次同连接 AOF 确认，保持租约、顺序、至少一次与原预算。新 Linux 重建/真实存储回归和同负载 300+900 秒、300 秒停载观察通过，终态 3870 个事件全发布；轻量负载的短时结果不替代故障、真实存档容量或完整长稳资格。

云上 [2C4G / 100 玩家故障长稳](docs/cloud-fault-soak-2026-10-02.md) 的旧制品完整 30 / 60 / 120 / 240 分钟均已独立复核通过；日志候选 `cceb223` 于 **2026-10-03 05:22:24**通过 diagr3 完整 480 分钟、40 次故障、300s 空载及独立 SQL/Stream/原始 SHA 复核。后续 960 分钟客户端负载跑满 57600.40s、80 次故障恢复和最终可见状态通过，但 **21:22:37** 客户端控制组 OOM 杀死 Node 驱动，空载和独立对账未完成；该旧轮终态 failed、整轮资格零，1440 分钟未启动。目标 DBProxy 2C4G 组 OOM/swap 零，保护业务正常；失败证据与既有 480 分钟资格保留。旧驱动保留对象没有堆快照，不能唯一归因；控制修复和新轮状态见下文，不把客户端完成或正常窗口零错误当作整轮通过。原产品 ELF、100 人 / PG和AOF 2秒 / SDK 5秒与数据门禁保持，未 push。

用户于 2026-10-03 确认本轮只完成 DBProxy 本身的完整性、可靠性与[通用性能验收](PERFORMANCE.md#07-验收范围2026-10-03-用户确认)。SLG 的实际业务压力测试由用户在业务接近完成后另行安排，2 万在线 / 300 万注册 / 50 区服不作为本轮验收目标。MemoryBackend、真实存储和长稳结果分别报告，不把合成负载人数或历史基准当成生产容量承诺。

失败960m的控制工具已增加显式V8预算、有界历史驻留、完整分块报告及控制进程观测。新memory-r4完整30m于 **2026-10-03 23:41:15 北京时间**独立通过，资格保留；后续960m实际负载57600.42s、80次故障、300.70s空载和驱动完整报告完成，但 **2026-10-04 15:46:50** 独立全量SQL超过验收工具自己的5s查询期限，整轮failed、所属资源已停止、1440m未启动。目标/控制组OOM/swap零，保护业务正常；该错误不是DP请求2s或SDK5s期限。原样本离线预审还发现尾6份Outbox均值36.5超过原32门槛，空载已清零但原因待定位，不能只修SQL就重跑或放宽门禁取绿。此前模拟编号夹具只测内存/输出，未执行真实查询。原始错误、SHA清单、失败资格和隔离预检方案见上述云上报告，不继承失败时长、重启旧现场或push。

0.7 增加独立[只读容量命令](docs/capacity-observation.md)，观察分区表/回执/事实/Outbox 的估算行数和物理字节，可显式开启有期限的服务器时间扫描；不执行迁移或自动清理。真实恢复契约与本轮隔离验证见[恢复验收记录](docs/v0.7-recovery-acceptance.md)。

[![Rust CI](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/ci.yml)
[![nightly acceptance](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/ci.yml/badge.svg?event=schedule)](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/ci.yml?query=event%3Aschedule)
[![security](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/moulo1982Google/TiangZ-DBProxy/actions/workflows/security.yml)
[![tag](https://img.shields.io/github/v/tag/moulo1982Google/TiangZ-DBProxy?label=tag&sort=semver)](https://github.com/moulo1982Google/TiangZ-DBProxy/tags)
[![license](https://img.shields.io/github/license/moulo1982Google/TiangZ-DBProxy?label=license)](LICENSE)

本地新增多租户入口 `--tenants`：凭据绑定独立后端，可共用 PostgreSQL/Redis 实例但分别使用独立 database/逻辑 DB；连接配额与观测按租户区分。原 --config 不变。用法、约束及真实存储未验收范围见 [多租户 v1](docs/multitenancy.md)。

本地集成分支新增 `CommitRecords`：多记录 CAS、不可变追加事实和 Outbox 同一事务提交，不要求交易状态或账本规则。旧接口保持兼容，旧 Trade API 暂留兼容入口。实施状态与发布前验证见[通用持久化计划](docs/generic-persistence-plan.md)；当前远程七天演练不使用这些修改。

TiangZ DBProxy 是独立的 Rust 持久化服务项目。

它负责通用的：

- 玩家、Item、任务等快照的读取与写入
- Revision 和 Compare-And-Swap 版本校验
- 重试幂等与请求去重
- Redis 缓存、PostgreSQL/MySQL/MongoDB 等存储适配
- 监控、故障恢复和部署协议

DBProxy 不依赖 TiangZ Runtime，也不包含任何游戏玩法。TiangZ 只向它提供稳定的快照 Payload、Schema 和 Repository 适配逻辑。

## 当前状态

`v0.6.2` 是当前版本（2026-09-21 发布）。`v0.6.0` 起为同一轮能力；`v0.6.1` 补回停机完成日志 `TiangZ DBProxy stopped`；`v0.6.2` 修好标签验收里故障矩阵缺少缓存 Redis 的问题，并把版本号对齐为 0.6.2。它在已有快照、关键事务、多 Endpoint 和跨记录原子事务之上，增加持久缓存修复队列、AOF 确认与重连、交易托管状态机、不可变账本、PostgreSQL Outbox 和权威快照 HASH 分区：

本次发布另外包含入队与连接层改动，协议格式不变，新旧客户端与服务端互相兼容：

- 入队组提交：同一时刻到达的入队合并为一次脚本写入和一次确认，带排队上限4096与2秒排队期限，过载时快速返回可重试错误。
- 入队确认档位 `backlog.enqueueAck`：`aof`（默认，等本地AOF落盘）或 `memory`（写入Redis内存即确认，Redis崩溃可能丢约1秒已确认入队）。整个部署统一生效。
- 同一连接多请求在途：`server.maxInFlightPerConnection`（默认64）限制每条连接并发处理的请求；同一连接上共享RecordKey、operation ID或trade ID的请求按到达顺序执行，其余并发，响应按`rpc_id`对应。
- 排队写落库防护序号：入队分配严格递增序号，落库只有序号更大才覆盖，迟到的旧值不再回写；配套迁移12给快照表加 `queued_sequence`。租约时间改用Redis `TIME`，落库事务带10秒语句超时。

- `RecordKey`：`namespace + key`
- `Revision`：由 DBProxy 生成的单调版本号
- `SnapshotWrite`：带 `expected_revision` 的条件写入
- `request_id`：重试时必须保持不变的幂等键
- `InMemorySnapshotStore`：只用于测试，不保证重启恢复
- `TransactionalWrite`：带 `operation_id`、期望版本、完整快照和持久化操作结果
- `TransactionReceipt`：按`operation_id + RecordKey`读取第一次提交保存的Revision和业务结果
- `MultiRecordTransactionalWrite`：在一个 PostgreSQL 事务中原子提交多条完整记录，适合跨玩家交易、奖励转移等业务
- `MultiRecordTransactionReceipt`：按`operation_id + 多个RecordKey`恢复整组提交结果；重复提交返回同一结果
- `InMemoryTransactionalStore`：验证事务提交、CAS 冲突和原始结果重试语义
- `PostgresSnapshotStore`：PostgreSQL 权威快照、CAS、幂等写入和关键事务收据
- `dbproxy_snapshots`：按完整 RecordKey 路由到 32 个 PostgreSQL HASH 叶子分区，DBProxy 始终访问逻辑父表
- `RedisSnapshotCache`：只缓存 PostgreSQL 已提交的快照
- `TieredSnapshotStore`：先提交 PostgreSQL 和 durable repair，再尽力刷新 Redis；缓存失败不改变权威提交结果
- `PostgresCacheRepairQueue`：与权威写入同事务提交的缓存修复目标，支持租约、指数退避、死信和定点重放
- `TieredSnapshotStore::repair_cache`：从 PostgreSQL 重建缓存，或删除数据库中已不存在的旧缓存
- `SnapshotFlushQueue`：按 `RecordKey` 合并普通快照，只保留最新值；关键事务不进入该队列
- `SnapshotFlushQueue::flush` 与 `flush_until_empty`：限制每轮写入量和最大轮数，失败保留请求并返回剩余积压
- `RedisSnapshotBacklog`：把尚未落 PostgreSQL 的普通快照保存到独立 Redis backlog，支持 lease、ACK、释放、续租和过期回收
- `TradeTransaction`：原子提交交易状态、多记录CAS、不可变平衡账本、Outbox和完整回执
- `PostgresOutboxQueue`：租约/重试/死信 worker，把交易事件至少一次发布到 AOF 确认的 Redis Stream
- `dbproxy-protocol`：v2 Protobuf、协议指纹、8 MiB frame 和 1 MiB 应用 Payload 默认上限
- `dbproxy-server`：内部令牌握手、按 RecordKey 分片的真实存储连接，以及 backlog/cache-repair/outbox worker
- `MemoryBackend`：保留Revision、CAS、幂等和多记录原子语义的易失后端，用于隔离网络/协议/调度成本；不会连接PostgreSQL或Redis
- `dbproxy-client`：Rust 异步客户端及多连接池；TiangZ 不需要引用存储 crate
- `@tiangz/dbproxy-sdk`：TypeScript稳定类型、参数校验、防御性Payload复制、多记录事务和可插拔Transport；不绑定Node、Deno或TiangZ
- `fault_matrix.ps1`：在笔记本限额容器中显式停止/恢复 Redis/PostgreSQL，验证 AOF、积压、自动重连和缓存修复边界
- `network_smoke.ps1`：验证 Rust SDK -> TCP -> DBProxy -> Redis/PostgreSQL 完整闭环

TiangZ主仓库已经提供首个Player Snapshot Repository和Rust Host Transport适配；这些领域Payload与恢复逻辑不属于本仓库。交易 API 只提供通用状态/CAS/账本/Outbox 原子边界，所有权、价格、余额和风控仍由主工程的领域 Repository 决定。架构、演练、分区和审视结果分别见[架构说明](docs/architecture.md)、[恢复手册](docs/durability-recovery-runbook.md)、[两小时故障演练报告](docs/fault-soak-report-2026-08-27.md)、[PostgreSQL 快照分区](docs/postgresql-partitioning.md)、[交易安全说明](docs/trade-safety-and-outbox.md)和[代码审视记录](docs/dbproxy-code-review.md)。

如果需要向外部介绍 DBProxy 的能力、接口选择和边界，见[DBProxy 能力清单](docs/capabilities.md)。

## 启动配置

默认`load/load_multi`读取PG主库已提交状态，失败返回错误，不退回缓存；批量使用同一数据库语句快照。允许旧数据须显式调用`load_cached/load_cached_multi`（TypeScript：`LoadCached/LoadCachedMulti`），可携带版本下限。保留配置`authoritativeReadNamespaces`作为额外禁止缓存读取的保护，而非默认正确性的前提。见[默认读取契约与升级](docs/default-read-contract.md)。

容器停止时，服务入口统一处理 SIGTERM/SIGINT，Windows 保留 Ctrl+C。关闭顺序、等待预算、镜像升级要求及回归方法见[优雅关闭说明](docs/graceful-shutdown.md)。

缓存与 PG 回源的等待预算现分离：`storage.cacheOperationTimeoutMs` 默认 200 ms，`cacheFallbackTimeoutMs` 仍默认 2,000 ms。升级兼容、正确性边界与测试见[缓存操作预算](docs/cache-operation-budget.md)。可靠 Redis AOF/MQ 确认不使用该缓存预算。

PG 请求分片现使用独立的连接排队预算与重连失败冷却：`storage.postgresConnectionWaitTimeoutMs` 默认 2,000 ms，`storage.postgresReconnectCooldownMs` 默认 500 ms。只限制取得连接前的等待，不缩短已发送 SQL/事务的执行时间；提交后修复 ACK 排队失败保留修复目标。范围、兼容和验证见[PG 请求预算](docs/postgres-request-budget.md)，后续演练安排见[交接记录](docs/handoff-2026-09-07.md)。

通用 Outbox Relay 的 Redis 路由、禁用的未来 MQ 声明、离线检查和审计管理命令见[Outbox Relay](docs/outbox-relay.md)与[配置示例](configs/outbox-relay.example.json)。旧配置及旧 Stream 地址保持兼容；新来源不能在所有 worker 升级前启用。

DBProxy使用带`configVersion: 1`的严格JSON保存普通启动参数，默认读取`configs/local.json`，并由`configs/dbproxy.schema.json`提供编辑器提示。`runtime.workerThreads`可以固定Tokio Runtime工作线程数，省略时沿用Tokio按逻辑CPU选择的行为；它与只负责Redis积压消费的`backlog.workers`不是同一个参数。连接串和认证令牌不能写进JSON；配置文件只记录环境变量名，由部署环境注入实际密钥：

```powershell
cargo run -p tiangz-dbproxy-server -- --config configs/local.json
```

未知字段、零worker、零lease、空密钥变量会在建立网络连接前直接报错。每个 DBProxy 实例只配置一个监听地址；部署两个实例时使用两份 JSON，二者共享同一 PostgreSQL 和 Redis 服务。`redisUrlEnv`承载必须保留 AOF 的 backlog/Outbox；可选的`cacheRedisUrlEnv`把快照缓存放到独立、无持久化的 Redis。省略后者时继续复用`redisUrlEnv`，兼容单 Redis 开发环境。多 Endpoint 写在业务客户端配置中，而不是 DBProxy 服务端配置中：第一个地址是首选，后续地址是故障切换候选。

存储后端必须显式选择。正式和恢复测试使用`postgresRedis`；`memory`只用于本地开发与性能隔离，进程退出后数据全部丢失，并且`EnqueueSnapshot`会直接写入内存权威快照，不模拟Redis AOF与异步刷盘：

```json
{
  "runtime": { "workerThreads": 4 },
  "observability": { "listenAddr": "127.0.0.1:9090" },
  "storage": { "backend": "memory", "shards": 16 }
}
```

配置`observability.listenAddr`后，DBProxy在独立HTTP端口提供`/live`、`/ready`、`/dependencies`和Prometheus格式的`/metrics`。真实存储模式只有在 PostgreSQL 与可靠队列 Redis 都可达时才 Ready；独立快照缓存不可达时安全回源 PostgreSQL，并由缓存读写错误与回源指标告警，不把可降级缓存误判为持久依赖。本地Compose会启动Prometheus与Grafana并自动加载Dashboard；指标、告警和部署边界见[可观测性指南](OBSERVABILITY.md)。观测端口不要求业务认证，默认只允许loopback；私网或通配绑定必须显式设置`observability.allowNonLoopback=true`并限制网络来源，直接公网或多播地址被拒绝。禁止经Nginx暴露公网。容器抓取所需设置见[本地部署说明](deploy/local/README.md)。

仓库提供`configs/perf-memory-4.json`，固定使用4个Runtime工作线程和MemoryStub。该配置只测DBProxy自身的网络、协议、调度、分片锁和事务语义，不把PostgreSQL或Redis性能混入结果。

```text
DBProxy-1: 127.0.0.1:7800 ─┐
                           ├─ 同一 PostgreSQL + 可靠队列 Redis
DBProxy-2: 127.0.0.1:7801 ─┘                    + 可选易失缓存 Redis
客户端: [7800, 7801]
```

两个实例是无状态对等节点，不需要互相同步或 Leader 选举。客户端切换地址时必须复用原 `requestId`/`operationId`，因此“提交成功但响应丢失”不会重复发奖或重复扣物品。

## 开发

`v0.6.0` 标签发布时已统一审查锁文件、版本、协议指纹与完整测试。标签之后继续开发时，`Cargo.toml`、`Cargo.lock`和工作区版本号仍不作为冻结契约；日常修改依赖时允许Cargo重新解析，CI也不使用`--locked`。下一次发布正式Tag前，同样要统一执行锁文件、版本、协议指纹和完整测试审查。

```powershell
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
npm run test:typescript
```

GitHub Actions 的普通分支和 Pull Request 只运行开发门禁；推送 `v*` Tag 或发布对应的 GitHub Release 时会自动进入发布验收门，使用 `npm ci`、`cargo ... --locked`，并启动 PostgreSQL/Redis 完成真实存储、网络闭环和故障矩阵测试。发布 Tag 只有在这组测试全部通过后才算验收完成。

同一套完整验收另外每天在主分支跑一次（UTC 18:41，北京时间次日 02:41），也可手动触发，避免问题拖到发版当天才暴露；README 的 nightly acceptance 徽章只反映定时运行，Rust CI 徽章只反映主分支推送。`security` 工作流每周一跑 `cargo audit`，改动 `Cargo.lock` 或 crate 清单时也跑：有漏洞才失败，无人维护与 yank 只作为警告。

本机启动 PostgreSQL 和 Redis：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml up -d
$env:DBPROXY_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz"
$env:DBPROXY_REDIS_URL = "redis://:tiangz_dev@127.0.0.1:6379/15"
$env:DBPROXY_TEST_POSTGRES_URL = $env:DBPROXY_POSTGRES_URL
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION = "1"
$env:DBPROXY_CACHE_REDIS_URL = $env:DBPROXY_REDIS_URL
cargo test -p tiangz-dbproxy-storage --test postgres_redis -- --ignored --nocapture --test-threads=1
```

运行这组直接存储集成测试前先停止本机 DBProxy。测试会主动认领 backlog、缓存修复和 outbox 任务；若业务 worker 同时运行，会消费测试刚写入的任务并造成竞争性假失败。database 15 用于隔离测试数据，执行前仍应确认其中没有需要保留的数据。

缓存修复并发回归使用独立、可丢弃且无业务 worker 的 PostgreSQL 数据库，设置 `DBPROXY_TEST_POSTGRES_URL` 和 `DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1` 后执行：

```powershell
cargo test -p tiangz-dbproxy-storage --test cache_repair_concurrency -- --ignored --nocapture --test-threads=1
```

该组验证目标合并、实际修复 revision/缺失记录的 ACK、排队顺序、退避/死信保留、过期租约、同名 worker 和删除后重新入队隔离。它不在默认 `cargo test --workspace` 的通过数量中；真实 Redis 缓存读写还需执行上面的 `postgres_redis` 测试。迁移 010 的受控切换要求见[恢复手册](docs/durability-recovery-runbook.md)。

运行故障矩阵。该命令会短暂停止并恢复本机 PostgreSQL/Redis 容器，但不会删除数据卷：

```powershell
powershell -ExecutionPolicy Bypass -File tools/fault_matrix.ps1
```

DBProxy 已启动后，用 100 个模拟玩家执行两小时真实 TCP 故障演练（会按时间表停止、强杀并恢复本机 PostgreSQL/Redis 容器）：

```powershell
cargo build --release --bin dbproxy_fault_soak
powershell -ExecutionPolicy Bypass -File tools/run_fault_soak.ps1
```

正式运行前可用 `-DurationSeconds 120` 做同比压缩预演。完整阶段、断言和结果文件说明见[持久化与故障恢复手册](docs/durability-recovery-runbook.md)，2026-08-27 的 100 玩家正式结果见[两小时故障演练报告](docs/fault-soak-report-2026-08-27.md)。

启动本机网络服务：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_local.ps1
```

默认监听`127.0.0.1:7800`。运行真实网络闭环：

```powershell
powershell -ExecutionPolicy Bypass -File tools/network_smoke.ps1
```

## 业务持久化性能

登录相关测量须区分保活Actor复用与离线快照加载。先结合PG配置做并发阶梯，不以过载档位作为正常性能；本地工具、复测方法及完整登录尚未覆盖的部分见[登录容量测量](docs/login-capacity-method.md)与[存储阶段对比](docs/login-storage-comparison-20260917.md)。

当前4-worker MemoryBackend基线中，100并发拾取事务达到约3.53万次/秒，NPC商店事务约3.84万次/秒；30个领域使用`LoadMultiSnapshot`后，玩家恢复吞吐相对逐条读取提高约12.23倍。完整环境、延迟、并发曲线、结果边界和复现命令见[性能基线](PERFORMANCE.md)。

`dbproxy_business_load`直接经过Rust客户端、TCP协议和DBProxy后端，使用Starter当前的持久化形状：

- `playerDataSingle`：每个玩家领域各发一个`LoadSnapshot`，作为批量读取的对照组。
- `playerDataBatch`：用一个`LoadMultiSnapshot`读取全部玩家领域。
- `playerSaveSingle`：按旧周期Flush路径为每个领域依次发送一个`SaveSnapshot`。
- `playerSaveBatch`：用一个`SaveMultiSnapshot`保存全部玩家领域并逐条推进Revision。
- `pickup`：原子提交`inventory + quest + wallet`三条记录及拾取回执。
- `npcShop`：原子提交`inventory + wallet`两条记录及买卖回执。

它刻意不运行战斗、AOI、怪物死亡或NPC距离检查，因此测到的是DBProxy链路，不是整服玩法吞吐。Payload大小会写入结果，所有写操作都持续推进真实Revision，任何冲突都会计为失败并停止对应虚拟玩家。

```powershell
cargo build --release -p tiangz-dbproxy-server --bin tiangz-dbproxy-server
cargo build --release --bin dbproxy_business_load
$env:DBPROXY_AUTH_TOKEN = "local-perf-token-1234"
./target/release/tiangz-dbproxy-server --config configs/perf-memory-4.json

# 在另一个终端运行。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 32 --players 100 --duration 30

# 模拟30个玩家持久化领域，比较逐条Load和LoadMulti。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 64 --players 100 --duration 30 --domain-count 30 --workloads playerDataSingle,playerDataBatch

# 比较5/10/30领域的逐条Save和SaveMulti；正式结论至少运行三轮。
./target/release/dbproxy_business_load --endpoint 127.0.0.1:7810 --pool-size 32 --players 100 --duration 30 --domain-count 30 --workloads playerSaveSingle,playerSaveBatch
```

正式测试必须保持服务端`runtime.workerThreads`、`storage.shards`、客户端连接池、玩家数、时长和机器环境一致。每种工作负载使用全新DBProxy进程，至少运行三轮，报告`ops/s、p50/p95/p99、失败数、DBProxy CPU/RSS`。该结果是DBProxy自身的性能上界，不代表PostgreSQL容量，也不用于评价数据库选型。

本机开发账号只绑定回环地址：PostgreSQL 用户和数据库都是 `tiangz`，密码是 `tiangz_dev`；Redis 密码也是 `tiangz_dev`。这些凭据只适用于本地开发，不能复制到线上。

## 设计原则

1. DBProxy 只理解记录地址、Schema、Revision 和二进制 Payload，不理解游戏业务字段。
2. 快照写入必须支持重试，重试不能导致重复扣物品、重复发奖励或重复保存。
3. Redis 不是最终一致性的替代品。缓存和持久库的责任、故障恢复顺序必须由适配器明确实现。
4. 普通快照、关键事务和至少一次事件投递使用不同 ACK；同库多记录/交易事务不能被普通批量写入替代。
5. TiangZ 的主工程不直接依赖 DBProxy 的内部模块，只依赖版本化协议或客户端 SDK。
6. 普通Entity可以由TiangZ的`.native`生成版本化Codec和通用Repository；DBProxy仍只维护固定通用表。复杂查询、二级索引和跨玩家事务必须使用专门的领域存储设计。

## TypeScript SDK

SDK以`DbProxyTransport`隔离宿主I/O。业务或框架适配层创建`DbProxyClient`后，只调用版本化SDK；SDK不会生成幂等ID，也不会在失败后偷偷换ID重试。玩家由多个持久化领域组成时，应使用`LoadMulti`一次恢复最多64条快照，避免为每个领域单独产生网络往返。

```ts
import { DbProxyClient, type DbProxyTransport } from "@tiangz/dbproxy-sdk";

const transport: DbProxyTransport = createHostTransport();
const client = new DbProxyClient(transport);
const snapshot = await client.Load({ namespace: "player", key: "1001" });
```

协议版本和SHA-256指纹由`tools/generate_typescript_protocol_lock.mjs`从权威`dbproxy.proto`生成。修改协议后必须同时运行Rust测试和`npm run test:typescript`，禁止手工维护两份指纹。

## 网络边界

当前 v2 协议提供十四类 RPC（包含通用 `CommitRecords`）：

```text
LoadSnapshot       读取已提交权威快照
LoadMultiSnapshot  按请求顺序批量读取最多64条权威快照，缺失记录保留空位
SaveSnapshot       同步写 PostgreSQL，再刷新 Redis；成功才表示本次提交完成
SaveMultiSnapshot  最多64条普通快照按shard并行，逐记录返回revision或错误，不提供原子性
EnqueueSnapshot    写入 Redis AOF backlog；成功只表示已可靠接收，不表示 PostgreSQL 已落库
EnqueueMultiSnapshot 最多64条普通快照一次写入Redis backlog，禁止携带expectedRevision
ApplyTransaction   提交单记录关键事务并保存原始业务结果
LoadTransaction    按operationId与RecordKey读取已提交事务回执
ApplyMultiTransaction  在一个 PostgreSQL 事务中原子提交多条记录
LoadMultiTransaction   按operationId和记录集合读取跨记录事务回执
CommitRecords          原子提交多记录快照、不可变追加事实、Outbox和业务回执
ApplyTradeTransaction  原子提交交易状态、多记录、账本、Outbox和回执
LoadTrade              读取交易当前版本、状态和不透明Payload
LoadTradeTransaction   按operationId和tradeId读取已提交交易回执
```

每条连接先校验`protocol_version + protocol_fingerprint + auth_token`，之后才允许 RPC。帧使用大端四字节长度前缀，默认上限 8 MiB。一条连接可同时有多个在途请求（服务端`server.maxInFlightPerConnection`、客户端`max_in_flight`，默认均为64），同一连接上涉及同一记录、操作或交易的请求按到达顺序执行，其余并发，见[网络协议](docs/network-protocol.md#连接并发和-endpoint)；`DbProxyClientPool::connect`保持读写共享连接的兼容行为，`connect_split`可把读写分到独立连接组，避免慢写和 AOF ACK 阻塞读连接。两种模式都按`RecordKey`或 operation ID 稳定路由。服务端存储连接也按相同原则分片，避免所有玩家共享一个事务锁。

详细错误码、ACK语义、Endpoint故障切换和跨记录限制见[网络协议说明](docs/network-protocol.md)。

`SnapshotFlushQueue`是 DBProxy 进程内的协调器；`RedisSnapshotBacklog`是独立的 Redis AOF 持久积压区。前者随进程消失，后者只有在入队脚本后通过 `WAITAOF` 才返回成功，DBProxy/Redis 重启后可以重新领取。同一时刻的入队按组提交合并为一次写入和一次 `WAITAOF`，并带排队上限和2秒排队期限，过载时快速返回可重试错误；部署配置`backlog.enqueueAck: "memory"`可改为写入Redis内存即确认（Redis崩溃可能丢约1秒已确认入队，默认`"aof"`），见[架构说明](docs/architecture.md#redis-aof-普通快照-backlog)。两者都只适合等级、任务进度、角色位置等允许小范围回退的数据；关键经济事务必须走 PostgreSQL 事务。AOF 和本地数据卷不等于 Redis 多副本高可用。

## 许可证

Apache-2.0，Copyright 2025-2026 郑昕。

SLG联合验收的辅助程序为`cargo build --bin dbproxy_acceptance_probe`：正式protobuf解码、显式缓存/版本栅栏与原子批量读探针，仅供隔离控制器使用，要求`DBPROXY_ACCEPTANCE_ISOLATED=1`。通过stdin/stdout JSON-lines传输，不输出Hello令牌；缓存修改限SLG/acceptance命名空间，连接目标由隔离控制器分配。它不是运维修复命令，不对业务数据运行。编排、覆盖范围及尚未执行项见[SLG权威读取验收](../TiangZ-Examples/packages/slg/docs/authoritative-read-acceptance.md)。
