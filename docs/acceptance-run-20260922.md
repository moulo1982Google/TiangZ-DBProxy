# 索引与回执清理：第一批实际执行结果

状态：**部分完成，不能据此宣布 38 项综合验收通过。** 本轮由用户“请开始”授权执行，未测试旧库升级、未提交、未部署。当前完成隔离环境、基础回归、部分真实进程验证、第一批故障和 SQL 规模验证；固定速率性能对照和两小时测试尚未执行。

## 环境与证据

- Docker 项目 `dp-acceptance-20260922`，独立数据卷；PG / 可靠 Redis / 缓存 Redis 对应宿主端口 15432 / 16379 / 16380。旧项目已停止，旧卷保留。
- PG 18.6，4 CPU 配额、8 GiB 内存、shared_buffers 2 GiB；可靠与缓存 Redis 8.8.1。故障后三个容器均恢复 healthy。
- 真实进程用重新构建的 release 服务端；SQL/库级集成用 Rust test 构建。不能将后者的耗时说成 release 服务吞吐。
- 各测试组使用独立 PG 数据库；普通测试 Redis DB 5/6，故障 DB 15，多租户 DB 1/2。测试串行；故障结束后才运行百万回执验证。未执行 FLUSHDB。
- 原始证据目录：`target/acceptance-20260922/`。`base-commit.txt`、`tracked-changes.patch`、`source-snapshot.zip`、`sha256.json` 保存基线、修改、源代码和服务二进制摘要。`process/` 保存进程配置、输出与真实监控响应。目录中的日志不纳入 Git。

## 已运行测试

以下是 **65 个不同的集成测试函数**，不是计划中 65 个验收项。规模测试修正计时后另跑一次，不重复计数。所有列出的运行均通过。

| 自动化入口 | 数量 | 夹具与断言 | 原始日志 |
| --- | ---: | --- | --- |
| storage/schema_indexes | 3 | 当前空库、结构与索引损坏检测 | schema_indexes.log |
| storage/postgres_redis | 22 | 快照、事务、事实、缓存和排队保存 | postgres_redis.log |
| storage/cache_repair_concurrency | 8 | 修复领取、版本竞争、租约与锁 | cache_repair_concurrency.log |
| storage/outbox_concurrency | 8 | 消息领取、队首阻塞、顺序与租约 | outbox_concurrency.log |
| storage/query_plans | 3 | 批量读取、领取与执行时点 | query_plans.log |
| storage/repair_claim_plans | 2 | 修复队列分布、计划与结果 | repair_claim_plans.log |
| storage/outbox_hybrid_plans | 1 | 九种积压分布、两条领取路径 | outbox_hybrid_plans.log |
| storage/receipt_retention | 5 | 时间边界、分批、锁、冲突间隙和重试；明确跳过旧库升级用例 | receipt_retention.log |
| server/receipt_cleanup_worker | 1 | 1,101 过期+1 近期，后台分批、保护事务表、空闲退出 | receipt_cleanup_worker.log |
| server/postgres_redis_network | 2 | 真实 TCP 基础链路 | postgres_redis_network.log |
| storage/outbox_relay | 1 | 发布与确认基础链路 | outbox_relay.log |
| server/acceptance_process | 1 | release 进程、两租户、清理、强杀后重启、缺索引拒绝启动、监控 | process-test.log、process/ |
| storage/receipt_cleanup_faults | 1 | 三次精确中断未提交删除，全部回滚，恢复续清 | receipt_cleanup_faults.log |
| storage/fault_matrix | 7 | PG/Redis 停启、缓存强杀、持久积压恢复；有实际注入事件 | fault_matrix.log |
| storage/receipt_scale | 1 | 1万/10万/100万近期回执 × 5 种过期数量，正确删除且无全表扫描 | receipt_scale_r2.log |

另通过 workspace 189 项、TypeScript SDK 21 项、格式检查、Clippy（警告视为错误）、actionlint。未修改协议，没有运行协议代码生成。新增测试与脚本属于本轮测试工具补充，未改变生产业务行为。

### 百万回执的观察结果

每行近期回执使用 1 KiB 固定内容，100 万行表及索引总计约 1.18 GiB。所有规模与过期数量组合的清理计划都使用保留时间索引，没有 Seq Scan；每批实际删除不超过 500，核对后近期数据数量不变。

| 100 万近期回执中另加的过期条数 | 完成清理调用的合计耗时 |
| ---: | ---: |
| 0 | 2 ms |
| 1 | 5 ms |
| 500 | 5 ms |
| 501 | 8 ms |
| 1,101 | 11 ms |

这是单轮、已执行 EXPLAIN 的热数据 SQL 样本，包含末尾确认空批的调用；不包含后台每满批等待 1 秒、不包含测试核对 COUNT，不是完整服务的吞吐、p99 或清空百万积压所需时间。不能据此通过 P02/P04/P05/P08。

首次脚本把核对 COUNT 也计入耗时，100 万行空批显示 1,256 ms。检查后发现是测试计时范围错误，已把计时终点移到清理调用完成处，并在全新 `accept_receipt_scale_r2` 数据库完整重跑。原 `receipt_scale.log` 保留；性能引用只用 `receipt_scale_r2.log`。生产清理 SQL 没有增加 COUNT。

## 与 38 项计划的对应及缺口

“部分”表示相关断言通过，但尚未覆盖计划整项；“待测”不算通过。

| 编号 | 本轮证据 | 剩余内容 / 状态 |
| --- | --- | --- |
| A01 | schema_indexes、acceptance_process | 空库与重复启动有证据；进程与结构组合继续整理，部分 |
| A03 | schema_indexes、acceptance_process | 实际进程仅验证保留索引缺失，其他损坏类型仍为库级，部分 |
| A04 | postgres_redis、network、process | 基础功能通过 |
| A05 | postgres_redis、worker | 缺与持续清理并发的完整事务账本，部分 |
| A06 | retention、process | 精确边界与不续期有证据；完整过去/未来业务时间矩阵待补，部分 |
| A07 | retention、worker | 真实进程大批清理及计数对应待补，部分 |
| A08 | retention、process | 保留期重放、过期后重新应用和 CAS 保护通过 |
| A09 | retention | 精确冲突间隙、读锁保护通过 |
| A10 | retention | 8 路已有；32 路、双进程待补，部分 |
| A11 | cache_repair_concurrency | 当前并发正确性组通过 |
| A12 | outbox_concurrency | 40 条前缀已有；31/32/33 与全路由组合待补，部分 |
| A13 | process | 相同键及请求号隔离通过；持续负载隔离待测，部分 |
| A14 | worker | 仅函数级空闲退出；真实进程各停止阶段待测，部分 |
| A15 | worker | 保护表样本通过；全表业务账本待补，部分 |
| A16 | process | 成功清理真实监控已有；失败与锁超时监控矩阵待补，部分 |
| P01 | query_plans、network | 无冷热缓存定速 TCP 对照，部分 |
| P02 | 无 | 索引写入成本与 16 KiB 场景待测 |
| P03 | receipt_scale_r2 | 全部指定数量 SQL 样本通过；仅作本项 SQL 证据 |
| P04 | 无 | 百万全部过期及混合积压待测 |
| P05 | 无 | B1/B2 固定速率对照待测 |
| P06 | repair_claim_plans | 分布样本通过；50% 锁及完整压力待测，部分 |
| P07 | hybrid、concurrency | SQL 分布通过；持续轮询、全边界及负载指标待测，部分 |
| P08 | 无 | **两小时持续运行尚未开始** |
| P09 | process | 双租户基本行为已有；并发容量与双进程待测，部分 |
| F01/F02 | 无 | 写提交前、提交后丢响应屏障待补 |
| F03 | receipt_cleanup_faults | 已删除但未提交时终止专用 PG 连接，连续三轮通过；尚非 DP 进程强杀变体 |
| F04 | 无 | 清理已提交、指标前强杀进程待补 |
| F05 | fault_matrix | PG 中断写入/队列恢复已有；与清理并发待补，部分 |
| F06 | 无 | 保持连接的网络阻断待补 |
| F07 | retention | 库级行锁/表锁已有；真实后台退避节奏待测，部分 |
| F08 | process | 缺索引拒绝启动已有；运行中索引问题待测，部分 |
| F09/F10 | 无 | 连接耗尽、受限存储故障夹具待补 |
| F11 | fault_matrix | 可靠 Redis 持久队列恢复已有；清理并发与确认间隙待补，部分 |
| F12 | fault_matrix | 缓存强杀后不会恢复已确认前的旧版本，通过 |
| F13 | relay、concurrency | 基础发布和租约已有；发布后确认前进程崩溃待补，部分 |
| F15 | 无 | 租户故障压力与监控中断待补 |

本轮先执行现有隔离故障回归以验证工具可用，因此不是按计划完成所有功能和性能后开展的最终故障验收。最终轮仍须覆盖上述缺口。尚未建立完整 B0/B1/B2 对照、固定速率请求账本及资源采样，现有闭环业务负载工具不能直接冒充它们。

后续执行见[第二批执行记录](acceptance-probe-20260922.md)。千万级和自然七天不在本轮已执行结果内。

## 13:05 之后的状态补充（2026-09-22 评审时整理）

上表是 13:05 的快照。之后的[第二批](acceptance-probe-20260922.md)、[读池实施](acceptance-read-pool-20260922.md)、[分段定位](acceptance-read-stage-diagnosis-20260922.md)、[定时定位](acceptance-pacing-diagnosis-20260922.md)、[校准短测](acceptance-calibrated-short-20260922.md)和[SDK/TCP 故障验证](acceptance-tcp-fault-20260922.md)改变了以下条目的状态；未列出的条目状态不变。证据目录均在本地 `target/`。

| 编号 | 新增证据 | 现状态 |
| --- | --- | --- |
| A10 | 32 路调用方复用 8 个 PG 连接；两个真实 release 进程共用同一新库，32 路重试与清理并行，只有 1 次合法新应用（`probe_20260922*`、`read_pool_20260922/process-repeat.log`） | 通过（范围内） |
| A12 | 31/32/33/40 四种锁定前缀各 8 个调用方竞争通过（第二批） | 通过（范围内）；全路由组合仍待补 |
| P01 | 200 请求/秒、1 KiB、读写各半、读池 4 写 + 2 读、SDK split4 的多轮定速 TCP 短测，含 T/U 两轮失败与 AE 三分钟通过 | 部分：有定速 TCP 证据，无冷热缓存对照 |
| P05 | 2026-09-23 服务器容器 200 请求/秒，清理关/开交替各 3 轮（预热 2 分钟、测 5 分钟），无积压与 30 万过期回执两个场景：吞吐 0% 变化，读写 P99 变化 −2.3% 到 +5.8%（`acceptance-p05-20260923.md`） | 通过（固定速率；承载能力阶梯未做） |
| P02 | 2026-09-23 服务器容器 200 请求/秒，1 KiB/16 KiB × 有无迁移 013–015 的 6 个新索引 × 2 次，共 8 轮全部零错误：写 P99 差 +0.3%/+1.8%（≤0.11 ms），WAL 每次写入多约 290 B（在轮间波动内），回执索引多约 9 MB/14 万条（约 68 B/条）（`acceptance-p02-20260923.md`） | 完成量化（固定速率；载荷可压缩，16 KiB 绝对值不代表真实数据） |
| P04 | 2026-09-24 服务器容器 200 请求/秒 45 分钟，100 万过期 + 100 万近期回执：约 2,000 秒清完（约 490 条/秒，即满批停 1 秒的节奏上限）；清理期间与清完后读写 P99 相差不到 0.13 ms；54 万请求零错误零漏发，27.1 万写入零差异，近期回执不少；首轮 `p04_a` 漏传核对时限判失败，保留（`acceptance-p04-20260924.md`，`p04_b`） | 通过（固定速率；同一请求号跨清理的不同时刻重试未专门安排） |
| P09 | 双进程同库正确性有证据；并发容量、连接预算竞争未测 | 部分（不变，证据增加） |
| F01 | 用触发器内 advisory lock 做确定性事务屏障，提交前终止写连接 3 轮：回滚干净、同号重试 Applied 版本 1、重放 Duplicate（`tcp_fault_20260922/real.log`） | 部分：屏障已有；独立 DP 进程与持续负载下的变体待补 |
| P08 | 2026-09-23 服务器容器两小时两轮：`long_p08_a` 缺后台任务；`long_p08_b` 含积压落库、缓存修复、Outbox 任务，144 万请求零错误零漏发，72.1 万写入零差异，资源与队列有界（见 `acceptance-p08-2h-20260923.md`） | 通过（本环境、单实例单租户、普通读写） |
| F02 | 测试进程内 TCP 转发在 PG 提交后吞掉响应，四轮：吞 1 帧 SDK 同号重发得 Duplicate，吞 2 帧客户端得结果未知后显式重试得 Duplicate；回执恰好 1 条、版本 1（`tcp_fault_20260922b`、`docs/acceptance-fault-under-load-20260922.md`） | 通过（范围内）；独立部署与真实网络设备未测 |
| F06 | 无网络阻断，但杀掉该库全部客户端 PG 连接后前台读写自动重连通过；200/s 负载中杀 8 条连接两轮，最多 1 个在途写回滚、无漏发、立即恢复（`fault_load_fault_kill_20260922a/b`） | 待补（重连证据不替代阻断/延迟） |
| F06 补充 | 2026-09-24 经 TCP 转发层保持连接：停转 5 秒、30 秒、每方向延迟 20 ms，200/s 下全部按预定规则通过；排队请求 2 秒明确失败并回滚，卡住的写返回“结果未知”（其中 4 次恢复后实际提交，核对一致），无假成功，无新建连接，释放后毫秒级恢复（`acceptance-fault-network-storage-20260924.md`，`f06_*_b`） | 通过（转发层模拟；真丢包下卡住 SQL 的上限取决于系统 TCP 重传，建议配置保活/发送超时） |
| F10 | 2026-09-24 200/s 下：只读 10 秒，写入全部明确失败、读取正常、零漏发、释放即恢复；一次性 PG 放在 1 GiB 内存盘上写满 15 秒，1,471 次写入十几毫秒内明确失败并全部回滚，PG 未崩溃，删除占位文件即恢复；两轮核对零差异（`acceptance-fault-network-storage-20260924.md`，`f10_ro_a`、`f10_disk_a`） | 部分：只读与数据文件满通过；WAL 写满崩溃重启、日志写入失败未测 |
| F09 | 2026-09-24 真实进程、非超级用户角色：额度少 1 条时 0.3 秒明确退出（53300 too many connections），不接客、不退回内存；额度正好 6 条启动占 6 条；运行中断连 11 秒写读全部 StorageUnavailable、无假成功、最慢 32 ms、重连约每秒 6 次；恢复后 31 ms 自动可写，失败写入一条未落库（`acceptance-fault-process-20260924.md`，`fp_a`） | 通过 |
| F15 | 同进程双租户，A 断库约 56 秒：A 写入全部失败且未落库、A 过期回执未动；B 7,294 次请求零错误、清理照常；互不串库；指标按租户归属正确。首轮日志 393 条告警只有 1 条带租户、原因只显示 `db error`；修复后复测 `fp_f` 396 条全部带租户并写出 SQLSTATE（同上文件） | 部分：租户故障通过；监控服务短时不可用未测 |
| F04 | 清理中随机强杀 8 次（2 万过期 + 5,000 近期）：每次删除都是整批 500 的倍数，近期回执、业务回执、其余全部表行数不变，最终续清完成、重放全部 Duplicate（`fp_d`；`fp_b`/`fp_c` 为测试自身错误，保留） | 通过（随机时刻；精确“提交后记日志前”时刻无钩子未覆盖） |
| F01 补充 | 200/s 负载中卡住一条写 5 秒：同分片其他写等连接超 2 秒预算后返回可重试错误 31 次并全部回滚，354 次漏发，释放后 3 ms 恢复，读取不受影响（`fault_load_fault_blocked_20260922a`） | 部分：负载下的屏障变体已有证据 |

**保留的失败结论**：`probe_20260922t`、`probe_20260922u` 两轮读池短测漏发 2 / 9，最慢读 112.754 / 146.367 ms，与 PG 日志 47.383 / 80.517 ms 的慢 COMMIT 同轮出现。漏发的定义是压测端 32 个在途名额被占满时直接标记未发出，不是数据库丢数据；写入核对全部正确。后续无慢提交条件的 W–Z、AA–AE 均零漏发，但读取慢在哪一段仍未定位，不能据 AE 通过覆盖 T/U 失败。暂定验收线“无故障固定负载下不出现非预期错误或超时”在 T/U 条件下未满足。

**证据补齐**：AE 读/写分开的 P50/P95/P99/最大值现已由 `target/calibrated_20260922/split_percentiles.mjs` 写入 `analysis.json` 的 `ae.read_write_split`（读 P99 2.525 ms、最大 6.556 ms；写 P99 7.151 ms、最大 18.426 ms），此前只在报告正文中。
