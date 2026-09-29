# 索引、回执清理与性能验收交接（2026-09-22）

> 当前进度以 [2026-09-29 剩余清单](acceptance-remaining-20260929.md) 为准。下文保留历史交接状态。F03 已删未提交的真实 DP 强杀三轮通过，见 [精确强杀报告](acceptance-cleanup-sigkill-20260929.md)；F04 提交后、指标前仍待补。

> F08 运行中索引缺失/无效、慢 SQL 超时及修复续清已通过，见 [运行中索引报告](acceptance-runtime-index-20260929.md)。故障期与恢复期 478 笔前台写入均核对一致。

这是本轮接手入口。用户要求保存工作、换另一个 AI 继续；本轮停止新增开发和测试。**代码已落盘但尚未提交，综合验收未完成，两小时验收未开始，旧自然延迟尖峰仍未定位完成。** 不要把后续通过的短测覆盖掉旧失败结论。

> **2026-09-22 评审与改进轮（接在上述交接之后）**：对本交接和 README 做了方案与实现核对，结论是读池、7 天回执清理、索引硬校验三项方案正确，`target/` 原始证据与各报告数字一致。随后按用户指定顺序完成：A 实现与文档对齐的小修；D 台账补齐；B 观测补齐；C 两个故障缺口的测试工具。**旧尖峰定位与磁盘慢注入按用户决定本轮不做。** 具体见文末“评审轮改动”一节；本文件其余段落保留原交接内容，与改动冲突处以该节为准。

## 用户要求与操作边界

- 用简单中文沟通，少用术语。已有授权内直接推进，不要每一步重复问是否继续。
- 最初工作是全库索引检查，以及普通 `dbproxy_idempotency` 回执至少保留 7 天、分批清理。事务回执不按此规则删除。
- 用户要求验收、性能和故障测试计划与实际证据；**不测试旧库升级**，当前开发阶段用新建库。
- Docker 环境及受控本地短测、故障验证已经授权。先解决短测问题，再讨论两小时验收；不得自行启动两小时运行。
- 不提交、推送、部署或清空现有数据。保留所有已有改动及失败记录；不能 reset/clean 工作区，也不能把所有脏文件都假定为本 AI 独占修改。
- 使用 TiangZ backend skill，编辑前检查实际仓库规则。主工程是同级 `../TiangZ`，本仓库为独立 `TiangZ-DBProxy`。

## 工作区与保存

- 仓库：`D:\UGit\TiangZ\TiangZ-DBProxy`。
- 交接时 HEAD：`e54b545caab38085416eb632959db1434f57274c`；工作区大量已修改和未跟踪文件，**HEAD 不包含本轮成果**。
- 另保存本地快照到 `target/handoff_20260922/`：`working-tree.zip`（Git 列出的已修改/未跟踪、非忽略文件）、`tracked.patch`、`status.txt`、`manifest.json`。快照包含交接时全部本地改动，不声称均由本 AI 创建。
- `target/` 内原始测试证据未打入源文件快照，仍保留在当前磁盘。换机器或删除 target 前必须另行复制下述证据目录；只 clone GitHub 无法取回这些代码和证据。
- 当前无仍在进行的本任务压测；测试进程退出，Docker 测试容器继续运行。接手时重新检查实际状态。

## 建议先读的文档

1. [总体测试计划](acceptance-performance-fault-test-plan.md)：38 项（功能 15、性能 9、故障 14），尚未全部验收。
2. [PG 读取连接池](postgres-read-pool.md)和[实现验证](acceptance-read-pool-20260922.md)：正式代码、连接预算及 T/U 失败。
3. [定时等待定位](acceptance-pacing-diagnosis-20260922.md)：压测程序本身晚发的原因证据与可选校准。
4. [校准后三分钟短测](acceptance-calibrated-short-20260922.md)：最新自然负载结果。
5. [SDK/TCP 故障验证](acceptance-tcp-fault-20260922.md)：最新慢写隔离、断连与重试结果及未测边界。

前面的索引及清理成果见：`database-index-audit-20260922.md`、`database-index-validation-20260922.md`、`index-query-validation-20260922.md`、`cache-repair-index-validation-20260922.md`、`outbox-backlog-validation-20260922.md`、`outbox-hybrid-validation-20260922.md`、`record-deletion-and-receipt-retention.md`。更早短测的失败链见 `acceptance-probe`、`acceptance-stall-diagnosis`、`acceptance-client-split`、`acceptance-equal-connections` 等同日期报告。

## 代码进度

### 索引及七天清理

工作区已有迁移 013/014/015、schema 索引检查、Outbox 与缓存修复领取 SQL、回执过期索引和分批清理 worker，以及相关实际查询计划/故障/规模测试。具体范围以以上报告和当前文件为准，不把这些已有改动重做一遍。

### 正式 PG 读取池

- `crates/dbproxy-storage/src/postgres_read_pool.rs` 实现每租户共享、有界的小读池，读取仍访问 PG 主库。
- `storage.postgresReadConnections` 默认 2，0 保留旧共享连接方式用于对照；最大 64。写连接由 `storage.shards` 控制。
- 六类独立 PG 读取路径接入；写事务仍使用原写连接。取消/错误时归还槽位，断连接按已有机制重连。
- 每租户连接数为 `shards + readConnections + 2`，4 写 + 2 读时共 8 条。跨实例/租户必须合计，**两实例两租户可达 32，超过当前 PG max_connections=30**；不要沿用总体计划中的旧连接估算。
- `config.rs/main.rs`、配置 schema/local、server/storage、对应测试和文档已有修改。

### 分段计时及 Rust 压测

- `request_timing.rs` 和 `DbProxyMetrics::request_stage_snapshot()` 区分任务启动、同记录顺序等待、实际处理。测试 host 写 `storage-stages.jsonl`；新分段尚未加入 Prometheus 输出。
- 存储耗时分读取/写入，保留旧合计，不能重复相加。PG 客户端计时包含网络/重连，不等于服务端 SQL 时间。
- 压测由 Rust `acceptance_load.rs` 调用真实 Rust SDK；PowerShell 只做启动、准备库和收集结果。
- `acceptance_pacing.rs` 是纯定时对照。当前 Windows 上 Tokio 定时等待单独就能迟到约 28 ms；标准线程 sleep 对照约 1.4 ms。
- `tools/run_receipt_probe.ps1` 提供 `-PacingTimer std`（默认仍 tokio，**接手短测须显式选 std**）、`-ReadConnection pooled -ClientConnections split4`。绝对发送计划、200/s、32 在途、计时起点和核对规则不变。
- `examples/support/client_timing.rs` 诊断通道容量 65,536，测试结束和读回后才排出。190 秒负载约 57,001 条事件可容纳；**不能直接用现实现跑两小时**。先改为有界持续排出并检查观察开销，溢出必须失败，不能静默丢事件。
- 当前负载只测普通读取/普通写入各半，不是完整六类业务混合；example host 也不等于启动所有生产后台 worker 的完整服务。

## 最重要的结论

1. 旧共享 PG 连接确实会让慢写拖住无关读取；只拆 SDK TCP 连接不能根治。正式 PG 读池的隔离性已有确定性 SDK/TCP 测试证明。
2. 修正压测端定时后，AC/AD 两轮一分钟及 AE 一轮三分钟均无漏发或数据错误。AE 共 38,000 请求，核对 19,000 写入；读取 P99 2.525 ms、最大 6.556 ms。十万过期夹具回执清理完，十万新回执保留。
3. **不能宣称旧尖峰已解决**：T/U 最慢读取在 SDK 调用后仍耗时约 102/139 ms，不能全归因于压测晚发。AE 没出现超过 20 ms 的慢 COMMIT；不能当作相同故障条件下通过。
4. 最新四项真实集成全部通过。新增断连用例三轮：写请求卡住时每轮完成 20 次无关读取；杀进行中的写连接返回错误且无残留；同号重试 Applied 版本 1，再重放 Duplicate；杀正在读取的连接返回错误；再杀本库其他客户端连接，原 SDK 后续读写恢复，回执仍只有一条。
5. 最新断连测试是提交前中断、事务回滚；**尚未验证提交成功但客户端没收到响应**。它在测试进程中运行真实 server/TCP/PG，不是独立 DP 进程重启，也不是持续 200/s 故障压测。维护 worker 恢复未由该用例证明。

## 下一步如何接

优先补两个缺口，而不是继续重复无故障三分钟测试：

1. **提交成功、响应丢失**：用测试专用 TCP 转发/屏障等方式，确认 PG 已提交后丢弃响应。客户端应得到结果未知/连接错误；保持原请求号重试，确认返回原版本且只有一份数据与回执。不要在生产逻辑添加无保护故障开关，也不要把提交前杀连接冒充此场景。
2. **持续负载中的单项注入**：沿用校准后的固定速率 Rust 工具，先正常、再单项慢写或断连接、再恢复；分阶段记录错误、漏发、恢复时间，最后核对所有写入。故障阶段允许什么错误、恢复阶段必须满足什么条件，应在跑前写清。不能直接放宽现有正常短测“零错误”门槛。
3. 发现自然尖峰时，用服务端顺序等待/处理、PG 读写计时和客户端发送/往返区分来源；不要凭没有复现就修改同记录顺序契约或增加并发上限掩盖问题。
4. 更新 38 项计划的执行状态和剩余项；满足短测条件、补齐长测工具边界后，再向用户说明具体两小时方案。用户尚未授权现在直接开始两小时验收。

## 环境与命令

Docker 项目 `dp-acceptance-20260922`：

| 角色 | 容器 | 主机端口 |
|---|---|---:|
| PG 18.6-bookworm | dp-acceptance-20260922-postgres | 15432 |
| 可靠 Redis 8.8.1 | dp-acceptance-20260922-redis | 16379 |
| 易失缓存 Redis 8.8.1 | dp-acceptance-20260922-cache | 16380 |

PG 4 CPU/8 GiB、shared_buffers 2 GiB、max_connections 30；持久化开启。可靠 Redis AOF/noeviction，缓存 Redis 无持久化。连接凭据从本地 Compose/已有脚本取得，不在交接文档重复。短测 Redis DB14；最新断连用例 DB13。旧 `tiangz-dbproxy-index-validation` 项目此前已停止、卷保留，不清理。

每次新建唯一 PG 测试库及 RunId，绝不复用旧证据目录。最新故障库 `tcp_fault_20260922a` 已使用；新测试名必须更换。断连用例额外要求 `DBPROXY_FAULT_DATABASE` 精确匹配当前库且以 `tcp_fault_` 开头；先检查容器项目归属。

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
npm run test:typescript
cargo build -p tiangz-dbproxy-server --release --examples --bins --locked
# 配置专用新 PG/Redis 与故障库名后，按故障报告中的环境变量运行：
cargo test -p tiangz-dbproxy-server --test read_write_contention --locked -- --ignored --test-threads=1 --nocapture
```

最新检查：格式、Clippy、release 重建通过；默认 Rust 191 项、TypeScript 21 项通过，另有上述 4 项真实集成通过。默认忽略的外部数据库测试不算已经重新执行。计时期间不要同时编译或运行其他测试。

原始证据重点保留：`target/read_pool_20260922/`、`read_stages_20260922/`、`pacing_20260922/`、`calibrated_20260922/`、`tcp_fault_20260922/`，以及 `target/probe_20260922*` 各轮目录，尤其失败 T/U。独立审计脚本和结果位于对应诊断目录。`target` 被 Git 忽略，报告不是这些原始证据的替代品。

## 评审轮改动（2026-09-22，接手评审后）

### 评审结论

- 方案正确：读池把独立读取从写分片连接拆出，根因“共享一条 PG 连接”已由确定性测试证明；回执清理只删 `dbproxy_idempotency`、168 小时是编译期常量、按 ctid 分批删除；索引检查在启动时硬失败。
- 证据一致：AE、T/U、读池 9 项集成、tcp_fault 3 轮、pacing 三组定时、`handoff_20260922/status.txt` 与当时工作区逐项核对无出入。
- 漏发的定义是压测端 32 个在途名额被占满时直接标记未发出（`acceptance_load.rs`），T/U 的漏发是慢提交把写请求堆在名额里的连带结果，不是丢数据。读取 102/139 ms 慢在哪一段仍未定位。
- 实现与文档的一处不一致：普通/排队保存返回“重复请求”后，为刷新缓存回读快照的那一次读取走读池（`crates/dbproxy-storage/src/lib.rs` `save_batch_with_fences`）。语义正确（已提交数据在新连接必可见），已在 `postgres-read-pool.md` 写明；写事务本身不占读池名额。

### A 小修（代码 + 文档）

- `postgres_read_pool.rs`：两处 `lock().unwrap()` 改为中毒后 `into_inner()`；`connect` 文档注明 0 由服务端处理、池本身只接受 1..=64；错误文案改为说明 0 的含义。
- `crates/dbproxy-storage/src/lib.rs`：`load_authoritative_multi` 与 `postgres_request.rs` 的过期注释改正；重复写回读处加注释说明走读池。
- `crates/dbproxy-server/src/lib.rs`：新增 `MAINTENANCE_POSTGRES_CONNECTIONS = 2` 与 `StorageBackendConfig::postgres_connection_budget()`；`main.rs` 启动日志由此推导，不再写死。
- `receipt_retention.rs`：新增单测断言 `receipt_cleanup.sql` 的 `LIMIT` 与 `RECEIPT_CLEANUP_BATCH_SIZE` 一致，防止两处各改一份。
- 文档：读池文档改为按代码六个读取入口描述并写明写路径回读；回执文档写明清理连接是“每实例每租户一条”、多实例各跑一个任务；主工程 `TiangZ/AGENTS.md` 的 DBProxy tag 文字由 v0.6.1 改为实际的 v0.6.2。

### D 台账

- `acceptance-run-20260922.md` 末尾新增“13:05 之后的状态补充”，逐项更新 A10、A12、P01、P05、P09、F01、F02、F06，并把 T/U 失败结论单列保留。计划文档开头指向该表。
- `target/calibrated_20260922/split_percentiles.mjs` 把 AE 读/写分开的 P50/P95/P99/最大值写进 `analysis.json`（`ae.read_write_split`）。本机没有可用的 Python，故用 Node。

### B 观测

- 读池新增 `PostgresReadPool::usage()`；`StorageBackend::read_pool_usage()`；Prometheus 新增 `dbproxy_postgres_read_pool_capacity` / `dbproxy_postgres_read_pool_in_use`（存储轮询采样，0 表示未启用读池）。
- 请求三阶段计时进入 Prometheus：`dbproxy_request_stage_seconds{operation,stage}` 直方图与 `dbproxy_request_stage_max_seconds`，标签固定 15 类操作 × 3 阶段。说明见 `docs/dbproxy-metrics.md`。
- `acceptance_host` 的 `storage-stages.jsonl` 每次采样新增 `read_pool` 字段。
- `examples/support/client_timing.rs` 改为容量 8,192 的有界队列 + 独立写线程持续落盘；溢出仍失败；`summary.json` 新增 `client_timing_drain`（写出条数、单条最大/累计写入耗时、最大队列深度、刷盘次数）和 `client_timing_stop_us`。原“65,536 一次性容量、结束后才排出”的限制已不存在，但长测前仍要看这些开销数字。

### C 故障缺口（工具已写，真实库运行见下）

- F02“提交成功但响应丢失”：`read_write_contention.rs` 新增 `committed_write_with_lost_response_replays_as_duplicate_over_tcp`。测试内起一个只在测试里存在的 TCP 转发，SDK 只连转发；写请求先被 advisory lock 卡在 PG 事务里，确认在等锁后让转发吞掉下一帧服务端响应并断开客户端。四轮交替：吞 1 帧时 SDK 用同一请求号自动重连重发，应得到 Duplicate 版本 1；吞 2 帧时 SDK 返回结果未知的连接错误，随后显式重试得到 Duplicate。每轮核对回执恰好 1 条、快照版本 1、改内容重放报冲突。没有加生产故障开关。
- 持续负载中的单项注入：`acceptance_load.rs` 加 `ACCEPT_FAULT_MODE=1`（错误/漏发交给阶段判定，数据不一致仍直接失败；核对区分“确认成功必须已提交”与“出错的写要么没有要么恰好一份”）；新增 `examples/acceptance_fault_injector.rs`（`blocked_write` / `kill_connections`，要求 `DBPROXY_FAULT_DATABASE` 与 `fault_load_*` 库名一致，记录注入/观察到阻塞/释放时刻）；`tools/analyze_fault_load.mjs` 按发送时刻分正常/故障/恢复三阶段判定；`tools/run_fault_load.ps1` 编排并在跑前写 `fault-plan.json` 规则。方法与规则见 `docs/acceptance-fault-under-load.md`。

### 本轮检查与证据

- 每段结束都跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`cargo test --workspace --locked`、`npm run test:typescript`、`cargo build -p tiangz-dbproxy-server --release --examples --bins --locked`。日志在 `target/improve_20260922/`（`*-a/b/c.log` 对应 A、B、C 段）。
- 真实库验证：评审时 Docker 引擎处于停止状态，用户授权后启动 Docker Desktop 并拉起三个已存在的容器。随后全部执行并通过：`postgres_read_pool` 忽略用例（含新增 usage 断言）、`read_write_contention` 五项（含新 F02 用例，四轮）、`probe_20260922ag` 一分钟 std 短测（诊断持续排出 21,001 条零丢失、开销 6.5 ms）、`fault_load` 的 `blocked_write` 一轮与 `kill_connections` 两轮。结果、数字与保留的第一版 FAIL 判定见 `docs/acceptance-fault-under-load-20260922.md`。
- 编排脚本在 Windows PowerShell 5.1 下的三处问题已修（`docker inspect` 模板引号、`Start-Process` 的 `ExitCode` 为空、`docker logs` 的 stderr 报错），不改测试逻辑；`fault_matrix.ps1` 等其他脚本可能有同类写法，尚未逐一检查。
- 阶段判定规则从“按发送时刻分段”改为“按在途区间与故障窗口重叠分段”（第 2 版），原因与第一版 FAIL 证据都在结果报告里。
- 没有提交、推送、部署、清数据；没有启动两小时验收；没有做磁盘慢注入。

### 服务器容器对照（2026-09-23）

在一台共享 Linux 主机上新增了"只在容器里跑"的方案（`deploy/remote-test/`：工作台镜像、依赖容器脚本、两个编排脚本的 bash 版本），主机只依赖 Docker。同参数一分钟短测和两轮故障注入全部通过，结果与笔记本并排见 `docs/acceptance-container-server-20260923.md`。要点：服务器端发送迟到 P99 从 743 µs 降到 248 µs、合计 P99 从 6.57 ms 降到 4.95 ms、运行时心跳最大延迟从 25 ms 降到 2.1 ms，说明笔记本确有整机停顿；但**服务器上仍出现单次 43 ms 的慢写**，这类写入尖峰不能全部归因于笔记本环境（初版称“PG 无慢 SQL”是读到空日志导致的错误，已更正：同时刻有 28–38 ms 的 COMMIT）。故障注入的错误数和漏发数两边几乎一致，说明那是 DBProxy 连接预算决定的行为。证据已拉回 `target/server_20260923/`。

### 写入尖峰定位（2026-09-23）

服务器容器环境下三轮定位短测：所有 15 ms 以上的慢写都发生在 PostgreSQL 新建 WAL 段文件时（`wal_init_zero=on`，每段写零加刷盘约 25–27 ms，期间所有提交排队）。复用旧段的轮次零尖峰、写最大 7.6 ms。不是 DBProxy 缺陷，不需要改服务代码；可选的 PG 参数方向（`min_wal_size`、`wal_init_zero`）尚未实测。笔记本 T/U 的上百毫秒尖峰机制吻合但未验证。详见 `docs/acceptance-spike-localization-20260923.md`，工具为 `deploy/remote-test/{sample_host,run_spike_round}.sh` 与 `tools/analyze_spikes.mjs`。

### P08 两小时（2026-09-23）

服务器容器环境跑满两小时（`long_p08_a`）：144 万请求零错误、零漏发，72.1 万写入逐条核对零差异，每分钟都无错误；DBProxy 内存、PG 匿名内存、连接、Redis、缓存、回执清理积压都无持续增长；7.2 万条运行中陆续到期的回执全部清完。慢写 132 次（0.018%），归因于 WAL 新段与检查点。**缺口**：测试宿主只启动回执清理任务，没启动缓存修复、积压落库、Outbox 任务，缓存修复队列因此涨到 72 万行，P08 暂记“部分”。另发现回执保存完整写入内容（约 1.5 KB/条）。详见 `docs/acceptance-p08-2h-20260923.md`。长时运行工具：`deploy/remote-test/{run_long_round,launch_long,sample_containers,finish_long_round}.sh`、`pg_periodic.sql`、`tools/analyze_long_run.mjs`。

### P08 第二轮通过（2026-09-23）

测试宿主新增 `ACCEPT_BACKGROUND=all`，按服务端默认配置启动积压落库、缓存修复、Outbox 任务；长时脚本默认启用。`long_p08_b` 两小时：144 万请求零错误零漏发，72.1 万写入零差异，缓存修复队列最多 134 行、结束为 0，内存与连接稳定，读 P99 3.99 ms、写 P99 5.22 ms。慢写 52 次，全在后一小时的三段、都在检查点之后（两段由 WAL 写满 1 GB 的 `max_wal_size` 触发）。P08 记为通过（本环境、单实例单租户、普通读写）。已提交到分支 `acceptance/index-retention-p08-20260923`（`8fb39e1`、`939e32b`），未推送。

### 普通回执保留期改为可配置（2026-09-23）

用户决定：默认 24 小时、下限 1 小时，做成配置。新增 `storage.receiptRetentionHours`（1–8760，默认 24，schema 与 `configs/local.json` 已加）；存储层 `cleanup_expired_receipts(retention)` 改为必传保留时长，越界返回 `InvalidReceiptRetention`，清理 SQL 用绑定参数 `make_interval(secs => $1)`，不再写死 168 小时；`run_receipt_cleanup_worker` 增加保留时长参数，`main.rs` 从配置传入并在启动时打日志。测试：边界改为 23/25 小时与 24 小时、1 小时精确截止，新增配置与校验单测；测试宿主读 `ACCEPT_RECEIPT_RETENTION_HOURS`，两套编排脚本和 `pg_periodic.sql` 跟随保留时长。依据与存储测算见 `docs/record-deletion-and-receipt-retention.md` 的“保留期如何选择”。迁移 015 未改。

### 仍未完成

- 旧尖峰定位（用户决定放到其他工作完成后再讨论；kill 第一轮恢复段出现一簇 99–131 ms 慢写并伴随压测端迟到与慢 COMMIT，线索记在结果报告里）；两小时验收；38 项计划中 P02/P04/P05/P08、F04/F06/F09/F10/F15 等仍待补。
