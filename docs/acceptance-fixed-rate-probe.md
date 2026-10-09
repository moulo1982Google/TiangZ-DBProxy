# 普通读写与回执清理：定速校准工具

关于已发现的提交等待与读写连接排队，参见[诊断报告](acceptance-stall-diagnosis-20260922.md)。`-ReadConnection dedicated` 只启用诊断适配器，不能用其结果宣称正式服务已经修复。

`-ReadConnection pooled` 使用正式 StorageBackend 的每租户 2 条共享读池，与原 4 条写分片分开；`shared` 显式禁用读池用于旧行为对照，`dedicated` 仍是历史单条读取诊断适配器。正式配置说明见[PG 读取连接池](postgres-read-pool.md)。

这是综合验收前的工具校准，不是完整性能验收。入口为 `tools/run_receipt_probe.ps1`，两个 release example 分别作为独立的服务进程、发压进程运行。

## 工具覆盖范围

- `acceptance_host` 使用正式 TCP 服务、4 个 PG 请求分片、独立缓存 Redis 和正式回执清理函数。`ACCEPT_CLEANUP=off/on` 只存在于 example，不是生产服务新增开关。两种模式都保留相同连接配置。
- 这个宿主没有启动全部生产后台任务，因此开启清理模式不能称作完整生产 B2。
- `acceptance_load` 按单调时钟计划发送。达到在途上限时记为 `not_sent`，不会等待空位后悄悄降低负载。端到端耗时从计划发送时刻算起，另记实际发送等待。
- 客户端连接由 `-ClientConnections shared4|shared8|split2|split4` 选择：默认 4 条共用 TCP 连接；`shared8` 为 8 条共用连接；`split2` 调用现有 `connect_split(config, 2, 2)`，总连接数仍为 4；`split4` 为读写各 4 条，总共 8 条。它与 `-ReadConnection` 控制的 DP → PG 连接是不同层，必须分别记录和对照。历史 A–H 轮使用 `shared4`，不能声称已经启用客户端读写分离。
- 当前工作负载为 50% 固定记录的权威读取、50% 不同键的新建条件写；固定 payload 可由请求编号重建。不是正式六种业务的混合比例，也不是冷热缓存、持续更新热点或全部事务负载。
- 每次发请求前写 intent，完成后持续写 response。无响应错误保留错误文本，按未知结果处理，不能说成一定未落库。每阶段完成后落盘；不承诺宿主突然断电时最后一条日志可恢复。
- 测量结束后逐条通过权威读取核对所有已发送写入的 payload 和 revision。此阶段与建库、预置、清理后 COUNT 均不计入发压延迟。
- 调度等待、大量 `not_sent`、发压进程资源必须一并看；不能只取已完成请求的最好耗时。当前工具还没有完整跨操作请求账本、数据库原始逐表核对与连续 PG/WAL/锁采样，仍不满足完整性能验收。

## 执行

先构建和检查，**不要一边正式计时一边编译或运行其他测试**：

```powershell
cargo build -p tiangz-dbproxy-server --release --examples --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

已获用户授权的隔离环境中运行；连接地址通过调用参数显式提供，不能填业务库：

```powershell
./tools/run_receipt_probe.ps1 -RunId probe_unique_id `
  -Project $testProject -PostgresContainer $testPgContainer `
  -PostgresBaseUrl $testPgBaseUrl -RedisUrl $testRedisUrl -CacheRedisUrl $testCacheUrl `
  -Rate 200 -Seconds 30 -WarmupSeconds 10 -Rounds 3 `
  -PayloadBytes 1024 -Concurrency 32
```

`PostgresBaseUrl` 为不含数据库名的测试 URL；脚本以 `tiangz` 用户创建新库。调用前核对 URL 指向参数中的容器。每次用新的 RunId，旧日志和旧库不会清空。固定监听端口为本机 17980，若占用应先排查归属，不得停止未知进程。

脚本从当前版本新建基础库，预置 10 万近期和 10 万过期回执。每组复制同一基础库，按 off/on、on/off、off/on 交替执行。近期回执和过期回执的数量在结束后另存；预期近期不变、off 不删、on 有进展。

默认 10 秒预热、30 秒采样是短校准，**不得冒充计划规定的 2 分钟预热、5 分钟采样或两小时运行**。提高参数不会自动让当前单一工作负载变成完整验收。

`target/<RunId>/` 保存每组请求记录、核对记录、摘要、进程资源、退出日志、剩余回执数量和二进制摘要。任何请求错误、数据不一致或漏发都会让负载进程失败；脚本保留全部证据并停止后续组，只关闭本次启动的进程。

Windows 校准中观察到发送调度 p99 可接近 16 ms；这包含计时器与本机调度等影响，不能仅凭该数值确定根因。正式容量测试须评估发压端限制，分别报告发送等待和端到端耗时，禁止从两个 p99 相减推算数据库 p99。

宿主现另采集每约 100 ms 的存储阶段耗时桶、请求三阶段计时、读池容量与在用数，以及专用连接读取的 PG 会话等待状态，写入 `storage-stages.jsonl`，采样耗时和错误也保留。诊断采样会增加少量开销，不同采样版本不能混算性能。脚本给发压结束后的核对额外 180 秒期限，适用于短校准；不得直接把采样时间改成两小时就当作长测工具已完备。

发压端的 SDK 诊断事件（`client-timings.jsonl`）自 2026-09-22 起由独立写线程从容量 8,192 的有界队列持续写出，不再等到测试结束才落盘，因此运行时长不再受一次性缓冲容量限制。SDK 回调只做非阻塞入队；队列满则计入 `client_timing_dropped`，任何丢失都让负载进程失败。`summary.json` 的 `client_timing_drain` 记录写出条数、单条最大/累计写入耗时、观察到的最大队列深度和刷盘次数，用来判断诊断本身的开销；`client_timing_stop_us` 是结束时等待写线程收尾的时间。长测前仍须核对这些开销数字，不能默认为零。
