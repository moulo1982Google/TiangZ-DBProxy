# 客户端读写分离短测对照（2026-09-22）

结论：现有 `connect_split` 已实际启用并完成六轮对照。相同总数的 4 条 TCP 连接拆成读写各 2 条，仍出现一次漏发；读写各 4 条的两轮通过，但未遇到失败轮同等程度的 PG 提交尖峰，不能据此宣布问题已解决，也不能直接决定必须修改生产 PG 连接结构。

后续[相同连接数及受控阻塞测试](acceptance-equal-connections-20260922.md)已完成：读写各 4 条也出现失败轮，并确认同一 PG 请求连接上的读取仍会被写入阻塞。以该后续报告作为当前结论。

## 条件及结果

Docker PostgreSQL 18.6，4 CPU / 8 GiB，shared_buffers 2 GiB；可靠 Redis 与缓存 Redis 均为 8.8.1。DP 到 PG 保持正式后端的 4 个读写共用请求分片，未启用独立 PG 读取原型。服务线程 4、发压线程 32、在途上限 32。普通回执清理开启；每个新库预置 10 万过期、10 万近期回执。

每轮预热 10 秒、采样 60 秒，每秒 200 次请求，1 KiB，50% 固定记录读取、50% 不同记录新建写入。六轮顺序为 split2 / shared4 / split4 / split4 / shared4 / split2，使用同一对 release 二进制，计时阶段没有并行编译或其他测试。

| 测试目录后缀 | 客户端 → DP | 总 TCP 连接 | 采样漏发 | 最慢读取 ms | 已发送写入核对 | PG 日志最慢 COMMIT ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| i | 读写各 2 条 | 4 | 31 | 370.852 | 6,983 正确 | 59.424 |
| j | 共用 4 条 | 4 | 0 | 29.339 | 7,000 正确 | 未记录 ≥20 ms |
| k | 读写各 4 条 | 8 | 0 | 32.134 | 7,000 正确 | 22.077 |
| l | 读写各 4 条 | 8 | 0 | 33.776 | 7,000 正确 | 未记录 ≥20 ms |
| m | 共用 4 条 | 4 | 36 | 452.562 | 6,983 正确 | 70.234 |
| n | 读写各 2 条 | 4 | 0 | 38.918 | 7,000 正确 | 未记录 ≥20 ms |

漏发表示发压端在 32 个请求尚未完成时没有发出计划请求，不是已提交数据丢失。最慢读取为采样请求端到端耗时，包含发送等待。核对数量包含预热、不含一条初始化记录。

合计计划 84,000 次请求，83,933 次发出并收到正确响应，67 次未发送；41,966 次写入全部读回核对 payload 和 revision。独立解析日志确认 intent/response 一一对应、编号无重复、全部计划编号由已发送或未发送记录覆盖，写入核对记录完整。数据库快照及普通回执行数均为本轮写入数加一条初始化记录。每库 10 万近期夹具回执均保留；过期回执剩余 60,000 或 60,500，清理确实推进。

## 结论边界与下一步

- 客户端分离和 DP → PG 分离是两层独立机制。此前 A–H 使用普通 `connect(config, 4)`，没有开启 SDK 已有的分离功能；这一遗漏已在原诊断报告更正。
- split2 的失败证明此次条件下仅将相同数量的客户端连接分开，并不能消除偶发排队；不能把它解释成分离功能无效。
- split4 两轮通过是可继续验证的结果。它既分离读写又增加了连接数，而且没有遇到 59–70 ms 的提交尖峰，不能与失败轮作严格因果比较。
- 下一步应对共用 8 条与读写各 4 条做相同连接数对照，并以受控写入阻塞验证读取受影响程度。固定热点及其他读取/写入形态也需覆盖。先完成这些短测，再决定是否调整生产 PG 连接及其数量。
- 未做两小时验收，未完成正式六种操作混合负载；没有修改生产服务或 SDK 实现。

## 工具、验证与证据

`acceptance_load.rs` 增加 `ACCEPT_CLIENT_CONNECTIONS=shared4|split2|split4`，分别调用已有 `connect(config,4)`、`connect_split(config,2,2)`、`connect_split(config,4,4)`；默认保持 shared4。`run_receipt_probe.ps1` 增加同名含义参数 `-ClientConnections` 并记录 manifest。具体调用及限制见[定速工具](acceptance-fixed-rate-probe.md)。不要增加在途上限、跳过失败轮或仅凭最佳轮宣称通过。

复测固定参数：`-Rate 200 -Seconds 60 -WarmupSeconds 10 -Rounds 1 -CleanupMode on -HostWorkers 4 -LoadWorkers 32 -SqlLogThresholdMs 20 -ReadConnection shared`，逐轮指定新 RunId 和 `-ClientConnections`。测试 Docker 项目为 `dp-acceptance-20260922`，原始目录 `target/probe_20260922i` 至 `target/probe_20260922n`。每轮失败原样保留，比较驱动仅在确认属于漏发且错误和数据不一致均为零后继续下一独立轮，不将失败标记为通过。

汇总证据 `target/sdk_split_20260922/`：`run.ps1`、`run.log`、`audit.py`、`audited-results.json`、`database-counts.json`；各轮保留请求账本、读回核对、PG 日志、采样、二进制 SHA256 和启动模式。

release examples 构建、格式检查、Clippy（workspace/all-targets，拒绝 warning）、workspace 189 项、TypeScript SDK 21 项均通过；检查日志保存在上述汇总目录。SDK 测试调用官方 TypeScript 生成/构建入口，未修改协议或手工编辑生成物。真实数据库验证为本次六轮短测，未重跑整个故障矩阵。测试进程均已退出，容器和证据保留；未提交或部署。
