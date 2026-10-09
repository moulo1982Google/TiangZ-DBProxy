# 评审轮真实库验证结果（2026-09-22）

方法与规则见[持续负载中的单项故障注入（方法）](acceptance-fault-under-load.md)。本轮是评审改动后的真实 PG 18.6 / Redis 8.8.1 验证，环境为已授权的 Docker 项目 `dp-acceptance-20260922`（PG 4 CPU / 8 GiB、shared_buffers 2 GiB、max_connections 30）。**不是两小时验收，也没有定位旧尖峰。** 每项都用全新数据库与 RunId；失败轮与第一版判定原样保留。

## 环境状态

评审开始时 Docker 引擎处于停止状态，三个容器为 `Exited (255)`。用户授权后启动 Docker Desktop，用 `docker start` 拉起这三个已存在的容器（数据卷未动），全部 healthy 后才开始验证。期间发现三处只在 Windows PowerShell 5.1 下出现的编排脚本问题并修正（`run_receipt_probe.ps1`、`run_fault_load.ps1`）：`docker inspect` 模板里的内嵌双引号被吃掉导致“容器归属不符”；`Start-Process -PassThru` 没先读 `Handle` 时 `ExitCode` 为空，被当作停机失败；`docker logs` 走 stderr 触发 NativeCommandError 中断收尾。三处修正都不改变测试逻辑；`probe_20260922af` 是第一处问题的失败痕迹（只建了基础库，没有负载）。`probe_20260922ag` 负载通过后在第三处中断，其 `postgres.log` 与 `postgres-cpu.txt` 是脚本报错后按目录时间窗补采的，见同目录 `evidence-note.txt`。

## 结果一览

| 项 | 库 / RunId | 结果 |
|---|---|---|
| 1 分钟定速短测（std 定时、读池、split4、清理开启） | `probe_20260922ag` | 通过：14,000 请求零漏发零错误，7,000 写入核对一致，合计 P99 6.285 ms，发送迟到 P99 0.743 ms |
| 读池真实用例（含新增容量/在用断言） | `read_pool_usage_20260922` | 通过 |
| `read_write_contention` 五项（含新增响应丢失用例） | `tcp_fault_20260922b` | 5 项通过，5.88 s |
| 负载中单项注入：卡住一条写 5 秒 | `fault_load_fault_blocked_20260922a` | 按预定规则通过 |
| 负载中单项注入：杀全部 DBProxy 连接，第一轮 | `fault_load_fault_kill_20260922a` | 第 1 版规则 FAIL（分类缺陷），第 2 版规则 PASS；两份判定都保留 |
| 负载中单项注入：杀全部 DBProxy 连接，第二轮 | `fault_load_fault_kill_20260922b` | 第 2 版规则通过 |

### 诊断持续排出（短测 ag）

`client-timings.jsonl` 由独立写线程持续写出 21,001 条，队列容量 8,192，观察到的最大队列深度 1，单条写入最大 106 µs，70 秒累计 6.5 ms，刷盘 20 次，结束收尾 213 µs，零丢失。观察开销可以忽略，运行时长不再受一次性缓冲限制。PG 日志在 20 ms 阈值下没有慢 SQL。

### 提交成功但响应丢失（F02）

用例 `committed_write_with_lost_response_replays_as_duplicate_over_tcp` 四轮：

| 轮 | 吞掉的响应帧 | 客户端看到 | 显式重试 | 回执数 / 版本 |
|---|---:|---|---|---|
| 0、2 | 1 | SDK 用同一请求号自动重连重发，得到 Duplicate 版本 1 | Duplicate 版本 1 | 1 / 1 |
| 1、3 | 2 | 结果未知的连接类错误（不是“被拒绝”） | Duplicate 版本 1 | 1 / 1 |

每轮都确认 PG 已提交（回执恰好 1 条、快照版本 1），同请求号改内容重放报冲突。SDK 对连接断开、连接不可用、请求超时、IO 错误会用相同请求体自动重连重发一次，所以单次响应丢失通常对调用方表现为 Duplicate 而不是错误；连续两次丢失才把“结果未知”交给调用方。转发只存在于测试进程，服务端和协议没有改动。

### 负载中卡住一条写（blocked_write）

注入器在第 30 秒建触发器只卡目标请求 `…-6208`（当时进度 6008 再往后 1 秒），1.03 秒后在 `pg_stat_activity` 看到目标在等锁，5 秒后释放，4 ms 内等待者清空。

| 阶段 | 响应 | 成功 | 错误 | 漏发 | 最慢端到端 |
|---|---:|---:|---:|---:|---:|
| 正常（注入前完成） | 6,008 | 6,008 | 0 | 0 | 11.5 ms |
| 故障（与窗口重叠） | 1,650 | 1,619 | 31（全部允许类型） | 354 | 4,026 ms（被卡的那条写） |
| 恢复（窗口后发送） | 5,988 | 5,988 | 0 | 0 | 29.5 ms |

31 个错误都是 `StorageUnavailable: storage operation failed; retry with the same idempotency key`：同一写分片连接上的其他写排在被卡事务后面，等连接超过 2 秒预算后返回可重试错误；核对确认这 31 条全部回滚，没有半写入。354 次漏发是 32 个在途名额被排队的写占满。释放后最后一次扰动距释放 3 ms。读取走读池不受影响，读池在用峰值 1。共核对 6,843 次写入，零差异。

### 负载中杀全部连接（kill_connections）

两轮各在第 30 秒终止该库 8 条 DBProxy 后端连接（4 写 + 2 读 + 2 维护），保留注入器与宿主采样连接。

| 轮 | 正常段 | 故障段 | 恢复段 | 核对 |
|---|---|---|---|---|
| a | 6,005 全部成功，最慢 72.9 ms | 1,001 响应，1 个错误（杀前 4.3 ms 发出的写，已回滚），最慢 14.0 ms | 6,994 全部成功，最慢 130.8 ms | 7,000 零差异，1 条回滚 |
| b | 6,003 全部成功，最慢 11.5 ms | 1,002 全部成功，最慢 11.3 ms | 6,995 全部成功，最慢 16.0 ms | 7,000 零差异 |

服务端存储连接在下一次使用时自动重连，客户端无需重建连接池；两轮都没有漏发。第一轮按第 1 版规则判 FAIL，原因是那个在途受害请求按“发送时刻”被分到正常段；规则改为按在途区间与故障窗口重叠归段后判 PASS。blocked 轮在两版规则下判定相同。

## 留给尖峰定位的线索（本轮不处理）

kill 第一轮在恢复段第 45 秒左右出现一簇慢写：编号 8992–9008 端到端 99–131 ms，其中压测端发送迟到 39–77 ms（std 定时下正常是 1 ms 以内），同一时段 PG 日志有 6 条 21–43 ms 的慢 COMMIT；服务端累计写处理最大 110.9 ms，PG 写操作有 3 个样本落在 50–100 ms 桶，读处理最大 22.6 ms，读的同记录顺序等待最大 22.8 ms，读池未满。压测端与 PG 同时变慢，提示机器或虚拟机层面的停顿，但这只是线索，不是结论。原始数据在 `target/fault_kill_20260922a/` 的 `requests.jsonl`、`storage-stages.jsonl`、`postgres.log`。第二轮同配置没有复现。

## 证据

- `target/improve_20260922/`：各段格式/Clippy/workspace/SDK/构建日志，`probe-af.log`、`probe-ag.log`、`read-pool-real.log`、`contention-real.log`、`fault-blocked.log`、`fault-kill.log`、`fault-kill-b.log`；`hostcheck/` 是定位脚本 ExitCode 问题时的手工宿主运行。
- `target/probe_20260922ag/`、`target/fault_blocked_20260922a/`、`target/fault_kill_20260922a/`（含 `phase-analysis-rule1.json`）、`target/fault_kill_20260922b/`：账本、核对、诊断事件、服务端分段、注入事件、阶段判定、PG 日志、进程资源、二进制哈希。
- 本轮新建的库：`probe_20260922af_base`、`probe_20260922ag_base`、`probe_20260922ag_1_on`、`hostcheck_20260922a`、`read_pool_usage_20260922`、`tcp_fault_20260922b`、`fault_load_fault_blocked_20260922a`、`fault_load_fault_kill_20260922a`、`fault_load_fault_kill_20260922b`；Redis 逻辑库 8–12。均未删除。

## 边界

- 一次只注入一项，故障来自事务锁或终止连接，不模拟磁盘或网络变慢；宿主进程全程存活，不是独立 DP 进程崩溃。
- 1 分钟负载，不是两小时；没有回执夹具参与故障轮。
- 响应丢失用例在测试进程内完成，不是独立部署的 DP 与真实网络设备。
- 旧尖峰未定位；两小时验收未开始。
