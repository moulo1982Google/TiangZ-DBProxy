# 持续负载中的单项故障注入（方法）

补交接里的两个缺口之一：不再只在空闲时注入故障，而是在每秒 200 请求的固定负载中注入一项故障，分“正常、故障、恢复”三个阶段分别判定。规则在跑之前写进 `fault-plan.json`，不能事后放宽正常短测的“零错误”门槛。另一个缺口“提交成功但响应丢失”见 `read_write_contention.rs` 里的 `committed_write_with_lost_response_replays_as_duplicate_over_tcp`。

## 组成

| 部件 | 位置 | 作用 |
|---|---|---|
| 发压 | `crates/dbproxy-server/examples/acceptance_load.rs` | 与校准短测同一程序。`ACCEPT_FAULT_MODE=1` 时，错误与漏发交给阶段分析判定；数据不一致或诊断事件丢失仍直接失败。核对现区分“确认成功必须已提交”和“出错/未知的写要么没有、要么恰好一份正确”。 |
| 注入 | `crates/dbproxy-server/examples/acceptance_fault_injector.rs` | 连接专用 `fault_load_*` 库，要求 `DBPROXY_FAULT_DATABASE` 与实际库名一致。到点注入一项故障，记录注入、观察到阻塞、释放的精确时刻到 `fault-events.jsonl`。 |
| 判定 | `tools/analyze_fault_load.mjs` | 按每条请求的实际发送时刻分到三个阶段，对照 `fault-plan.json` 的规则，输出 `phase-analysis.json`，违规返回非零。 |
| 编排 | `tools/run_fault_load.ps1` | 核对容器归属、新建库、启动宿主/发压/注入器、收集 PG 日志、运行判定。 |

## 两种故障

- `blocked_write`：注入器先持有会话级 advisory lock，再建触发器只对一个目标请求号生效。目标是发压程序当前进度再往后约 1 秒的下一个写请求，这样屏障一定先于该请求就位，且只影响这一条写。目标写入在 PG 事务里等待 `FaultDurationSeconds`（默认 5 秒）后放行。期间同一写分片连接上的其他写会排队，32 个在途名额可能耗尽出现漏发；读取走读池不应受影响。注入器必须在 `pg_stat_activity` 看到目标真的在等锁，否则判定失败。
- `kill_connections`：一次性终止该库全部 DBProxy 后端连接（保留注入器自身和宿主采样连接）。请求会得到连接类错误；随后依赖已有的自动重连。

## 阶段规则（写在 fault-plan.json）

每条请求的发送时刻是“计划发送时刻 + 发送前等待”，完成时刻是“计划发送时刻 + 端到端耗时”。分段规则（第 2 版）：

- 正常阶段（在注入之前就已完成的请求）：零错误、零数据错误、零漏发。与普通短测相同。
- 故障阶段（在途区间与“注入到释放后 `RecoveryGraceSeconds`（默认 5 秒）”窗口有重叠的请求）：零数据错误；允许漏发（在途名额耗尽）；错误只允许连接/超时/不可用/被终止这几类文字，其他错误算违规。漏发没有完成时刻，按计划发送时刻归段。
- 恢复阶段（在窗口之后才发送的请求）：零错误、零漏发。任何更晚的错误或漏发都视为恢复未完成。

第 1 版规则只按发送时刻分段，把“注入瞬间正在途中、因故障出错”的请求算进了正常段；2026-09-22 的第一轮 `kill_connections` 因此按第 1 版判 FAIL（唯一一个错误是杀连接前 4.3 ms 发出、杀后 3.4 ms 返回的写，已回滚）。该轮证据与第 1 版判定保留，规则修订后用新 RunId 重跑。
- 写入核对：每个写请求要么库里没有（已回滚），要么恰好版本 1 且内容一致；确认成功的写必须存在。`committed_without_ok_response` 统计“客户端未确认但已提交”的写，这是允许的结果未知情形，不算错误。

阶段分析同时输出各阶段的响应数、成功数、错误数、允许错误数、漏发数和最大端到端耗时，以及释放后最后一次错误/漏发距释放的时间，作为恢复时间。

## 运行

先重建 release 二进制，再用全新 RunId 和已授权、归属核对过的容器：

```powershell
cargo build -p tiangz-dbproxy-server --release --examples --bins --locked
powershell -ExecutionPolicy Bypass -File tools/run_fault_load.ps1 -RunId <新id> -PostgresContainer dp-acceptance-20260922-postgres -Project dp-acceptance-20260922 -PostgresBaseUrl <PG基址> -RedisUrl <可靠Redis/DB> -CacheRedisUrl <缓存Redis/DB> -FaultKind blocked_write -Seconds 60 -FaultStartSeconds 30 -FaultDurationSeconds 5
```

库名固定为 `fault_load_<RunId>`，不复用旧库。证据目录 `target/<RunId>/`：`fault-plan.json`、`requests.jsonl`、`reconciliation.jsonl`、`client-timings.jsonl`、`storage-stages.jsonl`、`fault-events.jsonl`、`phase-analysis.json`、`postgres.log`、进程资源与二进制哈希。

## 边界

- 这不是两小时验收，也不是多故障叠加；一次只注入一项。
- 慢写来自事务锁，不模拟磁盘或 WAL 刷盘变慢。
- 没有回执夹具，清理任务开启但无过期数据可清。
- 不是独立 DP 进程崩溃重启；宿主进程全程存活。
- 运行结果见[评审轮真实库验证结果（2026-09-22）](acceptance-fault-under-load-20260922.md)；本文件只描述方法。
