# Rust 压测定时等待定位（2026-09-22）

本轮确认了压测端在当前 Windows 环境中的定时迟到，并提供可重复改善它的可选等待方式。没有修改 DP 服务、SDK、数据库配置或请求顺序契约。之前 T/U 的上百毫秒尖峰仍未复现，不能宣布整体性能问题已经解决，也没有开始两小时验收。前置证据见[分段定位报告](acceptance-read-stage-diagnosis-20260922.md)。

## 工具是谁写的、在哪里运行

发请求的是 Rust release 程序 `crates/dbproxy-server/examples/acceptance_load.rs`，调用 DP 的 Rust SDK，运行在 Windows 主机；不是 PowerShell 定时发请求。`tools/run_receipt_probe.ps1` 只准备新测试库、启动私有进程并收集结果。PG/Redis 在 Docker 中。

Rust 压测程序原先使用 `tokio::time::sleep_until` 等待每个绝对发送时刻。本轮增加四项逐请求计时：进入等待时是否已迟到、等待返回时的迟到、发送意图记录的写入耗时、请求任务启动等待。仍保留原来从计划时刻开始计时的总耗时。

## 定时器单独对照

新增 `acceptance_pacing.rs`：固定每秒 200 次、运行 20 秒。计时期间不访问网络或数据库，不写文件，也不派发请求任务；预分配保存 4000 条结果，结束后再写入独占创建的文件。以 32 个 Tokio 工作线程启动。

| 等待方式 | P99 迟到 | 最大迟到 |
|---|---:|---:|
| Tokio，真实读写前 | 22.345 ms | 28.430 ms |
| Tokio，真实读写后 | 22.649 ms | 26.782 ms |
| 标准线程 sleep | 0.996 ms | 1.379 ms |

只统计在截止时刻之前进入等待的样本，两个 Tokio 对照的最大迟到仍为 28.430 / 26.754 ms。因此现象不需要数据库负载、日志写入或此前一轮工作超时就可以出现。实验定位到当前机器上的等待/唤醒路径，尚未进一步拆分 Tokio 内部和 Windows 调度机制，不能推广为所有平台的 Tokio 性能结论。

## 真实读写对照

沿用 PG 18.6、4 CPU / 8 GiB、可靠及缓存 Redis 8.8.1，PG 4 写 + 2 读连接，SDK split4；每秒 200 请求，最多 32 在途，读写各半，1 KiB。清理开启，每轮 10 秒预热 + 60 秒测量。四轮均在编译结束后运行，无并行构建或其他压测。

| 运行 | 等待方式 | 发送迟到 P99 | 总耗时 P99（读写合计） | 最慢读取 | 漏发/错误/数据不符 |
|---|---|---:|---:|---:|---|
| AA | Tokio | 19.396 ms | 25.246 ms | 28.042 ms | 0 / 0 / 0 |
| AB | Tokio | 16.642 ms | 22.962 ms | 22.651 ms | 0 / 0 / 0 |
| AC | 标准线程 sleep | 0.930 ms | 6.468 ms | 3.430 ms | 0 / 0 / 0 |
| AD | 标准线程 sleep | 0.902 ms | 6.305 ms | 3.266 ms | 0 / 0 / 0 |

每轮计划请求（含预热）14,000，逐条核对 7000 次成功写入。四轮总计 56,000 请求和 28,000 次写入核对；独立脚本确认编号不重不漏、请求响应一一对应，写入均有读回核对，所有诊断事件保留完整。

AA/AB 的发送前日志写入最大分别为 0.146 / 2.192 ms，请求任务启动等待最大 0.218 / 0.280 ms；等待返回到 SDK 调用前的总时间最大 0.221 / 2.208 ms。相对二十多毫秒的迟到，这两处不是本轮主要来源。

按**同一个最慢读取**分解：AA 总计 28.042 ms，其中 SDK 调用前 26.345 ms、调用后 1.697 ms；AB 总计 22.651 ms，其中调用前 20.575 ms、调用后 2.076 ms。这两条请求进入等待前已经迟到，反映先前唤醒延迟的积累；不能误说它们各自在一次 sleep 中多睡了二十多毫秒。单独定时对照提供了“提前进入等待仍晚醒”的证据。

## 可选修正与复测

`run_receipt_probe.ps1` 新增 `-PacingTimer tokio|std`，默认仍为 tokio，便于复现旧基线；本机后续校准短测显式使用 `-PacingTimer std`。Rust 程序读取 `ACCEPT_PACING_TIMER`，manifest 与 summary 记录所选方式。未知值在连接数据库之前报错。

std 模式仅让压测程序的 main 线程睡眠到原定绝对发送时刻，SDK 网络及请求任务继续在多线程 runtime 工作线程运行。没有忙循环，没有改全局系统时钟设置，不在 DP 服务处理函数里阻塞线程。计划速率、并发上限、写入前记录意图、数据核对、失败判定和总耗时起点均不变。迟到请求也没有被重置起点或丢弃统计。

定时器单独复测（需选择一个不存在的输出文件）：

```powershell
cargo build -p tiangz-dbproxy-server --release --examples --locked
$env:TOKIO_WORKER_THREADS='32'
$env:ACCEPT_PACING_TIMER='std' # 改为 tokio 作对照
$env:ACCEPT_PACING_OUTPUT='target/new-pacing-control.jsonl'
./target/release/examples/acceptance_pacing.exe
```

真实测试沿用[固定速率短测脚本](acceptance-fixed-rate-probe.md)的参数，并显式指定 `-ReadConnection pooled -ClientConnections split4 -PacingTimer std` 和全新 RunId。不要拿改变发送起点、增加并发额度或去掉数据核对来代替校准。

本轮本地证据：`target/pacing_20260922/` 中的 timer-before/after/std.jsonl、analyze.py、analysis.json、运行与构建日志；`target/probe_20260922aa` 至 `ad` 中各轮 manifest、二进制校验值、请求与核对账本、PG 日志和服务端分段数据。测试证据不随 Git 文档提交。

## 当前边界

新增内容仅限 Rust 压测 examples、启动脚本和文档，协议未变，无须协议代码生成。Rust 格式检查与全 workspace/all-targets Clippy 通过；workspace 191 项、TypeScript SDK 21 项通过。SDK 测试执行了其既有生成与构建步骤，未手改生成文件。结果随本轮日志保存；默认忽略的外部存储测试不算重新执行。四轮真实 PG/Redis 短测另行记录如上，未执行旧库升级或两小时验收。

后续应在校准后的发送方式下继续捕获自然数据库尖峰。当前前后各两轮不是长稳证明，无法据此宣称已解决 T/U 漏发；也不应把压测端等待问题外推为 DP 线程池有缺陷。客户端诊断缓冲仍为短测方案，两小时测试前须另行解决持续排出和观察开销问题。
