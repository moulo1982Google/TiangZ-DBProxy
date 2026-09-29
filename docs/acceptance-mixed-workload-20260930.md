# 六类混合业务工具：功能核验

`mixs_0930a` 新库、正式 release 服务进程和真实 TCP SDK 短测通过，容器 exit=0/OOM=false；指定测试实际 `1 passed`，耗时 1.99 秒。仅为功能工具阶段，不是吞吐、容量或 120/300 秒三轮性能验收。

## 负载契约与结果

每 20 个请求依次安排 8 次单读、4 次批量读、4 次单写、2 次批量写、1 次单事务、1 次带效果的双记录原子事务。本次三个循环共 60 次，比例为 40/20/20/10/5/5。每个批量请求 30 条，快照 1 KiB，字节按 `(n+i+byte)%251` 生成；RunId、槽位、子项唯一确定请求 ID 和 key。读请求访问预先写入的 30 条不可变快照；写请求创建不同 key，期望版本均为零。

多记录事务调用生产 `commit_records`，两条快照之外固定产生一条追加记录和一条 Outbox 事件。单事务调用 `apply_transaction`，无 Outbox 效果。这是固定的工具负载分布，不代表任意业务效果组合。

- 六类实际请求计数 `[24,12,12,6,3,3]`，120 行意图/响应账本已拉回，逐类计数对应；意图落盘后才发请求。
- 231 条快照（含 30 条种子）：直查 PG 对比 payload、revision=1、schema、schema_version 和业务时间，全库记录数精确一致。
- 三个单事务、三个双记录事务读取回执并原 ID 重放，返回 Duplicate 及原结果。
- 三条追加记录及三条 Outbox 的 operation_id/payload 逐条核对，Outbox event_id/topic/partition_key 一致；重放后两表仍各三行。

没有注入故障、停共享服务或更改生产代码；只新增测试模块。工作台 4 CPU/16 GiB、固定 cpuset `20-27,48-55`，服务器测试串行。本短测未单独采集连续 cgroup 曲线；容器配置与退出状态已保存，不据此报告资源峰值。

## 工具与后续缺口

新增 `crates/dbproxy-server/tests/support/mixed_workload.rs`，由 `fault_process.rs` 的正式进程启动辅助方法运行；测试名 `mixed_workload::six_operations_preserve_snapshots_receipts_and_effects`。它绕开仅普通操作的 DedicatedReadProbe，使用正式服务的完整接口。函数 kind/writes/single/multi/effects/execute 可供下一阶段计时驱动复用。

当前 execute 使用断言核验成功，尚不是能持续收集失败/未知结果的压力调度器。下一步需要独立保存每次明确成功/失败/未知和未发送记录，固定速率+有上限并发，记录调度排队，限量保存完成数据，结束后核对所有已发送写入及效果；不能将本次串行循环直接称固定速率容量测试。正式阶段仍按原计划预热 120 秒、采样 300 秒、至少三轮，逐级并发并在明显排队或资源饱和时停止扩大。B1/B2 清理对照、整体修复/Outbox 成本和 50%/75% 承载率尚未测。

## 固定速率调度补充

新增 `support/mixed_paced.rs`，复用同一请求生成方法，另用返回结果的 issue 方法执行六类请求。四条真实 SDK 连接，固定速率计划槽位和独立的在途上限；每次意图 sync_data 后再发起请求，持续记录响应、RPC 耗时、调度延迟和端到端时延。达到并发上限的槽位明确记录 not_sent，不拖慢发压来掩盖排队。一次调用外层 10 秒上限；超时、断连接及 StorageUnavailable/Internal 均保守记 unknown，明确拒绝记 failed，批量响应逐项保存，错误不会中断后续核对。内存中的完成索引由速率上限 400/s、时长上限 420 秒限制；这不是无界运行工具。

所有已发送写入（含错误/未知）均直查 PG，验证内容、回执、双记录原子性、效果与快照同时存在，核对总行数及种子不变；正常负载必须 errors=0、not_sent=0、核对差异=0。断言在账本和 result.json 保存后执行。未知结果仍使验收失败，落库核对不把未知响应改写成明确成功。本轮没有实际注入超时故障；未知分类只做了定向单元检查。

- `mixp_0930a`：2 秒预热、5 秒采样，20 请求/秒、并发上限 8。140 个请求全完成、499 快照、14 条效果，错误/未发送/核对差异均为零，容器 exit=0/OOM=false，Linux `1 passed`。分析器标记 SMOKE_ONLY。
- `mixq_0930a`：故意将并发限制为 1，只发 1 秒、400 个计划槽位。65 个完成、335 个未发送，265 快照、2 条效果核对正确。测试和分析器均明确失败（REJECTED_LOAD），原始证据保留；这是验证限流拒绝及账本完整性，不是服务器承载能力结论，也未放宽零未发送门槛。

工具 `tools/analyze_mixed_paced.mjs` 核对槽位唯一、意图/响应/未发送全覆盖、六类分配、正式样本标记、所有已发送写入核对记录及汇总数，分别输出各类 P50/P99、最大值与排队。单次完整计时最多标记 COMPLETE_SINGLE_TIMED_ROUND，不能代替三轮或容量结论。两轮全部证据已拉回 `target/server_20260929/fault_process_mixp_0930a/` 和 `fault_process_mixq_0930a/`，容器配置在同级。定向分类单元测试、Clippy、格式和差异检查通过。

下一步先执行 B2（正式完整服务、清理开启）固定 20/s、并发上限 8 的三轮完整计时，验证整套负载/证据工具持续工作。入口 `launch_mixed.sh` → `run_mixed_rounds.sh`，每轮全新 RunId/数据库。尚未具备容量控制器的饱和停止决策，不使用此驱动单独扩大到高负载；B1 对照、容量阶梯和 50%/75% 承载率仍待后续阶段。

Windows 定向 Clippy `cargo clippy -p tiangz-dbproxy-server --test fault_process --locked -- -D warnings`、cargo fmt 和差异检查通过；Linux 重新编译并实际执行。无协议或生成文件变化，未运行无关全量测试。原始证据在 `target/server_20260929/fault_process_mixs_0930a/`，容器/日志为同级 `mixs_0930a.*`，服务器原件保留。
