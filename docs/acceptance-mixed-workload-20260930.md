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

Windows 定向 Clippy `cargo clippy -p tiangz-dbproxy-server --test fault_process --locked -- -D warnings`、cargo fmt 和差异检查通过；Linux 重新编译并实际执行。无协议或生成文件变化，未运行无关全量测试。原始证据在 `target/server_20260929/fault_process_mixs_0930a/`，容器/日志为同级 `mixs_0930a.*`，服务器原件保留。
