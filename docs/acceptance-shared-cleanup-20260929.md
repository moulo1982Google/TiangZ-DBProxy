# P09 部分：单进程与双进程清同库

RunId `shared_20260929a`，新库 `_p09_1/2`，两个配置串行运行，每组至少 45 秒，正式服务进程全部使用同一版本。Linux 测试通过，合计 92.07 秒。

每组 20,000 条过期回执、100 条近期回执。两个 SDK 客户端每 50ms 并发发送同一请求：单进程组均连同一个实例，双进程组分别连两个实例。每对回复必须恰好一个 Applied、一个 Duplicate，且都为 revision=1；随后两个客户端都读取核对内容。每秒记录剩余回执与连接数。

| 配置 | 请求对 | 清理首次观测完成 | 请求对 P99 | 请求对最大 | PG 业务连接 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 单进程 | 898 | 40.128 秒 | 10.066ms | 14.045ms | 6 |
| 双进程同库 | 897 | 20.179 秒 | 10.631ms | 20.772ms | 12 |

每组删除指标合计恰好 20,000，过期回执归零、近期 100 条保留，已确认的全部负载快照直接 PG 逐条核对键、载荷和 revision=1。没有重复应用、删除计数重叠或漏计，连接数每次采样均符合预算。

清理时间含启动和约一秒采样间隔；当前 worker 每秒至多推进一批，双实例共同清理更快不能解释成数据库极限吞吐翻倍。请求对耗时是两个并发写都完成的耗时，不等同于单请求延迟。每配置只有一轮、固定约 20 请求对/秒，不证明容量或稳定的尾延迟回归。P09 的双租户固定速率部分见 [A13 报告](acceptance-tenant-pressure-20260929.md)，容量阶梯和完整混合负载仍待补。

复测：`FAULT_TESTS=shared_cleanup_pressure::p09_shared_database_cleanup_competition bash deploy/remote-test/run_fault_process.sh <新RunId>`，旧镜像挂载当前 tests 与脚本目录。工作台总限额 4 CPU/16 GiB、固定 cpuset，两个正式进程共同受限；没有并行其他故障/性能测试。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_shared_20260929a/`，本地 `target/server_20260929/fault_process_shared_20260929a/`，包含 `p09-shared/result.json`、逐秒积压/连接采样、各实例指标、服务和 PG 日志。Windows 定向 Clippy、格式/差异检查与 Linux 实测通过；生产代码、SQL、协议未改。
