# F03：清理已删未提交时强杀服务

2026-09-29，RunId `kill_20260929a`，三个新库分别为 `_f03_0/1/2`。真实 release 服务，220 隔离工作台限制 4 CPU、16 GiB，镜像 `dbproxy-workbench:f06-20260929`，测试源码重新编译。

测试库的 AFTER DELETE 语句触发器等待 advisory lock。独立自动提交连接从 pg_stat_activity 确认清理 SQL 已到该等待点，立即对本测试持有的 DBProxy 子进程 SIGKILL，并断言退出信号确实为 9。生产 100ms 锁超时与 2 秒语句超时不变；不是随机睡眠后猜测执行位置。

锁保持到清理 PG 连接消失，再检查回滚。三轮均通过：

| 轮次 | 信号到退出 | 强杀后过期回执 | 重启删除 | 近期保留 | 原业务重放 |
| --- | ---: | ---: | ---: | ---: | --- |
| 0 | 1.020ms | 501 | 501 | 100 | Duplicate，revision=1 |
| 1 | 0.900ms | 501 | 501 | 100 | Duplicate，revision=1 |
| 2 | 0.876ms | 501 | 501 | 100 | Duplicate，revision=1 |

业务快照载荷均保持 `[3]`。总耗时 7.09 秒，实际运行一项包含三轮的 Linux 测试，零失败。此项补齐 F03 的真实 DP 强杀变体；F04“提交后、记指标前”仍未覆盖，不据此标记通过。未改生产代码、协议或超时配置。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_kill_20260929a/`，本地 `target/server_20260929/fault_process_kill_20260929a/`，含三轮服务日志、PG 日志、二进制哈希、`f03/result.json`。入口：`FAULT_TESTS=f03_kill_after_delete_rolls_back_and_restarts bash deploy/remote-test/run_fault_process.sh <新RunId>`。禁止用随机强杀替代等待点断言，或放宽生产超时来扩大测试窗口。
