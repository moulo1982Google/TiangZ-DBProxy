# F01：真实进程写入提交前终止数据库连接

RunId `precommit_20260929a`，三个新库 `_f01_0/1/2`，正式 release 服务。Linux 实际运行一项包含三轮的测试全部通过，耗时 2.60 秒。

测试仅在新库建立回执 AFTER INSERT 触发器，对 `f01-pending` 命名空间等待 advisory lock。独立自动提交连接从 pg_stat_activity 观察到该回执 SQL 的实际锁等待，确认写事务尚未提交，再用 pg_terminate_backend 终止该连接。不是随机延迟后断连接，也没有终止整个共享数据库。

三轮结果相同：原请求明确返回 StorageUnavailable；直接查询 PG，目标快照与回执均为零行，保留记录不变。解除测试屏障并移除触发器后，用原请求号重试返回 Applied/revision=1，再次重试返回 Duplicate/revision=1。目标和保留快照均直接查询 PG 核对载荷、版本，目标回执恰好一条。服务未重启。

该项补齐独立真实进程的普通快照写入提交前连接中断；不是多记录事务完整内容矩阵，也不是高吞吐压力测试。不得把连接错误视为写成功、改请求号重试或用总行数替代业务内容检查。

复测：`FAULT_TESTS=f01_terminate_uncommitted_write_then_retry_same_id bash deploy/remote-test/run_fault_process.sh <新RunId>`。220 工作台固定 cpuset、4 CPU、16 GiB，镜像 `dbproxy-workbench:f06-20260929`，测试源码重新编译。生产代码和协议未改；Windows 定向 Clippy、格式检查通过，Linux 实测通过。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_precommit_20260929a/`，本地 `target/server_20260929/fault_process_precommit_20260929a/`，包含三轮服务日志、PG 日志、二进制哈希和 `f01/result.json`。
