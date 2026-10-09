# F04：清理提交后、指标前强杀

RunId `commit_20260929a`，三个新库 `_f04_exact_0/1/2`，实际 Linux 测试全部通过，总耗时 23.20 秒。使用正式 release 二进制，无生产故障开关、无生产代码修改。

## 精确故障点

测试内部启动明文 PostgreSQL 转发器，客户端方向原样转发，数据库方向按协议帧解析。在同一连接观察到 `DELETE 500`，再收到 `COMMIT` 的 CommandComplete 时扣住提交回包。不会根据任意载荷内的字符串匹配，也不会把其他连接的提交当成清理提交。

此时 PG 已完成提交，但 DBProxy 仍等待提交结果，清理 worker 尚未记录成功批次或删除指标。通过另一条直连 PG 的会话确认 500 条确已不可见，并抓取指标确认批次数、删除数均为 0、服务日志没有清理完成消息，再对本测试持有的进程执行 SIGKILL，断言退出信号为 9。

这里覆盖数据库提交后、服务记指标前的确定时刻，不声称覆盖指标更新指令之间的每个机器指令窗口。转发器只存在于验收测试，关闭 TLS 仅限测试 PG 连接。监听任务与连接任务随测试释放，故障不影响共享 PG 容器。

## 三轮结果

每轮种入 499 条过期夹具、1 条过期的真实业务回执、100 条近期夹具，以及另一条保留期内的真实业务回执。

| 轮次 | 已提交删除 | 强杀前删除指标 | 重启后删除指标 | 近期夹具保留 | 两个业务版本 |
| --- | ---: | ---: | ---: | ---: | --- |
| 0 | 500 | 0 | 0 | 100 | 1、1 |
| 1 | 500 | 0 | 0 | 100 | 1、1 |
| 2 | 500 | 0 | 0 | 100 | 1、1 |

每轮重启均确认已删除回执没有恢复，空清理不重复计算原批次。保留回执的原请求返回 Duplicate；过期回执的原请求因 expectedRevision=0 而返回 RevisionConflict、actualRevision=1。两条快照逐条核对载荷与版本不变。允许进程内指标少记已提交批次，但不能用指标补偿触发重复业务写入。

## 复测与边界

入口：`FAULT_TESTS=f04_kill_after_commit_before_metrics_keeps_deletion bash deploy/remote-test/run_fault_process.sh <新RunId>`。环境：220 隔离容器，4 CPU、16 GiB、固定 cpuset，镜像 `dbproxy-workbench:f06-20260929`。测试源码重新编译，正式服务二进制哈希保存在证据中。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_commit_20260929a/`，本地 `target/server_20260929/fault_process_commit_20260929a/`，含 `f04-exact/result.json`、每轮强杀前/重启后指标、服务日志、PG 日志和构建日志。原先随机强杀八轮的记录保留，不能用随机强杀替代这次提交回包屏障。

校验：Linux 实际编译执行三轮通过，Windows 定向 Clippy、格式及差异检查通过。该用例仅 Unix，Windows Clippy 不检查其 Unix 分支；另尝试 Linux Clippy，但现有镜像未安装该组件，未执行成功。未改协议、未运行代码生成，本次未重复整套工作区及 TypeScript SDK 测试。
