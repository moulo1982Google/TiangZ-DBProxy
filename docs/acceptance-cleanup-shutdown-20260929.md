# A14：清理任务三个阶段的正常停止

入口：`fault_process::a14_cleanup_stops_gracefully_at_each_phase_and_resumes`，仅 Unix。真实 release 服务，SIGTERM 仅发送给本测试启动并持有的子进程 PID；每阶段使用新库，不停止 PG/Redis 容器。

## 故障点与断言

- 空闲：先确认真实清理批次完成且进入空闲，再发送信号。
- 等表锁：测试连接持普通回执表 EXCLUSIVE 锁，独立观测连接在 pg_stat_activity 中确认清理 SQL 正等待锁，再发信号。
- 已删未提交：测试库中增加 AFTER DELETE 语句触发器，用 advisory lock 暂停清理事务；观察到清理 SQL 的真实锁等待后发信号。沿用生产 100ms 锁超时，没有延长它来制造更容易通过的窗口。

三个阶段均要求退出码 0、有 shutdown requested 和 stopped 日志、没有 shutdown grace expired。等锁及已删未提交阶段的 501 条过期回执必须全部保留，不能出现半批删除；释放锁、移除测试触发器后重新启动服务，501 条应继续清完，100 条近期回执保留、业务原请求仍 Duplicate、快照内容不变。

这是正常 SIGTERM 停机验收，不替代 F03/F04 的精确 SIGKILL 故障。

## 首轮失败

`stop_20260929a` 空闲阶段通过，但表锁阶段未观察到等待而失败。日志显示服务实际发生了 100ms 锁超时。测试在持锁长事务里重复读取 pg_stat_activity，重复看到该事务缓存的活动快照，不能用来追踪后来出现的等待。

正确修法是使用独立的自动提交观测连接，原持锁事务不变。PostgreSQL 官方说明活动查询在事务中会复用采样快照，可通过结束事务或清除快照更新；见 [PostgreSQL 18 统计访问说明](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-STATS-VIEWS)。不增加随机 sleep，不放宽等待点断言。首轮库与证据保留。

## 完整复测

新 RunId `stop_20260929b` 三个新库、三个阶段全部通过，总耗时 7.04 秒：

| 阶段 | SIGTERM 到退出 | 退出码 | 停止后过期回执 | 重启删除 | 近期保留 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 空闲 | 0.013 秒 | 0 | 0（停后另加 501） | 501 | 100 |
| 等表锁 | 0.102 秒 | 0 | 501 | 501 | 100 |
| 已删未提交 | 0.102 秒 | 0 | 501 | 501 | 100 |

环境：220 的隔离 PG/Redis，工作台 4 CPU、16 GiB、固定 cpuset，镜像 `dbproxy-workbench:f06-20260929`，测试源码重新编译。证据位于服务器 `/data/dbproxy-test/evidence/fault_process_stop_20260929a/`、`...stop_20260929b/` 和本地 `target/server_20260929/` 同名目录。

复测：`FAULT_TESTS=a14_cleanup_stops_gracefully_at_each_phase_and_resumes bash deploy/remote-test/run_fault_process.sh <新RunId>`。Windows 不执行本 Unix 用例；本次 Linux 实际执行了一项包含三个阶段的测试，不是“过滤后零项通过”。
