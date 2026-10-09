# A07/A16：真实清理批次与故障监控

入口：`fault_process::a07_a16_receipt_counts_and_pg_failure_metrics`，正式 release 服务，100,000 条近期回执、1,101 条过期回执和一条真实业务写入。使用新库，固定 4 CPU/16 GiB 工作台，不停止共享测试 PG 容器。

## 断言

1. 空库启动后成功批次数为 1、删除数为 0、失败数为 0。
2. 加入夹具后重新启动真实服务：删除日志必须恰为 `[500,500,101]`，批次数为 3、删除数为 1,101、失败数为 0；数据库中过期行清空、近期行仍为 100,000。
3. 只禁止本轮新库的新连接，终止该库服务连接，保留测试核对连接。业务写返回 StorageUnavailable；等待正式清理任务下一轮，失败数增加为 1、删除数不虚增、日志包含租户。
4. 恢复连接，在本轮库追加一条过期回执；下一次正常退避后清掉，删除数变为 1,102、失败数仍为 1。近期行未删，故障写未落库，原业务请求重放 Duplicate、快照内容不变。

A16 的锁超时场景复用同版本 [F07 的真实监控与日志](acceptance-f07-a06-20260929.md)。本项采集 empty/deleted/unavailable/recovered 四份真实 `/metrics` 响应；标签不应含业务 namespace/记录键，清理计数来自实际批次，不通过扫描回执表更新指标。测试端 COUNT 仅作账本核对，不属于生产监控。

## 首轮失败与正确修法

`metrics_20260929a` 已执行正常清理，但故障注入前失败：在目标库的会话里执行 `ALTER DATABASE ... ALLOW_CONNECTIONS false`，PG 返回 `22023: cannot disallow connections for current database`。故障未实际注入，不能按“有错误”算故障验收通过。

修正测试：从 `postgres` 管理库的独立连接切换目标库的 ALLOW_CONNECTIONS；终止连接仍限定 `datname=current_database()` 且排除核对连接自身。只操作新测试库，恢复也经管理连接；不改 PG 配置、不修改生产代码或失败阈值。首轮日志和库保留，用 `metrics_20260929b` 从新库完整重跑。

## 结果

`metrics_20260929b` 全部通过（124.27 秒）：六条服务连接被终止，清理失败计数增加一次；释放后重新删除一条，总删除数 1,102；故障写未落库，近期回执仍为 100,000，原业务读写与重放正确。正常删除确为 `[500,500,101]`，空库/正常/PG 不可用/恢复的监控计数均满足断言。

离线检查四份真实监控响应，不含本轮业务 namespace 或记录键。结合 F07 的表锁超时指标与租户日志，A07 和 A16 的本轮矩阵通过。未宣称混合业务压力、监控采集服务掉线或全部保护表内容验收通过。

证据在服务器 `/data/dbproxy-test/evidence/fault_process_metrics_20260929a/` 和 `...metrics_20260929b/`，已下载到本地 `target/server_20260929/` 的同名目录。复测命令：`FAULT_TESTS=a07_a16_receipt_counts_and_pg_failure_metrics bash deploy/remote-test/run_fault_process.sh <新RunId>`。本地编译、Clippy 通过；未改生产逻辑。
