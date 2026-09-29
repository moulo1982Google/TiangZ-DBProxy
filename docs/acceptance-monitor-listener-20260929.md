# F15 补测：真实监控监听器停止，业务与清理继续

RunId `listener_20260929a`，新库 `_f15listener`，Linux 组件集成测试通过，耗时 6.71 秒。复用实际 DbProxyServer、ObservabilityServer、StorageBackend 和清理 worker，连接真实 PG/Redis。测试宿主给监控与业务分别传入现有 watch 关闭通道，因此能只停监控，不修改生产代码。

顺序：监控正常时写 20 笔；关闭并等待真实监控监听器退出；其关闭期间每次连接必须返回 ConnectionRefused，共 40 次，同时写入并读取 40 笔业务，清理 1,501 条过期回执；最后在原端口重建监控监听器，再写 20 笔。

结果：80 笔业务逐条直查 PG 的键、载荷、revision=1 全部正确，原请求重放全部 Duplicate。监控关闭期间 1,501 条过期回执清完，恢复端口后对应删除计数仍为 1,501，业务服务未重启。停监控期间最大写耗时 7.899ms。任务结束通过独立关闭通道收尾，失败路径也会取消测试任务。

## 覆盖范围

本轮确实停止了生产监控组件的监听器，弥补仅中断采集转发链路的不足。它在 Rust 测试进程内运行生产组件，不是正式可执行文件内的随机 listener panic/accept 错误注入；也不改变 main 在启动时绑定监控端口失败即拒绝启动的策略。结合原双租户数据库故障和 [采集路径故障](acceptance-monitoring-path-20260929.md)，F15 的数据库隔离与监控短时不可用业务独立性已有对应证据。不扩大为无限资源压力或全部内部崩溃形态。

入口：`FAULT_TESTS=monitor_listener_failure::f15_listener_stop_does_not_stop_business_or_cleanup bash deploy/remote-test/run_fault_process.sh <新RunId>`。旧镜像运行新增测试时，除 `fault_process.rs` 外也须挂载 `tests/support/`；F05 宿主脚本已补对应挂载。工作台固定 cpuset、4 CPU、16 GiB，未并行故障测试。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_listener_20260929a/`，本地 `target/server_20260929/fault_process_listener_20260929a/`，含 `f15-listener/result.json`、恢复后的指标、测试日志及 PG 日志。此次实际被测组件链接在测试二进制内，不能把 runner 附带的正式服务哈希当作组件测试二进制哈希。测试源码由本提交保存，Linux 实际重新编译执行，Windows 定向 Clippy、格式检查通过。无协议变更或代码生成。
