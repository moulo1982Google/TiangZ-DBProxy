# Docker 与进程停止信号

## 2026-09-15 修复

旧服务入口只等待 `tokio::signal::ctrl_c()`。Linux 容器直接以服务二进制作为 PID 1，镜像没有覆盖 Docker 默认停止信号 SIGTERM，因此 `docker stop` 没有触发原有关闭通道；观察到等待 45 秒后退出码 137，且 OOMKilled=false。

入口现在在报告 Ready 前注册 SIGINT 和 SIGTERM，两者共用原有 watch 关闭通道。监听任务开始处理信号前先完成 Ready 写入，避免提前收到的停止请求被后续 Ready 覆盖。收到信号时记录 `DBProxy shutdown requested` 及信号名，并撤下 Ready；Windows 保留 Ctrl+C 行为。Unix 信号注册失败则启动失败，不带病报告 Ready。

## 关闭顺序与边界

1. 收到停止信号，撤下 Ready 并通知现有关闭通道。
2. TCP 服务停止接入，通知连接任务；现有请求沿用原有 `shutdownGraceMs` 有限等待。
3. 等待 backlog、cache-repair、outbox 和依赖指标 worker 退出；超出原有等待预算时终止任务，并通过持久租约恢复未完成工作。
4. 停止观测监听，正常结束时记录 `TiangZ DBProxy stopped`。

连接和后台 worker 现有等待窗口分别使用 `shutdownGraceMs`，Docker 停止等待应大于两段窗口之和并留有余量。例如每段 5 秒时可使用 `docker stop --timeout 20 <container>`。SIGKILL 无法捕获，不提供优雅关闭保证；提交成功但应答丢失仍须沿用原 request/operation ID 查询或重试，不能生成新幂等键。

停整个本地环境时先停业务进程，再停 DBProxy，最后停 PostgreSQL/Redis。不要通过先关数据库来触发 DBProxy 退出。修复不修改协议、存储格式、队列语义或依赖版本；已存在的容器必须重建镜像并重新创建才能使用新入口，仅 restart 旧容器无效。

## 验证

新增 `crates/dbproxy-server/tests/shutdown_signals.rs`：仅 Unix 执行，启动真实服务子进程，保留已认证客户端连接，分别发送 SIGTERM/SIGINT，断言退出码 0、正确的信号日志、正常结束日志，且没有耗尽关闭等待。测试使用 memory 后端，不宣称验证持久数据落盘。

2026-09-15 验证结果：

- Windows：`cargo fmt --all -- --check`、`cargo test --workspace`、`cargo clippy --workspace --all-targets` 均通过；`npm run test:typescript` 18 项通过。按原有标记忽略的存储集成/故障测试未运行。
- Linux：使用 `Dockerfile.dev` 构建最终源码，`cargo test --release -p tiangz-dbproxy-server --test shutdown_signals -- --nocapture` 两项通过，覆盖保留已认证连接时的 SIGTERM/SIGINT。
- 已重建 `tiangz-wasteland-dbproxy:dev` 并重新创建 ModuleGame 专用 DBProxy 容器；旧镜像保留为 `tiangz-wasteland-dbproxy:pre-sigterm-fix`。
- 真实 PostgreSQL/Redis 后端，两次启动均 `/ready` 返回 HTTP 200；两次 `docker stop --timeout 20 tiangz-wasteland-dbproxy` 耗时分别 3.317 秒、2.289 秒，退出码均为 0，OOMKilled=false。两次均记录 SIGTERM 与正常 stopped，没有关闭超时告警。耗时包含 Docker 命令和容器清理开销。
- 验证结束后 DBProxy、PostgreSQL、Redis 均恢复停止状态；保留原有数据卷，没有清理业务数据。本次未进行负载、在途写入故障或持久性故障注入测试，没有修改协议，也无需代码生成。
