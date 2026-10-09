# F11：可靠 Redis 确认间隙、重启与并发清理

2026-09-29，220 隔离容器，`redis_20260929b` 通过，耗时 81.91 秒。生产代码未修改，新增故障夹具与运行脚本。

## 实际覆盖

使用生产 `StorageBackend`、入队组提交、AOF 确认、回执清理任务和 backlog 后台消费者。组件在测试进程内运行，不把本轮描述为正式 DP 进程强杀。

专用 Redis 8.8.1 开启 AOF、`appendfsync everysec`、`noeviction`；1 CPU、1 GiB 内存、256 MiB maxmemory。工作容器 4 CPU、16 GiB，固定 CPU 集合。正常停止和 SIGKILL 各用一个新 PG 库；两次 Redis 恢复均使用原命名数据卷，未影响共享 Redis。

每轮先收到 20 条真实入队成功确认，暂不启动消费。随后开启转发器的返回屏障，发送第 21 条请求：直连 Redis 确认队列已有 21 条且调用尚未完成，证明请求已到 Redis、回复被截住。此时停止/强杀 Redis，再断开被截住的连接，调用必须返回错误。

恢复时允许队列中有 20 或 21 条：第 21 条没有获得 AOF 确认，结果本来就是未知，不能规定它必须丢失。保留原请求号重试，再启动实际 backlog 消费任务；直接查 PG 并逐条重放确认，21 条均只有 revision=1，内容一致，pending/processing 均为 0。

| 观测 | 正常停止 | SIGKILL |
| --- | ---: | ---: |
| Redis 退出码 / OOM | 0 / false | 137 / false |
| 已确认 / 未确认 | 20 / 1 | 20 / 1 |
| 恢复时队列条数 | 21 | 21 |
| Redis 离线期间剩余过期回执 | 18,500 → 17,500 | 19,500 → 18,500 |
| 最终清理条数 | 20,000 | 20,000 |
| PG 内容、版本与重复请求核对 | 21 条通过 | 21 条通过 |

后端实例没有重建。故障期间运行 PG 清理任务，恢复并重放原请求号后才启动消费者，以隔离“已确认但尚未消费”的窗口。本轮是进程停止/强杀，不是主机掉电、磁盘丢失或多节点故障保证。

## 失败与修复

- 本地首编译误用 Redis `ConnectionInfo` 私有字段，改用公开 `addr()` 方法后 Clippy 通过。
- `redis_20260929a` 已验证正常停止、未确认返回错误和清理继续，但恢复后直接调用一次 `process_backlog_once`，遇到旧连接 `broken pipe` 即失败。夹具没有执行生产消费者已有的退避重连路径，不能据此判定数据丢失。改用真正的 `run_backlog_worker`，35 秒内必须队列清空且 PG 全部一致；未屏蔽失败、未删除原轮次，也未修改产品错误处理。

## 复测与证据

本地：`cargo fmt --all`、`cargo clippy -p tiangz-dbproxy-server --test fault_process --locked -- -D warnings`。服务器：脚本 `bash -n`，Linux 重编译并执行唯一指定测试，检查 `1 passed`。

```bash
bash deploy/remote-test/run_f11_restart.sh <全新run-id>
```

脚本拒绝已有 RunId、容器或数据卷；结束/失败时停止本轮专用容器，保留卷。执行器增加 `FAULT_REDIS_A_URL` 可选覆盖，默认地址不变。

本地证据：`target/server_20260929/fault_process_redis_20260929a/`、`fault_process_redis_20260929b/`、对应 `f11-host-*` 与 `*.host.log`。服务器原件在 `/data/dbproxy-test/evidence/` 及 `/data/dbproxy-test/f11-host-*`；包含两阶段 JSON、实际退出状态、AOF 状态、原卷挂载和全部日志。
