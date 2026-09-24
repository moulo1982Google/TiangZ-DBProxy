# 在共享 Linux 主机上只用容器跑验收探针

目的：在一台还跑着其他业务的机器上，把 PG/Redis/DBProxy 宿主/发压/注入器全部放进容器，主机上只依赖 Docker，不安装任何工具链，不留源码，数据和证据只写到一个绑定目录。它不是部署方案，只服务于验收与定位。

## 组成

- `Dockerfile.workbench`：`rust:1.91-bookworm` 加 psql、node、procps；复制整个工作区（含未提交改动），预编译 release examples/bins 和两组真实库测试二进制。
- `docker-compose.yml`：PG 18.6（4 CPU / 8 GiB / shared_buffers 2 GiB / max_connections 30，与笔记本性能覆盖一致）、可靠 Redis（AOF）、缓存 Redis（无持久化）。三者钉在 NUMA 节点 1，端口不发布，日志通过绑定目录共享给工作台。
- `common.sh`、`run_receipt_probe.sh`、`run_fault_load.sh`：`tools/run_receipt_probe.ps1`、`tools/run_fault_load.ps1` 的容器内 bash 版本，证据目录结构和字段与 Windows 版一致，便于并排对照。差别：PG 日志按共享日志文件的字节偏移截取窗口；`docker logs`/`cpu.stat` 换成 `workbench-cpu.txt`（工作台自己的 cgroup 统计）。

## 使用

主机上（用户需在 `docker` 组）：

```bash
mkdir -p /data/dbproxy-test/{src,evidence,pgdata,pglog,redis,cache}
# 把工作区打包上传到 /data/dbproxy-test/src 后：
cd /data/dbproxy-test/src && docker build -f deploy/remote-test/Dockerfile.workbench -t dbproxy-workbench:local .
docker compose -f deploy/remote-test/docker-compose.yml up -d
docker run --rm -it --name dbproxy-workbench --network dbproxy-test_default \
  --cpuset-cpus 20-27,48-55 --memory 16g \
  -v /data/dbproxy-test/evidence:/evidence -v /data/dbproxy-test/pglog:/pglog:ro \
  dbproxy-workbench:local bash
```

工作台里：

```bash
deploy/remote-test/run_receipt_probe.sh --run-id probe_srv_a --rate 200 --seconds 60 --warmup 10 --rounds 1 --cleanup on --sql-log-ms 20 --read pooled --client split4 --pacing std
deploy/remote-test/run_fault_load.sh --run-id fault_blocked_srv_a --fault blocked_write
REDIS_URL=redis://:tiangz_dev@redis:6379/9 CACHE_REDIS_URL=redis://:tiangz_dev@cache:6379/9 deploy/remote-test/run_fault_load.sh --run-id fault_kill_srv_a --fault kill_connections --fault-duration 0
```

每次运行用全新 RunId；库名不复用。`REDIS_URL`/`CACHE_REDIS_URL` 默认逻辑库 10，需要隔离时换编号。

## 定位与长时运行

- `run_spike_round.sh <run-id>`：一分钟短测外加主机采样（`sample_host.sh`，每 100 ms 磁盘计数与压力）、WAL 文件名采样和 PG 统计前后快照，结束后由 `tools/analyze_spikes.mjs` 把每条慢请求与同一时刻的信号对齐。
- `run_long_round.sh <run-id> <seconds> [probe 参数]`：长时运行，额外每 `PG_SAMPLE_SECONDS`（默认 30）执行 `pg_periodic.sql`，记录库和表、索引大小、活行和死行、自动清理次数、待清理回执、连接、WAL 与检查点。
- `launch_long.sh <run-id> <seconds> <redis-db> [probe 参数]`：在主机上运行，以脱离 SSH 的方式启动工作台容器，并启动主机侧 `sample_containers.sh`（每 10 秒各容器 cgroup 内存、CPU、IO 与两个 Redis 的 INFO）。结果用 `tools/analyze_long_run.mjs` 流式分析。
- `run_receipt_probe.sh` 新增 `--reconcile-budget`（核对时限）与 `--trickle N --trickle-span S`（在运行期间陆续到期的 N 条回执，让清理全程有活干）。核对改为 32 路并发。

## 故障项（2026-09-24）

- `run_fault_load.sh --fault` 在原有两种故障外，新增四种：
  - `pg_pause` / `pg_delay`（F06）：宿主经注入器里的 TCP 转发层连接 PG，故障期间停止转发或加延迟，连接保持打开。
  - `read_only`（F10）：本轮测试库改为只读，再切断连接，让 DBProxy 重连进只读状态。
  - `disk_full`（F10）：必须配合下面的 `run_f10_disk_full.sh` 使用。
- `run_fault_series.sh <日志> "<参数1>" "<参数2>" …`：在一个工作台容器里依次跑多轮故障，某轮失败时记录下来，继续跑下一轮。
- `run_f10_disk_full.sh <run-id> [1g] [参数…]`：在主机上运行。另起一个一次性 PG，数据目录放在指定大小的内存盘卷上；工作台挂同一个卷，用占位文件写满。占位目录不是 tmpfs 时拒绝运行。结束后删除这个 PG 容器和内存盘卷，日志保留在 `pglog-f10-<run-id>/`。
- `run_fault_process.sh <run-id>`：以 root 身份在工作台里用 cargo 跑 `tests/fault_process.rs`，包括 F09、F15、F04。
  - F09 和 F15 需要受 `CONNECTION LIMIT` 约束的非超级用户，脚本会在测试 PG 里建一次性角色 `dbproxy_fault_limited`。
  - 用 `FAULT_TESTS` 可以只跑其中几项。
  - 如果测试没有真正运行（被过滤掉），脚本判为失败。
  - 挂载新版测试文件时，要先 `touch` 一下，保证 cargo 会重新编译。

**PG 日志必须可读**：PostgreSQL 默认以 0600 创建日志文件，工作台以普通用户运行时读不到，早期几轮因此得到空日志。现在依赖容器以 `log_file_mode=0644` 启动，脚本在日志不可读时直接失败。已存在的容器需要 `ALTER SYSTEM SET log_file_mode='0644'`、重载，并对现有文件 `chmod 644`。`recover_pglog.mjs` 用于从完整日志补回空窗口。

## 边界

- 共享主机：CPU 靠 cpuset 隔离，内存带宽和磁盘仍共享，报告必须写明当时的其他负载。
- 凭据是本地开发凭据，PG/Redis 端口不发布到主机，只在 compose 网络内可达。
- 收尾：`docker compose -f deploy/remote-test/docker-compose.yml down` 只停容器；`/data/dbproxy-test` 下的数据与证据不自动删除。
