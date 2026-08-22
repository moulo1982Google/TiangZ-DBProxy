# TiangZ DBProxy 性能基线

本文记录 DBProxy 自身的可复现性能基线。测试使用 `MemoryBackend` 屏蔽 PostgreSQL 和 Redis，只测量 Rust Client、TCP 协议、Protobuf 编解码、Tokio 调度、分片锁和 DBProxy 事务语义。

这些结果不是数据库容量、整服容量或生产 SLA。真实部署还要单独验证 PostgreSQL、Redis、网络时延、数据卷和故障恢复。

## 测试环境

基线日期：2026-08-22。

| 项目 | 配置 |
|---|---|
| CPU | Intel Core i7-13700F，16 核 24 逻辑处理器 |
| 内存 | 64 GiB |
| 操作系统 | Windows 11 x64 |
| DBProxy Runtime | 4 个 Tokio worker threads |
| MemoryBackend | 16 shards |
| Client Pool | 64 条 TCP 连接 |
| 单轮时长 | 5 秒 |
| 正式结果 | 3 轮中位数 |

服务端和负载生成器均使用 Rust `--release` 构建。每个 workload、并发档位和轮次都启动全新的 DBProxy 进程，避免易失状态与 CPU 采样互相污染。

## 业务模型

| Workload | 持久化形状 |
|---|---|
| `playerDataSingle` | 每个领域分别发送一个 `LoadSnapshot` |
| `playerDataBatch` | 使用一个 `LoadMultiSnapshot` 读取全部领域 |
| `playerSaveSingle` | 每个领域依次发送一个 `SaveSnapshot`，模拟旧周期 Flush |
| `playerSaveBatch` | 使用一个 `SaveMultiSnapshot` 保存全部领域并逐条推进 Revision |
| `pickup` | 原子提交 inventory、quest、wallet 三条记录及拾取结果 |
| `npcShop` | 原子提交 inventory、wallet 两条记录及商店结果 |

Payload 使用 Starter MMORPG 当前的近似尺寸。写负载持续推进真实 Revision，并检查 CAS、幂等和多记录原子结果；任何错误都会计入失败数。

## 关键结果

### 关键事务

100 并发、三轮中位数：

| Workload | 业务操作/秒 | P50 | P95 | P99 | DBProxy CPU | 失败 |
|---|---:|---:|---:|---:|---:|---:|
| 拾取，3 条记录 | 35,322 | 2.13 ms | 7.53 ms | 11.09 ms | 3.51 核 | 0 |
| NPC 商店，2 条记录 | 38,384 | 1.98 ms | 6.74 ms | 9.84 ms | 3.54 核 | 0 |

4 个 worker 在 100 并发时已经使用约 3.5 个 CPU 核，继续提高客户端并发不会明显增加吞吐，只会增加排队时延。因此 100 并发是当前 4-worker 配置的合理饱和点。

### 30 领域玩家恢复

100 并发、30 个持久化领域、三轮中位数：

| 读取方式 | 玩家恢复/秒 | 线 RPC/秒 | P50 | P95 | P99 | DBProxy CPU | 失败 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 30 次 `LoadSnapshot` | 963 | 28,884 | 81.65 ms | 209.57 ms | 243.55 ms | 2.45 核 | 0 |
| 1 次 `LoadMultiSnapshot` | 11,774 | 11,774 | 0.75 ms | 37.40 ms | 45.92 ms | 2.12 核 | 0 |

`LoadMultiSnapshot` 将玩家恢复吞吐提高约 **12.23 倍**，P95 降低约 **82%**。收益来自减少 TCP 往返、协议编解码和客户端任务调度；真实存储后端还会按 shard 并行读取，每个 shard 使用 Redis `MGET`，并把缓存未命中合并为一次 PostgreSQL 多键查询。

30 领域批量负载每次约传输 30 KiB Payload，达到约 353 MiB/s 的本机 Payload 吞吐。此时结果已经明显受本机内存复制和序列化带宽影响，不能仅用逻辑操作数比较不同 Payload 大小的 workload。

### 并发增长

关键事务在 100、300、500 并发下的三轮中位数：

| Workload | 并发 | 业务操作/秒 | P95 | P99 | 失败 |
|---|---:|---:|---:|---:|---:|
| 拾取 | 100 | 35,322 | 7.53 ms | 11.09 ms | 0 |
| 拾取 | 300 | 34,599 | 24.73 ms | 35.61 ms | 0 |
| 拾取 | 500 | 35,455 | 38.77 ms | 55.24 ms | 0 |
| NPC 商店 | 100 | 38,384 | 6.74 ms | 9.84 ms | 0 |
| NPC 商店 | 300 | 38,153 | 21.02 ms | 30.50 ms | 0 |
| NPC 商店 | 500 | 37,512 | 37.23 ms | 53.73 ms | 0 |

吞吐在 100 并发后基本稳定，P95/P99 随排队深度增长，符合固定 4-worker 服务达到饱和后的表现。全部 27 个正式轮次均为零失败。

## 复现

仓库提供固定 4-worker 配置和自动采集脚本：

```powershell
# 默认测试 playerDataBatch、pickup、npcShop。
powershell -ExecutionPolicy Bypass -File tools/run_memory_business_perf.ps1

# 对比30领域逐条读取和批量读取。
powershell -ExecutionPolicy Bypass -Command "& {
  .\tools\run_memory_business_perf.ps1 `
    -Players @(100) `
    -Workloads @('playerDataSingle', 'playerDataBatch') `
    -DomainCount 30 `
    -DurationSeconds 5 `
    -Rounds 3 `
    -ClientPoolSize 64
}"

# 对比30领域逐条保存和批量保存；5/10领域只需修改DomainCount。
powershell -ExecutionPolicy Bypass -Command "& {
  .\tools\run_memory_business_perf.ps1 `
    -Players @(100) `
    -Workloads @('playerSaveSingle', 'playerSaveBatch') `
    -DomainCount 30 `
    -DurationSeconds 5 `
    -Rounds 3 `
    -ClientPoolSize 32
}"
```

脚本自动执行 release 构建、逐场景启停 DBProxy，并采集业务吞吐、P50/P95/P99、进程 CPU 和峰值 RSS。临时 JSON 与日志写入 `perf/results/`，该目录不进入 Git；更新本文件中的公开基线时必须保留完整参数并使用三轮或更多轮次。

## 结果边界

- MemoryBackend 进程退出后数据全部丢失，不能用于生产。
- 本报告不包含 PostgreSQL、Redis、磁盘、跨机网络和 TLS 成本。
- 本报告不包含 TiangZ 的登录、地图、AOI、技能、任务或客户端下行。
- 不同 CPU、操作系统、Payload、连接池和 worker 数量的结果不能直接横向比较。
- 单轮短测只用于开发回归；公开性能结论使用固定环境下的多轮中位数。
