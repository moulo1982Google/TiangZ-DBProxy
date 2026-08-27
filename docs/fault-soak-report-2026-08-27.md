# DBProxy 100 玩家两小时故障演练报告（2026-08-27）

## 结论

本次演练通过。100 个玩家经真实 Rust 客户端、TCP 协议和 DBProxy 连续运行 7,200 秒，期间按计划停止 Redis、停止 PostgreSQL、同时停止两者，并在 PostgreSQL 不可用且 Redis 已有积压时对 Redis 执行 `SIGKILL`。

最终结果满足本次数据安全与自动恢复验收条件：

- 100 个玩家的最终 revision、直接快照、AOF 入队快照和最后一笔交易逐一核对通过；
- 缺失快照、低于已确认 revision 的读取、revision/交易不变量错误均为 0；
- Redis 强杀前 backlog 为 100，重启后仍为 100，证明本机 AOF 保存了尚未进入 PostgreSQL 的快照；
- backlog、持久化缓存修复队列和 Outbox 最终全部排空，processing 和死信均为 0；
- 交易账本每笔严格一借一贷，总和为 0；Outbox 事件全部发布；
- PostgreSQL 和 Redis 最终恢复为 healthy，Redis AOF 最后写入与重写状态均为 `ok`。

计划停机期间出现的 RPC 错误是预期结果，不代表演练失败。验收原则是：后端无法满足承诺时明确失败，不伪造成功；恢复后使用相同幂等 ID 收敛到唯一结果。

## 环境与负载

| 项目 | 配置 |
| --- | --- |
| DBProxy | 0.6.0，4 个 PostgreSQL 请求 shard，backlog/cache-repair/outbox 各 1 个 worker |
| PostgreSQL | 18.4，重新创建的本地数据卷，1 GiB 容器上限 |
| 快照布局 | `(namespace, record_key)` 原生 HASH 分区，固定 32 个分区 |
| Redis | 8.8.1，AOF 开启，默认 `everysec`，384 MiB 容器上限、256 MiB `maxmemory`、`noeviction` |
| 客户端 | 32 条 TCP 连接，100 个玩家任务，1 秒一个玩家周期 |
| 读 | 每玩家每秒读取直接快照，每 5 个周期再读取入队快照 |
| 直接事务 | 每玩家每 10 个周期一次，携带 expected revision 和稳定 operation ID |
| AOF 快照 | 每 5 秒 100 条，按协议上限拆为 64 + 36 两批 `EnqueueMultiSnapshot` |
| 交易 | 每玩家每 600 个周期一次；同一 PostgreSQL 事务写交易状态、两条快照、两条零和账本和一个 Outbox 事件 |

运行时间为北京时间 2026-08-27 15:07:24 至 17:07:33。原始产物位于：

```text
D:\UGit\TiangZ\.build-tmp\dbproxy-fault-drill\20260827-150723-7200s
```

其中 `workload.stdout.log` 保存每分钟业务计数和最终对账，`events.jsonl` 保存故障事件，`samples.jsonl` 保存每分钟容器状态、资源和 DBProxy 指标。`workload.stderr.log` 为空。

## 故障时间线

| 分钟 | 故障动作 | 观察结果 |
| --- | --- | --- |
| 0–10 | 全部健康 | 基线约 7,100 次读、600 笔事务、1,200 条入队快照/分钟，零业务错误 |
| 10–20 | 停 Redis | 读请求回源 PostgreSQL，事务仍可提交；缓存修复任务按 RecordKey 合并，入队请求不能取得 AOF ACK 时明确失败 |
| 20–30 | 恢复 Redis | 缓存修复和 Outbox 自动排空，服务恢复健康吞吐 |
| 30–40 | 停 PostgreSQL | 热缓存短期可读；直接事务不伪成功；无条件快照可进入 Redis AOF backlog；缓存绝对 TTL 到期后读取明确失败 |
| 40–55 | 恢复 PostgreSQL | 原 operation ID 重试返回 `Duplicate`，没有重复增加 revision；backlog 排空 |
| 55–60 | 再停 PostgreSQL | backlog 合并为 100 个玩家的最新快照 |
| 60 | `SIGKILL` Redis | 非优雅退出，Redis 与 PostgreSQL 同时不可用 |
| 62 | 只恢复 Redis | backlog 从 AOF 恢复：强杀前 100、重启后 100 |
| 70 | 恢复 PostgreSQL | 98 个结果未知的事务被识别为 `Duplicate`，backlog 随后排空 |
| 90–95 | 同时停 Redis/PostgreSQL | 进入稳定故障态后成功数为 0，所有请求明确失败，正确性指标不变 |
| 95–105 | 只恢复 Redis | AOF 继续接收部分入队快照并按玩家合并为最多 100 条；PostgreSQL 相关操作继续明确失败 |
| 105–120 | 全部恢复 | 又有 98 个事务返回 `Duplicate`；负载恢复，backlog 最终排空并完成逐玩家对账 |

## 最终业务计数

| 指标 | 结果 | 解释 |
| --- | ---: | --- |
| 读取成功 | 529,718 | 包含直接快照和周期性入队快照读取 |
| 读取明确失败 | 55,689 | 集中在计划故障窗口 |
| 缺失快照 | 0 | 从未把已存在记录返回为不存在 |
| 低于已确认 revision 的读取 | 0 | 核心旧读断言 |
| 高于客户端本地 revision 的读取 | 891 | 请求结果未知但 PostgreSQL 已提交；随后用原 operation ID 重试并收敛，不是旧读 |
| 直接事务首次提交 | 42,022 | `Applied` |
| 直接事务幂等重复 | 293 | `Duplicate`，没有再次写入 |
| 直接事务明确失败 | 6,442 | 集中在 PostgreSQL 停机和切换窗口 |
| AOF 入队成功记录 | 132,668 | 逻辑记录数，不是批次数 |
| AOF 入队明确失败记录 | 11,232 | Redis 不可用或请求超时时未返回可靠 ACK |
| 交易首次提交 | 678 | `Applied` |
| 交易幂等重复 | 9 | `Duplicate` |
| 交易明确失败 | 94 | 故障期未部分提交 |
| revision/交易不变量错误 | 0 | 驱动内部一致性断言 |

最终数据库交叉检查进一步确认：

- `soak:transaction:1787814443911:*` 共 42,315 行，等于 42,022 次首次提交加 293 次幂等重复；
- `soak-trade:1787814443911:*` 共 687 笔唯一交易，全部为 version 1、Escrowed；
- 账本共 1,374 条，恰好每交易 2 条，`sum(amount) = 0`；
- Outbox 共 687 条，687 条已发布、0 条未发布、0 条死信；
- 正式运行共有 200 条玩家快照和 1,374 条交易快照；32 个分区全部使用，单分区 33–68 行，平均 49.19 行；
- 迁移登记 001–007 全部存在；数据库最终大小约 56 MiB。

## 队列与资源

每分钟采样观察到的峰值：

| 指标 | 峰值 | 最终值 |
| --- | ---: | ---: |
| backlog pending | 100 | 0 |
| backlog processing | 1 | 0 |
| backlog oldest age | 106.509 秒 | 0 |
| cache repair pending | 294 | 0 |
| cache repair dead letter | 0 | 0 |
| outbox pending | 96 | 0 |
| outbox dead letter | 0 | 0 |

| 容器 | 采样 CPU 峰值 | 采样内存峰值 |
| --- | ---: | ---: |
| PostgreSQL | 39.76% | 133.3 MiB |
| Redis | 10.53% | 18.97 MiB |

这是带长时间故障的正确性/恢复演练，不是纯性能基准；CPU 也是 Docker 一分钟快照而不是高频 profiler 数据，不能据此推导正式服务器容量。

## 演练发现的改进项

1. **readiness 语义不反映依赖状态。** 所有采样中 `dbproxy_ready` 都是 1，包括 Redis 和 PostgreSQL 同时停止时。当前实现只表示 DBProxy 已完成启动且未进入关闭。部署前需要明确是保留“进程可接流量”的语义，还是增加 dependency/degraded 指标与按场景判定的流量摘除策略；不应让编排系统把完全不可服务误判为健康。
2. **客户端连接存在队头阻塞。** 单条 TCP 连接按请求串行，RecordKey 稳定映射到固定连接。后端故障导致慢请求占住连接后，会拖慢同连接上的其他玩家请求。短期可按读写用途分池并调整池大小；长期若确有容量证据，再评估协议多路复用，避免先引入复杂度。
3. **Redis-only 时 backlog 共享连接会放大等待。** enqueue、worker claim/release、stats 共用一个 `Mutex<ConnectionManager>`，可靠 ACK 又需要 `WAITAOF`。PostgreSQL 停机时 worker 的失败循环与入队互相排队，本次观察到入队部分成功、部分超时。现阶段应继续优先使用批量 API；下一步可把写入与 worker/监控连接拆开，并用真实延迟指标决定是否引入专门批处理 actor。
4. **本地配置的降级吞吐有限。** Redis 停机后，100 玩家读全部回源到 4 条串行 PostgreSQL shard，并受 16 并发 fallback、2 秒超时和熔断参数约束，吞吐显著低于热缓存基线。数据安全正确，但正式环境需要按目标降级 QPS 压测并调参。
5. **测试驱动曾补跑错过的入队周期。** 故障恢复后出现最高约 3,000 条/分钟的追赶尖峰。本次结果保留该尖峰作为额外压力证据；驱动已改为跳过错过周期，从当前时间重新计时，后续演练保持恒定负载。

## 演练后的实施与回归

上述发现随后完成了不改变持久化语义的收敛改进：

- Redis backlog 的 enqueue、worker lease/ACK、stats 改用三条独立 `ConnectionManager`，避免 worker 重连或 `WAITAOF` 阻塞入队与监控；
- 真实存储模式的 `/ready` 现在要求 PostgreSQL 和 Redis 都通过最近一次 5 秒采样；新增 `/dependencies`、`dbproxy_dependency_up`、Prometheus 告警和 Grafana 面板；
- `DbProxyClientPool::connect` 保持原行为，新增显式 `connect_split(read_size, write_size)`；故障脚本默认把总计 32 条连接拆成 24 读 + 8 写；
- 驱动跳过故障期间错过的入队周期，不再在恢复后突发补跑。

新版以 100 玩家又运行了一次 120 秒同比压缩回归，输出位于：

```text
D:\UGit\TiangZ\.build-tmp\dbproxy-fault-drill\20260827-173056-120s
```

回归再次覆盖 Redis/PostgreSQL 单停、双停和 Redis `SIGKILL`：AOF 强杀前后 backlog 为 93/93，最终逐玩家验证通过，缺失快照、旧读和不变量错误均为 0，三个队列全部归零且无死信。另做定点停机确认：Redis 停机时 `/dependencies` 返回 `postgresql=up, redis=down`，PostgreSQL 停机时返回相反状态，两种情况下 `/ready` 均为 503，恢复后自动回到 200。检测延迟约一个 5 秒采样周期。

压缩回归的交易密度远高于两小时正式参数，单个 Outbox worker 在负载停止后仍继续排空约两分钟；这是压缩预演的预期容量放大，不据此修改至少一次投递语义。正式环境应按真实事件率调整 worker 数量。

历史归档、其他表分区和物理分库仍按当前范围延期。相关日常操作见[持久化与故障恢复手册](durability-recovery-runbook.md)，分区边界见[PostgreSQL 快照分区](postgresql-partitioning.md)，代码层结论见[代码审视记录](dbproxy-code-review.md)。

## 外网 4C8G 部署前短跑

同日把 release 构建部署到 4 核、8 GiB 的外网开发机，并使用 PostgreSQL 18.4、Redis 8.8.1、32 个连接和 100 个玩家做了 120 秒无故障短跑。结果为：读取成功 16,110、AOF 入队 2,300、事务首次提交 1,326、交易首次提交 426；所有读取、入队、事务、交易和不变量错误均为 0，最终逐玩家验证 `passed=true`。

压缩交易间隔设为 30 秒，因此负载结束时 Outbox 尚有 116 个 pending、2 个 processing；两个 peer 的 worker 在约两分钟内自动排空，最终 backlog、cache repair 和 outbox 的 pending/processing/dead-letter 全为 0。这再次确认短跑结束不能只看驱动退出码，正式验收必须继续等待持久队列收敛。

前两轮外网短跑没有注入故障，只是七日演练的构建与基础持久化门禁。后续仍须依次完成 100 玩家完整故障序列和 500 玩家 2–4 小时全链路预演。

发现七日 unit 尚使用旧的 32 连接单池参数后，又将正式配置收敛为 24 读 + 8 写的拆分池，并按这个最终参数运行 100 玩家 60 秒：读取成功 8,934、AOF 入队 1,100、事务首次提交 737、交易首次提交 481；错误与 revision/交易不变量仍全部为 0，最终验证再次通过。正式 unit 保持 disabled/inactive，等待后续带故障分阶段预演。

随后又修正了两处 peer 故障测试缺口：驱动新增 `--failover-endpoint 127.0.0.1:7801`，七日 unit 从 `Requires=tiangz-dbproxy@1` 改为只 `Wants` 两个 peer，防止停止 peer 1 时 systemd 连负载一起停止。定点验证以 100 玩家运行 90 秒，并让 peer 1 实际停止约 35 秒；负载全程保持 active，经 peer 2 继续完成请求。最终读取 13,238、AOF 入队 1,700、事务首次提交 1,102、交易首次提交 348，所有错误与不变量异常为 0，逐玩家验证通过。peer 1 随后恢复 healthy。
