# 可靠 Redis 的 AOF 确认与总预算（0.7）

本轮把写死的 2 秒改为可配置、有总预算约束的等待。默认 AOF 上限仍为 2,000 ms；它是兼容默认值，并非适合所有磁盘的性能承诺。`appendfsync everysec` 不保证每个请求在一秒内返回，磁盘、重写、调度和连接排队均可能增加尾延迟。

## 配置与启动检查

| JSON 字段 | 默认 ms | 含义 |
|---|---:|---|
| `backlog.enqueueQueueWaitTimeoutMs` | 2000 | 组提交入口到本批开始执行的排队上限 |
| `backlog.enqueueTimeoutMs` | 4500 | 组提交入口起，排队、连接/重连、写入、AOF 确认共享的本地期限 |
| `backlog.aofAckTimeoutMs` | 2000 | 入队的本地 AOF 等待上限 |
| `backlog.redisResponseTimeoutMs` | 3000 | 入队连接和单次 Redis I/O 上限 |
| `outboxRelay.publishTimeoutMs` | 5000 | Publisher 等锁、连接、发送、AOF 确认共享的期限 |
| `outboxRelay.aofAckTimeoutMs` | 2000 | Outbox 独立的本地 AOF 等待上限 |
| `outboxRelay.redisResponseTimeoutMs` | 3000 | Outbox 连接和单次 Redis I/O 上限 |

配置和所有重连使用同一策略。联网前检查：

- `1 <= AOF < Redis I/O <= 60000`，禁止 `WAITAOF` 的无限等待值零。
- 入队总预算不超过 60 秒，且大于 Redis I/O；排队为正且小于总预算。`enqueueAck=aof` 时还要求 `排队 + AOF < 总预算`。
- Outbox 继续要求 `3000 <= publishTimeoutMs <= 60000`，同时要求 `Redis I/O < publishTimeoutMs`，并至少比 `outbox.leaseMs` 短 1,000 ms。
- 所有阶段实际只消费剩余预算；上述关系为配置留出空间，不保证网络/磁盘在期限内成功。

旧配置省略新字段即可沿用默认 AOF/Redis I/O 参数，但入队现在增加 4,500 ms 的本地总期限。旧配置若显式使用 `publishTimeoutMs=3000`，必须把 Redis I/O 配为大于 AOF、小于发布期限的值，或增加发布期限；不能接受彼此矛盾的配置后再等线上失败。

入队队列容量仍为 4096、组提交目标仍为 512 条、确认等级仍由既有 `enqueueAck` 决定。缓存预算与 worker 的 lease/ACK、统计连接不使用这些可靠写入参数。

## 总预算与 SDK 的边界

入队接收时间使用本进程单调时钟。排队超时与 worker 开始之间用原子状态竞争：尚未开始的超时请求不会在以后偷偷写入；开始后的整批写入与确认使用本批最早的原期限，不能在开始写、重连或等待 AOF 时重新计时。

`WAITAOF 1 0 timeout` 的 `timeout` 为配置值与本地剩余整毫秒的较小者；不足 1 ms 立即失败，不能发送零。Redis I/O 可能先耗尽剩余总预算，返回的是结果可能未知。单调期限限制异步等待，不承诺在操作系统暂停或运行时不能调度时仍严格准点返回。

本地入队期限从组提交入口开始，不包含此前的帧接收、请求调度或编码。Rust SDK 的 `request_timeout` 和 TS 的 `WithRequestBudget` 仍负责各自一次逻辑调用的总预算，覆盖其排队、网络、重连与重试，默认 5 秒。服务端不能验证远端客户端的配置，也没有通过本次改动新增跨进程绝对时钟或协议字段。

生产配置应给 SDK 留出网络和服务端准入余量；高延迟部署仅增加 AOF 而保留 5 秒 SDK 上限，仍可能出现客户端先超时。后台 Outbox 不占发起业务事务的 SDK 预算，PG 提交后由独立投递预算和租约控制。

供隔离验证的三组配置（毫秒，非性能保证）：

| 组别 | 入队排队 | AOF | Redis I/O | 入队总预算 | Outbox 发布 | SDK 总预算起点 |
|---|---:|---:|---:|---:|---:|---:|
| 兼容默认 | 2000 | 2000 | 3000 | 4500 | 5000 | 5000 |
| 三秒对照 | 2000 | 3000 | 4000 | 6000 | 6000 | 7000 |
| 五秒对照 | 2000 | 5000 | 6000 | 8500 | 8500 | 10000 |

SDK 列只是实验起点；实际应根据网络、准入和业务响应 SLO 选择。三个 Outbox 配置均可沿用默认 30 秒租约。短测全通过不能证明五秒或三秒更优，扩大等待也不能提高已经饱和的磁盘吞吐。

## 确认、断线和重试

入队组提交独占一条 `MultiplexedConnection`。EVAL 与 WAITAOF 必须在同一连接完成；成功前把连接移出持有槽。写入/确认失败或任务取消后丢弃连接，下一次调用重新连接并重新提交原请求；不能让自动重连在另一条新连接上确认旧写入。Outbox 的 XADD 与 WAITAOF 使用同样所有权规则。

超时只代表没有取得所需结果，不撤销 Redis 可能已经执行的写入。客户端按原 `request_id` / `operation_id` 和原始内容重试；Outbox 保留原 `event_id`，消费者仍必须去重。组提交失败不在底层偷偷重放整批。`enqueueAck=aof` 只有本地 AOF 返回至少一份确认才成功，不自动降级为 memory；Outbox 只有确认成功后才能进入 PG ACK。既有显式 memory 模式的风险不变。

这些确认不代表 PostgreSQL 已落库、不代表多副本容灾、不代表 Outbox 消费者已处理，也不等于取消了未知结果。

## 观测与资格

复用 `dbproxy_storage_stage_seconds` 的有限阶段标签，加入 `enqueue_queue`、`enqueue_write`、`enqueue_aof`、`enqueue_total`、`outbox_write`、`outbox_aof`、`outbox_total`；桶增加 3 秒边界。写入阶段包含本批必要的重连，Outbox 总阶段包含等连接锁。

- 入队排队/总耗时按逻辑提交计数，写入/AOF 按合并批次计数，不能直接相除当成功率。
- 错误、取消仍保留已经等待的耗时；直方图不是成功操作计数。
- `dbproxy_storage_stage_in_flight` 记录仍活跃的作用域。排队取消、receiver 关闭、任务丢弃都释放原计数。
- `dbproxy_storage_stage_timeouts_total` 只记录显式观察到的期限耗尽或 AOF 未确认，不把所有网络错误当超时。入队 worker 与调用方竞态只计一次。外层 Relay 取消另看既有投递 `timeout` 指标；外部取消不会被内部阶段猜测为自身超时。
- 同时查看已有 backlog pending/processing、Outbox 未发布/死信、Redis AOF 延迟与磁盘 I/O；不能只看错误率下降。

复测：

```powershell
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
npm run test:typescript
# 仅在已授权的、独立且空的 AOF Redis；不借用开发业务或正在长稳的容器
$env:DBPROXY_RUN_REDIS_BUDGET_TESTS = '1'
cargo test -p tiangz-dbproxy-storage --test redis_durability --locked -- --ignored --nocapture --test-threads=1
```

回归覆盖配置不一致、排队过期不写、原预算扣除、卡住批次恢复、断线/取消后的新连接重新发送、定制等待在重连后生效、超时观测和真实 Redis 三组配置。RESP 替身证明连接与命令语义；真实 Redis 短对照每组 30 批、每批 40 条 128 字节快照，并发发布一个 Outbox 事件，检查最终快照、fence、事件条数和队列排空，不执行 PostgreSQL 提交或 Redis 强杀，不能代替故障长稳。

本修改会改变 DBProxy 运行时，必须重新构建并重启。原 R4 健康窗口两秒 AOF 超时的失败证据保留；R6 旧二进制的联合 30 分钟已于 2026-09-27 16:26 通过独立复查，但不覆盖这份修改。新版本须以独立源码/二进制身份重新开始联合 30 分钟，再按通过结果递增至连续 24 小时，不能拼接旧时长或扩大故障窗口来消除错误。


## 本轮验证记录（2026-09-27）

Windows 候选 `0.7.0-rc.2`：工作区全部 target 测试 222 通过、49 项外部条件测试默认跳过；格式与 Clippy `-D warnings` 通过；TypeScript SDK 29 项通过。TS 正式生成器和 Cargo 协议构建已执行，协议/生成锁没有变化；Cargo/npm 锁由官方工具更新包版本，第三方依赖版本不变。

独立 Redis 8.8.1、AOF everysec、NVMe bind 的实际对照每组 30 批。修订后的完整 Rust 源码与这轮真实测试逐文件一致；测试编译在包版本升至 rc.2 之前完成，版本元数据及最终发行二进制另作冻结验证。三组全部成功，每组 1,200 条入队、30 条事件、最终 40 个快照逐值核对及队列排空；另 4 项既有 backlog 真实回归通过。证据位于本地 `temp/aof-budget-validation-r2/real-report.json`，容器按完整身份核对后停止，数据保留。

| AOF 上限 | 同批入队与发布并发完成的 p95 | 最大值 |
|---|---:|---:|
| 2 秒 | 1006.050 ms | 1184.074 ms |
| 3 秒 | 1005.712 ms | 1005.744 ms |
| 5 秒 | 1011.676 ms | 1015.022 ms |

这不是 AOF 单独阶段 p95，也不是无其他负载干扰的吞吐比较；同机当时仍运行旧版本联合基线。有限样本未复现旧 HDD 路径尾延迟，不足以选择统一新默认值，暂保留两秒。

测试中新增的 RESP 替身曾在主动丢弃超时连接后把 Windows `ConnectionAborted` 当作意外 panic；实际写入超时分类及重连断言已通过。替身现在仅将 TCP reset/aborted 视为预期关闭，其他读取错误仍失败。原失败日志保留；修正后的完整工作区重新通过，不归类为服务端数据故障。
