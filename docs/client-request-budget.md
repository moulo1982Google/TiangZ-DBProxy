# Rust SDK 请求总预算（0.7 开发）

`ClientConfig.request_timeout` 现在是一次逻辑调用的总墙钟预算，默认仍为 5 秒。预算从具体操作的 API 首次被 poll 时建立，覆盖同步请求准备、等待在途名额、等待写锁、写入、响应、自动重连及一次重试。同步编解码受协议大小限制，期限不抢占同步 CPU；已过期的请求不会因为锁立即可用而获得新的发送机会。

`connect_timeout` 保持既有连接阶段上限；请求内部的重连还受逻辑请求剩余时间约束。显式 `DbProxyClient::connect` 的多候选策略保持不变。`request_timeout` 必须大于零且可由单调时钟表示。

## 错误与资源归属

| 结果 | 语义 | 调用方处理 |
| --- | --- | --- |
| `RequestNotSentTimeout` | 这次逻辑操作没有任何尝试进入帧写入阶段 | 服务端未接收本次请求；超时后 SDK 不继续偷发 |
| `RequestTimeout` | 至少一次尝试进入写入，业务结果可能未知 | 保留 request_id/operation_id、记录身份及相同 payload，按原操作恢复 |
| 业务 `Remote` 错误 | 按服务端业务错误解释 | 不因冲突、鉴权或协议拒绝盲目切换服务端 |

重连中的排队超时不能抹掉先前可能已发送的历史。自动重试共享原期限与原业务身份；预算耗尽就返回，不重新得到另一段 5 秒。等待许可/写锁失败时不留下 pending 关联，已有许可通过 RAII 释放。部分帧写入被取消或超时会关闭写端并标记流不可再用；完整发送后的响应超时只终结该关联，保留既有活性判定和其他在途响应，迟到响应不会交给其他请求。

配置迁移影响：旧版本在排队之后才开始计时，且自动重试可重复获得预算；0.7 在过载或故障时可能更早报告超时。这是明确的语义修正，不能通过静默增大配置值掩盖。新增 Rust 错误枚举分支需要更新消费方的穷尽匹配；发行号与宿主依赖 tag 在联合发布时更新，本开发修改不代表已经发布。

## 实际验证与限制

2026-09-27 联合故障长稳在暂停两个 DBProxy 写入者、取证 AOF 积压时，验收客户端收到 `RequestNotSentTimeout`。SDK 按上表返回确定未发送，旧 `dbproxy_fault_soak` 却只匹配 `RequestTimeout`，将合法预算到期误计为永久契约错误并终止整轮；`dbproxy_relay_soak` 有相同遗漏。修复仅为两个验收程序补齐此分支，没有增大五秒预算、吞掉冲突/协议错误、重置操作号或修改服务端。故障窗口外的可用性错误、旧读/缺失、账本与最终对账门槛保持原样。

先在旧实现补入两个回归断言，分别实际失败（`temp/v0.7-soak-budget-red.log`、`temp/v0.7-relay-budget-red.log`）；修复后五条验收程序测试通过，完整 `cargo test --workspace --locked` 为 204 通过、48 ignored，格式和全目标 Clippy 通过，TS SDK 29 条通过。命令分别为 `cargo test --locked --bin dbproxy_fault_soak --bin dbproxy_relay_soak`、`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`npm run test:typescript`，对应日志前缀 `temp/v0.7-soak-budget-`。SDK 的 28 条预算/网络回归包含在工作区结果中。Rust 验收客户端必须重新构建后才可复跑；不能用原二进制或把这次约 22 分 40 秒的失败拼接为完整 30 分钟。原始联合失败及清理后只读积压证据在 TiangZ 工程 `temp/v0.7-joint-soak-r2/joint-uOk7T5/` 与 `temp/v0.7-joint-review-30-r2/`；40 条积压前后相同只作补充证据，不替代中断的故障窗口、最终对账或完整时长。

2026-09-26：最初 5 个隔离 TCP 反例全部在旧实现失败，证据 `target/test-results/v0.7-budget-red.log`。完成修复和补充验证后，`cargo test --locked -p tiangz-dbproxy-client` 共 28 条通过，证据 `target/test-results/v0.7-budget-green-complete.log`，包含排队、写锁、共享响应预算、重连锁、候选握手、过期 ready 路径、结果未知保留及既有多路复用/连接恢复测试。

部分写的取消/超时使用 64 字节 Tokio duplex 流，先读取编码帧头并确认仍持有写锁，再验证截断、EOF 和写端不可复用。最初尝试本机 TCP 配置小缓冲，在该 Windows 环境中仍观察到整段写入已完成、进入响应等待，故不能把那次超时记为“部分写”；失败日志 `target/test-results/v0.7-write-budget-fixture.log`、`v0.7-write-budget-controlled.log` 保留。这个夹具修正不改变 Socket 默认值，也不放宽断言；它证明通用写路径的 RAII，不替代操作系统慢写验收。

后续 TS 候选已补充独立预算作用域、单调时钟与可选 Transport 剩余预算参数，29 条通过，详见 [TS SDK](typescript-sdk.md)。TiangZ 候选 Repository 的读取/迁移/同 ID 重试共享范围；Host 已验证裸 V8 + 隔离 TCP 的超时回收。其 npm 候选与默认已发布 tag 分开记录，Rust SDK 联合链接及发布依赖仍需最终冻结；不能把本 SDK 结果称为真实存储端到端验收。没有运行真实 PostgreSQL/Redis、服务故障或长稳，没有改变网络协议、Generated 或依赖锁。

同轮完整工作区复跑 `cargo test --workspace --locked` 为 200 条通过、47 条 ignored，`cargo clippy --workspace --all-targets --locked -- -D warnings` 通过；日志分别为 `target/test-results/v0.7-budget-workspace.log`、`target/test-results/v0.7-budget-clippy.log`。
