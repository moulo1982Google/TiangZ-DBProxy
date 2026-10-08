# Outbox 有界批量发布

2026-10-02 改进版独立重建与 R2 复测完成：Linux Rust 228/格式/Clippy/Release 和另行实际执行的 9 项真实 PG/Redis 检查通过，同一冻结客户端 300+900 秒健康负载及 300 秒停载观察通过。终态 3870 个事件全发布，独立 SQL 趋势/有界积压/排空门禁和 Redis 原始载荷一致性通过；平均 0.385 核、父组峰值 333.32 MiB，保持原 AOF/PG/请求预算。实际轻量快照最大 34 字节；故障、真实存档或 24 小时资格未由本轮覆盖。保护业务复核、本轮资源回收及首次后置工具失败详见[容量复测报告](remote-capacity-2c4g-100-2026-10-01.md)；不继承旧制品长稳结论，未 push。

2026-10-01：云上 2C4G/100 玩家修改前基线确认 Outbox 输入约 3.24 条/s，两个默认 worker 串行 AOF 确认只能发布约 1.99 条/s，终态仍有 1530 条未发布。资源余量不能替代队列跟得上的证据。用户要求修改后重测，当前为本地开发修复，尚未完成改进版云上验收；详见[容量基线与复测](remote-capacity-2c4g-100-2026-10-01.md)。

## 实现边界

默认 Outbox worker 每轮先领取最老的一个可用头部，再从同一持久 Publisher 领取最多 15 个独立排序组的头部。PostgreSQL SQL 仍使用 `NOT EXISTS` 检查前序未发布事件、`FOR UPDATE SKIP LOCKED`，每个 `(publisher_id,destination,partition_key)` 最多一个；正在租赁、退避或死信的前序不被跳过。同组后续事件仍须等待前序 ACK，这次提升的是不同组的并行投递，不提高单组串行上限。

存储 API `claim_batch_for_publisher` 只接受 1..=64；后台默认取 16，不增加配置面。`claim`/`claim_for_publisher` 的单条调用保留兼容。批量数不是所有 worker/进程的内存上限，大 payload 的峰值仍随事件大小和 worker 并发增长。

`Publisher::publish_batch` 按输入顺序返回每项结果，默认实现逐条调用 `publish`，兼容现有自定义 Publisher；Redis 实现合并 XADD，完整收到对应 Stream ID 后在**同一原连接**执行一次 `WAITAOF 1 0 timeout`。它确认此连接前面的写入，不能拿新连接确认旧写入；契约见 [Redis WAITAOF](https://redis.io/docs/latest/commands/waitaof/)。不使用 Redis MULTI 中的非阻塞 WAITAOF，不降为内存 ACK，不更改 appendfsync。

发布包含连接互斥排队、重连、写入与确认，沿用原 5000 ms 总预算和 2000 ms AOF/3000 ms I/O；排队或部分发送消耗原期限。仅收到本地 AOF 成功后才逐事件尝试 PostgreSQL ACK，仍检查 worker/token/租约未过期。成功计数按事件保留，存储 Outbox 阶段直方图的一次 scope 现在可以覆盖一个批次，不能将 scope 个数误作事件数。

批量部分写错误、AOF 未确认、取消或超时会丢弃该连接，未知发送保留至少一次重投边界。有效事件不会因批中一个非法信封被当作永久失败；非法项单独 Permanent，其他项只按自己的发送结果处理。输出数错误/批量未完成不能 ACK。MQ 成功而 PG ACK 丢失仍可能重投原 event_id，消费者 inbox/业务事务/ACK 规则不变。此改动不把至少一次升级为任意故障下全局严格顺序。

代码拆分限定为租约队列 `outbox.rs`、Redis 发送 `outbox_publisher.rs`、服务器 Outbox 调度 `outbox_worker.rs`；公共类型及现有单条入口保持。没有协议、SDK 网络格式或数据库迁移，不提升版本号冒充已发布制品；本地改进以提交与二进制 SHA 标识，必须重建/重启。

## 回归与失败教训

RESP fixture 验证 16 个 XADD 共用一次确认、AOF 失败与取消后全批重新连接/发送、部分写错误不返回成功、非法信封不连带死信其他项、空批/超限不写入，并检查每个 event_id/route/operation/trade/partition/payload。专用 PostgreSQL 并发回归验证八个 worker 抢批次仍只取得组头，后序不能穿透租约或死信，逐项 ACK/token 防护不变。真实 Redis/PG 与云上对照另行记录；fixture 不是断电持久性或长稳证明。

第一次新增批量 fixture 在 Windows 失败：对每条命令调用 `tokio::sleep(Duration::ZERO)`，实际引入累计调度延迟，使 150 ms 测试取消发生在写入阶段而非计划中的 AOF 等待，观察到只收 15 条。零延迟现在直接执行，原非零注入保留；9 项 Publisher 回归通过，首次失败日志保留。禁止调大产品预算或删除失败断言绕过。复测 `cargo test -p tiangz-dbproxy-storage --lib outbox_publisher --locked`。

Windows Rust 226/Clippy/TS SDK 29 与 Publisher 9 项 fixture 已通过；新 Linux Rust 228、格式/Clippy、Release 重建和另行执行的 9 项真实 PG/Redis 检查，以及云上 R2 短时容量已完成，详细身份/命令/日志见上述报告。默认 ignored 项、故障/完整长稳或真实游戏大载荷仍按各自证据报告，不能继承旧 RC2 字节的完整通过历史。
