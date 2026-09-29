# F13：实际发布后、PG 确认前强杀与消费去重

RunId `publish_20260929d`，新库 `_f13`，正式 release 服务 + 实际 Redis Streams，Linux 测试通过，耗时 3.21 秒。测试运行要求 Unix；测试主体也在 Windows 编译检查，但不在 Windows 执行 SIGKILL 验收。

## 精确屏障及结果

两个业务提交各含一个 Outbox 事件，同一 topic/partition key，先后为 head、tail。测试专用 PG 转发器按前端协议帧解析，在服务已完成 Redis 发布、准备发送 `UPDATE dbproxy_outbox SET published_at ...` 时扣住该 SQL 的 Parse/Query 帧，不让 PG 收到确认。普通业务及此前租约 SQL 正常通过；仅测试链路禁用 TLS。

直接读取 Redis 确认只有 head 一条、PG published 行数为零后，SIGKILL 本测试启动的 DP，退出信号为 9。随后验证：

- 有效 head 租约仍在，其他 worker 不能领取 head 或越过它领取 tail。
- 仅在测试行上人为设为过期，再以同一 worker 身份领取，token 从 1 增至 2。使用旧 token 调实际 acknowledge API 返回 false，有效租约下仍不能越序。
- 再使测试租约过期，重启正式服务。Redis 得到 `head, head, tail`，PG 两条均最终确认；过期 token 再确认仍返回 false。
- 创建实际 Redis 消费组消费三条消息；PG inbox 按 event_id 去重，inbox 插入与效果计数在同一事务提交，再 XACK。最终两个唯一 inbox、两次效果，而不是三次。
- 两个原业务请求重放均 Duplicate，直接 PG 核对载荷、revision=1，没有重复业务写入。

租约过期通过隔离库测试行加速，不声称自然等待全租期；消费 inbox/projection 是测试应用侧示例，未给 DBProxy 增加业务规则。该轮不是所有发布故障点的穷举。

## 失败、真实原因和修法

- `publish_20260929a` 在 Linux 编译发现测试存储句柄少了 mut，未进入故障测试。
- `publish_20260929b` 用 PG BEFORE UPDATE 触发器卡住确认，杀 DP 后解除锁，错误地假定 PG 必然取消已收到的 SQL。实际查询证据显示 head 已 published，tail 被合法领取，测试断言失败。**客户端死亡不等于数据库中已收到的语句必然回滚**，不能以此宣布生产队列越序。
- 改用发送前协议屏障后，`publish_20260929c` 编译发现转发协程缺少 Result 类型标注，未进入故障测试。
- 修正类型，并让平台无关主体在 Windows 也参与编译/Clippy，只将 Unix 信号检查条件编译；新 RunId d 完整通过。

禁止放宽队首顺序断言、手工抹去 published 状态或用取消 PG 语句掩盖屏障位置错误。a/b/c 全部失败证据保留，d 不覆盖它们。

## 复测与证据

入口：`FAULT_TESTS=outbox_publish_crash::f13_published_message_survives_process_crash_before_ack bash deploy/remote-test/run_fault_process.sh <新RunId>`。旧镜像挂载当前 tests 与脚本目录。工作台 4 CPU/16 GiB、固定 cpuset，串行运行。

证据：服务器 `/data/dbproxy-test/evidence/fault_process_publish_20260929{a,b,c,d}/`，本地 `target/server_20260929/` 同名目录；d 的 `f13/result.json` 记录真实消息 ID 顺序、流名、token、信号和消费效果。每轮日志、PG 日志及二进制哈希保留，测试 Redis stream/消费组也保留。Windows 定向 Clippy、格式/差异检查及 Linux 实测通过；生产代码和协议未改。
