# v0.7.1

TiangZ 套件 0.7.1 中的 DBProxy（GitHub Release，不上架 npm/crates.io）。之前的标签与附件保持不变。

## 内容

合入 DBProxy 评审与验收分支 `acceptance/index-retention-p08-20260923`（2026-09-23 至 10-08，149 个提交）。该分支从 0.7 主线分叉前（2026-09-21）开出，0.7.0 未包含它：

- **普通快照回执保留期**：配置 `storage.receiptRetentionHours`（默认 24，范围 1–8760），后台清理任务分批删除过期回执（只清 `dbproxy_idempotency`）。
- **主库 PG 读池**：配置 `storage.postgresReadConnections`（0 = 不用，1–64），读请求优先走读池，不占写分片连接；未配置时仍走写连接，保留 0.7 的操作诊断与连接等待期限。
- **查询索引**：账本分录、Outbox、缓存修复领取、回执保留期相关索引；启动时校验关键索引。
- **Outbox 领取**：0.7 的批量领取改用快速路径（队首前若干条）+ 分组后备的查询，降低积压时的扫描量。
- 日志带租户 span；连接预算日志；共享启动入口 `server_process`（验收宿主复用同一套启动代码）。
- 验收工具、故障注入、回执超时探针、并行矩阵与对应文档。

协议版本、`dbproxy.proto`、Rust `tiangz-dbproxy-client/core/protocol` 与 TS SDK 不变，与 0.7.0 客户端/服务端互通；TiangZ 0.7.1 与 Examples 0.7.1 依赖的 DBProxy `v0.7.0` 客户端与本版本一致，无需修改。

## 升级注意

- **启动时执行迁移 013–015**：013 账本分录 / Outbox 的 `operation_id` 索引；014 缓存修复领取部分索引；015 给 `dbproxy_idempotency` 加 `recorded_at`（默认值为迁移时间，不改写已有行）与保留期索引。三者都是普通 `CREATE INDEX`，**建索引期间会阻塞对应表的写入**；表很大时请在维护窗口升级。
- 015 的注释写着“七天宽限”，但保留期默认已改为 24 小时：升级前写入的回执以迁移时间为起点，**升级约 24 小时后开始被清理**。需要更长请先设置 `storage.receiptRetentionHours`。
- 迁移不提供自动回滚。迁移后再换回 0.7.0 服务端的情况本次没有验证；需要回退时请先在隔离库确认。

## 验证

- 本地：`cargo fmt`、`cargo clippy -D warnings` 通过；`cargo test --workspace` 253 通过、111 项需真实 PG/Redis 的用例默认忽略；TS SDK 29/29。
- PR CI：格式/测试/Clippy 与真实 PostgreSQL 回归（索引、查询计划、Outbox 顺序与并发、回执保留期）；合入前另以 workflow_dispatch 运行“发布前完整验收”（真实 PG/Redis 与故障矩阵）。结果记入 PR。
- 未执行：长稳测试（用户安排在后）。
