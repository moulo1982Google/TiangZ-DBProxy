# 0.7 容量观测与保留边界

2026-09-26，先冻结契约后实施，现已通过本页验收。D4 只交付只读观测与容量说明，不执行删除、归档、TTL、墓碑迁移或协议变更。[原删除设计](record-deletion-and-receipt-retention.md)仍未实施，不能把其中示例保留天数当成可执行契约。

2026-10-04 已记录未来[2000 人单服连续 30 天的前置条件](../../TiangZ-0.7/docs/design/v0.7-30-day-soak-prerequisites.md)：容量需覆盖回执原始 payload/写集、账本、Outbox/Stream、索引/TOAST、WAL/复制槽、备份、日志和完整收尾空间，按实际字节/日与剩余可用天数判定。清理需先定义结果未知重试、操作号退休、旧请求拒绝/墓碑或归档回执查找，不能按日期直接删回执。该稿未实施、不启动业务压测、不改变当前 DP 验收范围；本命令保持只读。

## 入口与预算

```powershell
cargo run -p tiangz-dbproxy-server --bin dbproxy_capacity -- --postgres-url-env DBPROXY_CAPACITY_URL --schema public
# 仅显式需要时增加全表服务器时间查询；可输出到本地 JSON 留档。
cargo run -p tiangz-dbproxy-server --bin dbproxy_capacity -- --postgres-url-env DBPROXY_CAPACITY_URL --include-server-age --timeout-ms 10000
```

命令从已配置的环境变量读取连接串，以上不会创建变量、连接配置或服务。JSON `format_version=1`；`status` 和 `oldest_server_time.status` 必须分别检查。可选年龄查询失败时整体容量报告仍成功，但该表的时间状态为 `query-timeout` / `query-error`，不能将它当作已获得完整年龄数据。工具将 `row_security=off`，受 RLS 隐藏的行导致年龄查询明确报错，避免误报空表；并不绕过 RLS 权限。

独立 `dbproxy_capacity` CLI，明确指定连接串所在环境变量和 schema（默认 public），一次运行输出一个 JSON 快照。连接及所有查询共用默认 10 秒、可选 1..60000 毫秒总预算；独立 PostgreSQL 连接、只读事务、事务内 statement_timeout，不占 DBProxy 请求连接、不执行迁移或启动 worker。连接串不放命令行，错误仅报告类别/SQLSTATE，不输出连接串或业务数据。连接安全与现有适配器一致，NoTls 用于本地或已经受保护的链路，不隐式提供 TLS。

默认从 catalog 获取 18 个固定逻辑表的用途、是否存在、叶子表数量、估算行数、表/索引/总字节。分区父表与叶子不能重复计算；TOAST 与其索引计入表字节，主表索引单列。未知估算保留 null；缺表和不支持的 relation 类型明确列出，不能报成空表。单次最多接收 10000 条 relation 观测，超过上限失败，不输出截断的成功报告。catalog/大小查询失败时整体非零退出。

`--include-server-age` 才查询现有服务器生成的时间列，逐表扫描预算固定 250ms，查询超时/无权限等返回明确状态并继续报告容量。查询可能扫描全表，不放进 scrape、请求或默认后台任务。没有服务器时间的回执报告 `no-server-clock`，不拿 `updated_at_unix_ms`、`occurred_at_unix_ms` 或账本业务时间冒充服务器保留年龄。总预算到期仍使整次命令失败；不会输出半份成功快照。没有 schema 时非零退出，schema 中尚未部署的表列为 missing。

## 表的责任

| 表（统一 dbproxy_ 前缀） | 所有权与用途 | 现有服务器时间 |
| --- | --- | --- |
| snapshots | 权威当前状态；按完整 RecordKey 分区 | 无 |
| idempotency | 普通 CAS 重试回执与请求指纹 | 无 |
| transactions | 单记录事务首次回执/结果 | 无 |
| multi_transactions / multi_transaction_records | 多记录原子事务回执及原始写集 | 无 |
| multi_transaction_effects | CommitRecords 不可变效果内容 | 无 |
| operation_claims | 跨事务类型操作号唯一性 | claimed_at |
| trades | 交易状态 | 无 |
| trade_operations / trade_operation_records | 交易首次回执及写集 | 无 |
| ledger_postings | 不可变账本事实 | 无；created_at_unix_ms 是业务字段 |
| append_records | 不可变追加事实 | 无 |
| outbox | 至少一次投递内容/进度/死信 | created_at |
| cache_repairs | 权威提交后的缓存修复责任 | requested_at |
| outbox_publishers / outbox_routes | 发布目标配置 | 无 |
| outbox_admin_audit | 人工操作审计事实 | created_at |
| schema_migrations | 已应用迁移元数据 | applied_at |

服务器时间只说明这些行的时间，不证明重试已退休、事件已消费或可安全删除。Operation claim 时间不自动赋给其他回执。角色需要 catalog/大小函数权限；启用年龄观测还需目标表 SELECT 权限，推荐独立只读账号。

## 频率、增长和备份

首轮建议由运维显式运行，正常采样间隔至少 10 分钟，年龄扫描按需开启；本工具没有自建调度器。相邻同数据库/schema 的完整快照可比较物理字节差和时间差。估算行数来自 VACUUM/ANALYZE 等维护时点，可能滞后或未知；工具不执行这些维护命令。负增长可能来自维护或表变更，不能据此推断删除数量。

按各表增长率分别计算容量余量；当前行/索引字节不包含 WAL、备份、复制槽、Redis backlog/Stream、临时文件和其他数据库。备份空间还取决于全量/增量、压缩、保留副本和恢复演练临时空间，不能用表字节直接当备份大小。保留回执会持续增长，但仅按 TTL 删除会让旧请求再次执行；未冻结退休边界和归档查询/拒绝机制前，默认保留。

PostgreSQL 依据：[reltuples 与未知值](https://www.postgresql.org/docs/18/catalog-pg-class.html)、[关系大小与事务局部配置](https://www.postgresql.org/docs/18/functions-admin.html)、[statement_timeout](https://www.postgresql.org/docs/18/runtime-config-client.html)。这些观测不是逻辑一致的数据库备份，也不是精确行数统计。

## 验收范围

在新建、明确归属本轮的 PostgreSQL 18.4 容器中，真实迁移、32 分区汇总、只读角色、未知估算/缺表/不支持类型、引号 schema、年龄成功/空表/RLS 错误、锁等待超时及 savepoint 恢复、总预算与连接释放通过。证据 `temp/v0.7-capacity-postgres-first.log`。独立测试只读取 `DBPROXY_CAPACITY_TEST_URL`，要求数据库名 `v07_capacity`；会创建测试数据与只读角色，只用于可丢弃的独立数据库。

实际 CLI 在只读角色下输出 18 表 JSON，缺 schema 与非法连接串均退出 1 且 stdout 无半份 JSON，解析错误未泄漏凭据标记；见 `temp/v0.7-capacity-cli-report.json`、`v0.7-capacity-cli-checks.json`。工作区全目标 204 条通过、48 ignored，Clippy `-D warnings` 通过（`v0.7-capacity-workspace-tests.log`、`v0.7-capacity-clippy.log`）。ignored 不计入通过；上述真实容量用例另行显式执行。既有 PostgreSQL/Redis 容器不参与，本工具不新增生产统计任务和清理策略。

夹具补充外层 30 秒保护、连接测试父任务 2 秒保护后，最终四个 capacity 测试（含真实 PG）再次通过，最终 storage/server 全目标 Clippy 通过（`v0.7-capacity-final-tests.log`、`v0.7-capacity-final-clippy.log`）。本轮临时容器已按 ID 与专用 label 核对后停止并自动移除，身份/清理后容器列表保存在 `temp/v0.7-storage-fixture-identity.json`、`v0.7-storage-after-cleanup.log`。
