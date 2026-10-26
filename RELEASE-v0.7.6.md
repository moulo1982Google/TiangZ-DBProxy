# v0.7.6

DBProxy 单独发布（GitHub Release，不上架 npm/crates.io）。之前的标签与附件保持不变。TiangZ 套件其他仓库本次不变；TiangZ 对 DBProxy 的客户端依赖仍可固定在 `v0.7.3`（本版没有改客户端、协议或 SDK 代码），见下文“兼容与升级”。

本版只有一项：多记录事务回执退休后留下的操作号标记，再保留一段时间（默认 7 天）后删除，操作号表不再随历史操作总数一直增长。

## 内容

### 新增：退休标记的第二级保留期 `storage.retiredOperationRetentionDays`

- **现象**：苟道三国长稳（1000 个机器人、回执保留 1 天、26 小时）中，`dbproxy_operation_claims` 一行都没删，约 1080 万行/天、3.3 GB/天。0.7.3 的回执退休只删事务头和回执记录，把操作号标为“已退休”，标记设计上永久保留，所以随历史操作总数线性增长。
- **做法**：退休满 `storage.retiredOperationRetentionDays`（默认 7 天，取值 1–3650）后删除标记这一行。
  - 按服务端退休时间 `retired_at` 计算，不用客户端时间。
  - 由原有的退休后台任务执行：每轮先退休、再删标记，任一步满批（500）1 秒后继续，否则 60 秒。`FOR UPDATE SKIP LOCKED`，语句超时 5 秒、锁超时 100 ms，与回执清理共用维护连接。
  - 迁移 017：部分索引 `dbproxy_operation_claims_retired_purge(retired_at, operation_id) WHERE retired_at IS NOT NULL AND operation_kind = 'multi'`，启动时的索引核对同步登记。
  - 指标：`dbproxy_operation_marker_purge_batches_total`、`_deleted_total`、`_failures_total`。启动日志 `multi-record receipt retirement started` 增加 `marker_retention_days`。
- **语义变化**：标记在的时候，查询或重提这个操作号仍返回 `OPERATION_EXPIRED`（2007）。标记删掉以后，这个操作号与从没用过一样：
  - `LoadMultiTransaction` 返回“没有回执”；
  - 用它提交按新操作处理。多记录事务都带期望版本号，记录已经变化时返回版本冲突，不会重复写入；记录在这期间一直没变时，重提会再执行一次。
  - 所以“回执保留期 + 标记保留期”必须长于调用方可能重发同一个操作号的最长时间。
- **测试**：真实 PostgreSQL 用例 `retired_markers_are_deleted_after_their_retention`：到期删除、未到期保留、被重提持有（`FOR SHARE`）时跳过并在释放后删除、删除后查询为“没有”、原字节重提被版本冲突拒绝且快照版本不变。配置边界与默认值、SQL 守卫、指标各有单元测试；迁移清单与索引核对测试同步。
- 详见 `docs/record-deletion-and-receipt-retention.md` 附录二“标记删除”与“标记的上限”。

## 运维建议（不是本版改动）

同一次长稳里，回执退休后删掉的空间没有被复用、表继续按插入量增长。原因是 PostgreSQL 默认要死行达到表的 20% 才自动清理（`autovacuum_vacuum_scale_factor = 0.2`），2000 万行的回执表要攒约 400 万死行。私有环境复现后验证：PostgreSQL 18 设 `autovacuum_vacuum_max_threshold`（死行达到这个数就清理）后，删除腾出的空间被新写入复用，表不再增长。建议回执表千万行级的部署设 `autovacuum_vacuum_max_threshold = 200000`（可热加载）。另外，PostgreSQL 删除后库文件不会变小，只在表内复用，正确的观察是“满一个保留期后涨速趋近 0”。

## 兼容与升级

- 不改协议、SDK 与客户端代码；旧客户端（0.7.0–0.7.5）照常连接。TiangZ 宿主内置的 DBProxy 客户端与 TypeScript SDK 可继续固定在 `v0.7.3`。
- 新配置有默认值，不写也行；老配置文件不用改。
- 升级执行迁移 017：普通 `CREATE INDEX`，建索引期间阻塞对 `dbproxy_operation_claims` 的写入（也就挡住新的多记录提交），千万行级估计几十秒，本版没有实测，建议在低峰升级。已经退休超过 7 天的标记会在启动后分批删除（每批之间停 1 秒）。
- 同一个库的多个实例应一起升级：旧实例不删标记，但对已删标记的操作号与新实例同样回答“没有回执”，混跑期间行为一致，只是删除只由新实例执行。
- 回滚：迁移 017 只加索引，可以换回 0.7.5（索引留着不影响）；已经删掉的标记不会恢复，这些操作号在 0.7.5 上同样被当作没用过。
