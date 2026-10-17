# 未发布 / Unreleased

本文件是下一版发布说明的草稿。发版时并入 `RELEASE-v<版本>.md` 后删除本文件。

## 内容

### 修复：缓存修复批量清理与请求事务死锁（40P01）

- **现象**：v0.7.4、`postgresRedis` 后端、2 个请求分片、1000 客户端 16 小时长稳中，DBProxy 日志出现 10 次 `durable queue worker failed error=postgres error: ERROR 40P01: deadlock detected ... queue="cache repair"`。PostgreSQL 日志显示一方是维护连接上的批量清理 `DELETE FROM dbproxy_cache_repairs ... USING unnest(...)`，另一方是请求连接上 `commit_records`（2–3 条记录）事务里的修复行 upsert。修复行没有丢失，结束时 `dbproxy_cache_repairs` 为 0 行；受害方一直是清理语句，工作线程按 `baseRetryDelayMs` 重试。
- **根因**：请求事务（`commit_records`、交易、`save_batch`）在同一事务里逐条 `INSERT ... ON CONFLICT DO UPDATE` 修复行，持锁到提交；批量清理一条 DELETE 按执行计划顺序（数组或堆顺序）逐行加锁，遇到被锁的行就等待。两边对同一批行的加锁顺序不一致，各持一行再等对方即成环。服务端的请求路径都按记录排序后再写，但存储层的 `save_batch` 对直接调用者不保证顺序，SQL 排序默认也不是 Rust 字节序，所以选择让清理一侧不等待，而不是要求两侧同序。
- **修复**：批量清理（`acknowledge_cached_multi` 与服务端后台提示刷新）先用 `FOR UPDATE ... SKIP LOCKED` 锁住空闲的修复行再删除，被请求事务锁住的行直接跳过。清理语句不再等待行锁，不会进入等待环。跳过的行仍是持久修复目标，之后的清理提示或原 CacheRepair worker 会把它确认掉；删除条件仍是 `target_revision <= 已缓存版本`。单条清理 `acknowledge_cached` 只锁一行、不会成环，保持不变。
- **日志**：持久队列工作线程（缓存修复与 Outbox）遇到 PostgreSQL 死锁（40P01）或序列化冲突（40001）时改记 `WARN durable queue worker hit a retryable PostgreSQL conflict; retrying`，其他错误仍是 `ERROR durable queue worker failed`。重试间隔不变。新增公开方法 `StorageError::is_retryable_conflict()`。
- **指标**：`dbproxy_cache_repair_worker_polls_total`、`dbproxy_outbox_worker_polls_total` 新增标签值 `result="retryable_conflict"`，这类轮次不再计入 `failure`。只按 ERROR 日志告警会看不到持续的死锁，请按这个标签的持续速率告警。
- **测试**：`cache_repair_concurrency` 新增两条真实 PostgreSQL 用例：请求事务持一行时批量清理不死锁、两行都保留新目标，缓存到新版本后的下一次清理会把两行都删掉（修复前 5/5 次以 40P01 失败）；真实 40P01/40001 判为可重试、其他错误不判。
- **未验证**：修复后尚未经长稳复验，是否完全不再出现 40P01 待下一轮长稳确认。
- 详见 `docs/cache-repair-cleanup.md`“批量清理与请求事务的死锁”一节。

## 兼容与升级

- 不改协议、SDK、存储结构与迁移，客户端无需升级。
- 繁忙时被跳过、且之后的提示也没覆盖的修复行，会多一次后台修复：一次 PG 读、一次 Redis 写和 claim/acknowledge 两个小事务，量级可忽略，不影响请求结果与持久性。
- 监控：`*_worker_polls_total` 多一个 `result` 标签值；按 `result` 分组的面板会自动显示，按固定标签集合校验的外部规则需要加上它。

## 已知问题

- 排序规则不一致（未在本版修复，单独立项）：`commit_records` 与交易的重复请求路径按 SQL `ORDER BY namespace, record_key`（数据库排序规则）读回记录，再与按 Rust 字节序排好的请求逐条比对，两种顺序不同的键会把合法重试判成 `OperationIdConflict`。当前已知的游戏键不触发。
