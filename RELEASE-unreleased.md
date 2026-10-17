# 未发布 / Unreleased

本文件是下一版发布说明的草稿。发版时并入 `RELEASE-v<版本>.md` 后删除本文件。

## 内容

### 修复：缓存修复批量清理与请求事务死锁（40P01）

- **现象**：v0.7.4、`postgresRedis` 后端、2 个请求分片、1000 客户端 16 小时长稳中，DBProxy 日志出现 10 次 `durable queue worker failed error=postgres error: ERROR 40P01: deadlock detected ... queue="cache repair"`。PostgreSQL 日志显示一方是维护连接上的批量清理 `DELETE FROM dbproxy_cache_repairs ... USING unnest(...)`，另一方是请求连接上 `commit_records`（2 条记录）事务里的修复行 upsert。修复行没有丢失，结束时 `dbproxy_cache_repairs` 为 0 行；受害方一直是清理语句，工作线程按 `baseRetryDelayMs` 重试。
- **根因**：请求事务（`commit_records`、交易、`save_batch`）在同一事务里逐条 `INSERT ... ON CONFLICT DO UPDATE` 修复行，持锁到提交；批量清理一条 DELETE 按执行计划顺序（数组或堆顺序）逐行加锁，遇到被锁的行就等待。两边对同一批行的加锁顺序不一致，各持一行再等对方即成环。`save_batch` 按调用方顺序写，本身没有统一顺序，所以只在清理一侧排序也不能消除。
- **修复**：批量清理（`acknowledge_cached_multi` 与服务端后台提示刷新）先用 `FOR UPDATE ... SKIP LOCKED` 锁住空闲的修复行再删除，被请求事务锁住的行直接跳过。清理语句不再等待行锁，不会进入等待环。跳过的行仍是持久修复目标，由原 CacheRepair worker 正常重建缓存后确认；删除条件仍是 `target_revision <= 已缓存版本`。单条清理 `acknowledge_cached` 只锁一行、不会成环，保持不变。
- **日志**：持久队列工作线程（缓存修复与 Outbox）遇到 PostgreSQL 死锁（40P01）或序列化冲突（40001）时改记 `WARN durable queue worker hit a retryable PostgreSQL conflict; retrying`，其他错误仍是 `ERROR durable queue worker failed`。指标仍记一次 `failure`，重试间隔不变。新增公开方法 `StorageError::is_retryable_conflict()`。
- **测试**：`cache_repair_concurrency` 新增两条真实 PostgreSQL 用例：请求事务持一行时批量清理不死锁且两行都保留新目标（修复前 5/5 次以 40P01 失败）；真实 40P01/40001 判为可重试、其他错误不判。
- 详见 `docs/cache-repair-cleanup.md`“批量清理与请求事务的死锁”一节。

## 兼容与升级

- 不改协议、SDK、存储结构与迁移，客户端无需升级。
- 繁忙时被跳过的修复行会多一次后台缓存重建（只是一次 Redis 写），不影响请求结果与持久性。
