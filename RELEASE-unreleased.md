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

### 修复：重复请求与回执查询的键顺序（误判 OperationIdConflict）

- **现象**：数据库默认排序规则不是按字节（如官方 PostgreSQL 镜像的 `en_US.utf8`）时，键里有大小写混排（`B` 与 `a`）或标点与字母数字同位（`a_1` 与 `a1`、`a-b` 与 `aa`）的多记录事务或交易，用原 operation ID 合法重试会返回 `OperationIdConflict` 而不是 `Duplicate`；多记录回执查询（`load_multi_receipt`）同样报冲突；交易回执查询返回的记录、分录号、事件号顺序可能与提交时不同。命名空间只差大小写也会触发。当前已知的游戏键不触发。
- **根因**：请求在 Rust 里按 `(namespace, record_key)`、分录号、事件号的字节序规范化；读回已存行的 SQL `ORDER BY` 没有指定排序规则，用的是库的默认语言排序，再与请求逐位比对。涉及 `crates/dbproxy-storage/src/lib.rs` 的多记录回执查询与多记录重复路径，`trade.rs` 的 `load_receipt_parts`（记录、分录、事件三条查询，交易重复路径与交易回执查询共用）。
- **修复**：这五处 `ORDER BY` 的每个排序列都显式加 `COLLATE "C"`。PostgreSQL 的 C 排序对 UTF-8 按字节比较（与 `strcmp` 一致），等价于 Rust `str` 的 `Ord`（UTF-8 字节字典序、前缀较短者在前），也就是按 Unicode 码点排序。没有改成“读出后在 Rust 端再排序”：SQL 一处改动就让所有读回路径（含交易重复路径写修复行的顺序）与请求一致。
- **执行计划**：这些查询都按 `operation_id` 过滤，仍走 `(operation_id, …)` 索引，只对本操作的几行多一个内存排序。在每表 20 万–30 万行的一次性数据上核对过，具体计划与通用计划都是 Index Scan + Sort，每条查询 5–6 个缓冲页。
- **测试**：新增真实 PostgreSQL 用例 `receipt_key_order`（2 项），覆盖多记录事务的 `commit_records`/`apply_multi` 重试与 `load_multi_receipt`、交易的重试与 `load_trade_receipt`。修复前 3/3 次两项都失败（五条路径全部报冲突或顺序不符），修复后 3/3 通过。
- 不改存储结构、迁移和已存数据，只改读回时的排序。

## 兼容与升级

- 不改协议、SDK、存储结构与迁移，客户端无需升级。
- 繁忙时被跳过、且之后的提示也没覆盖的修复行，会多一次后台修复：一次 PG 读、一次 Redis 写和 claim/acknowledge 两个小事务，量级可忽略，不影响请求结果与持久性。
- 监控：`*_worker_polls_total` 多一个 `result` 标签值；按 `result` 分组的面板会自动显示，按固定标签集合校验的外部规则需要加上它。
- 键顺序修复只改读回排序：升级后，此前被误判为 `OperationIdConflict` 的同一请求用原 operation ID 重试会得到 `Duplicate`；已存数据不变。
