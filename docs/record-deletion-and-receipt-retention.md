# 记录删除与幂等回执保留期（设计稿）

状态：设计稿，待评审，未实现。涉及迁移 13、14 和一次协议指纹变更。

## 1. 为什么做

**删除**：协议里只有 Save、Load、Enqueue、Transaction、Trade、CommitRecords，没有删除。删角色、过期邮件、解散公会，业务只能写一个空载荷占位：记录永远留在 `dbproxy_snapshots` 和缓存里，读取方分不清"空"和"不存在"，也做不了合规删除。

**回执保留期**：每一次快照写入（Save、SaveMulti 的每一项、排队写的每次落库）都会在 `dbproxy_idempotency` 留一行回执，内含完整载荷副本，而且永不删除。2026-09-20 写法长稳库（`dbproxy_write_modes_soak`）的只读实测：

| 表 | 行数 | 大小 |
| --- | ---: | ---: |
| `dbproxy_snapshots`（活数据） | 2,000 | 2.2 MB |
| `dbproxy_idempotency`（回执） | 60,369 | 19 MB |
| `dbproxy_multi_transactions` + records | 31,952 | 25 MB |
| `dbproxy_operation_claims` | 31,952 | 4.2 MB |

回执行数约等于该轮确认的普通写 28,522 + 排队写 32,755，即**每次确认的写入一行**。该轮载荷平均只有 8 字节，每行仍约 330 字节（主键索引与行头）；载荷越大，每行越接近"载荷 + 330 字节"。按 500 玩家、每人每分钟一次普通写加一次排队写估算，每天新增约 144 万行：8 字节载荷约 475 MB/天，1 KB 载荷约 1.9 GB/天，且只增不减。

## 2. 现状中与本设计相关的事实

- 快照表主键 `(namespace, record_key)`，按它做 32 个 HASH 分区；列有 `revision`、`payload`、`updated_at_unix_ms`（**客户端提供**，不一定是墙钟：`GlobalIdRangeAllocator` 用它编码号段时间）以及迁移 12 的 `queued_sequence`。
- 一条记录只允许一种写法（直接写 / 排队写 / 事务写），见 TiangZ 业务开发手册"持久化写法标记"。
- 直接写 `save_snapshot_in_transaction`：先插回执占位（`revision = 0`），再做 `expected_revision` 条件 UPDATE 或首次 INSERT，成功后把回执的 `revision` 改成新版本；回执比较内容包括载荷、schema、expected、updated_at。
- 排队写：Redis 里每条记录只保留最新条目（按记录合并），条目格式 `\0Q1:<序号>:<bincode>`；落库走 `save_fenced_snapshot`，序号更大才覆盖。
- 事务写（ApplyMultiTransaction / CommitRecords / Trade）：`expected_revision` 必填，0 表示创建；回执写在 `dbproxy_multi_transactions` 等表，且可以通过 `LoadTransaction` / `LoadMultiTransaction` **被业务查询**，用于判定结果未知的操作是否已生效。
- 缓存（Redis）每条记录四个键：数据、版本、fresh-until、negative。数据写入脚本按版本比较，旧版本不能覆盖新版本。负缓存脚本在带版本调用时会**连版本键一起删掉**；缓存修复发现 PG 没有记录时调用 `delete` 删四个键。
- 协议兼容：服务端精确接受当前指纹和一组已知旧指纹；新客户端只连接指纹完全一致的服务端。升级顺序是服务端先行。

## 3. 设计一：记录删除

### 3.1 核心决定：删除是写一个墓碑版本，不是物理删行

删除把记录改成"墓碑"：`revision + 1`，载荷清空，`deleted_at` 置为 PostgreSQL 当前时间。行本身保留，过保留期后再由后台回收（3.7）。

为什么不直接 `DELETE`：现有的正确性机制都依赖"同一记录的版本号单调递增"。

- 缓存按版本比较：删行后若重建从 revision 1 开始，删除前还在路上的 revision 5 缓存回写会压过重建后的新数据。
- 读取的 `min_revision` 栅栏：删行让版本号倒退，栅栏失效。
- 排队写防护序号存在行上：删行即丢掉序号，迟到的旧排队写会把记录"复活"。
- CAS 的 ABA：旧请求携带 `expected_revision = 3`，重建后的新记录恰好也走到 3，旧载荷就会被接受。

墓碑保住了版本号和防护序号，上述机制不用改就继续成立。

### 3.2 对外语义：客户端眼里墓碑就是"不存在"

对客户端来说墓碑和从未存在完全一样，内部版本号照常单调递增：

| 操作 | 记录存活（版本 N） | 墓碑（内部版本 N） | 从未存在 |
| --- | --- | --- | --- |
| Load / LoadMulti | 返回快照 | **返回不存在** | 返回不存在 |
| Save，expected 不填（盲写） | 写入，版本 N+1 | 重建，版本 N+1 | 创建，版本 1 |
| Save，expected = 0 | 冲突，actual N | **重建，版本 N+1** | 创建，版本 1 |
| Save，expected = K > 0 | K = N 则写入 | 冲突，**actual 0** | 冲突，actual 0 |
| Delete，expected 不填 | 删除，返回 N+1 | 空操作，返回 N | 空操作，返回 0 |
| Delete，expected = K > 0 | K = N 则删除 | 冲突，actual 0 | 冲突，actual 0 |
| Delete，expected = 0 | 请求无效 | 请求无效 | 请求无效 |

要点：

- "不存在就用 expected 0 创建"这一现有业务写法在删除后仍然成立，不需要业务知道墓碑。
- 重建后返回的版本号会从 0 直接跳到 N+1。客户端本来就应该把版本号当作不透明、单调递增的令牌；实现时要逐一检查宿主和 SDK 有没有"创建后版本必为 1"的假设（初查只在 `LocationDirectory` 找到一处 `revision: 1n`，看起来是本地结构，实现时核实）。
- 删除返回墓碑的内部版本号，调用方拿它作 `min_revision`，就能做到"读到我自己的删除"。

### 3.3 协议

不新增 RPC，在已有写入消息上加一个布尔字段：

```proto
message SaveSnapshotRequest {
  ...
  bool delete_record = 8;   // true：写墓碑；payload 必须为空
}
message TransactionalRecordWrite {
  ...
  bool delete_record = 7;   // 同上；expected_revision 必须 > 0
}
message ServerHello {
  ...
  bool supports_record_delete = 6;
}
```

- 因为 `EnqueueSnapshotRequest`、`SaveMultiSnapshotRequest`、`EnqueueMultiSnapshotRequest` 都内嵌 `SaveSnapshotRequest`，直接写、批量写、排队写一次就全部具备删除能力。
- `TransactionalRecordWrite` 同时被 ApplyMultiTransaction 和 CommitRecords 使用，两者都支持删除；**ApplyTransaction（单记录）和 Trade 收到 `delete_record = true` 一律返回 `INVALID_REQUEST`，不能静默当作普通写**。
- 校验：`delete_record = true` 且载荷非空、或 `expected_revision = 0`，返回 `INVALID_REQUEST`。
- 字段名不用 `delete`，避开部分语言的关键字。
- 指纹变化：把当前指纹登记为 `PRE_DELETE_PROTOCOL_FINGERPRINT_V2`，与既有旧指纹一样精确接受。旧客户端从不发送该字段，行为不变。

为什么加字段而不是新 RPC：CommitRecords 单独开 RPC，是为了防止旧服务端静默丢弃事务效果。这里有两层防护：新客户端只连接指纹完全一致的服务端，旧服务端根本收不到带删除字段的请求；`supports_record_delete` 再兜一层，客户端可据此拒绝调用。

### 3.4 存储（迁移 14）

```sql
ALTER TABLE dbproxy_snapshots ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS dbproxy_snapshots_tombstones
    ON dbproxy_snapshots (deleted_at) WHERE deleted_at IS NOT NULL;   -- 分区父表建索引会同步到各分区

ALTER TABLE dbproxy_idempotency ADD COLUMN IF NOT EXISTS delete_record BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE dbproxy_multi_transaction_records ADD COLUMN IF NOT EXISTS delete_record BOOLEAN NOT NULL DEFAULT false;
```

加的都是可空列或常量默认值，PostgreSQL 不会重写表；旧版本服务端的 SQL 不读这些列，迁移本身对旧版本兼容（混跑的限制见 3.8）。

SQL 改动的统一规则（`lib.rs` 7 处写快照、8 处读快照，`trade.rs` 1 处读）：

1. **有效版本**：CAS 比较一律改用 `CASE WHEN deleted_at IS NULL THEN revision ELSE 0 END`，冲突时报告的 actual 也用它。
2. **新版本**：一律用行内的 `revision + 1`，不再由调用方预先计算 `expected + 1`。事务写 `persist_transactional_snapshot` 当前传入预先算好的版本，并用 `EXCLUDED.revision` 写入，这里要改成以数据库返回的版本为准，回执和缓存修复目标也跟着用返回值。这是本设计改动风险最高的一处。
3. **普通写清墓碑**：所有写活数据的语句都置 `deleted_at = NULL`（即重建）。
4. **读取过滤**：Load / LoadMulti / 缓存回源遇到 `deleted_at IS NOT NULL` 一律视为不存在，但把内部版本带给缓存层（3.6）。
5. 直接删除：

```sql
UPDATE dbproxy_snapshots
SET revision = revision + 1, payload = '', deleted_at = now(),
    schema_name = $3, schema_version = $4, updated_at_unix_ms = $5
WHERE namespace = $1 AND record_key = $2
  AND deleted_at IS NULL
  AND ($6::BIGINT IS NULL OR revision = $6)
RETURNING revision
```

没有命中时按 3.2 的表处理：盲删返回当前内部版本（不存在为 0）；CAS 删返回冲突 actual 0。回执照常占位、比较，比较内容加上 `delete_record`，因此同一 `request_id` 不能一次是写、一次是删。

### 3.5 三种写法各自怎么删

删除沿用记录本身的写法，"一条记录只允许一种写法"的规则不变。

**直接写的记录**：`SaveSnapshot(delete_record = true)`，推荐带 `expected_revision`（仓库层默认使用已加载实体的版本）。

**排队写的记录**：`EnqueueSnapshot(delete_record = true)`，走同一条积压队列：

- 条目格式升级为 `\0Q2:<序号>:<bincode>`，bincode 结构多一个删除标志；解码继续接受旧格式和 `Q1`。只有删除条目写成 `Q2`，普通排队写仍写 `Q1`，降低回滚面。
- 按记录合并天然正确：先写后删 → 最终是删；先删后写 → 最终是重建。
- 落库用带防护序号的墓碑 UPSERT：记录不存在时**也插入一行墓碑**。这行墓碑承载防护序号，才能把之后迟到的旧排队写挡掉；不插墓碑的话，旧值会把记录复活。

```sql
INSERT INTO dbproxy_snapshots (..., revision, payload, deleted_at, queued_sequence)
VALUES (..., 1, '', now(), $seq)
ON CONFLICT (namespace, record_key) DO UPDATE
SET revision = dbproxy_snapshots.revision + 1, payload = '', deleted_at = now(),
    queued_sequence = EXCLUDED.queued_sequence
WHERE dbproxy_snapshots.queued_sequence IS NULL
   OR dbproxy_snapshots.queued_sequence < EXCLUDED.queued_sequence
RETURNING revision
```

**事务写的记录**：在 CommitRecords / ApplyMultiTransaction 的 `writes` 里放 `delete_record = true` 的一项，例如删角色时在同一事务里改账号的角色列表。事务写本来就要求 `expected_revision`，因此事务删除总是 CAS。

### 3.6 缓存

墓碑在缓存里用**版本键 + 墓碑标记**表示，不能走现有的负缓存路径：

- 数据键写入固定的墓碑标记，版本键写入墓碑的内部版本，沿用 `REVISION_AWARE_CACHE_PUT_SCRIPT`，脚本本身不用改。这样删除前还在路上的旧版本回写会被版本比较挡掉。
- 读取命中墓碑标记时按"不存在"返回，计入 negative 命中指标。
- **不能**用 `NEGATIVE_CACHE_PUT_SCRIPT` 表示删除：它会删掉版本键，丢了版本下限，旧回写就能写进来。
- 缓存修复从 PG 读到墓碑时写墓碑标记，而不是调用 `delete`；只有 PG 真的没有这行（墓碑已被回收）时才维持现有的 `delete` 行为。

### 3.7 墓碑回收

墓碑行很小，但删邮件这类场景量大，需要回收：

- 挂在现有后台维护连接上，每个维护周期删一批：`deleted_at < now() - 墓碑保留期`，每批上限（默认 1,000 行），批间让出。时间一律用 PostgreSQL 的 `now()`，不受各 DBProxy 节点时钟偏差影响。
- 多个 DBProxy 实例用 `pg_try_advisory_xact_lock`（独立 scope）保证同一时刻只有一个实例在回收。
- **暂停条件**：排队积压最老条目的年龄超过墓碑保留期的 1/4 时暂停回收并告警。长时间积压里可能有比墓碑更老的排队写，墓碑一删它就能复活记录。
- 墓碑保留期默认 30 天，启动校验**必须不小于回执保留期**。回收后该记录的版本号会从 1 重新开始；只要超过保留期的旧请求不再重放（第 4 节的契约），就不会出现 ABA。

### 3.8 升级与回滚

- **全部 DBProxy 实例升级完成之前，不得升级会调用删除的客户端。** 旧服务端不认识墓碑：读取时会把墓碑当成载荷为空的活记录返回；写入时会更新墓碑却不清 `deleted_at`，客户端以为写成功，记录其实仍是"已删除"。新客户端只连指纹一致的实例，这一点天然成立；但旧客户端经旧实例读写同一数据库的窗口必须为零，所以要求先完成全部实例替换，再放开删除。
- 一旦库里出现墓碑或积压里出现 `Q2` 条目，就**不能回滚**到不认识它们的版本。回滚前必须确认：积压排空，且墓碑回收完毕或已人工处理。

## 4. 设计二：幂等回执保留期

### 4.1 范围：只清快照写回执

| 表 | 本设计 | 原因 |
| --- | --- | --- |
| `dbproxy_idempotency` | **清理** | 纯去重用，没有查询接口；增长最快 |
| `dbproxy_transactions`、`dbproxy_multi_transactions`(+records)、交易操作表 | 不动 | `LoadTransaction` 等接口允许业务事后查询操作是否生效；删掉会让"已生效"变成"查无此操作"，业务可能重做扣费 |
| `dbproxy_operation_claims` | 不动 | 全局操作号注册表，Outbox 外键引用 |
| 账本、Outbox、追加事实、事务效果 | 不动 | 业务事实，归档属于路线图里另一项 |

### 4.2 安全窗口论证

回执只在"同一 `request_id` 被再次提交"时起作用。重放来源和时长：

1. 客户端在结果未知时重试：`VersionedEntityRepository` 在进程内按原请求重试，进程退出即放弃，窗口是分钟到小时级（存储故障期间）。
2. 排队写的积压重新落库：窗口等于积压年龄（PG 故障时长）。这类写入同时受防护序号保护，即使回执已被清掉，重复落库也是空操作。
3. 业务持久化操作号、重启后重试：只发生在事务类操作上，其回执不在本次清理范围。

回执过期后才到达的重放：

| 写入类型 | 结果 |
| --- | --- |
| CAS 写 / CAS 删 | 版本已前进，返回冲突，安全 |
| 排队写 / 排队删 | 防护序号挡掉，安全 |
| 盲写 / 盲删 | **按新请求执行**：可能用旧值覆盖新数据，或删掉重建后的记录 |

盲写这一行是所有幂等键系统的共同契约：幂等键有有效期，重试必须在有效期内完成。据此规定：**同一 `request_id` 的重试必须在回执保留期内完成。** 默认 7 天，比已知的重放窗口长两到三个数量级。

曾考虑用"拒绝 `updated_at_unix_ms` 过旧的请求"来兜底，已放弃：该字段由客户端提供，不保证是墙钟（号段分配器就把它当编码时间用），拿它判断请求年龄会误伤正常写入。

### 4.3 存储（迁移 13）

```sql
-- 分两步加列：带 clock_timestamp() 默认值直接 ADD COLUMN 会整表重写并长时间持锁。
ALTER TABLE dbproxy_idempotency ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE dbproxy_idempotency ALTER COLUMN created_at SET DEFAULT clock_timestamp();
CREATE INDEX IF NOT EXISTS dbproxy_idempotency_created_at
    ON dbproxy_idempotency (created_at) WHERE created_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS dbproxy_idempotency_legacy
    ON dbproxy_idempotency (request_id) WHERE created_at IS NULL;
```

迁移前已有的行 `created_at` 为空，视为"迁移 13 的 `applied_at`"时刻创建（从 `dbproxy_schema_migrations` 读），到期后按第二个部分索引分批清掉。空值集合只减不增，清完后该索引可在后续迁移中删除。

一般的 `CREATE INDEX` 会阻塞写入。开发库可以直接建；生产库若表已很大，运维先用 `CREATE INDEX CONCURRENTLY` 手工建好同名索引，迁移里的 `IF NOT EXISTS` 就会跳过。

### 4.4 清理任务

- 与墓碑回收共用后台维护连接和单实例 advisory lock，互不重叠。
- 每批：

```sql
DELETE FROM dbproxy_idempotency
WHERE request_id IN (
    SELECT request_id FROM dbproxy_idempotency
    WHERE created_at < now() - make_interval(secs => $1)
      AND revision <> 0
    ORDER BY created_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED)
```

- 条件 `revision <> 0`：`revision = 0` 是正在进行中的占位，不会存活超过一个事务；这是防御性条件。
- 暂停条件同 3.7：排队积压最老条目年龄超过回执保留期的 1/4 时暂停并告警。
- 删除速率上限（默认每周期 10 批 × 1,000 行），避免与业务写争用 WAL 和 I/O；积累很久的库第一次开启时，按这个速度逐步清完。

### 4.5 配置

```json
"retention": {
  "idempotencyReceiptSeconds": 604800,
  "tombstoneSeconds": 2592000,
  "purgeBatchSize": 1000,
  "purgeBatchesPerCycle": 10
}
```

启动校验：回执保留期不小于 1 天；墓碑保留期不小于回执保留期；批大小 1..10,000。未知字段按现有规则直接报错。设为 0 表示关闭对应清理（显式选择，不是默认）。

### 4.6 指标与告警

- `dbproxy_retention_purged_total{kind="receipt"|"tombstone"}`
- `dbproxy_retention_oldest_age_seconds{kind}`：最老未清行的年龄。超过保留期 × 1.5 告警，表示清理跟不上或被暂停。
- `dbproxy_retention_paused{reason="backlog_age"}`
- 删除请求沿用现有 RPC 指标，按 `delete` 维度计数。

## 5. 测试计划

- **单元 / 内存后端**：3.2 表格逐格一个用例；MemoryBackend 与 PostgreSQL 行为一致（沿用现有对照测试方式）。
- **真实 PG/Redis**（`postgres_redis`、`postgres_redis_network`）：
  - 删除后重建版本号单调；旧版本缓存回写不能覆盖墓碑；`min_revision` 栅栏跨删除有效。
  - 排队写：写 → 删 → 迟到的旧写（伪造更小序号）必须被挡；删除不存在的记录也要留下带序号的墓碑。
  - 事务删除与另一记录更新原子提交，失败整体回滚；重试同一操作号返回同一结果；把删改成写重试，返回幂等冲突。
  - ApplyTransaction / Trade 收到删除字段返回 `INVALID_REQUEST`。
  - 回执清理：按时间删除；进行中的占位不被删；积压超龄时暂停；迁移前旧行按 `applied_at` 到期；两个实例并发只有一个在清。
  - 墓碑回收后重建从 1 开始，且缓存里无残留。
- **故障矩阵**：在已有 Redis/PG 故障窗口里混入删除与重建，最终对账把"已确认删除的记录读到了数据"和"已确认写入的记录读不到"都算违例。
- **写法长稳**：`soak:write-modes` 为三种写法各加一定比例的删除与重建，账本记录删除确认，最终 PostgreSQL 对账覆盖墓碑。
- **协议**：新旧指纹握手矩阵；新客户端连旧服务端被拒。

## 6. 实施顺序

1. **回执保留期**（迁移 13 + 清理任务 + 配置 + 指标）。与删除无依赖、改动小，而且表每天都在长，先做。
2. **删除，直接写路径**（迁移 14、协议字段与指纹、有效版本 SQL 改造、缓存墓碑、Load 过滤、MemoryBackend、Rust 客户端、TypeScript SDK）。
3. **删除，排队写路径**（`Q2` 条目、防护墓碑 UPSERT）。
4. **删除，事务写路径**（`persist_transactional_snapshot` 改为以数据库返回版本为准，是风险最高的一步）。
5. **墓碑回收**。
6. TiangZ 宿主与仓库层 API（另起设计，依赖 2–4）。

每一步单独合并、单独过完整验收；第 2–4 步都完成之前，`supports_record_delete` 保持 false，客户端不暴露删除。

## 7. 明确不做

- 按前缀或 namespace 批量删除。
- 软删除 / 撤销删除：墓碑清空载荷。回档类需求留给以后的"历史版本"功能。
- 事务类回执、账本、Outbox 的保留期与归档。
- 回执改存载荷摘要：实测小载荷时每行开销主要在索引和行头，行数才是问题，按时间清理已经解决；等出现大载荷证据再考虑。
- 跳过排队写的回执：防护序号理论上已经能挡住重复落库，但回执目前还承担着"同一请求号并发去重"的作用，去掉要单独论证，本次不动。

## 8. 待拍板

1. **删除后是否保留旧载荷？** 建议清空。保留便于客服恢复，但做不了合规删除，而且会让"删除"变成隐形存储。
2. **回执清理默认开启 7 天，还是默认关闭？** 建议默认开启：已知重放窗口是分钟到小时级，7 天余量足够，而且不开的话表会无限增长。
3. **墓碑保留期默认 30 天**，是否合适。
4. **第一版是否包含事务内删除（第 4 步）？** 建议包含：删角色要和账号索引原子更新，没有它业务只能自己拼两次写入，重新引入半成功状态。
