# PostgreSQL 快照分区

## 当前范围

DBProxy 只对权威快照表 `dbproxy_snapshots` 启用 PostgreSQL 原生声明式分区。交易、账本、Outbox、幂等回执和缓存修复队列仍保持普通表；历史归档和物理分库继续延期。

`namespace` 仍是逻辑记录地址的一部分，不是表名。DBProxy 的所有 SQL 继续访问父表 `dbproxy_snapshots`，不会按请求拼接子表名，也不会把分区数量暴露为运行时配置。

## 固定布局

父表使用完整主键作为 HASH 分区键：

```sql
PARTITION BY HASH (namespace, record_key)
```

当前 schema 固定创建 32 个叶子分区：

```text
dbproxy_snapshots_p00 ... dbproxy_snapshots_p31
MODULUS 32, REMAINDER 0 ... 31
```

选择 32 而不是按 namespace 建表，是为了让热点 namespace 仍能均匀分散。每个点查、CAS 和缓存修复请求都携带完整的 `(namespace, record_key)`，PostgreSQL 可以裁剪到一个叶子分区。多记录事务可能访问多个叶子分区，但它们仍在同一个 PostgreSQL 数据库事务中原子提交。

分区数量属于数据库 schema，不等于 DBProxy 进程数、连接数、CPU 数或物理数据库数量。配置中的 `storage.shards` 当前只是共享同一 PostgreSQL/Redis 地址的连接并发分片，也不控制这 32 个数据库分区。

## 启动检查

`dbproxy_schema_migrations` 记录 001 到 007 是否已经提交。多个实例启动时仍先取得迁移 advisory lock，但只执行尚未登记的 DDL，避免后启动实例反复 `CREATE/DROP TRIGGER` 或锁住正在承载业务写入的分区表。

每次建立 PostgreSQL 存储时，DBProxy 随后验证：

- `dbproxy_snapshots` 必须是分区父表；
- 分区键必须为 `HASH (namespace, record_key)`；
- 必须恰好存在 `p00` 到 `p31`；
- 每个叶子分区必须具有对应的 modulus/remainder 边界。

任何一项不一致都会拒绝启动。这样可以避免某个实例连接到旧普通表或不完整 schema 后静默运行。

## 开发数据库升级

项目当前不实现“已有普通表在线转换为分区表”。PostgreSQL 也不能用一条 `ALTER TABLE` 把普通表原地转换成声明式分区父表。旧开发数据库连接到本版本时会收到明确错误：

```text
dbproxy_snapshots exists but is not partitioned; reset the development database before starting this DBProxy version
```

确认本地开发数据不再需要后，可删除 Compose 数据卷并重新创建：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml down -v
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 up
```

`down -v` 会永久删除本地 PostgreSQL、Redis 以及观测数据卷；有需要的数据必须先导出。普通的 `tools/local_laptop.ps1 down` 只停止容器并保留卷，不能完成这次 schema 重建。

## 将来扩充分区数量

HASH 分区数量在创建 schema 时确定，但不是永久容量上限。将来可以按倍数扩展，例如 `32 -> 64 -> 128`；扩展需要受控地重新分配现有行，不是修改 DBProxy JSON 或增加一个空表。

扩展时保持以下契约：

1. DBProxy 始终只查询父表；
2. 新 migration 定义新的 canonical 布局并同步修改启动校验常量；
3. DBA/运维负责建表、搬迁、锁与 WAL 预算、校验及切换；
4. DBProxy 负责在新布局上回归 Load、Save、CAS、Multi、Trade、幂等和缓存修复语义；
5. 不能在普通请求路径自动创建或拆分分区。

如果未来已有生产数据，需要另行设计影子分区表、增量同步、写入围栏、校验和回滚流程；本次迁移没有隐藏实现这些行为。

## 验证

真实 PostgreSQL 集成测试 `postgres_snapshot_table_uses_32_hash_partitions` 会验证完整 catalog 布局，写入 64 条独立快照，并确认 PostgreSQL 实际把它们路由到了多个叶子分区。其余真实存储测试继续覆盖快照、CAS、多记录事务、交易、账本、Outbox 和缓存修复。
