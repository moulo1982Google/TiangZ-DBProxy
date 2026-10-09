# 全库索引检查与第一批修复验证（2026-09-22）

本轮已经落地全库结构检查、两项回执查询索引，以及 Outbox 全局统计改写。普通回执保留 7 天仍是下一阶段，当前没有自动删除回执。

后续已按用户授权升级至 PostgreSQL 18.6，并完成旧库数据校验、2 项升级后读写与 35 项新库回归，见[18.6 升级验证](postgresql-18.6-upgrade-20260922.md)。下文保留先前 18.4 与 Windows 环境的实际历史结果。

## 经用户授权的 Docker 复验

用户准备好 Docker 后，明确授权使用手册环境验证。2026-09-22 在 Docker Desktop 的 Linux 容器中重新执行全部 35 项真实依赖测试，全部通过；本节是本轮规定镜像环境的验证依据，后面的 Windows 数据仅保留为历史记录。

使用 `deploy/local/docker-compose.yml` 加 `docker-compose.laptop.yml`，设置 `DBPROXY_REDIS_APPENDONLY=yes`，只启动 PostgreSQL、Redis。Compose 项目名为 `tiangz-dbproxy-index-validation`，创建独立数据卷，没有使用已有业务库。实际运行版本与配置已读取确认：

- PostgreSQL `18.4`，镜像 `postgres:18.4-bookworm`，摘要 `sha256:882236b897e39051d2368c5ccc6cda944904723506b2dfc97f2a8f5bc9afa382`；容器限额 1 GiB，shared_buffers 128 MB，max_connections 30，JIT 关闭。
- Redis `8.8.1`，镜像 `redis:8.8.1-trixie`，摘要 `sha256:3eafabb4c93fcb8b36b666e07a43f096cb157bc6b07dce4b2492b895c63cf37f`；容器限额 384 MiB，AOF 开启、everysec、noeviction。
- 测试客户端和 DBProxy 测试程序在 Windows 主机运行，连接上述 Linux 容器；并非所有程序都在 Linux 容器中运行。

| 测试 | 独立 PostgreSQL 测试库 | 结果 |
| --- | --- | --- |
| `schema_indexes` | `index_regression_docker` | 3 项通过，5.52 秒 |
| `postgres_redis` | `storage_regression_docker` | 22 项通过，11.34 秒 |
| `cache_repair_concurrency` | `repair_regression_docker` | 8 项通过，4.99 秒 |
| `postgres_redis_network` | `network_regression_docker` | 2 项通过，2.19 秒 |
| `check_schema_indexes` 只读命令 | `index_regression_docker` | 18 张表、32 个分区检查通过 |

存储测试使用 Redis DB 15，网络测试使用 DB 14。索引专项仍为 100,002 条账本和 100,004 条 Outbox 数据，参数绑定和前后对照方式与下文一致，没有禁用顺序扫描：

| 查询 | 修改前 / 后共享页访问次数 | 修改前 / 后单次耗时 |
| --- | --- | --- |
| 恢复账本 | 3,126 / 4 | 7.323 / 0.013 ms |
| 恢复 Outbox | 4,167 / 4 | 8.557 / 0.013 ms |
| Outbox 全局统计 | 4,167 / 4 | 11.131 / 0.031 ms |

以上是限定数据分布的功能与查询计划验证，不是容量、写入吞吐或长稳结论。没有执行 Docker 故障矩阵或远端 CI。本次没有修改 Rust 代码，因此未重复运行上一轮已通过的纯代码/SDK 测试。

本地原始日志、执行脚本和查询结果保存在 `target/docker-index-validation/`。测试完成后通过该 Compose 项目的 `down` 停止并移除容器，保留数据卷和镜像。此前 Windows 环境进程已确认未运行；用户授权清理后，两次删除命令均被工具安全策略拒绝，未绕过限制，旧二进制、数据和日志仍保留在 `target/index-audit-runtime/`。

## 实现范围

- 迁移 `013_query_indexes.sql` 新增账本 `(operation_id, posting_id)`、Outbox `(operation_id, event_id)` 索引。历史已发布消息也必须能恢复回执，因此这两个索引不带过滤条件。
- 正常存储初始化在迁移事务提交前验证全部 18 张逻辑表的主键、13 项必需辅助/唯一索引及 32 个快照分区主键。检查字段顺序、唯一性、索引方法、排序、过滤条件、有效/就绪状态以及叶子索引与父索引的挂接关系。允许额外索引；主键按结构识别，辅助索引按迁移名称及结构识别。
- 迁移已登记也会重新检查真实结构；检查失败则停止初始化并指出对象。不会因为索引缺失而在业务运行中偷偷重建。新增只读命令 `check_schema_indexes`，读取系统目录，不读取业务表行。
- Outbox 全局统计分别读取“未发布且非死信”和“全部死信”集合，保留原来的计数含义，避免每轮统计已发布正常历史。仍然需要遍历活跃/死信集合，不能理解为任何积压规模都固定成本。
- `.github/workflows/ci.yml` 增加独立 PostgreSQL 索引检查作业，随 push/PR 执行；发布验收也依赖该作业。工作流已修改，本轮未推送，远端 CI 尚未执行。

## 早先 Windows 环境记录（不作为规定镜像验收）

该环境由助手在未取得环境变更授权时自行搭建，流程不符合随后明确的[测试环境与授权约定](../deploy/local/README.md#测试环境与授权约定)。保留真实结果以便追溯，不能用这些结果代替上面的 Docker 复验。

Windows 本机隔离 PostgreSQL 18.6，使用 EDB 的 Windows 二进制发行包；新建临时集群，`shared_buffers=128MB`、`max_connections=40`，仅监听回环地址。Redis 使用 redis-windows 项目的 8.8.1 MSYS2 便携版本，核对发行附件 SHA-256，开启 AOF/everysec。均未安装系统服务，也未连接业务数据库。Linux CI 使用仓库已有的 PostgreSQL 18.4 镜像。

索引专项测试由空库开始，插入 100,002 条账本记录和 100,004 条 Outbox 记录。其中目标操作各有两条记录；Outbox 有十万条已发布正常历史、两条活跃消息和两条死信。执行 ANALYZE 后，以实际参数绑定方式读取目标操作，并对生产统计 SQL 执行 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`。没有禁用顺序扫描。

同一数据集内，临时撤去新增索引并执行旧统计 SQL 取得对照，随后回滚索引变更：

| 查询 | 修改前共享页访问次数 | 修改后共享页访问次数 | 修改前 / 后单次耗时 |
| --- | ---: | ---: | ---: |
| 按操作恢复账本 | 3,126 | 4 | 8.406 / 0.015 ms |
| 按操作恢复 Outbox | 4,167 | 4 | 9.394 / 0.012 ms |
| Outbox 全局统计 | 4,167 | 4 | 143.338 / 0.048 ms |

两处恢复查询原先扫描整个表并过滤十万条无关记录；修复后用新增索引直接取得两条目标记录。统计使用既有部分索引，结果仍为待处理 1、处理中 1、死信 2。页访问次数按计划根节点的 Shared Hit Blocks + Shared Read Blocks 计算，包含缓存命中，不能当作磁盘实际读取量。耗时为单次本地样本，仅说明此次执行，不据此推算线上吞吐或加速倍数。

此数据下两个新增索引分别约 5,792 KiB、5,784 KiB。写入会增加索引维护成本，本轮没有测出可靠的写入吞吐差异。

| 验证 | 结果 |
| --- | --- |
| Rust 工作区普通测试 | 188 项通过；默认忽略的外部依赖测试另列如下 |
| 新增索引专项 | 3 项通过：空库/重复启动，删失或错误索引，失败并发建索引留下无效索引，分区脱离，以及十万条历史对照 |
| 真实 PG + Redis 存储测试 | 22 项通过，包含 CAS、幂等、事务、账本与 Outbox、可靠入队、重连及提交结果恢复 |
| 缓存修复租约与并发 | 8 项通过 |
| Rust 客户端 → TCP → DBProxy → PG/Redis | 2 项通过 |
| TypeScript SDK | 21 项通过；先通过 npm ci 安装锁定依赖 |
| 格式、Clippy、差异空白检查、actionlint 工作流检查 | 通过 |
| 只读巡检命令 | 在已初始化的独立数据库执行成功 |

专项测试与存储、修复、网络测试分别使用独立数据库，均完成初始化。索引检查不在数据库间共享“已验证”结果；这不等于已经完成多租户服务整体压力测试。原始执行日志保存在本次本地 `target/index-audit-runtime/`，属于忽略的临时产物；可用下列命令重建证据。

## 复现

普通检查：

```powershell
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
npm ci
npm run test:typescript
```

索引专项会修改索引并生成固定名称的测试数据，必须使用全新、可丢弃的测试库，串行运行。先创建测试库，再设置该库连接信息：

```powershell
$env:DBPROXY_TEST_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/index_regression"
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION = "1"
cargo test -p tiangz-dbproxy-storage --test schema_indexes --locked -- --ignored --nocapture --test-threads=1
```

只读检查已有库时，设置 `DBPROXY_POSTGRES_URL` 后执行以下命令，不需要允许迁移的测试开关。多租户独立数据库逐库执行：

```powershell
cargo run -p tiangz-dbproxy-storage --example check_schema_indexes --locked
```

## 后续边界

1. 普通回执 7 天清理：新增可靠时间字段及对应索引，先建好并验证，再启用分批清理；事务回执和业务事实不随之删除。迁移 013 已用于本轮索引，下一阶段另分配迁移版本。
2. 批量快照恢复、Outbox 领取、缓存修复领取需要覆盖大量活跃数据、重试和死信阻塞等不同分布。现有索引检查确认结构，不证明所有查询都高效。
3. 统计仍与队列共享维护连接，未合并来源统计，也未增加统计专用超时；大量活跃/死信仍可能产生负担。
4. 已完成上述规定镜像的 Docker 复验；尚未做云数据库、亿级数据、多租户整体容量或长时间故障演练。Windows Redis 便携版本的历史结果不作为规定镜像验收。
5. 迁移 013 沿用现有事务内建索引流程，会锁住相关表的写入。当前开发期采用此流程；已有大规模数据的部署需要单独安排建索引窗口，不能把只读巡检命令当成在线修复工具。
