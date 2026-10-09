# PostgreSQL 18.4 → 18.6 升级验证（2026-09-22）

用户明确授权后，已在本机 Docker 完成旧测试库原地升级和新库回归。DP 当前配置统一使用 `postgres:18.6-bookworm`；Redis 保持 `redis:8.8.1-trixie`。没有连接或升级远程业务环境。

## 环境和升级过程

使用本地 Compose 加 laptop 配置，项目名 `tiangz-dbproxy-index-validation`；Redis 设置 `DBPROXY_REDIS_APPENDONLY=yes`。PostgreSQL 容器限额 1 GiB、shared_buffers 128 MB、max_connections 30、JIT 关闭，与上一轮 18.4 验证一致。测试客户端与 DBProxy 测试程序在 Windows 主机运行，数据库在 Linux 容器运行。

1. 重新启动上一轮保留的 PostgreSQL 18.4 / Redis 8.8.1 测试数据卷。
2. 在没有业务 worker 的情况下执行 `pg_dumpall -U tiangz -f /tmp/pg184-before-upgrade.sql`，复制到本机保存。备份大小 81,557,147 字节，SHA-256 为 `AC9EADCD88B7E2F5BCCA18C7391DE3E849D6346799600836D0830C36DEC7D2F3`。本轮没有执行备份恢复演练。
3. 对原有四个测试库逐表记录行数及内容校验值。每库 50 个表项，包含 18 张逻辑表及 32 个快照分区；按每行 JSON 内容的 MD5 排序再汇总校验，避免物理行顺序影响对照。
4. 临时 Compose 覆盖镜像为 18.6，正常重建 PostgreSQL 容器并挂接同一数据卷；未清库、未重新导入数据、未运行 `pg_upgrade`。
5. 确认实际运行 `PostgreSQL 18.6 (Debian 18.6-1.pgdg12+2)`，再次计算四库全部表的行数和内容校验值，全部一致。
6. 在升级后的旧存储测试库运行 DP 快照及事务测试，随后创建四个全新测试库，重跑 35 项测试。

镜像摘要：

- PostgreSQL：`sha256:3725f4e2499eef5134592b3b4ab79a543ed7f8e533b05b5b637af926630f6650`
- Redis：`sha256:3eafabb4c93fcb8b36b666e07a43f096cb157bc6b07dce4b2492b895c63cf37f`

18.x 小版本升级不要求导出再导入整个数据库，但部分扩展、配置及历史数据可能需要额外处理，依据[PostgreSQL 18.6 官方说明](https://www.postgresql.org/docs/release/18.6/)。本次 DP 迁移未使用其中需要专项处理的 GIN、btree_gist、ltree 索引；实际检查的旧索引测试库只有默认 plpgsql 扩展。

## 验证结果

| 范围 | 数据库 | 结果 |
| --- | --- | --- |
| 旧数据校验 | `index_regression_docker`、`storage_regression_docker`、`repair_regression_docker`、`network_regression_docker` | 四库逐表行数及内容一致 |
| 升级后 DP 快照/事务读写、重复请求和版本冲突 | `storage_regression_docker` | 2 项通过 |
| 新库全库索引、错误索引检测及十万条历史 | `index_regression_pg186` | 3 项通过 |
| 新库 PG + Redis 存储 | `storage_regression_pg186` | 22 项通过 |
| 缓存修复并发 | `repair_regression_pg186` | 8 项通过 |
| 客户端 → TCP → DBProxy → PG/Redis | `network_regression_pg186` | 2 项通过 |
| 只读索引巡检 | `index_regression_pg186` | 18 张表、32 个分区通过 |

共 37 项真实依赖测试通过。新库存储测试使用 Redis DB 13，网络测试使用 DB 12；旧库验证使用原 DB 15。

18.6 中的十万条历史样本继续显示两项新索引及统计改写有效：

| 查询 | 修复前 / 后共享页访问次数 | 修复前 / 后单次耗时 |
| --- | --- | --- |
| 按操作恢复账本 | 3,126 / 4 | 7.478 / 0.014 ms |
| 按操作恢复 Outbox | 4,167 / 4 | 8.661 / 0.014 ms |
| Outbox 全局统计 | 4,167 / 4 | 11.043 / 0.035 ms |

这里的“修复前/后”指同一个 18.6 实例中的索引与 SQL 对照，不是 18.4 与 18.6 的性能对照。单次耗时不能用于宣称版本升级的性能收益。

## 配置同步与边界

已同步本机部署、external 演练 Compose、CI 索引作业、三个直接启动 PG 的验证脚本和当前开发手册。已有历史报告里的 18.4 保留原值，不改写已发生的测试环境。Redis 版本和运行参数未变。

完成 Compose 配置解析、JavaScript 语法检查、CI 工作流静态检查及差异空白检查。没有修改 Rust 或协议代码，没有运行 codegen，也没有重复上一轮纯代码/SDK 测试。没有运行七日演练、Docker 故障矩阵或远端 CI，不能将本轮结果扩大为这些项目已通过。

本地证据保存在 `target/pg186-validation/`：备份 SQL、升级前后逐表校验、升级读写日志、四组回归日志、查询计划和执行脚本。测试完成后停止并移除本次容器，保留原数据卷（现在为 18.6 使用的测试卷）、镜像和备份。之前 18.4 的验证日志仍在 `target/docker-index-validation/`。
