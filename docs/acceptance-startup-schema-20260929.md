# A01/A03：真实进程启动与结构损坏矩阵

## A01 空库与重复启动

入口：`fault_process::a01_empty_database_and_restarts_preserve_schema_and_business`。使用全新数据库 `schema_20260929a_a01`，先验证 public 中没有对象，然后由正式 release 服务创建结构，连续启动三次。

结果通过（1.70 秒）：

- 每次确认 15 条迁移记录、18 个根表主键、32 个快照分区，并调用生产结构校验覆盖必需辅助索引及分区索引。
- 三次比较完整迁移记录（含应用时间）和所有 public 索引的 OID、名字、定义，完全不变，没有重新迁移或重建索引。
- 真实 SDK 首次写入 Applied，两次重启后同号重放均 Duplicate，业务版本始终 1，读取内容一致。

这里用测试进程结束后再启动来验证持久状态；不据此通过 A14 正常停机各阶段。

## A03 结构损坏后必须拒绝服务

入口：`fault_process::a03_schema_damage_refuses_clients_and_recovers_after_repair`。每种损坏使用独立新库，正式进程建库并写入业务后才停止和破坏结构。再次启动必须非零退出、不得打开业务端口，日志必须定位到对应对象或明确的分区布局错误。随后按原定义修复，正式进程必须恢复，原请求重放 Duplicate、业务内容不变。

六类：普通回执主键缺失、快照主键列顺序错误、回执保留索引缺失、缓存修复索引条件错误、Outbox 辅助索引列顺序错误、快照分区脱离父表。

### 首轮失败保留

`schema_20260929a` 的 A03 前五类已完成拒绝和修复；第六类被更早的分区布局检查正确拒绝，输出 `InvalidSnapshotPartitionLayout("expected 32 canonical hash partitions, found 31")`。测试错误地要求输出分区名字，因而失败，整项没有通过。

只修测试预期：第六类严格检查上述 32/31 布局错误，仍要求非零退出、端口拒绝和修复后业务一致。不更改生产启动检查，不允许空错误或正常启动冒充通过。失败库/日志保留。

`schema_20260929b` 使用六个新库完整重跑通过（5.95 秒）：每类都非零退出、拒绝业务连接，修复后均恢复并重放 Duplicate。A03 在上述六类损坏矩阵内通过；未枚举每一个索引的全部可能损坏方式。

## 环境与证据

- 220 的隔离测试 PG/Redis；工作台 4 CPU、16 GiB、固定测试 cpuset，未停止数据库服务。
- 二进制来自 `dbproxy-workbench:f06-20260929`，测试从本轮挂载源码重新编译；不做旧库升级。
- 服务端原始日志、配置、各轮目录、迁移和索引快照在服务器 `/data/dbproxy-test/evidence/fault_process_schema_20260929a/`，复测用相邻 `...schema_20260929b/`。
- 本地 `cargo test --locked -p tiangz-dbproxy-server --test fault_process --no-run` 与 Clippy 通过。
