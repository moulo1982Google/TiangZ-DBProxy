# v0.7.3

DBProxy 单独发布（GitHub Release，不上架 npm/crates.io）。之前的标签与附件保持不变。TiangZ 套件其他仓库本次不变；TiangZ 对 DBProxy 的依赖仍固定在原版本，见下文“兼容”。

## 内容

### 多记录事务回执退休（有运行时改动、新增迁移 016）

此前，多记录事务回执（`ApplyMultiTransaction`，以及不带效果的 `CommitRecords`）只增不减。按游戏开发库实测，每个操作长期占约 2.5–3 KB。

- **新配置** `storage.operationReceiptRetentionDays`：默认 30 天，取值 1–3650 天。按服务端认领时间计算，不使用客户端填的时间。
- **到期处理**：删除事务头和回执记录；操作号留一个约 175 字节的“已退休”标记。
- **新错误码** `OPERATION_EXPIRED`（2007）：退休后再查询，或用这个操作号提交（原字节重提或内容不同），都返回它。**不会返回“查无回执”，也不会重新执行。** 从未用过的操作号仍然正常查无。
- **不退休的情况**：
  - 带追加记录或 Outbox 事件的操作，因为它们是不可变的业务事实；
  - 单记录事务和交易回执。
- **后台任务**：每个租户一个，和普通回执清理共用维护连接。每批最多 500 个；满批后停 1 秒，否则停 60 秒。
- **新指标**：`dbproxy_operation_retirement_batches_total`、`_retired_total`、`_failures_total`。
- **并发**：提交时对操作号加 `FOR SHARE` 锁，退休时用 `FOR UPDATE SKIP LOCKED`，正在重提的操作号会被跳过。重提和退休之间顺序总是确定的。
- **迁移 016**：给 `dbproxy_operation_claims` 加 `retired_at` 列，并加一个部分索引；启动时的索引检查也加上了它。
- 设计、上线规则与磁盘估算见 `docs/record-deletion-and-receipt-retention.md` 附录二；负载对照见 `docs/acceptance-operation-retirement-20261015.md`。

### 协议与 SDK

- proto 新增 `ERROR_CODE_OPERATION_EXPIRED = 2007`，指纹随之改变。
- 服务端继续接受上一版指纹（`PRE_OPERATION_EXPIRED_PROTOCOL_FINGERPRINT_V2`）和更早的已知指纹，旧客户端可以直接连接。
- TypeScript SDK 新增 `DbProxyErrorCode.OperationExpired`，`protocol-lock.ts` 已重新生成。

### 测试与覆盖率

- 验收工具：
  - `acceptance_load` 新增 `ACCEPT_WRITE_KIND=commit`，写入改为两条记录的事务，并核对两条记录是否一致；
  - `acceptance_host` 新增 `ACCEPT_RETIREMENT`；
  - `run_receipt_probe.sh` 新增 `--write-kind`、`--retirement`、`--fixture-ops-*`；
  - 新增 `run_retire_ab.sh`。
- `coverage` 工作流：
  - Rust 部分会把 CI 原本就在真实 PostgreSQL/Redis 上跑的回归一起统计，每组用新库，和默认测试合并成一份报告；
  - TypeScript SDK 改为每个测试文件单独出报告。原先 Node 合并多个测试进程时会丢掉大量已执行的行，把约 90% 报成了 43%；
  - `codecov.yml` 不再把 `src/bin` 下的压测、浸泡、探针工具和示例程序算作服务代码。
- `Rust CI` 的索引回归作业加入回执退休回归。

## 兼容与升级

- **旧客户端**（包括 TiangZ 宿主内置的 DBProxy 客户端）不认识 2007，会解成 `INTERNAL`，也就是结果未知。只要调用方把 `INTERNAL` 当结果未知处理，就不会重复执行，业务代码不用改。新客户端可以单独识别 2007。
- **连同一个数据库的所有 DBProxy 实例必须一起升级**：先停旧的，再起新的。旧版本看不懂退休标记，查不到事务头时会回答“查无回执”。
- **退休执行过以后不能回滚到旧版本。** 回滚前必须确认 `SELECT count(*) FROM dbproxy_operation_claims WHERE retired_at IS NOT NULL` 为 0。
- **从旧版本升级后**，已经超过保留期的回执会在启动后分批退休，速度约每秒 500 个，期间 WAL 和自动清理的压力会增加。
- **保留期必须长于调用方未决操作能存活的最长时间。** 超过保留期还没处理完的操作，以后只能得到 `OPERATION_EXPIRED`。

## 验证

- 本地：
  - `cargo fmt --check`、`cargo clippy --workspace --all-targets -D warnings`、`cargo test --workspace` 均通过；
  - TypeScript SDK 29/29 通过。
- 真实库（一次性 PG）：
  - 回执退休 3 项：边界、过期应答、带效果的操作不退休、批次上限、并发重提时跳过；
  - 索引检查 3 项；
  - 普通回执回归 7 项。
- 负载对照（220 服务器容器，每秒 200 请求，写入全部为两条记录的事务，25 万个回执到期，回收关、开交替各 2 轮）：
  - 读 P99 −0.4%，写 P99 −0.1%；
  - 零错误、零漏发，14.4 万次事务写入核对一致；
  - 回收约每秒 537 个；
  - 有一轮“开”出现 4 次 28–39 ms 的慢提交，当时检查点、自动清理和回收产生的 WAL 碰在一起，报告里如实保留。
- 标签 CI 与 Codecov 的数字见 Release 页面。
- 本次未执行：长稳测试、接近饱和的压测、多实例同时退休。
