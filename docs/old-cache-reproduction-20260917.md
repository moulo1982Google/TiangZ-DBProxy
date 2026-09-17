# 缓存写超时旧读：本机复现与代码分析

2026-09-17，经用户授权在本机独立PG/Redis执行。计划30分钟，用户确认已复现后要求提前停止。没有修改服务实现、没有部署外网。

## 实际结果

- 负载开始：北京时间11:46:21；最后一轮11:53:59；11:54:11完成停止。
- 16轮全部命中 `successful ACK followed by stale cache read`：返回读版本1，已确认写版本2。
- 停止前直接SQL检查：16条测试记录均为版本2。此实验没有发现这些写入丢失。
- 本地提交 `0a45f12f2d1fd8af6cdc353812764ab47f1fb581` 加测试改动；核心storage/lib.rs与外网e656529版本无差异。
- 二进制SHA-256：`727a9c17cd96d0a5d40667478fcba885ab439ab575a0c4234bfc611e123b1745`。
- 证据：`target/old-cache-repro-XEmCXD/report.json`、`round-1.log`至`round-16.log`、`changes.patch`、`controller.mjs`。target未纳入Git，迁机需另行保留原始证据。
- 独立容器 `old-cache-repro-xemcxd-pg`、`old-cache-repro-xemcxd-redis` 已停止，没有删除数据；Redis退出0，PG停止后退出137，因此不能称PG优雅关闭验收通过。既有SLG和kind未操作。

## 已证实的复现路径

1. seed版本1，并通过独立reader读到它，确认缓存路径可用。
2. CLIENT PAUSE WRITE使Redis写暂停10秒，读仍可用。writer缓存操作预算40ms。
3. writer.apply提交PG版本2，缓存更新超时，但仍返回Applied成功。
4. reader在ACK之后发起load，命中版本1。
5. UNPAUSE先恢复依赖，然后断言失败；下一轮使用独立键。

这是实际存储层、真实PG/Redis的受控复现，不是TCP服务器、SDK故障切换或七日500玩家重跑；本机40ms预算和外网200ms不相同。已确认该代码路径可造成同类旧读，外网五次响应仍没有逐请求读来源追踪，不能宣称逐条完整还原。

## 代码原因与修复约束

`crates/dbproxy-storage/src/lib.rs`：

- `AsyncTransactionalStore::apply`先PG提交，再调用`synchronize_committed_cache`，最后返回原提交结果。
- 缓存同步失败只记录日志并保留durable repair，函数不向上返回错误。
- `load`对Fresh直接返回，对Stale也返回并异步刷新。Fresh标记只说明缓存时间策略，不证明与PG的当前revision一致。
- Redis revision-aware Lua防止低版本覆盖已经存在的高版本，但新版本未成功写入Redis时，Redis不知道版本2，无法阻止返回版本1。
- 批量保存、多记录事务共享best-effort缓存同步路径，也要纳入修复审查；不能只改当前单记录用例。
- 当前LoadSnapshotRequest只有record，没有客户端确认版本下限；读端不能从请求获知至少应读到哪个版本。

不能简单把缓存超时改成普通“写失败”：PG已经提交，调用方必须保留原操作ID处理结果未知，不能重新扣费。删除缓存也可能失败，且无法覆盖写入/删除之间的并发窗口。进程本地dirty标记无法覆盖另一个DBProxy节点或重启。重试和缩短修复周期只改变概率。

可评审的修复方向：

1. 若维持现有LoadSnapshot默认严格的读取承诺，权威PG读取是复杂度最低的正确性基线（包括批量、负缓存路径），代价是PG负载增加；保留缓存快路径则需要跨节点一致的版本验证机制。
2. 请求携带最低revision可支持已知版本的read-your-writes，但会修改协议/SDK，且不能解决新客户端没有下限的严格读取；不能作为现有契约的静默替换。

当前仅分析，不选择并实施新的读取语义。下一步修复应同时验证正常缓存、缓存写失败、跨节点、重启、单条/批量、提交成功丢ACK与幂等重试，并与正常读性能基线比较。

## 复测命令

已授权隔离环境下，复用 `tools/reproduce_old_cache.mjs` 会新建独立容器，默认跑30分钟。它目前没有优雅停止入口，本轮按用户要求核对PID后停止控制器、UNPAUSE并停止精确命名的容器；不要直接杀未知PID或清理整个Docker环境。

短回归使用 `cargo test -p tiangz-dbproxy-storage --test postgres_redis acknowledged_write_timeout_must_not_expose_old_cache -- --exact --ignored --nocapture --test-threads=1`，必须指向独立测试PG/Redis并禁止业务worker竞争。
