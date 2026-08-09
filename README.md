# TiangZ DBProxy

TiangZ DBProxy 是独立的 Rust 持久化服务项目。

它负责通用的：

- 玩家、Item、任务等快照的读取与写入
- Revision 和 Compare-And-Swap 版本校验
- 重试幂等与请求去重
- Redis 缓存、PostgreSQL/MySQL/MongoDB 等存储适配
- 监控、故障恢复和部署协议

DBProxy 不依赖 TiangZ Runtime，也不包含任何游戏玩法。TiangZ 只向它提供稳定的快照 Payload、Schema 和 Repository 适配逻辑。

## 当前状态

`v0.1.0` 只冻结第一版核心语义：

- `RecordKey`：`namespace + key`
- `Revision`：由 DBProxy 生成的单调版本号
- `SnapshotWrite`：带 `expected_revision` 的条件写入
- `request_id`：重试时必须保持不变的幂等键
- `InMemorySnapshotStore`：只用于测试，不保证重启恢复

Redis、真实数据库和网络服务尚未加入，避免在核心语义未验证前绑定具体数据库方案。

## 开发

```powershell
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked
```

## 设计原则

1. DBProxy 只理解记录地址、Schema、Revision 和二进制 Payload，不理解游戏业务字段。
2. 快照写入必须支持重试，重试不能导致重复扣物品、重复发奖励或重复保存。
3. Redis 不是最终一致性的替代品。缓存和持久库的责任、故障恢复顺序必须由适配器明确实现。
4. 事务、多记录写入和事件 Outbox 不放进第一版 Snapshot API，等单记录语义稳定后再扩展。
5. TiangZ 的主工程不直接依赖 DBProxy 的内部模块，只依赖版本化协议或客户端 SDK。

## 许可证

Apache-2.0，Copyright 2025-2026 郑昕。
