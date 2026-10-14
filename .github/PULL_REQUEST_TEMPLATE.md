## 这个 PR 做了什么 / What

<!-- 一两句话：动机和改动。 -->

## 怎么验证的 / How it was verified

<!-- 写实际跑过的命令和结果。没跑的写“未验证”和原因。 -->

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --workspace --locked`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] `npm run test:typescript`（改了 TypeScript SDK 或协议时）
- [ ] 真实 PostgreSQL/Redis 的 `--ignored` 用例（改了存储或持久化语义时，命令见 CI 配置）

## 清单 / Checklist

- [ ] 一个 PR 只做一件事
- [ ] 改了数据库结构时新增了迁移，没有修改已发布的迁移
- [ ] 改了协议时同步了 Rust 与 TypeScript SDK，并更新协议锁
- [ ] 文档已同步；用户可见的变化写进了发布说明
- [ ] 涉及协议、存储结构或幂等/回执语义的改动已事先在 Issue 中讨论

## 未做与风险 / Not done and risks

<!-- 没有就写“无”。 -->
