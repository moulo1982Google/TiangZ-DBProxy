# 参与贡献 / Contributing

感谢你愿意改进 TiangZ DBProxy。这份文件只讲提交改动前最需要知道的事，更多背景在 [README](README.md) 和 [docs/](docs/)。

## 先谈再写

- 小修复（错别字、明显的缺陷、补测试）可以直接提 PR。
- 改协议、改存储结构（数据库迁移）、改幂等/回执/版本校验语义：请先开 Issue 说明动机和方案。这些改动会影响已经在运行的数据，兼容与回滚路径需要先谈清楚。

## 本地准备

需要稳定版 Rust 工具链和 Node.js（构建 TypeScript SDK）。提交前至少跑：

```powershell
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
npm ci
npm run test:typescript
```

默认的 `cargo test` 不需要数据库。涉及存储或持久化语义的改动，还应运行需要真实 PostgreSQL/Redis 的 `--ignored` 用例，命令和服务配置见 [.github/workflows/ci.yml](.github/workflows/ci.yml) 与 `deploy/local/`。

## 提交规则

- 一个 PR 做一件事，说明里写清动机、做了什么、怎么验证的、哪些没有验证。
- 数据库结构的改动用**新增迁移**，不要修改已发布的迁移。
- 改了协议要同步 Rust 与 TypeScript SDK，并更新协议锁（`npm run codegen:typescript`）。
- 行为变化要同步更新文档，用户可见的变化写进发布说明。
- 不要提交 `target/`、`node_modules/` 等构建产物，也不要提交真实的账号数据或凭据。

## 报告问题

- 缺陷和功能建议：用 Issue 模板。
- 安全问题：不要开公开 Issue，见 [SECURITY.md](SECURITY.md)。

参与本项目即表示你同意遵守[行为准则](CODE_OF_CONDUCT.md)，并同意你的贡献按本仓库的 [Apache-2.0 许可证](LICENSE)授权。
