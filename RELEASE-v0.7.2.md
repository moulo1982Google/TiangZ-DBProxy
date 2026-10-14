# v0.7.2

TiangZ 套件 0.7.4 中的 DBProxy（GitHub Release，不上架 npm/crates.io）。之前的标签与附件保持不变。

## 内容

相对 0.7.1 **没有运行时改动**：协议、Rust client/core、TypeScript SDK、配置字段与数据库迁移都不变，升级不会执行新迁移，服务与客户端可以按原来的方式混用 0.7.1 与 0.7.2。版本号升到 0.7.2 是为了让套件内各仓库的版本与 TiangZ 0.7.4 的发布线对齐。

- **代码覆盖率上传 Codecov。** 新增 `coverage` 工作流：Rust 用 `cargo llvm-cov` 统计整个工作区的默认测试（不含需要真实 PostgreSQL/Redis 的 `#[ignore]` 用例，那部分仍由 `Rust CI` 的专项作业和每晚验收运行），TypeScript SDK 用 Node 自带的测试覆盖率并通过 source map 映射回 `sdk/typescript/src`；两份报告分别带 `rust`、`typescript` 标记上传，仓库密钥为 `CODECOV_TOKEN`。`codecov.yml` 先把项目与补丁检查设为只提示，不拦合并。
- **README。** 顶部“本轮发布”改为 0.7.2，版本说明同步；徽章（Rust CI、nightly acceptance、security、codecov、tag、license）从第 47 行移到标题下。

## 套件内各仓库标签

| 仓库 | 标签 | 版本 | 相对 0.7.1 套件 |
|---|---|---|---|
| TiangZ-DBProxy | `v0.7.2` | 0.7.2 | 上述改动，无运行时变化 |
| TiangZ | `v0.7.4` | 0.7.4 | 见 TiangZ 的 `RELEASE-v0.7.4.md`；对 DBProxy 的依赖仍固定为 `v0.7.0`（协议与 SDK 自 0.7.0 未变） |

## 验证

- 本地：`npm run test:typescript` 通过；Cargo.lock 只变更六个工作区包的版本号。
- GitHub CI 结果与覆盖率数字见发布提交与 Release 页面。
- 未在本次执行：真实 PostgreSQL/Redis 长稳、性能对比（本次没有运行时改动）。
