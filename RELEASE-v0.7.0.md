# v0.7.0

TiangZ 六仓库套件 0.7.0 正式版（GitHub Release，不上架 npm/crates.io）。之前的候选标签 `v0.7.0-rc1`、`v0.7.0-rc2` 与附件保持不变。

本仓库相对 v0.7.0-rc2 **没有代码或协议改动**：workspace 与五个 crate、`@tiangz/dbproxy-sdk` 版本定为 0.7.0；`PROTOCOL_VERSION` 与 `dbproxy.proto` 不变，与 rc1/rc2 的客户端、服务端可以互通。

套件内 0.7 的实际改动在 TiangZ（Scene HTTP、`outerIp` 允许域名、异步结果唤醒）与 Developer Tools（HTTP Handler 热更规则）。TiangZ 与 Examples 的 DBProxy 依赖改为本仓库 `v0.7.0` 标签。

工具仓库已有同名旧标签，所以套件内各仓库标签为：TiangZ、DBProxy、Examples、AI-Plugins 用 `v0.7.0`；Developer Tools 用 `v0.16.1`；Native Language 用 `v0.17.1`。

验证：本仓库 PR 的 CI（Rust workspace、TypeScript SDK、安全检查）在合入前全部通过；`v0.7.0-rc2` 标签触发的完整验收（真实 PostgreSQL/Redis 与故障矩阵）通过，本次源码只有版本号不同。以后的缺陷按小版本（0.7.x）修补。
