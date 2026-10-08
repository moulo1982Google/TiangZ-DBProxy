# v0.7.0-rc2

六仓库统一套件标签 v0.7.0-rc2 的预发行版本（GitHub prerelease，不上架 npm/crates.io）。rc1 标签 `v0.7.0-rc1`（8d0f343）与其附件保持不变。

本仓库相对 v0.7.0-rc1 **没有代码或协议改动**：workspace 与五个 crate、`@tiangz/dbproxy-sdk` 版本由 0.7.0-rc1 改为 0.7.0-rc2，以与套件版本一致；`PROTOCOL_VERSION` 与 `dbproxy.proto` 不变，rc1 与 rc2 的客户端、服务端可以互通。

套件内本轮实际改动在 TiangZ（Scene HTTP、`outerIp` 允许域名、异步结果唤醒）与 Developer Tools（HTTP Handler 热更规则），详见各自仓库的 RELEASE-v0.7.0-rc2.md。TiangZ 与 Examples 的 DBProxy 依赖改为本仓库 `v0.7.0-rc2` 标签。

验证：本仓库 PR 的 CI（Rust workspace、TypeScript SDK、安全检查）在合入前全部通过；发布后的独立消费者与长稳验收按需另行安排，不由本说明代替。
