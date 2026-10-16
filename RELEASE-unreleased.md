# 未发布 / Unreleased

本文件是下一版发布说明的草稿。发版时并入 `RELEASE-v<版本>.md` 后删除本文件。

## 内容

### 崩溃诊断（新配置，默认关闭）

- **新配置** `diagnostics.crash`：`enabled`（默认 `false`）、`directory`（默认 `logs/crash`，相对配置文件所在目录）、`recentLogLines`（默认 200，0–2000）、`miniDump`（仅 Windows，默认 `false`）、`coreDump`（仅 Linux，默认 `false`）、`maxReports`（默认 50）、`maxDumps`（默认 5）。`configs/dbproxy.schema.json` 已同步。
- **环境变量** `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS=on|off` 覆盖 `enabled`，优先于配置文件；取值错误启动失败。
- **panic 报告**：`crash-dbproxy-<pid>-<UTC 时间>.txt`，含版本、pid、线程、消息、位置、强制采集的回溯和最近日志。写完再调用原钩子，退出码与 Tokio 任务内 panic 的处理不变。
- **最近日志**：启用时标准输出的日志同时按行进入有界环形缓冲（≤ 2000 行 × 4 KiB）；关闭时日志初始化与以前相同。
- **Windows**：未处理异常写文本记录（异常码、地址、访问冲突详情），`miniDump` 开时另写 `.dmp`；进程仍以原异常码结束。`abort()` 无法捕获，见文档。
- **Linux**：`coreDump` 开时启动期放开 `RLIMIT_CORE`；core 位置由宿主机 `core_pattern` 决定，Docker 需 `--ulimit core=-1`。
- **保留上限**：启动时与每次写入后只保留最新的 `maxReports` 个报告、`maxDumps` 个转储，只处理 `crash-dbproxy-` 开头的文件。
- **多租户**：各租户解析后的 `diagnostics.crash` 必须一致。
- **自测**：`TIANGZ_DBPROXY_CRASH_SELF_TEST=panic|abort|access-violation`，仅在启用时于启动完成后主动崩溃，只用于验证。
- 依赖：`tiangz-dbproxy-server` 直接使用依赖树中已有的 `libc`（Unix）与 `windows-sys 0.61`（Windows，新开 `Win32_System_Diagnostics_Debug` 等特性），`Cargo.lock` 未引入新包。
- 详见 `docs/crash-diagnostics.md`。

## 兼容与升级

- 不改协议、存储结构、迁移，客户端无需升级。
- 未配置 `diagnostics` 且未设置环境变量时行为与上一版相同。
- `.dmp` 与 core 含进程内存（可能包括令牌、连接串、玩家数据），启用前规划好目录权限。

## 验证（2026-10-16，Windows 本机）

- `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`cargo test --workspace --locked` 通过；默认测试不连接任何数据库。
- `cargo clippy -p tiangz-dbproxy-server --all-targets --locked --target x86_64-unknown-linux-gnu -- -D warnings` 通过（只做类型与 lint 检查，未在 Linux 上运行）。
- 新增单元测试 19 项（服务端库单元测试合计 80 项）、集成测试 4 项（Windows 上 4 项全部执行，含访问冲突转储；Unix 专用 1 项未在本机执行）通过；手动触发 panic 与访问冲突自测，核对了报告正文、回溯帧、`.dmp`（约 355 KB，`MDMP` 文件头）与退出码。
- 未验证：Linux 上的 `abort` 自测与 core 上限调整（只在 CI 运行）；用 WinDbg 实际打开 `.dmp`；release 构建的回溯与 `.pdb`；TypeScript SDK 未改动，未重跑。
