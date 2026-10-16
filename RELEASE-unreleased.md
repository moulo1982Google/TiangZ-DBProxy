# 未发布 / Unreleased

本文件是下一版发布说明的草稿。发版时并入 `RELEASE-v<版本>.md` 后删除本文件。

## 内容

### 崩溃诊断（新配置，默认关闭）

- **新配置** `diagnostics.crash`：`enabled`（默认 `false`）、`directory`（默认 `logs/crash`，相对配置文件所在目录）、`recentLogLines`（默认 200，0–2000）、`miniDump`（仅 Windows，默认 `false`）、`coreDump`（仅 Linux，默认 `false`）、`maxReports`（默认 50）、`maxDumps`（默认 5）。`configs/dbproxy.schema.json` 已同步。
- **环境变量** `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS=on|off` 覆盖 `enabled`，优先于配置文件；取值错误启动失败。
- **panic 报告**：`crash-dbproxy-<pid>-<UTC 时间>.txt`，含版本、pid、线程、消息、位置、强制采集的回溯和最近日志。写完再调用原钩子，退出码与 Tokio 任务内 panic 的处理不变。
- **限流与补写**：可存活的 panic 每 10 秒最多写一份报告；每次 panic 都廉价采集（未符号化回溯），限流只拦符号化与写盘。被限流的只计数（下一份报告写 `suppressed_since_last: N`），最后一次存进只保留一份的缓存槽。致命 panic（主线程，或消息以 `panic in a function that cannot unwind` / `panic in a destructor during cleanup` 开头）不受限流，并先把缓存的那次补写成报告（多出可选键 `deferred: true`、`deferred_reason`）。报告的 `thread` 改为 `<线程名> (ThreadId(n))`，与 TiangZ 一致。
- **文件权限**：Unix 上新建的崩溃目录 `0700`、文件 `0600`；Windows 继承目录 ACL。`.gitignore` 忽略 `/configs/logs/`（默认目录相对配置文件解析会落在这里）。
- **最近日志**：启用时标准输出的日志同时按行进入有界环形缓冲（≤ 2000 行 × 4 KiB）；关闭时日志初始化与以前相同。
- **Windows**：未处理异常（含栈溢出，已用自测验证）写文本记录（异常码、地址、访问冲突详情），`miniDump` 开时另写 `.dmp`；进程仍以原异常码结束。`abort()` 无法捕获，见文档。
- **Linux**：`coreDump` 开时启动期放开 `RLIMIT_CORE`；core 位置由宿主机 `core_pattern` 决定，Docker 需 `--ulimit core=-1`。
- **保留上限**：只保留最新的 `maxReports` 个报告、`maxDumps` 个转储，只处理 `crash-dbproxy-` 开头的文件；启动时扫描一次，之后按内存计数超限才扫描。上限按目录计，多实例请各用一个目录。
- **多租户**：各租户解析后的 `diagnostics.crash` 必须一致。
- **自测**：`TIANGZ_DBPROXY_CRASH_SELF_TEST=panic|thread-panic|panic-after-thread-panic|abort|access-violation|stack-overflow`，仅在启用且为内存后端时于启动完成后主动崩溃，只用于验证；其他后端设置了该变量直接启动失败（错误退出，不崩溃）。
- 依赖：`tiangz-dbproxy-server` 直接使用依赖树中已有的 `libc`（Unix）与 `windows-sys 0.61`（Windows，新开 `Win32_System_Diagnostics_Debug` 等特性），`Cargo.lock` 未引入新包。
- 详见 `docs/crash-diagnostics.md`。

## 兼容与升级

- 不改协议、存储结构、迁移，客户端无需升级。
- 未配置 `diagnostics` 且未设置环境变量时行为与上一版相同。
- `.dmp` 与 core 含进程内存（可能包括令牌、连接串、玩家数据），启用前规划好目录权限。

## 验证（2026-10-16，Windows 本机）

- `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`cargo test --workspace --locked` 通过；默认测试不连接任何数据库。
- `cargo clippy -p tiangz-dbproxy-server --all-targets --locked --target x86_64-unknown-linux-gnu -- -D warnings` 通过（只做类型与 lint 检查，未在 Linux 上运行）。
- 新增单元测试：Windows 上执行 27 项（名字含 crash 的库测试，另有 1 项 Unix 权限测试只在 Unix 执行）；集成测试 `tests/crash_diagnostics.rs` Windows 上执行 8 项（含访问冲突与栈溢出转储、`thread-panic` 限流且继续服务、致命 panic 补写被限流的那次、非内存后端拒绝自测），Unix 专用 1 项未在本机执行；全部通过。全工作区 `cargo test --workspace --locked`：292 项通过、0 失败、115 项按原标记忽略（需真实 PG/Redis）。手动触发 panic 与访问冲突自测，核对了报告正文、回溯帧、`.dmp`（约 355 KB，`MDMP` 文件头）与退出码。
- 未验证：Linux 上的 `abort` 自测、core 上限调整、`stack-overflow` 行为与 Unix 文件权限测试（只在 CI 运行）；用 WinDbg 实际打开 `.dmp`；release 构建的回溯与 `.pdb`；TypeScript SDK 未改动，未重跑。
