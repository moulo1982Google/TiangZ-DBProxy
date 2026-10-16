# 崩溃诊断 / Crash diagnostics

2026-10-16 新增，未发布。目的是让 DBProxy 进程崩溃后留下可查、可分析的材料，作用类似 C/C++ 的 core dump。**默认关闭**，不改配置时行为与以前完全相同。

TiangZ 框架在另一个仓库里实现同一套能力，两边代码各自独立，对外约定保持一致：配置字段名、文件命名规则和报告格式见下文“对外约定”。

## 打开方式

在 DBProxy 配置文件里加 `diagnostics.crash`：

```json
{
  "diagnostics": {
    "crash": {
      "enabled": true,
      "directory": "logs/crash",
      "recentLogLines": 200,
      "miniDump": false,
      "coreDump": false,
      "maxReports": 50,
      "maxDumps": 5
    }
  }
}
```

| 字段 | 默认 | 取值 | 作用 |
|---|---|---|---|
| `enabled` | `false` | 布尔 | 总开关。关闭时不装钩子、不建目录、日志路径与以前相同。 |
| `directory` | `logs/crash` | 非空，前后不能有空白 | 报告与转储目录。**相对路径以配置文件所在目录为基准**（与 `--tenants` 清单里 `config` 的相对路径规则相同），不是进程当前目录。启用时启动阶段自动创建，建不出来进程不启动。 |
| `recentLogLines` | 200 | 0–2000 | 报告里附带的最近日志行数；0 表示不保留最近日志，但仍写报告。 |
| `miniDump` | `false` | 布尔 | 仅 Windows：未处理异常时写 `.dmp` 小型转储。Linux 上忽略并记一条 info。 |
| `coreDump` | `false` | 布尔 | 仅 Linux/Unix：启动时把 core 文件大小上限（`RLIMIT_CORE`）放开。Windows 上忽略并记一条 info。 |
| `maxReports` | 50 | 1–10000 | 文本报告（panic 报告与原生异常记录）最多保留几个。 |
| `maxDumps` | 5 | 1–1000 | `.dmp` 最多保留几个。 |

未知字段、越界取值在建立任何连接、读取任何密钥之前报错，风格与其他配置项相同。`--check-config` 也会检查这一节。

### 环境变量与优先级

- `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS=on|off`：覆盖配置文件里的 `enabled`。不区分大小写；未设置或为空时以配置文件为准；其他取值启动失败。
- 优先级：环境变量 > 配置文件 `enabled` > 默认（关闭）。其余字段只能在配置文件里设置。
- 用环境变量打开时，其余字段取配置文件里的值，没写就取默认值（目录 `logs/crash` 相对配置文件所在目录）。
- 启动日志 `crash diagnostics enabled` 的 `environment_override` 字段说明是否被环境变量覆盖。

### 多租户（`--tenants`）

崩溃诊断是进程级的（一个 panic 钩子、一个目录），所以各租户配置解析后的 `diagnostics.crash` 必须完全相同（包括解析后的目录），否则启动失败。

## 会产生什么

文件都在 `directory` 下，文件名 `crash-dbproxy-<pid>-<UTC 时间>.<扩展名>`，时间形如 `20261016T031500.123Z`（不含冒号，Windows 可用）。同名已存在时加 `-2`、`-3` 后缀，不覆盖。

| 情况 | Windows | Linux |
|---|---|---|
| Rust panic（任意线程） | `.txt` 报告 | `.txt` 报告 |
| 访问冲突、栈溢出等未处理异常 / SIGSEGV | `.txt` 原生异常记录；`miniDump` 开时另有 `.dmp` | `coreDump` 开时由内核按 `core_pattern` 写 core；**不写文本记录** |
| `abort()`（含 panic 中再 panic） | 不能捕获，见下文“局限” | `coreDump` 开时写 core |

### panic 报告

panic 钩子同步写报告，写完再调用原来的钩子，所以 panic 语义完全不变：

- 原钩子照常往 stderr 打印 `thread '…' panicked at …`；
- 主线程 panic 仍以退出码 101 结束；
- Tokio 任务里的 panic、请求隔离（`catch_unwind`）吸收的 panic 仍按原有逻辑处理（进程继续运行；后台 worker 的 `JoinSet` 在停机汇总时记录 `DBProxy worker stopped unexpectedly`）。这类可存活的 panic 也会写报告，进程不退出。

**限流**：符号化回溯、建文件、fsync 都在发生 panic 的线程上同步进行，可存活的 panic 连续发生时会拖慢工作线程。因此对**可存活的 panic 每 10 秒最多写一份报告**（常量 `PANIC_REPORT_MIN_INTERVAL_MS = 10000`，不可配置）。

- 每次 panic 都先廉价采集现场：消息、位置、线程名与线程号，以及 `Backtrace::force_capture()` 得到的**未符号化**回溯（只遍历栈帧）。限流只拦后面的“符号化 + 写盘 + fsync”。
- 被限流的 panic 不写报告，只计数，并往 stderr 打一行 `crash report suppressed by the rate limit`。最后一次被限流的现场存进一个**只保留一份**的缓存槽（内存有界）；钩子里只 `try_lock` 一次，拿不到就放弃缓存，不阻塞也不会 panic。下一份报告的 `suppressed_since_last:` 一栏写出此前被跳过的个数。
- **致命 panic 不受限流**：主线程（`block_on` 所在线程，panic 后进程以 101 退出）的 panic，以及消息以 `panic in a function that cannot unwind` 或 `panic in a destructor during cleanup` 开头的 panic（之后进程必然 abort）。Tokio 任务里的 panic 被 Tokio 兜住，不算致命。
- **补写**：致命 panic 发生时，先把缓存槽里那次被限流的 panic 补写成报告，报告在 `suppressed_since_last` 之后多两行 `deferred: true` 与 `deferred_reason: main-thread-panic`（或 `non-unwinding-panic`）；然后再写致命那一份。所以“可存活 panic 之后 10 秒内的致命 panic”不会丢报告。补写时缓存槽最多等约 100 ms，拿不到就跳过补写，致命那一份照写。
- 正常写出一份报告后清空缓存槽；只在致命路径补写，进程正常运行时被限流的 panic 只留计数。
- 限流只管 panic 报告；Windows 原生异常本来就只处理一次，不受限流影响。

写报告失败（目录被删、磁盘满等）只往 stderr 打一行 `could not write a crash report`，不会二次 panic。

### 最近日志

启用且 `recentLogLines > 0` 时，日志照常写标准输出，同时把**同一份文本**按行放进内存环形缓冲，崩溃时取出写进报告。这解决的是：日志经管道或采集器转发时，崩溃前最后几行可能还在缓冲里没落地。

- 内存有界：最多 2000 行，每行最多保留 4096 字节（超出截断并标 `…[truncated]`），最坏约 8 MiB。
- 关闭时不创建缓冲、日志初始化代码走与以前相同的分支，没有额外开销。
- 崩溃路径不会无限等待缓冲锁：约 100 ms 拿不到就在报告里写 `recent_logs: unavailable (buffer busy at crash time)`。

### Windows 原生异常与小型转储

启用时安装未处理异常过滤器（`SetUnhandledExceptionFilter`）。崩溃线程只登记请求并等待，转储与文本记录由启动时预先创建的 `crash-dump` 线程完成，所以栈溢出这类栈空间不够的情况也能写出来（2026-10-16 已用 `stack-overflow` 自测验证：Rust std 先打印 `has overflowed its stack`、再交给过滤器，退出码 `0xC00000FD`，`.dmp` 与文本记录都写出）。崩溃线程最多等 120 秒；超时时往 stderr 打一行 `crash dump timed out`，此时转储和文本记录可能缺失或不完整。写完后交回原过滤器或系统默认处理，进程照常以原异常码结束（例如访问冲突为 `0xC0000005`），Windows 错误报告（WER）照常工作。只处理第一次未处理异常。

`miniDump` 开时用 `MiniDumpWriteDump` 写 `.dmp`，类型为 Normal + ThreadInfo + IndirectlyReferencedMemory + UnloadedModules + HandleData：包含所有线程的栈、栈上指针间接引用到的内存、句柄与模块列表，**不含完整堆**，通常几百 KB 到几 MB。分析时用 WinDbg 或 Visual Studio 打开，需要同一版本的 `tiangz-dbproxy-server.exe` 与 `.pdb`。

`miniDump` 关时仍写文本记录（异常码、异常地址、访问冲突的读/写/执行与目标地址、最近日志），`minidump:` 一栏写 `not requested`。

### Linux core dump

`coreDump` 开时启动阶段执行 `setrlimit(RLIMIT_CORE, 无限)`；硬上限不允许时退一步把软上限提到硬上限，并记一条 warn。日志里的结果以调整后重新读到的软上限为准。启动日志会带上读到的 `/proc/sys/kernel/core_pattern`。

**core 写到哪里不由 DBProxy 决定**，而由宿主机内核参数 `kernel.core_pattern` 决定，它在容器里是只读的、所有容器共用：

- `core_pattern` 是管道（如 `|/usr/lib/systemd/systemd-coredump …` 或 apport）时，core 交给宿主机的收集程序，用 `coredumpctl list`、`coredumpctl gdb <pid>` 查看；apport 默认忽略非发行版软件包的程序。
- `core_pattern` 是路径（如 `/var/crash/core.%e.%p.%t`）时，容器里必须存在且可写这个目录，通常要挂载出来。
- Docker 运行需要 `--ulimit core=-1`（Compose：`ulimits: { core: -1 }`），否则容器的硬上限可能是 0，进程放不开。
- systemd 服务需要 `LimitCORE=infinity`（本仓库 `deploy/external/tiangz-dbproxy@.service` 目前没有设置，部署时按需加）。

Linux 上原生崩溃不写文本记录：信号处理函数里能安全做的事情太少，core 文件本身就是记录。用 `gdb tiangz-dbproxy-server core` 分析，需要同一版本的二进制。

### 保留上限

按文件修改时间只保留最新的 `maxReports` 个 `.txt` 和 `maxDumps` 个 `.dmp`。只处理 `crash-dbproxy-` 开头的文件，目录里其他文件不动。Linux core 文件不在这个目录里，由宿主机的收集程序或运维清理。

- 启动时扫描目录清理一次并记下报告个数；之后每写一份报告只在内存里加一，超过上限时才再扫描目录删除最旧的。转储在写出后清理。
- **上限按目录计，不按进程。** 多个 DBProxy 实例共用一个目录时，它们会互相删除对方的旧文件，而且各自的计数互不知情、可能暂时超出上限。多实例请各用一个目录（例如每个实例一份配置，`directory` 写不同路径）。

### 文件权限

- Unix：新建的崩溃目录权限为 `0700`，报告与转储文件为 `0600`（再受进程 umask 影响，只会更严）；已经存在的目录不改权限。
- Windows：目录和文件继承上级目录的 ACL，不单独设置。需要收紧时请给崩溃目录配置 ACL。

### 默认目录与版本库

默认目录 `logs/crash` 相对配置文件所在目录解析，直接用仓库里的 `configs/*.json` 启动时会落在 `configs/logs/crash`。仓库 `.gitignore` 已忽略 `/configs/logs/`，避免把报告或转储误提交。

## 自测（只用于验证）

设置 `TIANGZ_DBPROXY_CRASH_SELF_TEST`、崩溃诊断已启用、并且**存储是内存后端**（`storage.backend = "memory"`，多租户时所有租户都是）时，服务在打印 `TiangZ DBProxy started` 之后**主动崩溃**：

| 取值 | 行为 | 预期结果 |
|---|---|---|
| `panic` | 主线程 panic | `.txt` 报告，退出码 101 |
| `thread-panic` | 3 个后台线程依次 panic，被 join 吸收 | 进程继续服务；只写 1 份报告，另 2 次被限流计数 |
| `panic-after-thread-panic` | 2 个后台线程依次 panic（第 2 次被限流），随后主线程 panic | 3 份报告：第 1 次、补写的第 2 次（`deferred: true`、`deferred_reason: main-thread-panic`）、主线程那份；退出码 101 |
| `abort` | `std::process::abort()` | Linux：SIGABRT，`coreDump` 开时有 core；Windows：不写报告（见局限） |
| `access-violation` | 写地址 `0x10` | Windows：`.txt` 记录，`miniDump` 开时有 `.dmp`，退出码 `0xC0000005`；Linux：SIGSEGV，`coreDump` 开时有 core |
| `stack-overflow` | 无限递归 | Windows：`.txt` 记录（`EXCEPTION_STACK_OVERFLOW`），`miniDump` 开时有 `.dmp`，退出码 `0xC00000FD`；Linux：Rust std 命中栈守护页后打印提示并 abort，进程以 SIGABRT 结束，`coreDump` 开时有 core（未在本机验证） |

- 崩溃诊断关闭时设置了这个变量只记一条 warn，不崩溃；取值不在上表时启动失败。
- **非内存后端**（例如 `postgresRedis`）设置了这个变量时直接启动失败、以错误退出（退出码 1），不崩溃、不写转储，也不会连接数据库。这样生产环境误留的变量不会导致每次启动必崩，也不会在真实实例上产生含敏感内存的 `.dmp`。

## 局限

- **Windows 上的 `abort` 捕获不到**：Rust 在 Windows 上 abort 用的是 `__fastfail`，直接交给系统，不经过未处理异常过滤器。需要时用 WER LocalDumps（注册表 `HKLM\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\tiangz-dbproxy-server.exe`）由系统写转储。
- 进程被调试器附加时，Windows 不调用未处理异常过滤器。
- `SIGKILL`、OOM killer、断电不会产生任何材料，这些要看容器/系统日志。
- panic 报告在线程上下文里同步执行；如果 panic 发生在持有日志缓冲锁的地方，最近日志一栏会写 `unavailable`，报告本身照写。

### release 版本的回溯

本功能**没有改** `[profile.release]` 的调试信息设置。release 构建默认没有行号调试信息：

- Linux：Cargo 的 release 默认只剥离调试信息、保留符号表，回溯里有函数名、没有文件行号；
- Windows：函数名要靠和 exe 匹配的 `.pdb`，缺了基本只有地址；`.dmp` 分析同样需要对应的 `.pdb`。release 构建是否产出可用的 `.pdb` 取决于当时的工具链与 profile 设置，发布时请确认并随 exe 一起归档（本次未验证 release 产物）。

需要行号时可以在发布构建中设置 `[profile.release] debug = "line-tables-only"`（或 `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`），代价是二进制变大；这是部署方的选择，本仓库默认不开。

## 安全

- `.dmp` 和 core 含进程内存，**可能包含认证令牌、数据库连接串（含密码）和玩家数据**。崩溃目录要限制访问权限，转交分析前按敏感数据处理。
- 文本报告只含日志里本来就有的内容（DBProxy 日志不打印令牌与连接串）和回溯。

## 对外约定（与 TiangZ 保持一致）

- 配置字段：`diagnostics.crash.{enabled, directory, recentLogLines, miniDump, coreDump, maxReports, maxDumps}`，默认值与范围同上表。
- 文件名：`crash-<进程名>-<pid>-<YYYYMMDDTHHMMSS.mmmZ>.<txt|dmp>`，DBProxy 的进程名固定为 `dbproxy`。
- 报告格式（`format_version: 1`）：开头一行 `TiangZ crash report`，随后每行 `键: 值`，固定顺序为 `format_version`、`process`、`version`、`pid`、`platform`（`<os>-<arch>`）、`time_utc`（ISO 8601，毫秒，`Z` 结尾）、`kind`（`panic` 或 `native-exception`）；
  - panic 再接 `thread`（`<线程名> (ThreadId(n))`）、`message`、`location`、`suppressed_since_last`（上一份报告之后被限流跳过的 panic 数），**可选**的 `deferred: true` 与 `deferred_reason: <原因>`（只在补写被限流的 panic 时出现，`format_version` 仍为 1），空行，`backtrace:` 和回溯正文；
  - panic 报告限流：可存活的 panic 每 10 秒最多一份；致命 panic（主线程、不能展开）不受限流，并先补写缓存的最后一次被限流 panic；
  - 原生异常再接 `thread_id`、`exception_code`（`0x%08X` 加可选名称）、`exception_address`、可选 `access_violation`、`minidump`；
  - 最后空行加 `recent_logs: <行数> of <容量>`（或 `disabled` / `unavailable`）和日志行。
  - 值里有换行时，续行以两个空格开头。
- 环境变量：DBProxy 用 `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS`、`TIANGZ_DBPROXY_CRASH_SELF_TEST`；TiangZ 使用自己的变量名，取值约定（`on|off`；`panic|thread-panic|abort|access-violation|stack-overflow`）相同；DBProxy 另有 `panic-after-thread-panic` 用于验证补写。
- 文件权限：Unix 目录 `0700`、文件 `0600`；Windows 继承 ACL。

## 验证

- 单元测试（`cargo test -p tiangz-dbproxy-server --lib`）：配置默认值与越界、环境变量覆盖、自测取值、报告格式逐字节、UTC 时间换算、环形缓冲上限与截断、fmt 日志进入缓冲、同名不覆盖、保留清理只删最旧且不碰其他文件、多租户不一致时拒绝。
- 集成测试 `crates/dbproxy-server/tests/crash_diagnostics.rs`：用内存后端启动真实服务子进程，不连接数据库。
  - panic 自测：退出码 101、原钩子输出仍在、报告含回溯（含 `trigger_self_test` 帧）与启动日志、`maxReports` 生效；
  - 环境变量 `off` 覆盖配置文件、自测被忽略；
  - 开关或自测取值错误时启动失败；
  - `thread-panic` 自测：3 次后台线程 panic 只写 1 份报告、2 次被限流，进程仍存活且监听端口仍接受连接；
  - `panic-after-thread-panic` 自测：退出码 101，共 3 份报告——第 1 次后台 panic、补写的第 2 次（`deferred: true`、`deferred_reason: main-thread-panic`）、主线程致命 panic；
  - 非内存后端设置自测变量：退出码 1、不崩溃、不建崩溃目录（连接串指向不可解析的地址，进程在连接前就退出）；
  - Windows：访问冲突自测，退出码 `0xC0000005`，`.dmp` 以 `MDMP` 开头，文本记录含异常码与访问地址，`maxDumps` 生效；
  - Windows：栈溢出自测，退出码 `0xC00000FD`，`.dmp` 与 `EXCEPTION_STACK_OVERFLOW` 文本记录都写出；
  - Unix：`abort` 自测，进程死于 SIGABRT，启动日志含 core 上限调整结果（只在 CI 的 Linux 上跑）。
- 2026-10-16 本机 Windows 结果与未验证项见发布说明草稿 `RELEASE-unreleased.md`。

---

## English summary

Crash diagnostics are **off by default**. Enable them with `diagnostics.crash.enabled` in the DBProxy config or with `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS=on|off`, which takes precedence over the file. A relative `directory` (default `logs/crash`) is resolved against the config file's directory.

- **Panics** (any thread): a report `crash-dbproxy-<pid>-<UTC>.txt` with version, pid, thread, message, location, a forced backtrace and the last `recentLogLines` (0–2000, default 200) log lines. The previous panic hook still runs afterwards, so exit codes and Tokio task handling are unchanged. Report-writing failures never panic.
- **Rate limit**: at most one report every 10 s for survivable panics (constant). Every panic is captured cheaply (unsymbolised backtrace); the limit only gates symbolising, writing and fsync. Suppressed panics are counted (`suppressed_since_last: N` in the next report) and the latest one is kept in a single-entry slot (try-lock only in the hook). Fatal panics (main thread, or messages starting with `panic in a function that cannot unwind` / `panic in a destructor during cleanup`) bypass the limit and first flush the cached panic as a report with the optional keys `deferred: true` and `deferred_reason: main-thread-panic|non-unwinding-panic`.
- **Recent logs**: stdout output is copied line by line into a bounded ring buffer (≤ 2000 lines × 4 KiB). Nothing is added when disabled.
- **Windows native crashes**: an unhandled-exception filter hands the work to a pre-spawned `crash-dump` thread, which writes a text record (exception code, address, access details) and, with `miniDump`, a `.dmp` (threads, stacks, indirectly referenced memory, handles; no full heap). The previous filter or the system default then runs, so the exit code is unchanged. `abort()` uses `__fastfail` and cannot be caught; use WER LocalDumps.
- **Linux**: with `coreDump`, `RLIMIT_CORE` is raised at startup. Where cores go is set by the host's `kernel.core_pattern`; Docker needs `--ulimit core=-1` and a mounted directory when the pattern is a path; systemd units need `LimitCORE=infinity`. No text record is written for native crashes on Linux.
- **Retention**: only the newest `maxReports` (default 50) `.txt` and `maxDumps` (default 5) `.dmp` files are kept; the directory is scanned at startup and again only when the in-memory count exceeds the limit. Other files are untouched. Limits are per directory, not per process: give each instance its own directory.
- **Permissions**: on Unix new crash directories are `0700` and files `0600`; on Windows they inherit the parent ACL. `/configs/logs/` is git-ignored.
- **Self-test** (verification only): `TIANGZ_DBPROXY_CRASH_SELF_TEST=panic|thread-panic|panic-after-thread-panic|abort|access-violation|stack-overflow` crashes the process right after startup when crash diagnostics are on **and the storage backend is memory**; with any other backend startup fails with an error instead.
- **Release builds**: the release profile is unchanged. Linux backtraces keep function names but no line numbers; Windows needs the matching `.pdb` (check that the release artifacts include it; not verified here). Set `debug = "line-tables-only"` if line numbers are needed.
- **Security**: dumps and cores contain process memory, including tokens and connection strings; restrict access to the directory.
