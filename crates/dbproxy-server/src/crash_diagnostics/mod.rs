//! 崩溃诊断：panic 报告（含回溯与最近日志）、Windows 小型转储、Linux core 上限与保留清理。默认关闭。
//! 说明与运维步骤见 `docs/crash-diagnostics.md`。
//! Crash diagnostics: panic reports with backtrace and recent logs, Windows minidumps, the Linux core limit and
//! retention. Off by default. See `docs/crash-diagnostics.md`.

mod recent_logs;
mod report;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub use recent_logs::{
    MAX_RECENT_LOG_LINE_BYTES, MAX_RECENT_LOG_LINES, RecentLogs, RecentLogsBusy, TeeMakeWriter,
    TeeWriter,
};
pub use report::{
    DUMP_EXTENSION, FILE_PREFIX, NativeDetails, PANIC_REPORT_MIN_INTERVAL_MS, PROCESS_NAME,
    PanicCapture, PanicDetails, REPORT_EXTENSION, REPORT_FORMAT_VERSION, RecentLogSection,
    ReportHeader, ReportLimiter, UtcTimestamp, access_operation, count_crash_files,
    create_new_file, create_private_dir_all, exception_name, file_stem, prune_oldest,
    render_native_report, render_panic_report, write_new_text_file,
};

use std::{
    error::Error,
    fmt,
    io::{self, Write},
    panic::PanicHookInfo,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, TryLockError,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde::Deserialize;

use crate::config::ConfigError;

/// 覆盖 `diagnostics.crash.enabled` 的环境变量，取值 `on` 或 `off`（不区分大小写，空值等于未设置）。
/// Environment variable overriding `diagnostics.crash.enabled`: `on` or `off` (case-insensitive; empty = unset).
pub const CRASH_DIAGNOSTICS_ENV: &str = "TIANGZ_DBPROXY_CRASH_DIAGNOSTICS";
/// 仅用于验证：`panic`、`thread-panic`、`panic-after-thread-panic`、`abort`、`access-violation` 或
/// `stack-overflow`。启用崩溃诊断且存储为内存后端时，启动完成后主动触发；其他后端拒绝启动。
/// Verification only: `panic`, `thread-panic`, `panic-after-thread-panic`, `abort`, `access-violation` or
/// `stack-overflow`. Triggered after startup when crash diagnostics are on and the storage backend is memory; other
/// backends refuse to start.
pub const CRASH_SELF_TEST_ENV: &str = "TIANGZ_DBPROXY_CRASH_SELF_TEST";

pub const DEFAULT_CRASH_DIRECTORY: &str = "logs/crash";
pub const DEFAULT_RECENT_LOG_LINES: usize = 200;
pub const DEFAULT_MAX_REPORTS: usize = 50;
pub const DEFAULT_MAX_DUMPS: usize = 5;
pub const MAX_MAX_REPORTS: usize = 10_000;
pub const MAX_MAX_DUMPS: usize = 1_000;

/// 配置文件里的 `diagnostics` 节。 / The `diagnostics` section of the configuration file.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiagnosticsSection {
    #[serde(default)]
    pub crash: CrashSection,
}

/// `diagnostics.crash`。 / `diagnostics.crash`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrashSection {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_directory")]
    pub directory: String,
    #[serde(default = "default_recent_log_lines")]
    pub recent_log_lines: usize,
    #[serde(default)]
    pub mini_dump: bool,
    #[serde(default)]
    pub core_dump: bool,
    #[serde(default = "default_max_reports")]
    pub max_reports: usize,
    #[serde(default = "default_max_dumps")]
    pub max_dumps: usize,
}

impl Default for CrashSection {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: default_directory(),
            recent_log_lines: DEFAULT_RECENT_LOG_LINES,
            mini_dump: false,
            core_dump: false,
            max_reports: DEFAULT_MAX_REPORTS,
            max_dumps: DEFAULT_MAX_DUMPS,
        }
    }
}

fn default_directory() -> String {
    DEFAULT_CRASH_DIRECTORY.to_string()
}
const fn default_recent_log_lines() -> usize {
    DEFAULT_RECENT_LOG_LINES
}
const fn default_max_reports() -> usize {
    DEFAULT_MAX_REPORTS
}
const fn default_max_dumps() -> usize {
    DEFAULT_MAX_DUMPS
}

/// 解析后的崩溃诊断设置。目录已按配置文件所在目录解析成实际路径。
/// Resolved crash-diagnostics settings; the directory is already resolved against the config file's directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedCrashDiagnostics {
    pub enabled: bool,
    pub directory: PathBuf,
    pub recent_log_lines: usize,
    pub mini_dump: bool,
    pub core_dump: bool,
    pub max_reports: usize,
    pub max_dumps: usize,
    /// 环境变量覆盖后的值；None 表示未覆盖。 / Value forced by the environment variable; None when not overridden.
    pub environment_override: Option<bool>,
}

impl CrashSection {
    /// 校验取值并解析目录；相对目录以配置文件所在目录为基准（与租户清单里的相对路径规则一致）。
    /// Validate values and resolve the directory; relative paths are based on the config file's directory, the same
    /// rule the tenant deployment file uses.
    pub(crate) fn resolve(self, source: &Path) -> Result<ResolvedCrashDiagnostics, ConfigError> {
        if self.recent_log_lines > MAX_RECENT_LOG_LINES {
            return Err(ConfigError(format!(
                "diagnostics.crash.recentLogLines must be between 0 and {MAX_RECENT_LOG_LINES}"
            )));
        }
        if !(1..=MAX_MAX_REPORTS).contains(&self.max_reports) {
            return Err(ConfigError(format!(
                "diagnostics.crash.maxReports must be between 1 and {MAX_MAX_REPORTS}"
            )));
        }
        if !(1..=MAX_MAX_DUMPS).contains(&self.max_dumps) {
            return Err(ConfigError(format!(
                "diagnostics.crash.maxDumps must be between 1 and {MAX_MAX_DUMPS}"
            )));
        }
        if self.directory.is_empty() || self.directory.trim() != self.directory {
            return Err(ConfigError(
                "diagnostics.crash.directory must be a nonempty path without surrounding whitespace"
                    .into(),
            ));
        }
        let directory = PathBuf::from(&self.directory);
        let directory = if directory.is_absolute() {
            directory
        } else {
            source.parent().unwrap_or(Path::new("")).join(directory)
        };
        Ok(ResolvedCrashDiagnostics {
            enabled: self.enabled,
            directory,
            recent_log_lines: self.recent_log_lines,
            mini_dump: self.mini_dump,
            core_dump: self.core_dump,
            max_reports: self.max_reports,
            max_dumps: self.max_dumps,
            environment_override: None,
        })
    }
}

impl ResolvedCrashDiagnostics {
    /// 应用 `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS`：环境变量优先于配置文件里的 `enabled`。
    /// Apply `TIANGZ_DBPROXY_CRASH_DIAGNOSTICS`; the variable wins over `enabled` in the file.
    pub fn with_environment_override(mut self, value: Option<&str>) -> Result<Self, ConfigError> {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(self);
        };
        let enabled = if value.eq_ignore_ascii_case("on") {
            true
        } else if value.eq_ignore_ascii_case("off") {
            false
        } else {
            return Err(ConfigError(format!(
                "{CRASH_DIAGNOSTICS_ENV} must be on or off"
            )));
        };
        self.enabled = enabled;
        self.environment_override = Some(enabled);
        Ok(self)
    }
}

/// 自测方式。 / Self-test mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelfTest {
    /// 主线程 panic，进程退出。 / Main-thread panic; the process exits.
    Panic,
    /// 后台线程连续 panic 三次，进程继续服务；验证可存活 panic 与限流。
    /// Three panics on background threads while the process keeps serving; checks survivable panics and the limit.
    ThreadPanic,
    /// 后台线程先 panic 两次（第二次被限流），然后主线程 panic；验证致命 panic 不受限流并补写被限流的那一次。
    /// Two background-thread panics (the second rate-limited), then a main-thread panic; checks that the fatal panic
    /// bypasses the limit and the suppressed one is written as deferred.
    PanicAfterThreadPanic,
    Abort,
    AccessViolation,
    StackOverflow,
}

/// `thread-panic` 自测连续 panic 的次数。 / Number of panics the `thread-panic` self-test raises.
pub const THREAD_PANIC_SELF_TEST_COUNT: usize = 3;

impl SelfTest {
    pub fn parse(value: Option<&str>) -> Result<Option<Self>, CrashDiagnosticsError> {
        match value.map(str::trim) {
            None | Some("") => Ok(None),
            Some("panic") => Ok(Some(Self::Panic)),
            Some("thread-panic") => Ok(Some(Self::ThreadPanic)),
            Some("panic-after-thread-panic") => Ok(Some(Self::PanicAfterThreadPanic)),
            Some("abort") => Ok(Some(Self::Abort)),
            Some("access-violation") => Ok(Some(Self::AccessViolation)),
            Some("stack-overflow") => Ok(Some(Self::StackOverflow)),
            Some(_) => Err(CrashDiagnosticsError(format!(
                "{CRASH_SELF_TEST_ENV} must be panic, thread-panic, panic-after-thread-panic, abort, access-violation or stack-overflow"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Panic => "panic",
            Self::ThreadPanic => "thread-panic",
            Self::PanicAfterThreadPanic => "panic-after-thread-panic",
            Self::Abort => "abort",
            Self::AccessViolation => "access-violation",
            Self::StackOverflow => "stack-overflow",
        }
    }
}

#[derive(Debug)]
pub struct CrashDiagnosticsError(String);

impl fmt::Display for CrashDiagnosticsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CrashDiagnosticsError {}

struct InstalledState {
    directory: PathBuf,
    recent: Option<Arc<RecentLogs>>,
    max_reports: usize,
    #[cfg_attr(not(windows), allow(dead_code))]
    max_dumps: usize,
    #[cfg_attr(not(windows), allow(dead_code))]
    mini_dump: bool,
    self_test: Option<SelfTest>,
    /// 单调时钟起点，供限流使用。 / Monotonic origin for the rate limit.
    started: Instant,
    limiter: ReportLimiter,
    /// 目录里 `.txt` 报告的个数：启动时数一次，之后只在超过上限时才扫描目录。
    /// Number of `.txt` reports: counted once at startup; the directory is scanned again only when over the limit.
    report_count: AtomicUsize,
}

impl InstalledState {
    /// 记一份新报告；超过上限时才扫描目录删除最旧的。 / Count a new report; scan and prune only when over the limit.
    fn note_new_report(&self) {
        let count = self.report_count.fetch_add(1, Ordering::AcqRel) + 1;
        if count > self.max_reports {
            let remaining = count_after_prune(&self.directory, REPORT_EXTENSION, self.max_reports);
            self.report_count.store(remaining, Ordering::Release);
        }
    }
}

/// 清理后重新数一次；数不了时按上限计，下次仍会触发清理。
/// Prune, then recount; if counting fails assume the limit so the next report prunes again.
fn count_after_prune(directory: &Path, extension: &str, keep: usize) -> usize {
    let _ = prune_oldest(directory, extension, keep);
    count_crash_files(directory, extension).unwrap_or(keep)
}

static INSTALLED: OnceLock<InstalledState> = OnceLock::new();

/// 启用且 `recentLogLines > 0` 时返回最近日志缓冲，交给日志初始化；否则返回 None，日志路径保持原样。
/// Returns the recent-log buffer for logging setup when enabled with `recentLogLines > 0`; otherwise None and the
/// logging path stays exactly as before.
pub fn recent_log_buffer(settings: &ResolvedCrashDiagnostics) -> Option<Arc<RecentLogs>> {
    (settings.enabled && settings.recent_log_lines > 0)
        .then(|| Arc::new(RecentLogs::new(settings.recent_log_lines)))
}

/// 在日志初始化之后、Runtime 启动之前调用。关闭时什么也不做。
/// 启用时：创建目录、先按上限清理旧文件、安装 panic 钩子（写报告后再调用原钩子）、Windows 安装异常过滤器、
/// Unix 按 `coreDump` 放开 core 上限。目录建不出来时返回错误，进程不启动。
/// Call after logging is initialised and before the runtime starts. Does nothing when disabled.
/// When enabled: create the directory, prune old files, install the panic hook (report first, then the previous hook),
/// install the Windows exception filter, raise the Unix core limit when `coreDump` is set. Fails if the directory
/// cannot be created, so the process does not start.
///
/// `memory_backend_only`：所有租户都是内存后端时为 true。自测只允许在这种进程里运行，
/// 其他后端设置了自测变量直接启动失败（报错退出，不崩溃），防止生产环境残留变量导致每次启动必崩。
/// `memory_backend_only` is true when every tenant uses the memory backend. Self-tests are only allowed there; with any
/// other backend a set self-test variable fails startup (an error exit, not a crash), so a leftover variable in
/// production cannot crash every start.
pub fn install(
    settings: &ResolvedCrashDiagnostics,
    recent: Option<Arc<RecentLogs>>,
    memory_backend_only: bool,
) -> Result<(), CrashDiagnosticsError> {
    let self_test_value = std::env::var(CRASH_SELF_TEST_ENV).ok();
    if !settings.enabled {
        if self_test_value
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            tracing::warn!(
                variable = CRASH_SELF_TEST_ENV,
                "crash self-test ignored because crash diagnostics are disabled"
            );
        }
        return Ok(());
    }
    let self_test = SelfTest::parse(self_test_value.as_deref())?;
    if self_test.is_some() && !memory_backend_only {
        return Err(CrashDiagnosticsError(format!(
            "{CRASH_SELF_TEST_ENV} is only allowed with the memory storage backend; unset it on real instances"
        )));
    }
    create_private_dir_all(&settings.directory).map_err(|error| {
        CrashDiagnosticsError(format!(
            "cannot create crash diagnostics directory {}: {error}",
            settings.directory.display()
        ))
    })?;
    let report_count =
        count_after_prune(&settings.directory, REPORT_EXTENSION, settings.max_reports);
    let _ = prune_oldest(&settings.directory, DUMP_EXTENSION, settings.max_dumps);
    let state = InstalledState {
        directory: settings.directory.clone(),
        recent,
        max_reports: settings.max_reports,
        max_dumps: settings.max_dumps,
        mini_dump: settings.mini_dump,
        self_test,
        started: Instant::now(),
        limiter: ReportLimiter::new(PANIC_REPORT_MIN_INTERVAL_MS),
        report_count: AtomicUsize::new(report_count),
    };
    if INSTALLED.set(state).is_err() {
        return Err(CrashDiagnosticsError(
            "crash diagnostics are already installed in this process".into(),
        ));
    }
    install_panic_hook();
    install_native(settings)?;
    tracing::info!(
        directory = %settings.directory.display(),
        recent_log_lines = settings.recent_log_lines,
        mini_dump = settings.mini_dump,
        core_dump = settings.core_dump,
        max_reports = settings.max_reports,
        max_dumps = settings.max_dumps,
        environment_override = ?settings.environment_override,
        "crash diagnostics enabled"
    );
    Ok(())
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Some(state) = INSTALLED.get() {
            handle_panic(state, info);
        }
        previous(info);
    }));
}

/// std 在不能展开的位置 panic 时使用的消息前缀；这类 panic 之后进程必然 abort（与 TiangZ 一致）。
/// Message prefixes std uses for panics where unwinding is impossible; the process always aborts afterwards (same
/// list as TiangZ).
const NOUNWIND_PANIC_MESSAGES: [&str; 2] = [
    "panic in a function that cannot unwind",
    "panic in a destructor during cleanup",
];

/// 致命 panic 的原因：主线程（`block_on` 所在线程）panic 会结束进程；不能展开的 panic 之后进程 abort。
/// Tokio 任务里的 panic 由 Tokio 兜住，不算致命。
/// Why a panic is fatal: a main-thread panic (the `block_on` thread) ends the process; a non-unwinding panic is
/// followed by an abort. Panics inside Tokio tasks are caught by Tokio and are not fatal.
fn fatal_reason(thread_name: Option<&str>, message: &str) -> Option<&'static str> {
    if NOUNWIND_PANIC_MESSAGES
        .iter()
        .any(|prefix| message.starts_with(prefix))
    {
        Some("non-unwinding-panic")
    } else if thread_name == Some("main") {
        Some("main-thread-panic")
    } else {
        None
    }
}

/// 钩子主体。钩子里不能再 panic（std 会直接 abort）：所有错误只写 stderr，且不用会 panic 的 eprintln；
/// 也不能阻塞：缓存槽只 try_lock 一次，拿不到就放弃缓存。
/// 每次 panic 都廉价采集现场（未符号化的回溯）；限流只拦“符号化 + 写盘 + fsync”。被拦下的最后一次 panic 存进
/// 只保留一份的缓存槽，之后若发生致命 panic，先把它补写成报告（`deferred: true`），再写致命那一份。
/// Hook body. It must not panic (std would abort): errors only go to stderr without the panicking eprintln. It must
/// not block either: the slot is try-locked once and caching is skipped if that fails.
/// Every panic is captured cheaply (unsymbolised backtrace); the limit only gates symbolising, writing and fsync.
/// The latest suppressed panic is kept in a single-entry slot; a later fatal panic first writes it as a deferred
/// report, then its own.
fn handle_panic(state: &InstalledState, info: &PanicHookInfo<'_>) {
    let message = panic_message(info);
    let location = info.location().map(|location| {
        format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        )
    });
    let capture = PanicCapture::capture(message, location);
    let fatal = fatal_reason(std::thread::current().name(), message);
    let now_ms = u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
    if let Some(reason) = fatal {
        flush_deferred_panic(state, reason);
    }
    admit_and_report(
        &state.limiter,
        &DEFERRED_PANIC,
        capture,
        fatal.is_some(),
        now_ms,
        |capture, suppressed| {
            report_written(write_panic_report(state, capture, suppressed, None));
        },
    );
}

/// 限流判定与缓存槽维护；写报告由 `write` 完成（测试里可注入）。
/// 允许写报告时，**先**清空缓存槽再写：槽里那份一定早于本份、已计入本份的 `suppressed_since_last`；
/// 符号化与写盘期间新被限流、进槽的 panic 留给之后的致命路径补写，不会被本份清掉。
/// Rate-limit decision and slot upkeep; `write` produces the report (injectable in tests).
/// When a report is admitted the slot is cleared **before** writing: whatever it holds is older than this report and
/// already counted in its `suppressed_since_last`. A panic suppressed while this report is being symbolised and
/// written stays in the slot for a later fatal flush.
fn admit_and_report(
    limiter: &ReportLimiter,
    slot: &Mutex<Option<PanicCapture>>,
    capture: PanicCapture,
    fatal: bool,
    now_ms: u64,
    write: impl FnOnce(&PanicCapture, u64),
) {
    let admitted = if fatal {
        Some(limiter.admit_unconditionally(now_ms))
    } else {
        limiter.admit(now_ms)
    };
    match admitted {
        Some(suppressed) => {
            if let Some(mut cached) = lock_slot(slot, false) {
                *cached = None;
            }
            write(&capture, suppressed);
        }
        None => {
            let _ = writeln!(
                io::stderr(),
                "TiangZ DBProxy crash report suppressed by the rate limit (one per {PANIC_REPORT_MIN_INTERVAL_MS} ms)"
            );
            if let Some(mut cached) = lock_slot(slot, false) {
                *cached = Some(capture);
            }
        }
    }
}

/// 最近一次被限流的 panic；只存一份，内存有界。 / The latest rate-limited panic; a single entry, so memory is bounded.
static DEFERRED_PANIC: Mutex<Option<PanicCapture>> = Mutex::new(None);

fn lock_deferred(wait: bool) -> Option<MutexGuard<'static, Option<PanicCapture>>> {
    lock_slot(&DEFERRED_PANIC, wait)
}

/// `wait` 为 false 时只试一次；为 true 时（致命路径）最多重试约 100 ms。不会无限阻塞。
/// With `wait` false only one attempt is made; with `wait` true (fatal path) it retries for about 100 ms. Never
/// blocks indefinitely.
fn lock_slot(
    slot: &Mutex<Option<PanicCapture>>,
    wait: bool,
) -> Option<MutexGuard<'_, Option<PanicCapture>>> {
    let attempts = if wait { 150 } else { 1 };
    for attempt in 0..attempts {
        match slot.try_lock() {
            Ok(slot) => return Some(slot),
            Err(TryLockError::Poisoned(poisoned)) => return Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) if attempt < 50 => std::thread::yield_now(),
            Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    None
}

/// 致命路径：把缓存槽里最后一次被限流的 panic 补写成报告。槽为空时什么也不做。
/// Fatal path: write the latest rate-limited panic from the slot as a deferred report. Does nothing when empty.
fn flush_deferred_panic(state: &InstalledState, reason: &str) {
    let Some(capture) = lock_deferred(true).and_then(|mut slot| slot.take()) else {
        return;
    };
    // 被补写的这一次本身也计在跳过数里，扣掉它。 / The deferred panic itself was counted as skipped; exclude it.
    let suppressed = state.limiter.take_suppressed().saturating_sub(1);
    report_written(write_panic_report(
        state,
        &capture,
        suppressed,
        Some(reason),
    ));
}

fn report_written(result: io::Result<PathBuf>) {
    match result {
        Ok(path) => {
            let _ = writeln!(
                io::stderr(),
                "TiangZ DBProxy crash report written to {}",
                path.display()
            );
        }
        Err(error) => {
            let _ = writeln!(
                io::stderr(),
                "TiangZ DBProxy could not write a crash report: {error}"
            );
        }
    }
}

fn recent_section(state: &InstalledState) -> RecentLogSection {
    match &state.recent {
        None => RecentLogSection::Disabled,
        Some(recent) => match recent.try_snapshot() {
            Ok(lines) => RecentLogSection::Lines {
                capacity: recent.capacity(),
                lines,
            },
            Err(RecentLogsBusy) => RecentLogSection::Busy,
        },
    }
}

fn panic_message<'a>(info: &'a PanicHookInfo<'_>) -> &'a str {
    let payload = info.payload();
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "<non-string panic payload>"
    }
}

fn write_panic_report(
    state: &InstalledState,
    capture: &PanicCapture,
    suppressed_since_last: u64,
    deferred_reason: Option<&str>,
) -> io::Result<PathBuf> {
    let time = UtcTimestamp::now();
    let pid = std::process::id();
    // 到这里才符号化。 / Symbolised only here.
    let backtrace = capture.backtrace.to_string();
    let text = render_panic_report(
        &ReportHeader::new(pid, time, "panic"),
        &PanicDetails {
            thread: &capture.thread,
            message: &capture.message,
            location: capture.location.clone(),
            backtrace: &backtrace,
            suppressed_since_last,
            deferred_reason,
        },
        &recent_section(state),
    );
    let path = write_new_text_file(
        &state.directory,
        &file_stem(pid, &time),
        REPORT_EXTENSION,
        &text,
    )?;
    state.note_new_report();
    Ok(path)
}

#[cfg(windows)]
fn install_native(settings: &ResolvedCrashDiagnostics) -> Result<(), CrashDiagnosticsError> {
    if settings.core_dump {
        tracing::info!("diagnostics.crash.coreDump only applies on Linux; ignored on Windows");
    }
    windows::install(|exception| {
        if let Some(state) = INSTALLED.get() {
            write_native_report(state, exception);
        }
    })
    .map_err(|error| {
        CrashDiagnosticsError(format!("cannot install the Windows crash filter: {error}"))
    })
}

#[cfg(windows)]
fn write_native_report(state: &InstalledState, exception: &windows::NativeException) {
    let time = UtcTimestamp::now();
    let pid = std::process::id();
    let stem = file_stem(pid, &time);
    let minidump = if state.mini_dump {
        match report::create_new_file(&state.directory, &stem, DUMP_EXTENSION) {
            Ok((path, file)) => match exception.write_minidump(&file) {
                Ok(()) => {
                    let _ = file.sync_all();
                    let _ = prune_oldest(&state.directory, DUMP_EXTENSION, state.max_dumps);
                    path.file_name()
                        .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
                }
                Err(error) => {
                    drop(file);
                    let _ = std::fs::remove_file(&path);
                    format!("failed: {error}")
                }
            },
            Err(error) => format!("failed: {error}"),
        }
    } else {
        "not requested (diagnostics.crash.miniDump = false)".to_string()
    };
    let text = report::render_native_report(
        &ReportHeader::new(pid, time, "native-exception"),
        &report::NativeDetails {
            thread_id: exception.thread_id,
            exception_code: exception.code,
            exception_address: exception.address,
            access: exception.access,
            minidump: &minidump,
        },
        &recent_section(state),
    );
    match write_new_text_file(&state.directory, &stem, REPORT_EXTENSION, &text) {
        Ok(path) => {
            state.note_new_report();
            let _ = writeln!(
                io::stderr(),
                "TiangZ DBProxy native crash record written to {}",
                path.display()
            );
        }
        Err(error) => {
            let _ = writeln!(
                io::stderr(),
                "TiangZ DBProxy could not write a native crash record: {error}"
            );
        }
    }
}

#[cfg(unix)]
fn install_native(settings: &ResolvedCrashDiagnostics) -> Result<(), CrashDiagnosticsError> {
    if settings.mini_dump {
        tracing::info!("diagnostics.crash.miniDump only applies on Windows; ignored here");
    }
    if settings.core_dump {
        let pattern = unix::core_pattern();
        match unix::raise_core_limit() {
            Ok(unix::CoreLimit::Unlimited) => tracing::info!(
                core_pattern = pattern.as_deref().unwrap_or("unknown"),
                "core dump size limit raised to unlimited"
            ),
            Ok(unix::CoreLimit::Capped(bytes)) => tracing::warn!(
                soft_limit_bytes = bytes,
                core_pattern = pattern.as_deref().unwrap_or("unknown"),
                "core dump size limit raised only to the hard limit; raise it on the host or container (e.g. --ulimit core=-1)"
            ),
            Err(error) => tracing::warn!(
                %error,
                "cannot raise the core dump size limit; core dumps may be missing"
            ),
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn install_native(_settings: &ResolvedCrashDiagnostics) -> Result<(), CrashDiagnosticsError> {
    Ok(())
}

/// 自测入口：只有启用崩溃诊断、内存后端且设置了 `TIANGZ_DBPROXY_CRASH_SELF_TEST` 时才会主动崩溃
/// （`install` 已拒绝其他后端）；只用于验证。
/// Self-test hook: crashes deliberately only when crash diagnostics are on, the backend is memory (`install` rejects
/// others) and `TIANGZ_DBPROXY_CRASH_SELF_TEST` is set. Verification only.
pub fn run_self_test_if_requested() {
    let Some(mode) = INSTALLED.get().and_then(|state| state.self_test) else {
        return;
    };
    tracing::warn!(
        mode = mode.as_str(),
        variable = CRASH_SELF_TEST_ENV,
        "crash diagnostics self-test: triggering a deliberate crash"
    );
    trigger_self_test(mode);
}

fn trigger_self_test(mode: SelfTest) {
    match mode {
        SelfTest::Panic => {
            panic!("TiangZ DBProxy crash self-test ({CRASH_SELF_TEST_ENV}=panic)")
        }
        SelfTest::ThreadPanic => panic_background_threads(mode, THREAD_PANIC_SELF_TEST_COUNT),
        SelfTest::PanicAfterThreadPanic => {
            panic_background_threads(mode, 2);
            panic!(
                "TiangZ DBProxy crash self-test ({CRASH_SELF_TEST_ENV}=panic-after-thread-panic)"
            )
        }
        SelfTest::Abort => std::process::abort(),
        SelfTest::StackOverflow => {
            let depth = overflow_stack(0);
            // 走不到这里；万一走到，也要以失败结束。 / Unreachable; abort if it ever returns.
            tracing::error!(depth, "stack-overflow self-test returned unexpectedly");
            std::process::abort();
        }
        SelfTest::AccessViolation => {
            // 故意写入不可访问的低地址，制造真实的访问冲突 / SIGSEGV。
            // Deliberately write to an unmapped low address to cause a real access violation / SIGSEGV.
            let address = std::hint::black_box(16usize);
            // SAFETY: 不安全且故意如此：这是自测要制造的崩溃。 / Deliberately unsound: this is the crash under test.
            unsafe { std::ptr::without_provenance_mut::<u8>(address).write_volatile(0xA5) };
            std::process::abort();
        }
    }
}

/// 依次启动 `count` 个后台线程并让它们 panic；panic 被 join 吸收，进程继续服务。
/// Spawn `count` background threads one after another and let each panic; join absorbs the panic and the process
/// keeps serving.
fn panic_background_threads(mode: SelfTest, count: usize) {
    for index in 1..=count {
        let joined = std::thread::Builder::new()
            .name(format!("crash-self-test-{index}"))
            .spawn(move || {
                panic!(
                    "TiangZ DBProxy crash self-test ({CRASH_SELF_TEST_ENV}={}) #{index}",
                    mode.as_str()
                )
            })
            .map(std::thread::JoinHandle::join);
        tracing::warn!(
            index,
            panicked = matches!(joined, Ok(Err(_))),
            "crash diagnostics self-test: background thread panicked; service continues"
        );
    }
}

/// 无限递归，每层占 4 KiB 栈，直到栈溢出。调用后还有加法，不会被尾调用优化掉。
/// Unbounded recursion using 4 KiB of stack per frame until it overflows; the addition after the call prevents
/// tail-call optimisation.
#[inline(never)]
fn overflow_stack(depth: u64) -> u64 {
    let frame = std::hint::black_box([depth.to_le_bytes()[0]; 4096]);
    if std::hint::black_box(depth) == u64::MAX {
        return 0;
    }
    overflow_stack(depth + 1).wrapping_add(u64::from(frame[usize::from(frame[0]) % 4096]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(section: serde_json::Value) -> Result<ResolvedCrashDiagnostics, ConfigError> {
        let section: CrashSection = serde_json::from_value(section).unwrap();
        section.resolve(Path::new("configs/local.json"))
    }

    #[test]
    fn defaults_keep_crash_diagnostics_off() {
        let resolved = resolve(serde_json::json!({})).unwrap();
        assert_eq!(
            resolved,
            ResolvedCrashDiagnostics {
                enabled: false,
                directory: Path::new("configs").join("logs/crash"),
                recent_log_lines: 200,
                mini_dump: false,
                core_dump: false,
                max_reports: 50,
                max_dumps: 5,
                environment_override: None,
            }
        );
        assert!(recent_log_buffer(&resolved).is_none());
    }

    #[test]
    fn explicit_values_and_absolute_directory_are_kept() {
        let absolute = std::env::temp_dir().join("tiangz-crash-absolute");
        let resolved = resolve(serde_json::json!({
            "enabled": true,
            "directory": absolute.to_str().unwrap(),
            "recentLogLines": 2000,
            "miniDump": true,
            "coreDump": true,
            "maxReports": 3,
            "maxDumps": 1
        }))
        .unwrap();
        assert!(resolved.enabled && resolved.mini_dump && resolved.core_dump);
        assert_eq!(resolved.directory, absolute);
        assert_eq!(
            (
                resolved.recent_log_lines,
                resolved.max_reports,
                resolved.max_dumps
            ),
            (2000, 3, 1)
        );
        assert_eq!(recent_log_buffer(&resolved).unwrap().capacity(), 2000);
    }

    #[test]
    fn out_of_range_values_are_rejected() {
        for (section, field) in [
            (
                serde_json::json!({"recentLogLines": 2001}),
                "recentLogLines",
            ),
            (serde_json::json!({"maxReports": 0}), "maxReports"),
            (serde_json::json!({"maxReports": 10001}), "maxReports"),
            (serde_json::json!({"maxDumps": 0}), "maxDumps"),
            (serde_json::json!({"maxDumps": 1001}), "maxDumps"),
            (serde_json::json!({"directory": ""}), "directory"),
            (serde_json::json!({"directory": " logs"}), "directory"),
        ] {
            let error = resolve(section.clone()).unwrap_err().to_string();
            assert!(error.contains(field), "{section}: {error}");
        }
        assert!(serde_json::from_value::<CrashSection>(serde_json::json!({"unknown": 1})).is_err());
        assert!(
            serde_json::from_value::<CrashSection>(serde_json::json!({"maxReports": -1})).is_err()
        );
    }

    #[test]
    fn zero_recent_lines_disables_only_the_buffer() {
        let resolved = resolve(serde_json::json!({"enabled": true, "recentLogLines": 0})).unwrap();
        assert!(resolved.enabled);
        assert!(recent_log_buffer(&resolved).is_none());
    }

    #[test]
    fn environment_variable_overrides_the_file_both_ways() {
        let off = resolve(serde_json::json!({"enabled": false})).unwrap();
        let on = resolve(serde_json::json!({"enabled": true})).unwrap();
        let forced_on = off.clone().with_environment_override(Some(" ON ")).unwrap();
        assert!(forced_on.enabled);
        assert_eq!(forced_on.environment_override, Some(true));
        let forced_off = on.clone().with_environment_override(Some("off")).unwrap();
        assert!(!forced_off.enabled);
        assert_eq!(forced_off.environment_override, Some(false));
        assert_eq!(on.clone().with_environment_override(None).unwrap(), on);
        assert_eq!(
            on.clone().with_environment_override(Some("  ")).unwrap(),
            on
        );
        let error = on
            .with_environment_override(Some("true"))
            .unwrap_err()
            .to_string();
        assert!(error.contains(CRASH_DIAGNOSTICS_ENV), "{error}");
    }

    #[test]
    fn slot_is_cleared_before_writing_so_panics_suppressed_during_the_write_survive() {
        let limiter = ReportLimiter::new(10_000);
        let slot = Mutex::new(Some(PanicCapture::capture("older", None)));
        let mut written = Vec::new();
        admit_and_report(
            &limiter,
            &slot,
            PanicCapture::capture("first", None),
            false,
            0,
            |capture, suppressed| {
                // 写报告之前，比本份更早的缓存已被清掉。 / The older cached panic is gone before the write starts.
                assert!(slot.try_lock().unwrap().is_none());
                // 模拟符号化与写盘期间另一个线程被限流的 panic。 / Another thread is suppressed during the write.
                admit_and_report(
                    &limiter,
                    &slot,
                    PanicCapture::capture("during-write", None),
                    false,
                    1,
                    |_, _| panic!("the second panic must be rate-limited"),
                );
                written.push((capture.message.clone(), suppressed));
            },
        );
        assert_eq!(written, [("first".to_string(), 0)]);
        // 写盘期间进槽的那份保留给致命路径补写。 / The panic cached during the write is kept for a fatal flush.
        assert_eq!(
            slot.try_lock()
                .unwrap()
                .as_ref()
                .map(|capture| capture.message.as_str()),
            Some("during-write")
        );

        // 致命 panic 不受限流；它写报告前同样先清槽（补写由调用方在此之前完成）。
        // A fatal panic bypasses the limit and also clears the slot before writing (the caller flushes it first).
        let mut fatal = Vec::new();
        admit_and_report(
            &limiter,
            &slot,
            PanicCapture::capture("fatal", None),
            true,
            2,
            |capture, suppressed| fatal.push((capture.message.clone(), suppressed)),
        );
        assert_eq!(fatal, [("fatal".to_string(), 1)]);
        assert!(slot.try_lock().unwrap().is_none());
    }

    #[test]
    fn fatal_panics_are_main_thread_or_non_unwinding() {
        assert_eq!(
            fatal_reason(Some("main"), "boom"),
            Some("main-thread-panic")
        );
        assert_eq!(fatal_reason(Some("tokio-runtime-worker"), "boom"), None);
        assert_eq!(fatal_reason(None, "boom"), None);
        for message in NOUNWIND_PANIC_MESSAGES {
            assert_eq!(
                fatal_reason(Some("tokio-runtime-worker"), &format!("{message} (extra)")),
                Some("non-unwinding-panic")
            );
        }
        assert_eq!(
            fatal_reason(
                Some("worker"),
                "boom: panic in a function that cannot unwind"
            ),
            None
        );
    }

    #[test]
    fn self_test_values_are_strict() {
        assert_eq!(SelfTest::parse(None).unwrap(), None);
        assert_eq!(SelfTest::parse(Some("")).unwrap(), None);
        assert_eq!(
            SelfTest::parse(Some("panic")).unwrap(),
            Some(SelfTest::Panic)
        );
        assert_eq!(
            SelfTest::parse(Some("abort")).unwrap(),
            Some(SelfTest::Abort)
        );
        assert_eq!(
            SelfTest::parse(Some("access-violation")).unwrap(),
            Some(SelfTest::AccessViolation)
        );
        for mode in [
            SelfTest::Panic,
            SelfTest::ThreadPanic,
            SelfTest::PanicAfterThreadPanic,
            SelfTest::Abort,
            SelfTest::AccessViolation,
            SelfTest::StackOverflow,
        ] {
            assert_eq!(SelfTest::parse(Some(mode.as_str())).unwrap(), Some(mode));
        }
        assert!(SelfTest::parse(Some("segfault")).is_err());
    }
}
