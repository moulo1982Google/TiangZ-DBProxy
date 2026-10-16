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
    DUMP_EXTENSION, FILE_PREFIX, NativeDetails, PROCESS_NAME, PanicDetails, REPORT_EXTENSION,
    REPORT_FORMAT_VERSION, RecentLogSection, ReportHeader, UtcTimestamp, access_operation,
    create_new_file, exception_name, file_stem, prune_oldest, render_native_report,
    render_panic_report, write_new_text_file,
};

use std::{
    backtrace::Backtrace,
    error::Error,
    fmt, fs,
    io::{self, Write},
    panic::PanicHookInfo,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use serde::Deserialize;

use crate::config::ConfigError;

/// 覆盖 `diagnostics.crash.enabled` 的环境变量，取值 `on` 或 `off`（不区分大小写，空值等于未设置）。
/// Environment variable overriding `diagnostics.crash.enabled`: `on` or `off` (case-insensitive; empty = unset).
pub const CRASH_DIAGNOSTICS_ENV: &str = "TIANGZ_DBPROXY_CRASH_DIAGNOSTICS";
/// 仅用于验证：`panic`、`abort` 或 `access-violation`，启用崩溃诊断时启动完成后主动触发。
/// Verification only: `panic`, `abort` or `access-violation`, triggered after startup when crash diagnostics are on.
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
    Panic,
    Abort,
    AccessViolation,
}

impl SelfTest {
    pub fn parse(value: Option<&str>) -> Result<Option<Self>, CrashDiagnosticsError> {
        match value.map(str::trim) {
            None | Some("") => Ok(None),
            Some("panic") => Ok(Some(Self::Panic)),
            Some("abort") => Ok(Some(Self::Abort)),
            Some("access-violation") => Ok(Some(Self::AccessViolation)),
            Some(_) => Err(CrashDiagnosticsError(format!(
                "{CRASH_SELF_TEST_ENV} must be panic, abort or access-violation"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Panic => "panic",
            Self::Abort => "abort",
            Self::AccessViolation => "access-violation",
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
pub fn install(
    settings: &ResolvedCrashDiagnostics,
    recent: Option<Arc<RecentLogs>>,
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
    fs::create_dir_all(&settings.directory).map_err(|error| {
        CrashDiagnosticsError(format!(
            "cannot create crash diagnostics directory {}: {error}",
            settings.directory.display()
        ))
    })?;
    let _ = prune_oldest(&settings.directory, REPORT_EXTENSION, settings.max_reports);
    let _ = prune_oldest(&settings.directory, DUMP_EXTENSION, settings.max_dumps);
    let state = InstalledState {
        directory: settings.directory.clone(),
        recent,
        max_reports: settings.max_reports,
        max_dumps: settings.max_dumps,
        mini_dump: settings.mini_dump,
        self_test,
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
            // 钩子里不能再 panic：所有写入错误都只报告到 stderr，并且不用会 panic 的 eprintln。
            // The hook must not panic: write errors only go to stderr, without the panicking eprintln.
            match write_panic_report(state, info) {
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
        previous(info);
    }));
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

fn write_panic_report(state: &InstalledState, info: &PanicHookInfo<'_>) -> io::Result<PathBuf> {
    let time = UtcTimestamp::now();
    let pid = std::process::id();
    let thread = std::thread::current();
    let location = info.location().map(|location| {
        format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        )
    });
    let backtrace = Backtrace::force_capture().to_string();
    let text = render_panic_report(
        &ReportHeader::new(pid, time, "panic"),
        &PanicDetails {
            thread: thread.name().unwrap_or("<unnamed>"),
            message: panic_message(info),
            location,
            backtrace: &backtrace,
        },
        &recent_section(state),
    );
    let path = write_new_text_file(
        &state.directory,
        &file_stem(pid, &time),
        REPORT_EXTENSION,
        &text,
    )?;
    let _ = prune_oldest(&state.directory, REPORT_EXTENSION, state.max_reports);
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
                    let _ = fs::remove_file(&path);
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
    let _ = prune_oldest(&state.directory, REPORT_EXTENSION, state.max_reports);
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
            Ok(unix::CoreLimit::CappedAtHardLimit(bytes)) => tracing::warn!(
                hard_limit_bytes = bytes,
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

/// 自测入口：只有启用崩溃诊断且设置了 `TIANGZ_DBPROXY_CRASH_SELF_TEST` 时才会主动崩溃；只用于验证。
/// Self-test hook: crashes deliberately only when crash diagnostics are on and `TIANGZ_DBPROXY_CRASH_SELF_TEST` is
/// set. Verification only.
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
        SelfTest::Abort => std::process::abort(),
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
        assert!(SelfTest::parse(Some("segfault")).is_err());
    }
}
