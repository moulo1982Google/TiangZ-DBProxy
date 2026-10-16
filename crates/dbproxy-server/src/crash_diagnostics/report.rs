//! 崩溃报告的文件命名、文本格式与保留上限。这里只有纯函数和文件操作，不依赖全局状态。
//! Crash report naming, text format and retention. Pure functions and file operations only; no global state.

use std::{
    fmt::Write as _,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// 报告与转储文件名的固定前缀；保留清理只处理带这个前缀的文件。
/// Fixed prefix of report and dump names; retention only touches files with this prefix.
pub const FILE_PREFIX: &str = "crash-dbproxy-";
pub const REPORT_EXTENSION: &str = "txt";
pub const DUMP_EXTENSION: &str = "dmp";
/// 报告格式版本；字段增删时递增。 / Report format version; bump when fields change.
pub const REPORT_FORMAT_VERSION: u32 = 1;
pub const PROCESS_NAME: &str = "dbproxy";

/// UTC 时间，精确到毫秒；不引入日期库。 / UTC time with millisecond precision, without a date crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtcTimestamp {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millisecond: u32,
}

impl UtcTimestamp {
    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }

    pub fn from_system_time(time: SystemTime) -> Self {
        let (seconds, millisecond) = match time.duration_since(UNIX_EPOCH) {
            Ok(since) => (since.as_secs() as i64, since.subsec_millis()),
            Err(_) => (0, 0),
        };
        let days = seconds.div_euclid(86_400);
        let of_day = seconds.rem_euclid(86_400);
        let (year, month, day) = civil_from_days(days);
        Self {
            year,
            month,
            day,
            hour: (of_day / 3_600) as u32,
            minute: (of_day % 3_600 / 60) as u32,
            second: (of_day % 60) as u32,
            millisecond,
        }
    }

    /// 文件名用的紧凑格式，例如 `20261016T031500.123Z`（不含冒号，Windows 可用）。
    /// Compact form for file names, e.g. `20261016T031500.123Z` (no colons, valid on Windows).
    pub fn file_stamp(&self) -> String {
        format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}.{:03}Z",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond
        )
    }

    /// 报告正文用的 ISO 8601，例如 `2026-10-16T03:15:00.123Z`。 / ISO 8601 for the report body.
    pub fn iso8601(&self) -> String {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond
        )
    }
}

/// Howard Hinnant 的公历换算。 / Howard Hinnant's days-to-civil conversion.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month as u32, day as u32)
}

/// `crash-dbproxy-<pid>-<UTC 时间>`，不含扩展名。 / `crash-dbproxy-<pid>-<UTC time>` without extension.
pub fn file_stem(pid: u32, time: &UtcTimestamp) -> String {
    format!("{FILE_PREFIX}{pid}-{}", time.file_stamp())
}

/// 最近日志在报告里的状态。 / State of the recent-log section in a report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecentLogSection {
    /// `recentLogLines` 为 0。 / `recentLogLines` is 0.
    Disabled,
    /// 崩溃时缓冲正被占用，未能读取。 / The buffer was busy at crash time.
    Busy,
    Lines {
        capacity: usize,
        lines: Vec<String>,
    },
}

/// 所有报告共有的头部。 / Header shared by every report kind.
#[derive(Clone, Debug)]
pub struct ReportHeader {
    pub version: &'static str,
    pub pid: u32,
    pub time: UtcTimestamp,
    pub kind: &'static str,
}

impl ReportHeader {
    pub fn new(pid: u32, time: UtcTimestamp, kind: &'static str) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            pid,
            time,
            kind,
        }
    }
}

pub struct PanicDetails<'a> {
    pub thread: &'a str,
    pub message: &'a str,
    pub location: Option<String>,
    pub backtrace: &'a str,
    /// 上一份报告之后因限流跳过的 panic 个数。 / Panics skipped by the rate limit since the previous report.
    pub suppressed_since_last: u64,
}

pub struct NativeDetails<'a> {
    pub thread_id: u32,
    pub exception_code: u32,
    pub exception_address: usize,
    /// 访问冲突时的 (操作, 目标地址)。 / (operation, target address) for access violations.
    pub access: Option<(&'static str, usize)>,
    pub minidump: &'a str,
}

fn write_header(text: &mut String, header: &ReportHeader) {
    let _ = writeln!(text, "TiangZ crash report");
    let _ = writeln!(text, "format_version: {REPORT_FORMAT_VERSION}");
    let _ = writeln!(text, "process: {PROCESS_NAME}");
    let _ = writeln!(text, "version: {}", header.version);
    let _ = writeln!(text, "pid: {}", header.pid);
    let _ = writeln!(
        text,
        "platform: {}-{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let _ = writeln!(text, "time_utc: {}", header.time.iso8601());
    let _ = writeln!(text, "kind: {}", header.kind);
}

/// 单行字段里的换行改成“换行 + 两个空格”，保持每个字段名都在行首。
/// Newlines inside a single-line field become "newline + two spaces" so every key stays at a line start.
fn field_value(value: &str) -> String {
    value.replace('\n', "\n  ")
}

fn write_recent_logs(text: &mut String, recent: &RecentLogSection) {
    text.push('\n');
    match recent {
        RecentLogSection::Disabled => {
            let _ = writeln!(text, "recent_logs: disabled (recentLogLines = 0)");
        }
        RecentLogSection::Busy => {
            let _ = writeln!(text, "recent_logs: unavailable (buffer busy at crash time)");
        }
        RecentLogSection::Lines { capacity, lines } => {
            let _ = writeln!(text, "recent_logs: {} of {capacity}", lines.len());
            for line in lines {
                let _ = writeln!(text, "{line}");
            }
        }
    }
}

pub fn render_panic_report(
    header: &ReportHeader,
    details: &PanicDetails<'_>,
    recent: &RecentLogSection,
) -> String {
    let mut text = String::with_capacity(8 * 1024);
    write_header(&mut text, header);
    let _ = writeln!(text, "thread: {}", field_value(details.thread));
    let _ = writeln!(text, "message: {}", field_value(details.message));
    let _ = writeln!(
        text,
        "location: {}",
        details.location.as_deref().unwrap_or("unknown")
    );
    let _ = writeln!(
        text,
        "suppressed_since_last: {}",
        details.suppressed_since_last
    );
    let _ = writeln!(text);
    let _ = writeln!(text, "backtrace:");
    let _ = writeln!(text, "{}", details.backtrace.trim_end());
    write_recent_logs(&mut text, recent);
    text
}

pub fn render_native_report(
    header: &ReportHeader,
    details: &NativeDetails<'_>,
    recent: &RecentLogSection,
) -> String {
    let mut text = String::with_capacity(8 * 1024);
    write_header(&mut text, header);
    let _ = writeln!(text, "thread_id: {}", details.thread_id);
    let code = details.exception_code;
    match exception_name(code) {
        Some(name) => {
            let _ = writeln!(text, "exception_code: 0x{code:08X} ({name})");
        }
        None => {
            let _ = writeln!(text, "exception_code: 0x{code:08X}");
        }
    }
    let _ = writeln!(
        text,
        "exception_address: 0x{:016X}",
        details.exception_address
    );
    if let Some((operation, target)) = details.access {
        let _ = writeln!(text, "access_violation: {operation} at 0x{target:016X}");
    }
    let _ = writeln!(text, "minidump: {}", field_value(details.minidump));
    write_recent_logs(&mut text, recent);
    text
}

/// 常见 Windows 异常码的名字。 / Names of common Windows exception codes.
pub fn exception_name(code: u32) -> Option<&'static str> {
    Some(match code {
        0xC000_0005 => "EXCEPTION_ACCESS_VIOLATION",
        0xC000_00FD => "EXCEPTION_STACK_OVERFLOW",
        0xC000_0409 => "STATUS_STACK_BUFFER_OVERRUN",
        0xC000_001D => "EXCEPTION_ILLEGAL_INSTRUCTION",
        0xC000_0094 => "EXCEPTION_INT_DIVIDE_BY_ZERO",
        0xC000_0096 => "EXCEPTION_PRIV_INSTRUCTION",
        0xC000_0006 => "EXCEPTION_IN_PAGE_ERROR",
        0xC000_0374 => "STATUS_HEAP_CORRUPTION",
        0x8000_0003 => "EXCEPTION_BREAKPOINT",
        _ => return None,
    })
}

/// 访问冲突第一个参数的含义。 / Meaning of the first access-violation parameter.
pub fn access_operation(kind: usize) -> &'static str {
    match kind {
        0 => "read",
        1 => "write",
        8 => "execute",
        _ => "unknown access",
    }
}

/// 新建文件写入内容；同名已存在时加 `-2`、`-3` 等后缀，不覆盖已有报告。
/// Create a new file; on a name clash append `-2`, `-3`, ... so an existing report is never overwritten.
pub fn create_new_file(
    directory: &Path,
    stem: &str,
    extension: &str,
) -> io::Result<(PathBuf, fs::File)> {
    let mut last_error = None;
    for attempt in 1..=16u32 {
        let name = if attempt == 1 {
            format!("{stem}.{extension}")
        } else {
            format!("{stem}-{attempt}.{extension}")
        };
        let path = directory.join(name);
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        // Unix 上报告与转储只给属主读写；Windows 继承目录 ACL。
        // On Unix reports and dumps are owner-only; on Windows they inherit the directory ACL.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => last_error = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("no free crash report name")))
}

pub fn write_new_text_file(
    directory: &Path,
    stem: &str,
    extension: &str,
    contents: &str,
) -> io::Result<PathBuf> {
    let (path, mut file) = create_new_file(directory, stem, extension)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(path)
}

/// 按修改时间保留最新的 `keep` 个同类文件，删除更旧的；只处理 `crash-dbproxy-*.<extension>`。
/// 返回删除的个数。单个文件删除失败不影响其余文件。
/// Keep the newest `keep` files of one kind by modification time and delete older ones; only
/// `crash-dbproxy-*.<extension>` is touched. Returns how many were deleted; one failed removal does not stop the rest.
pub fn prune_oldest(directory: &Path, extension: &str, keep: usize) -> io::Result<usize> {
    let mut files = list_crash_files(directory, extension)?;
    if files.len() <= keep {
        return Ok(0);
    }
    files.sort();
    let excess = files.len() - keep;
    let mut deleted = 0;
    for (_, _, path) in files.into_iter().take(excess) {
        if fs::remove_file(path).is_ok() {
            deleted += 1;
        }
    }
    Ok(deleted)
}

/// `crash-dbproxy-*.<extension>` 的个数。 / Number of `crash-dbproxy-*.<extension>` files.
pub fn count_crash_files(directory: &Path, extension: &str) -> io::Result<usize> {
    Ok(list_crash_files(directory, extension)?.len())
}

fn list_crash_files(
    directory: &Path,
    extension: &str,
) -> io::Result<Vec<(SystemTime, String, PathBuf)>> {
    let suffix = format!(".{extension}");
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(FILE_PREFIX) || !name.ends_with(&suffix) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        files.push((modified, name.to_owned(), entry.path()));
    }
    Ok(files)
}

/// 创建崩溃目录（含上级）。Unix 上新建的目录权限为 0700（再受 umask 影响）；已存在的目录不改权限。
/// Windows 继承上级 ACL。
/// Create the crash directory and its parents. On Unix newly created directories get 0700 (subject to umask);
/// existing directories keep their permissions. Windows inherits the parent ACL.
pub fn create_private_dir_all(directory: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(directory)
}

/// 两份 panic 报告之间的最小间隔。可存活的 panic（Tokio 任务、请求隔离）可能连续发生，
/// 每份报告都要符号化回溯、建文件、fsync，不限流会拖慢工作线程。
/// Minimum interval between two panic reports. Survivable panics (Tokio tasks, request isolation) can repeat, and
/// each report symbolises a backtrace, creates a file and fsyncs; without a limit that would stall worker threads.
pub const PANIC_REPORT_MIN_INTERVAL_MS: u64 = 10_000;

/// panic 报告限流：间隔内的 panic 只计数，下一份报告写出被跳过的个数。
/// Panic-report rate limit: panics inside the interval are only counted; the next report states how many.
#[derive(Debug)]
pub struct ReportLimiter {
    interval_ms: u64,
    last_ms: std::sync::atomic::AtomicU64,
    suppressed: std::sync::atomic::AtomicU64,
}

impl ReportLimiter {
    const NEVER: u64 = u64::MAX;

    pub const fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            last_ms: std::sync::atomic::AtomicU64::new(Self::NEVER),
            suppressed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// `now_ms` 为单调时钟毫秒。允许写报告时返回此前被跳过的个数（并清零），否则返回 None 并计数。
    /// `now_ms` is a monotonic clock in milliseconds. Returns the number skipped so far (and resets it) when a
    /// report may be written; otherwise counts this panic and returns None.
    pub fn admit(&self, now_ms: u64) -> Option<u64> {
        use std::sync::atomic::Ordering;
        let mut last = self.last_ms.load(Ordering::Acquire);
        loop {
            if last != Self::NEVER && now_ms.saturating_sub(last) < self.interval_ms {
                self.suppressed.fetch_add(1, Ordering::AcqRel);
                return None;
            }
            match self.last_ms.compare_exchange_weak(
                last,
                now_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(self.suppressed.swap(0, Ordering::AcqRel)),
                Err(current) => last = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn timestamp(seconds: u64, millis: u64) -> UtcTimestamp {
        UtcTimestamp::from_system_time(
            UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_millis(millis),
        )
    }

    #[test]
    fn utc_timestamps_match_known_dates() {
        assert_eq!(timestamp(0, 0).iso8601(), "1970-01-01T00:00:00.000Z");
        // 2000-02-29 闰日 / leap day
        assert_eq!(
            timestamp(951_782_400, 7).iso8601(),
            "2000-02-29T00:00:00.007Z"
        );
        // 2026-10-16T03:15:00.123Z
        let time = timestamp(1_792_120_500, 123);
        assert_eq!(time.iso8601(), "2026-10-16T03:15:00.123Z");
        assert_eq!(time.file_stamp(), "20261016T031500.123Z");
        assert_eq!(
            file_stem(4321, &time),
            "crash-dbproxy-4321-20261016T031500.123Z"
        );
        assert_eq!(
            timestamp(4_102_444_799, 999).iso8601(),
            "2099-12-31T23:59:59.999Z"
        );
    }

    #[test]
    fn panic_report_has_stable_keys_backtrace_and_recent_logs() {
        let header = ReportHeader::new(77, timestamp(1_792_120_500, 123), "panic");
        let report = render_panic_report(
            &header,
            &PanicDetails {
                thread: "main",
                message: "boom\nsecond line",
                location: Some("src/main.rs:3:5".into()),
                backtrace: "   0: frame_one\n   1: frame_two\n",
                suppressed_since_last: 3,
            },
            &RecentLogSection::Lines {
                capacity: 200,
                lines: vec!["INFO first".into(), "WARN second".into()],
            },
        );
        let expected = format!(
            "TiangZ crash report\nformat_version: 1\nprocess: dbproxy\nversion: {}\npid: 77\nplatform: {}-{}\n\
             time_utc: 2026-10-16T03:15:00.123Z\nkind: panic\nthread: main\nmessage: boom\n  second line\n\
             location: src/main.rs:3:5\nsuppressed_since_last: 3\n\nbacktrace:\n   0: frame_one\n   1: frame_two\n\n\
             recent_logs: 2 of 200\nINFO first\nWARN second\n",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        assert_eq!(report, expected);
    }

    #[test]
    fn native_report_names_the_exception_and_access() {
        let header = ReportHeader::new(9, timestamp(0, 0), "native-exception");
        let report = render_native_report(
            &header,
            &NativeDetails {
                thread_id: 1234,
                exception_code: 0xC000_0005,
                exception_address: 0x7FF6_0000_1000,
                access: Some((access_operation(1), 0x10)),
                minidump: "crash-dbproxy-9-19700101T000000.000Z.dmp",
            },
            &RecentLogSection::Busy,
        );
        assert!(report.contains("kind: native-exception\n"));
        assert!(report.contains("thread_id: 1234\n"));
        assert!(report.contains("exception_code: 0xC0000005 (EXCEPTION_ACCESS_VIOLATION)\n"));
        assert!(report.contains("exception_address: 0x00007FF600001000\n"));
        assert!(report.contains("access_violation: write at 0x0000000000000010\n"));
        assert!(report.contains("minidump: crash-dbproxy-9-19700101T000000.000Z.dmp\n"));
        assert!(report.ends_with("recent_logs: unavailable (buffer busy at crash time)\n"));
        assert_eq!(exception_name(0x1234_5678), None);
        assert!(
            render_native_report(
                &header,
                &NativeDetails {
                    thread_id: 1,
                    exception_code: 0x1234_5678,
                    exception_address: 0,
                    access: None,
                    minidump: "not requested",
                },
                &RecentLogSection::Disabled,
            )
            .contains("exception_code: 0x12345678\n")
        );
    }

    fn temp_directory(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "tiangz-dbproxy-crash-{name}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn new_files_never_overwrite_existing_reports() {
        let directory = temp_directory("names");
        let first = write_new_text_file(&directory, "crash-dbproxy-1-x", "txt", "a").unwrap();
        let second = write_new_text_file(&directory, "crash-dbproxy-1-x", "txt", "b").unwrap();
        assert_eq!(first.file_name().unwrap(), "crash-dbproxy-1-x.txt");
        assert_eq!(second.file_name().unwrap(), "crash-dbproxy-1-x-2.txt");
        assert_eq!(fs::read_to_string(first).unwrap(), "a");
        assert_eq!(fs::read_to_string(second).unwrap(), "b");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pruning_keeps_the_newest_files_of_one_kind_and_ignores_others() {
        let directory = temp_directory("prune");
        let base = SystemTime::now() - Duration::from_secs(3_600);
        for index in 0..5u64 {
            let path = directory.join(format!("crash-dbproxy-{index}-stamp.txt"));
            fs::write(&path, "report").unwrap();
            let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_modified(base + Duration::from_secs(index * 60))
                .unwrap();
        }
        fs::write(directory.join("crash-dbproxy-9-stamp.dmp"), "dump").unwrap();
        fs::write(directory.join("other-file.txt"), "keep").unwrap();
        assert_eq!(prune_oldest(&directory, "txt", 2).unwrap(), 3);
        let mut remaining = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        remaining.sort();
        assert_eq!(
            remaining,
            [
                "crash-dbproxy-3-stamp.txt",
                "crash-dbproxy-4-stamp.txt",
                "crash-dbproxy-9-stamp.dmp",
                "other-file.txt"
            ]
        );
        assert_eq!(prune_oldest(&directory, "txt", 2).unwrap(), 0);
        assert_eq!(count_crash_files(&directory, "txt").unwrap(), 2);
        assert_eq!(count_crash_files(&directory, "dmp").unwrap(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn limiter_admits_one_report_per_interval_and_counts_the_rest() {
        let limiter = ReportLimiter::new(10_000);
        assert_eq!(limiter.admit(5), Some(0));
        assert_eq!(limiter.admit(6), None);
        assert_eq!(limiter.admit(10_004), None);
        assert_eq!(limiter.admit(10_005), Some(2));
        assert_eq!(limiter.admit(20_004), None);
        assert_eq!(limiter.admit(40_000), Some(1));
        // 时钟回退不会把间隔算成负数而放行。 / A clock that goes backwards never opens the gate early.
        assert_eq!(limiter.admit(39_000), None);
    }

    #[test]
    fn concurrent_panics_inside_one_interval_admit_exactly_one_report() {
        let limiter = std::sync::Arc::new(ReportLimiter::new(10_000));
        let admitted = (0..8)
            .map(|_| {
                let limiter = std::sync::Arc::clone(&limiter);
                std::thread::spawn(move || {
                    (0..100)
                        .filter_map(|_| limiter.admit(1_000))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(admitted.len(), 1, "{admitted:?}");
        // 首份报告的 CAS 与 swap(0) 之间，其他线程的计数可能被它带走，所以只核对总数：
        // 800 次中 1 次写出，其余 799 次一定出现在某一份报告的 suppressed 里，不多不少。
        // Another thread may count between the first report's CAS and its swap(0), so only the total is checked:
        // of 800 panics one is written and the other 799 appear in exactly one report's suppressed count.
        let last = limiter.admit(11_000).unwrap();
        assert_eq!(admitted.iter().sum::<u64>() + last, 799);
    }

    #[cfg(unix)]
    #[test]
    fn unix_reports_and_new_directories_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_directory("modes");
        let directory = root.join("nested").join("crash");
        create_private_dir_all(&directory).unwrap();
        let dir_mode = fs::metadata(&directory).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode & 0o077, 0, "directory mode {dir_mode:o}");
        let path = write_new_text_file(&directory, "crash-dbproxy-1-x", "txt", "r").unwrap();
        let file_mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600);
        fs::remove_dir_all(root).unwrap();
    }
}
