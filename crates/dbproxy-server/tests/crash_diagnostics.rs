//! 崩溃诊断端到端：在子进程里用自测入口真实崩溃，检查报告、回溯、最近日志、转储与保留上限。
//! 只用内存后端，不连接任何数据库。
//! Crash diagnostics end to end: crash a child process through the self-test hook and check the report, backtrace,
//! recent logs, dumps and retention. Memory backend only; no database is contacted.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime},
};

const TOKEN_ENV: &str = "CRASH_DIAGNOSTICS_TEST_TOKEN";
const TOKEN: &str = "crash-diagnostics-test-token";

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("dbproxy-crash-it-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    fn crash_dir(&self) -> PathBuf {
        self.root.join("crash")
    }

    fn log(&self) -> PathBuf {
        self.root.join("server.log")
    }

    fn read_log(&self) -> String {
        fs::read_to_string(self.log()).unwrap_or_default()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct Server {
    child: Child,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(scratch: &Scratch, crash: serde_json::Value, env: &[(&str, &str)]) -> Server {
    let mut crash = crash;
    crash["directory"] = scratch.crash_dir().to_str().unwrap().into();
    let config = scratch.root.join("config.json");
    fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "configVersion": 1,
            "server": {"listenAddr": "127.0.0.1:0", "authTokenEnv": TOKEN_ENV, "shutdownGraceMs": 1000},
            "runtime": {"workerThreads": 2},
            "storage": {"backend": "memory", "shards": 1},
            "logging": {"defaultFilter": "info"},
            "diagnostics": {"crash": crash}
        }))
        .unwrap(),
    )
    .unwrap();
    let output = fs::File::create(scratch.log()).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_tiangz-dbproxy-server"));
    command
        .args(["--config", config.to_str().unwrap()])
        .current_dir(&scratch.root)
        .env(TOKEN_ENV, TOKEN)
        .env_remove("RUST_LOG")
        .env_remove("TIANGZ_DBPROXY_CRASH_DIAGNOSTICS")
        .env_remove("TIANGZ_DBPROXY_CRASH_SELF_TEST")
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output));
    for (name, value) in env {
        command.env(name, value);
    }
    Server {
        child: command.spawn().unwrap(),
    }
}

fn wait_for_exit(server: &mut Server, scratch: &Scratch, limit: Duration) -> ExitStatus {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "server did not exit in time: {}",
            scratch.read_log()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_log(server: &mut Server, scratch: &Scratch, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let log = scratch.read_log();
        if log.contains(needle) {
            return;
        }
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before logging {needle:?}: {log}"
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {needle:?}: {log}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn files_with_extension(directory: &Path, extension: &str) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|value| value == extension))
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// 预置旧文件，修改时间设为一小时前，用来验证保留上限只删最旧的。
/// Seed old files (modified an hour ago) to check that retention removes only the oldest.
fn seed_old_files(directory: &Path, extension: &str, count: u64) -> Vec<PathBuf> {
    fs::create_dir_all(directory).unwrap();
    let base = SystemTime::now() - Duration::from_secs(3_600);
    (0..count)
        .map(|index| {
            let path = directory.join(format!("crash-dbproxy-1-old{index}.{extension}"));
            fs::write(&path, "old").unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(base + Duration::from_secs(index * 60))
                .unwrap();
            path
        })
        .collect()
}

fn new_report(directory: &Path, pid: u32, extension: &str) -> PathBuf {
    let prefix = format!("crash-dbproxy-{pid}-");
    let matches = files_with_extension(directory, extension)
        .into_iter()
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&prefix)
        })
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "{matches:?}");
    matches.into_iter().next().unwrap()
}

#[test]
fn panic_self_test_writes_report_with_backtrace_and_recent_logs_and_keeps_the_limit() {
    let scratch = Scratch::new("panic");
    let old = seed_old_files(&scratch.crash_dir(), "txt", 3);
    fs::write(scratch.crash_dir().join("unrelated.txt"), "keep").unwrap();
    let mut server = spawn(
        &scratch,
        serde_json::json!({"enabled": true, "recentLogLines": 50, "maxReports": 2}),
        &[("TIANGZ_DBPROXY_CRASH_SELF_TEST", "panic")],
    );
    let pid = server.child.id();
    let status = wait_for_exit(&mut server, &scratch, Duration::from_secs(60));
    let log = scratch.read_log();
    // 原有 panic 语义不变：主线程 panic 仍以 101 退出，原钩子仍打印 panic 信息。
    // Original panic semantics: a main-thread panic still exits with 101 and the previous hook still prints.
    assert_eq!(status.code(), Some(101), "{status}: {log}");
    assert!(log.contains("panicked at"), "{log}");
    assert!(log.contains("crash report written to"), "{log}");

    let report_path = new_report(&scratch.crash_dir(), pid, "txt");
    let report = fs::read_to_string(&report_path).unwrap();
    for expected in [
        "TiangZ crash report\n",
        "format_version: 1\n",
        "process: dbproxy\n",
        &format!("pid: {pid}\n"),
        "kind: panic\n",
        "thread: main\n",
        "message: TiangZ DBProxy crash self-test (TIANGZ_DBPROXY_CRASH_SELF_TEST=panic)\n",
        "location: ",
        "\nbacktrace:\n",
        "trigger_self_test",
        "\nrecent_logs: ",
        "crash diagnostics enabled",
        "TiangZ DBProxy started",
        "crash diagnostics self-test: triggering a deliberate crash",
    ] {
        assert!(
            report.contains(expected),
            "missing {expected:?} in:\n{report}"
        );
    }
    assert!(report.contains("of 50\n"), "{report}");

    // maxReports = 2：新报告 + 最新的一个旧报告，其余旧报告被删除，非本前缀文件不动。
    // maxReports = 2: the new report plus the newest old one remain; other files are untouched.
    let mut remaining = files_with_extension(&scratch.crash_dir(), "txt");
    remaining.retain(|path| path.file_name().unwrap() != "unrelated.txt");
    assert_eq!(remaining.len(), 2, "{remaining:?}");
    assert!(remaining.contains(&report_path));
    assert!(remaining.contains(&old[2]));
    assert!(scratch.crash_dir().join("unrelated.txt").exists());
}

#[test]
fn environment_off_overrides_the_file_and_the_self_test_is_ignored() {
    let scratch = Scratch::new("env-off");
    let mut server = spawn(
        &scratch,
        serde_json::json!({"enabled": true}),
        &[
            ("TIANGZ_DBPROXY_CRASH_DIAGNOSTICS", "off"),
            ("TIANGZ_DBPROXY_CRASH_SELF_TEST", "panic"),
        ],
    );
    wait_for_log(&mut server, &scratch, "TiangZ DBProxy started");
    let log = scratch.read_log();
    assert!(log.contains("crash self-test ignored"), "{log}");
    assert!(!log.contains("crash diagnostics enabled"), "{log}");
    assert!(!scratch.crash_dir().exists());
}

#[test]
fn invalid_switch_or_self_test_values_stop_startup() {
    for (name, env) in [
        (
            "bad-switch",
            [("TIANGZ_DBPROXY_CRASH_DIAGNOSTICS", "yes"), ("UNUSED", "")],
        ),
        (
            "bad-self-test",
            [
                ("TIANGZ_DBPROXY_CRASH_DIAGNOSTICS", "on"),
                ("TIANGZ_DBPROXY_CRASH_SELF_TEST", "segfault"),
            ],
        ),
    ] {
        let scratch = Scratch::new(name);
        let mut server = spawn(&scratch, serde_json::json!({"enabled": false}), &env);
        let status = wait_for_exit(&mut server, &scratch, Duration::from_secs(20));
        let log = scratch.read_log();
        assert!(!status.success(), "{name}: {log}");
        let variable = env[if name == "bad-switch" { 0 } else { 1 }].0;
        assert!(log.contains(variable), "{name}: {log}");
        assert!(!log.contains("TiangZ DBProxy started"), "{name}: {log}");
    }
}

#[cfg(windows)]
mod windows {
    use super::*;

    unsafe extern "system" {
        fn SetErrorMode(mode: u32) -> u32;
    }

    /// 子进程继承错误模式；关掉 Windows 错误报告弹窗，避免测试卡在对话框上。
    /// Children inherit the error mode; suppress the Windows Error Reporting dialog so the test cannot hang on it.
    fn suppress_crash_dialogs() {
        const SEM_FAILCRITICALERRORS: u32 = 0x0001;
        const SEM_NOGPFAULTERRORBOX: u32 = 0x0002;
        // SAFETY: 只修改本进程的错误模式标志。 / Only changes this process's error-mode flags.
        unsafe { SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX) };
    }

    #[test]
    fn access_violation_writes_minidump_and_native_record() {
        suppress_crash_dialogs();
        let scratch = Scratch::new("access-violation");
        let old_dumps = seed_old_files(&scratch.crash_dir(), "dmp", 2);
        let mut server = spawn(
            &scratch,
            serde_json::json!({"enabled": true, "miniDump": true, "maxDumps": 1}),
            &[("TIANGZ_DBPROXY_CRASH_SELF_TEST", "access-violation")],
        );
        let pid = server.child.id();
        let status = wait_for_exit(&mut server, &scratch, Duration::from_secs(120));
        let log = scratch.read_log();
        // 进程仍以原异常码结束。 / The process still ends with the original exception code.
        assert_eq!(
            status.code(),
            Some(0xC000_0005_u32 as i32),
            "{status}: {log}"
        );
        assert!(log.contains("native crash record written to"), "{log}");

        let dump = new_report(&scratch.crash_dir(), pid, "dmp");
        let bytes = fs::read(&dump).unwrap();
        assert!(bytes.starts_with(b"MDMP"), "not a minidump: {dump:?}");
        assert!(bytes.len() > 1024, "dump too small: {}", bytes.len());
        // maxDumps = 1：两个旧转储都被删除。 / maxDumps = 1: both old dumps were removed.
        assert_eq!(
            files_with_extension(&scratch.crash_dir(), "dmp").as_slice(),
            std::slice::from_ref(&dump)
        );
        assert!(old_dumps.iter().all(|path| !path.exists()));

        let record = fs::read_to_string(new_report(&scratch.crash_dir(), pid, "txt")).unwrap();
        for expected in [
            "kind: native-exception\n",
            "exception_code: 0xC0000005 (EXCEPTION_ACCESS_VIOLATION)\n",
            "access_violation: write at 0x0000000000000010\n",
            &format!(
                "minidump: {}\n",
                dump.file_name().unwrap().to_string_lossy()
            ),
            "TiangZ DBProxy started",
        ] {
            assert!(
                record.contains(expected),
                "missing {expected:?} in:\n{record}"
            );
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn abort_self_test_raises_the_core_limit_and_dies_by_sigabrt() {
        let scratch = Scratch::new("abort");
        let mut server = spawn(
            &scratch,
            serde_json::json!({"enabled": true, "coreDump": true}),
            &[("TIANGZ_DBPROXY_CRASH_SELF_TEST", "abort")],
        );
        let status = wait_for_exit(&mut server, &scratch, Duration::from_secs(60));
        let log = scratch.read_log();
        // SIGABRT = 6 on Linux and macOS.
        assert_eq!(status.signal(), Some(6), "{status}: {log}");
        assert!(log.contains("core dump size limit"), "{log}");
        assert!(log.contains("crash diagnostics self-test"), "{log}");
    }
}
