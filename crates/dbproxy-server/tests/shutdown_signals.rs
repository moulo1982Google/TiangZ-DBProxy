#![cfg(unix)]

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};

struct ServerProcess {
    child: Child,
    config: PathBuf,
    log: PathBuf,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_file(&self.config);
        let _ = fs::remove_file(&self.log);
    }
}

async fn exercise_signal(signal: &str) {
    let base =
        std::env::temp_dir().join(format!("dbproxy-shutdown-{}-{signal}", std::process::id()));
    let config = base.with_extension("json");
    let log = base.with_extension("log");
    fs::write(&config, serde_json::to_vec(&serde_json::json!({
        "configVersion":1,
        "server":{"listenAddr":"127.0.0.1:0","authTokenEnv":"SHUTDOWN_TEST_TOKEN","shutdownGraceMs":1000},
        "runtime":{"workerThreads":2},
        "storage":{"backend":"memory","shards":1},
        "logging":{"defaultFilter":"info"}
    })).unwrap()).unwrap();
    let output = fs::File::create(&log).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_tiangz-dbproxy-server"))
        .args(["--config", config.to_str().unwrap()])
        .env("SHUTDOWN_TEST_TOKEN", "shutdown-test-token-1234")
        .env_remove("RUST_LOG")
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let mut server = ServerProcess { child, config, log };
    let deadline = Instant::now() + Duration::from_secs(10);
    let endpoint = loop {
        let log = fs::read_to_string(&server.log).unwrap();
        if let Some(line) = log
            .lines()
            .find(|line| line.contains("TiangZ DBProxy started"))
        {
            break line
                .split("actual_addr=")
                .nth(1)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .to_owned();
        }
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "startup failed: {log}"
        );
        assert!(Instant::now() < deadline, "startup timed out: {log}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // 保持一条已认证连接，验证停止监听之外还会结束存量连接。
    // Keep an authenticated connection alive to exercise connection shutdown too.
    let client = DbProxyClient::connect(ClientConfig::new(
        endpoint,
        "shutdown-test-token-1234",
        "shutdown-test",
    ))
    .await
    .unwrap();
    let sent = Command::new("sh")
        .args([
            "-c",
            "kill -s \"$1\" \"$2\"",
            "shutdown-test",
            signal,
            &server.child.id().to_string(),
        ])
        .status()
        .unwrap();
    assert!(sent.success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            let log = fs::read_to_string(&server.log).unwrap();
            assert!(status.success(), "{signal}: {status}, {log}");
            assert!(log.contains(&format!("signal=\"SIG{signal}\"")), "{log}");
            assert!(log.contains("TiangZ DBProxy stopped"), "{log}");
            assert!(!log.contains("shutdown grace expired"), "{log}");
            break;
        }
        assert!(Instant::now() < deadline, "{signal} did not stop server");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(client);
}

#[tokio::test]
async fn sigterm_stops_server_with_connected_client() {
    exercise_signal("TERM").await;
}

#[tokio::test]
async fn sigint_stops_server_with_connected_client() {
    exercise_signal("INT").await;
}
