//! Fault acceptance against real server processes (2026-09-23):
//! F09 PostgreSQL connection limit at startup and while running; F15 a PostgreSQL connection and
//! cleanup fault on tenant A while tenant B keeps steady traffic in the same process; F04 the
//! server force-killed repeatedly while receipt cleanup is deleting.
//! Superusers ignore `CONNECTION LIMIT`, so tenant A logs in as a separate non-superuser role that
//! owns its database. Every test creates its own databases and fails if they already exist.
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tiangz_dbproxy_core::{RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome};
use tiangz_dbproxy_server::MAINTENANCE_POSTGRES_CONNECTIONS;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN_A: &str = "fault-tenant-a-token";
const TOKEN_B: &str = "fault-tenant-b-token";
// Tenant configs below use 2 shards and 2 primary read connections.
const BUDGET: usize = 2 + 2 + MAINTENANCE_POSTGRES_CONNECTIONS;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Env {
    admin_base: String,
    limited_base: String,
    role: String,
    run_id: String,
    artifacts: PathBuf,
    redis: [String; 2],
    cache: [String; 2],
}

fn env() -> Env {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    Env {
        admin_base: var("FAULT_PG_ADMIN_BASE"),
        limited_base: var("FAULT_PG_LIMITED_BASE"),
        role: var("FAULT_LIMITED_ROLE"),
        run_id: var("FAULT_RUN_ID"),
        artifacts: PathBuf::from(var("DBPROXY_ACCEPTANCE_ARTIFACTS")),
        redis: [var("FAULT_REDIS_A"), var("FAULT_REDIS_B")],
        cache: [var("FAULT_CACHE_A"), var("FAULT_CACHE_B")],
    }
}

fn free_port() -> String {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}

fn tenant_config(dir: &Path, id: &str, endpoint: &str, observability: &str) {
    let config = serde_json::json!({"configVersion":1,
        "server":{"listenAddr":endpoint,"authTokenEnv":format!("FAULT_AUTH_{id}"),"maxConnections":16},
        "runtime":{"workerThreads":4},
        "storage":{"backend":"postgresRedis","postgresUrlEnv":format!("FAULT_PG_{id}"),"redisUrlEnv":format!("FAULT_REDIS_{id}"),
            "cacheRedisUrlEnv":format!("FAULT_CACHE_{id}"),"shards":2,"postgresReadConnections":2},
        "observability":{"listenAddr":observability}});
    std::fs::write(
        dir.join(format!("{id}.json")),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
}

fn deployment(dir: &Path, endpoint: &str, ids: &[&str]) -> PathBuf {
    let tenants: Vec<_> = ids
        .iter()
        .map(|id| serde_json::json!({"id": id.to_lowercase(), "config": format!("{id}.json")}))
        .collect();
    let path = dir.join("tenants.json");
    std::fs::write(&path,serde_json::to_vec_pretty(&serde_json::json!({"configVersion":1,"listenAddr":endpoint,"maxConnections":32,"tenants":tenants})).unwrap()).unwrap();
    path
}

fn spawn(deployment: &Path, dir: &Path, label: &str, env: &Env, pg_a: &str, pg_b: &str) -> Server {
    let binary = std::env::var("DBPROXY_ACCEPTANCE_BINARY")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_tiangz-dbproxy-server").into());
    let child = Command::new(binary)
        .arg("--tenants")
        .arg(deployment)
        .env("FAULT_PG_A", pg_a)
        .env("FAULT_PG_B", pg_b)
        .env("FAULT_AUTH_A", TOKEN_A)
        .env("FAULT_AUTH_B", TOKEN_B)
        .env("FAULT_REDIS_A", &env.redis[0])
        .env("FAULT_REDIS_B", &env.redis[1])
        .env("FAULT_CACHE_A", &env.cache[0])
        .env("FAULT_CACHE_B", &env.cache[1])
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(dir.join(format!("server-{label}.stdout"))).unwrap())
        .stderr(std::fs::File::create(dir.join(format!("server-{label}.stderr"))).unwrap())
        .spawn()
        .unwrap();
    Server(child)
}

fn server_log(dir: &Path, label: &str) -> String {
    ["stdout", "stderr"]
        .iter()
        .map(|kind| {
            std::fs::read_to_string(dir.join(format!("server-{label}.{kind}"))).unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn client(endpoint: &str, token: &str, server: &mut Server) -> DbProxyClient {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server exited during startup"
            );
            if let Ok(client) =
                DbProxyClient::connect(ClientConfig::new(endpoint, token, "fault-acceptance")).await
            {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap()
}

async fn sql(url: &str) -> tokio_postgres::Client {
    let (sql, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sql
}

async fn metrics(endpoint: &str) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = tokio::net::TcpStream::connect(endpoint).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    })
    .await
    .unwrap()
}

/// Sum of every sample of one counter family, e.g. all `dbproxy_rpc_errors_total{...}` lines.
fn metric_sum(text: &str, family: &str) -> f64 {
    text.lines()
        .filter(|line| {
            line.strip_prefix(family)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn create_database(admin_base: &str, db: &str, owner: &str) {
    sql(&format!("{admin_base}/postgres"))
        .await
        .batch_execute(&format!("CREATE DATABASE \"{db}\" OWNER \"{owner}\""))
        .await
        .unwrap();
}

async fn set_limit(admin: &tokio_postgres::Client, role: &str, limit: i64) {
    admin
        .batch_execute(&format!("ALTER ROLE \"{role}\" CONNECTION LIMIT {limit}"))
        .await
        .unwrap();
}

async fn role_connections(admin: &tokio_postgres::Client, role: &str, db: &str) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND datname=$2",
            &[&role, &db],
        )
        .await
        .unwrap()
        .get(0)
}

/// Cut every existing connection of the role; with the limit at 0 none can come back.
async fn cut_connections(admin: &tokio_postgres::Client, role: &str, db: &str) -> i64 {
    let killed: i64 = admin
        .query_one(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename=$1 AND datname=$2",
            &[&role, &db],
        )
        .await
        .unwrap()
        .get(0);
    wait_until(
        Duration::from_secs(10),
        "role connections to close",
        || async move { role_connections(admin, role, db).await == 0 },
    )
    .await;
    killed
}

async fn wait_until<F, Fut>(limit: Duration, what: &str, mut check: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if check().await {
            return started.elapsed();
        }
        assert!(started.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn count(db: &tokio_postgres::Client, query: &str) -> i64 {
    db.query_one(query, &[]).await.unwrap().get(0)
}

fn write(namespace: &str, key: &str, payload: u8) -> SnapshotWrite {
    SnapshotWrite {
        request_id: format!("{namespace}-{key}"),
        record: RecordKey::new(namespace, key).unwrap(),
        schema: "test".into(),
        schema_version: 1,
        payload: vec![payload],
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    }
}

async fn insert_expired(db: &tokio_postgres::Client, namespace: &str, rows: i64) {
    db.execute(
        "INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
         SELECT $1||'-'||n,$1,n::text,'test',1,'',1,clock_timestamp()-interval '169 hours' FROM generate_series(1,$2::bigint) n",
        &[&namespace, &rows],
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "F09: isolated PG with a non-superuser role, real server process"]
async fn f09_postgres_connection_limit_fails_clearly_and_recovers() {
    let env = env();
    let db = format!("{}_f09", env.run_id);
    let dir = env.artifacts.join("f09");
    std::fs::create_dir_all(&dir).unwrap();
    create_database(&env.admin_base, &db, &env.role).await;
    let admin = sql(&format!("{}/{db}", env.admin_base)).await;
    let limited = format!("{}/{db}", env.limited_base);
    let endpoint = free_port();
    let observability = free_port();
    tenant_config(&dir, "A", &endpoint, &observability);
    let deployment = deployment(&dir, &endpoint, &["A"]);
    let namespace = format!("f09-{}", env.run_id);

    // 1. One connection short of the budget: startup must fail loudly and never serve clients.
    set_limit(&admin, &env.role, BUDGET as i64 - 1).await;
    let mut short = spawn(&deployment, &dir, "short", &env, &limited, &limited);
    let started = Instant::now();
    let exit = loop {
        if let Some(status) = short.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "server kept running without its PostgreSQL connections"
        );
        let served = tokio::time::timeout(
            Duration::from_secs(1),
            DbProxyClient::connect(ClientConfig::new(&endpoint, TOKEN_A, "fault-acceptance")),
        )
        .await;
        assert!(
            !matches!(served, Ok(Ok(_))),
            "server accepted a client while it lacked PostgreSQL connections"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let short_ms = started.elapsed().as_millis() as u64;
    let short_log = server_log(&dir, "short");
    assert!(
        !exit.success(),
        "startup under the limit exited successfully"
    );
    let error_line = short_log
        .lines()
        .find(|line| line.contains("too many connections"))
        .unwrap_or_else(|| {
            panic!("startup failure does not name the connection limit:\n{short_log}")
        })
        .to_string();
    let (admin_ref, role, db_ref) = (&admin, env.role.as_str(), db.as_str());
    let left_open = wait_until(
        Duration::from_secs(10),
        "failed start to release connections",
        || async move { role_connections(admin_ref, role, db_ref).await == 0 },
    )
    .await;
    drop(short);

    // 2. Exactly the budget: startup works and holds exactly the documented connections.
    set_limit(&admin, &env.role, BUDGET as i64).await;
    let mut server = spawn(&deployment, &dir, "run", &env, &limited, &limited);
    let client = client(&endpoint, TOKEN_A, &mut server).await;
    let at_start = role_connections(&admin, &env.role, &db).await;
    assert_eq!(
        at_start, BUDGET as i64,
        "connections at startup differ from the documented budget"
    );
    for n in 0..20u8 {
        assert_eq!(
            client
                .save(write(&namespace, &format!("base-{n}"), n))
                .await
                .unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }
    let before = metrics(&observability).await;

    // 3. Limit 0 and every connection cut: writes must fail quickly and clearly, never succeed.
    set_limit(&admin, &env.role, 0).await;
    let cut = cut_connections(&admin, &env.role, &db).await;
    let fault_started = Instant::now();
    let mut write_errors = Vec::new();
    let (mut read_ok, mut read_errors) = (0, Vec::new());
    let mut slowest_ms = 0u64;
    for n in 0..20u8 {
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            client.save(write(&namespace, &format!("fault-{n}"), n)),
        )
        .await
        .expect("write hung while PostgreSQL refused connections");
        slowest_ms = slowest_ms.max(started.elapsed().as_millis() as u64);
        match result {
            Ok(outcome) => {
                panic!("write reported {outcome:?} while PostgreSQL refused connections")
            }
            Err(error) => write_errors.push(error.to_string()),
        }
        let started = Instant::now();
        let record = RecordKey::new(&namespace, format!("base-{n}")).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), client.load(&record))
            .await
            .expect("authoritative read hung while PostgreSQL refused connections");
        slowest_ms = slowest_ms.max(started.elapsed().as_millis() as u64);
        match result {
            Ok(Some(snapshot)) => {
                assert_eq!(snapshot.payload, vec![n]);
                read_ok += 1;
            }
            Ok(None) => panic!("committed record reported missing during the fault"),
            Err(error) => read_errors.push(error.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        server.0.try_wait().unwrap().is_none(),
        "server exited during the fault"
    );
    let during = metrics(&observability).await;
    let fault_ms = fault_started.elapsed().as_millis() as u64;
    let during_fault = role_connections(&admin, &env.role, &db).await;
    assert_eq!(during_fault, 0);

    // 4. Restore the limit: service recovers without a restart, and no failed write was applied.
    set_limit(&admin, &env.role, BUDGET as i64).await;
    let probes = AtomicU64::new(0);
    let (client_ref, ns, probes_ref) = (&client, namespace.as_str(), &probes);
    let recovered = wait_until(
        Duration::from_secs(15),
        "writes to recover",
        || async move {
            let key = format!("probe-{}", probes_ref.fetch_add(1, Ordering::Relaxed));
            client_ref.save(write(ns, &key, 0)).await.is_ok()
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let failed_applied = count(
        &admin,
        &format!(
            "SELECT count(*) FROM dbproxy_idempotency WHERE request_id LIKE '{namespace}-fault-%'"
        ),
    )
    .await;
    assert_eq!(failed_applied, 0, "a write reported as failed was applied");
    for n in 0..20u8 {
        assert_eq!(
            client
                .save(write(&namespace, &format!("fault-{n}"), n))
                .await
                .unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        let record = RecordKey::new(&namespace, format!("base-{n}")).unwrap();
        assert_eq!(
            client.load(&record).await.unwrap().unwrap().payload,
            vec![n]
        );
    }
    let after_recovery = role_connections(&admin, &env.role, &db).await;
    assert!(after_recovery <= BUDGET as i64);
    let after = metrics(&observability).await;
    for (name, text) in [("before", &before), ("during", &during), ("after", &after)] {
        std::fs::write(dir.join(format!("metrics-{name}.txt")), text).unwrap();
    }
    let result = serde_json::json!({
        "budget": BUDGET,
        "short_start": {"limit": BUDGET - 1, "exit_code": exit.code(), "exit_ms": short_ms,
            "connections_released_ms": left_open.as_millis() as u64, "error_line": error_line},
        "connections_at_start": at_start,
        "fault": {"cut_connections": cut, "duration_ms": fault_ms, "writes_failed": write_errors.len(),
            "reads_ok": read_ok, "reads_failed": read_errors.len(), "slowest_request_ms": slowest_ms,
            "write_errors_sample": write_errors.iter().take(3).collect::<Vec<_>>(),
            "read_errors_sample": read_errors.iter().take(3).collect::<Vec<_>>(),
            "rpc_errors_delta": metric_sum(&during, "dbproxy_rpc_errors_total") - metric_sum(&before, "dbproxy_rpc_errors_total")},
        "recovery": {"first_write_ms": recovered.as_millis() as u64, "failed_writes_applied": failed_applied,
            "retried_failed_writes_applied_once": 20, "connections_after": after_recovery},
    });
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F09_RESULT {result}");
}

#[tokio::test]
#[ignore = "F15: two isolated PG databases, real two-tenant server process, about 3 minutes"]
async fn f15_tenant_a_postgres_fault_leaves_tenant_b_serving() {
    let env = env();
    let (db_a, db_b) = (
        format!("{}_f15a", env.run_id),
        format!("{}_f15b", env.run_id),
    );
    let dir = env.artifacts.join("f15");
    std::fs::create_dir_all(&dir).unwrap();
    let admin_user = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    create_database(&env.admin_base, &db_a, &env.role).await;
    create_database(&env.admin_base, &db_b, &admin_user).await;
    let admin_a = sql(&format!("{}/{db_a}", env.admin_base)).await;
    let admin_b = sql(&format!("{}/{db_b}", env.admin_base)).await;
    let endpoint = free_port();
    let (obs_a, obs_b) = (free_port(), free_port());
    tenant_config(&dir, "A", &endpoint, &obs_a);
    tenant_config(&dir, "B", &endpoint, &obs_b);
    let deployment = deployment(&dir, &endpoint, &["A", "B"]);
    set_limit(&admin_a, &env.role, BUDGET as i64).await;
    let mut server = spawn(
        &deployment,
        &dir,
        "run",
        &env,
        &format!("{}/{db_a}", env.limited_base),
        &format!("{}/{db_b}", env.admin_base),
    );
    let a = client(&endpoint, TOKEN_A, &mut server).await;
    let b = Arc::new(client(&endpoint, TOKEN_B, &mut server).await);
    let (ns_a, ns_b) = (
        format!("f15a-{}", env.run_id),
        format!("f15b-{}", env.run_id),
    );
    for n in 0..10u8 {
        assert_eq!(
            a.save(write(&ns_a, &format!("base-{n}"), n)).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }

    // Tenant B: one write and one authoritative read every 25 ms for the whole test.
    let stop = Arc::new(AtomicBool::new(false));
    let traffic = tokio::spawn({
        let (b, stop, ns_b) = (b.clone(), stop.clone(), ns_b.clone());
        async move {
            let (mut latencies, mut errors, mut n) = (Vec::new(), Vec::new(), 0u64);
            while !stop.load(Ordering::Relaxed) {
                let key = format!("load-{n}");
                let started = Instant::now();
                match b.save(write(&ns_b, &key, (n % 251) as u8)).await {
                    Ok(SnapshotWriteOutcome::Applied {
                        revision: Revision(1),
                    }) => {}
                    other => errors.push(format!("save {key}: {other:?}")),
                }
                latencies.push(started.elapsed().as_micros() as u64);
                let started = Instant::now();
                match b.load(&RecordKey::new(&ns_b, &key).unwrap()).await {
                    Ok(Some(snapshot)) if snapshot.payload == vec![(n % 251) as u8] => {}
                    other => errors.push(format!("load {key}: {other:?}")),
                }
                latencies.push(started.elapsed().as_micros() as u64);
                n += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            (n, latencies, errors)
        }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    let before_a = metrics(&obs_a).await;
    let before_b = metrics(&obs_b).await;

    // Fault on A only; both tenants get expired receipts to clean at the same moment.
    set_limit(&admin_a, &env.role, 0).await;
    let cut = cut_connections(&admin_a, &env.role, &db_a).await;
    insert_expired(&admin_a, &format!("{ns_a}-expired"), 600).await;
    insert_expired(&admin_b, &format!("{ns_b}-expired"), 600).await;
    let expired_a =
        format!("SELECT count(*) FROM dbproxy_idempotency WHERE namespace='{ns_a}-expired'");
    let expired_b =
        format!("SELECT count(*) FROM dbproxy_idempotency WHERE namespace='{ns_b}-expired'");
    let fault_started = Instant::now();
    let (mut a_errors, mut slowest_a_ms) = (0, 0u64);
    loop {
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            a.save(write(&ns_a, &format!("fault-{a_errors}"), 1)),
        )
        .await
        .expect("tenant A write hung during its fault");
        slowest_a_ms = slowest_a_ms.max(started.elapsed().as_millis() as u64);
        assert!(
            result.is_err(),
            "tenant A write succeeded during its fault: {result:?}"
        );
        a_errors += 1;
        let b_drained = count(&admin_b, &expired_b).await == 0;
        if b_drained && fault_started.elapsed() >= Duration::from_secs(10) {
            break;
        }
        assert!(
            fault_started.elapsed() < Duration::from_secs(90),
            "tenant B cleanup did not drain while tenant A was down"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let b_drained_ms = fault_started.elapsed().as_millis() as u64;
    assert_eq!(
        count(&admin_a, &expired_a).await,
        600,
        "tenant A receipts changed while it was down"
    );
    let during_a = metrics(&obs_a).await;
    let during_b = metrics(&obs_b).await;

    // Recover A; its own cleanup then drains its receipts.
    set_limit(&admin_a, &env.role, BUDGET as i64).await;
    let probes = AtomicU64::new(0);
    let (a_ref, ns, probes_ref) = (&a, ns_a.as_str(), &probes);
    let recovered = wait_until(
        Duration::from_secs(15),
        "tenant A to recover",
        || async move {
            let key = format!("probe-{}", probes_ref.fetch_add(1, Ordering::Relaxed));
            a_ref.save(write(ns, &key, 0)).await.is_ok()
        },
    )
    .await;
    let (admin_ref, query) = (&admin_a, expired_a.as_str());
    let a_drained = wait_until(
        Duration::from_secs(90),
        "tenant A cleanup to resume",
        || async move { count(admin_ref, query).await == 0 },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    let (b_requests, mut latencies, b_errors) = traffic.await.unwrap();
    assert!(
        b_errors.is_empty(),
        "tenant B saw errors: {:?}",
        &b_errors[..b_errors.len().min(5)]
    );
    latencies.sort_unstable();
    let p99_us = latencies[(latencies.len() * 99).div_ceil(100) - 1];
    let max_us = *latencies.last().unwrap();
    assert!(
        max_us < 1_000_000,
        "tenant B request took {max_us} us during tenant A's fault"
    );

    // Nothing crossed tenants, A's committed data survived, and failed A writes were not applied.
    let foreign_in_a = count(
        &admin_a,
        "SELECT count(*) FROM dbproxy_idempotency WHERE namespace LIKE 'f15b-%'",
    )
    .await;
    let foreign_in_b = count(
        &admin_b,
        "SELECT count(*) FROM dbproxy_idempotency WHERE namespace LIKE 'f15a-%'",
    )
    .await;
    assert_eq!((foreign_in_a, foreign_in_b), (0, 0));
    assert_eq!(
        count(
            &admin_a,
            &format!(
                "SELECT count(*) FROM dbproxy_idempotency WHERE request_id LIKE '{ns_a}-fault-%'"
            )
        )
        .await,
        0,
        "a tenant A write reported as failed was applied"
    );
    for n in 0..10u8 {
        let record = RecordKey::new(&ns_a, format!("base-{n}")).unwrap();
        assert_eq!(a.load(&record).await.unwrap().unwrap().payload, vec![n]);
    }
    let after_a = metrics(&obs_a).await;
    let after_b = metrics(&obs_b).await;
    let rpc_errors = |before: &str, after: &str| {
        metric_sum(after, "dbproxy_rpc_errors_total")
            - metric_sum(before, "dbproxy_rpc_errors_total")
    };
    let cleanup_failures = |before: &str, after: &str| {
        metric_sum(after, "dbproxy_receipt_cleanup_failures_total")
            - metric_sum(before, "dbproxy_receipt_cleanup_failures_total")
    };
    let (a_rpc, b_rpc) = (
        rpc_errors(&before_a, &during_a),
        rpc_errors(&before_b, &after_b),
    );
    assert!(
        a_rpc > 0.0,
        "tenant A errors are not attributed to tenant A"
    );
    assert_eq!(b_rpc, 0.0, "tenant B metrics recorded errors");
    let log = server_log(&dir, "run");
    let warn_b = log
        .lines()
        .filter(|line| line.contains("WARN") && line.contains("tenant=b"))
        .count();
    let warn_a = log
        .lines()
        .filter(|line| line.contains("WARN") && line.contains("tenant=a"))
        .count();
    assert_eq!(
        warn_b, 0,
        "tenant B logged warnings during tenant A's fault"
    );
    for (name, text) in [
        ("a-before", &before_a),
        ("a-during", &during_a),
        ("a-after", &after_a),
        ("b-before", &before_b),
        ("b-during", &during_b),
        ("b-after", &after_b),
    ] {
        std::fs::write(dir.join(format!("metrics-{name}.txt")), text).unwrap();
    }
    let result = serde_json::json!({
        "budget_a": BUDGET, "cut_connections_a": cut,
        "fault": {"a_failed_writes": a_errors, "slowest_a_ms": slowest_a_ms, "b_cleanup_drained_ms": b_drained_ms,
            "a_expired_untouched": 600, "a_rpc_errors": a_rpc,
            "a_cleanup_failures": cleanup_failures(&before_a, &during_a), "b_cleanup_failures": cleanup_failures(&before_b, &during_b)},
        "recovery": {"a_first_write_ms": recovered.as_millis() as u64, "a_cleanup_drained_ms": a_drained.as_millis() as u64},
        "tenant_b": {"iterations": b_requests, "requests": latencies.len(), "errors": 0, "p99_ms": p99_us as f64 / 1000.0, "max_ms": max_us as f64 / 1000.0, "rpc_errors": b_rpc},
        "log_warnings": {"tenant_a": warn_a, "tenant_b": warn_b},
        "cross_tenant_rows": 0,
    });
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F15_RESULT {result}");
}

/// Exact row count of every table except ordinary receipts, keyed by table name.
async fn protected_counts(db: &tokio_postgres::Client) -> std::collections::BTreeMap<String, i64> {
    let query: String = db
        .query_one(
            "SELECT string_agg(format('SELECT %L::text, count(*) FROM %I', table_name, table_name), ' UNION ALL ')
             FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'
             AND table_name <> 'dbproxy_idempotency'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    db.query(&query, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
#[ignore = "F04: isolated PG, real server process force-killed repeatedly during receipt cleanup"]
async fn f04_kill_server_during_cleanup_keeps_invariants() {
    const EXPIRED: i64 = 20_000;
    const RECENT: i64 = 5_000;
    const KILLS: usize = 8;
    let env = env();
    let db = format!("{}_f04", env.run_id);
    let dir = env.artifacts.join("f04");
    std::fs::create_dir_all(&dir).unwrap();
    let admin_user = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    create_database(&env.admin_base, &db, &admin_user).await;
    let url = format!("{}/{db}", env.admin_base);
    let admin = sql(&url).await;
    let endpoint = free_port();
    let observability = free_port();
    tenant_config(&dir, "A", &endpoint, &observability);
    let deployment = deployment(&dir, &endpoint, &["A"]);
    let namespace = format!("f04-{}", env.run_id);

    // Business data first (this start also creates the schema), then the receipt fixture.
    let mut server = spawn(&deployment, &dir, "seed", &env, &url, &url);
    let client_a = client(&endpoint, TOKEN_A, &mut server).await;
    for n in 0..20u8 {
        assert_eq!(
            client_a
                .save(write(&namespace, &format!("base-{n}"), n))
                .await
                .unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }
    // The cache-repair worker consumes the repair rows these writes queued; let it finish first,
    // otherwise its normal work shows up as a change in the baseline (run fp_b failed on that).
    let admin_ref = &admin;
    wait_until(
        Duration::from_secs(60),
        "cache repair queue to drain",
        || async move { count(admin_ref, "SELECT count(*) FROM dbproxy_cache_repairs").await == 0 },
    )
    .await;
    drop(client_a);
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    drop(server);
    insert_expired(&admin, &format!("{namespace}-expired"), EXPIRED).await;
    admin
        .execute(
            "INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
             SELECT $1||'-'||n,$1,n::text,'test',1,'',1,clock_timestamp()-interval '23 hours' FROM generate_series(1,$2::bigint) n",
            &[&format!("{namespace}-recent"), &RECENT],
        )
        .await
        .unwrap();
    let expired_query =
        format!("SELECT count(*) FROM dbproxy_idempotency WHERE namespace='{namespace}-expired'");
    let recent_query =
        format!("SELECT count(*) FROM dbproxy_idempotency WHERE namespace='{namespace}-recent'");
    let business_receipts_query = format!(
        "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='{namespace}' AND request_id LIKE '{namespace}-base-%'"
    );
    let protected_before = protected_counts(&admin).await;
    let business_receipts = count(&admin, &business_receipts_query).await;
    assert_eq!(business_receipts, 20);

    // Force-kill at pseudo-random moments while cleanup deletes one 500-row batch per second.
    // A precise "after commit, before the log line" kill needs a code hook that production code
    // does not have; random points over many batches are the closest honest substitute.
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64
        | 1;
    let mut rounds = Vec::new();
    let mut previous_expired = EXPIRED;
    for kill in 0..KILLS {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let after = Duration::from_millis(200 + seed % 2_800);
        let mut server = spawn(&deployment, &dir, &format!("kill-{kill}"), &env, &url, &url);
        let ready = client(&endpoint, TOKEN_A, &mut server).await;
        tokio::time::sleep(after).await;
        server.0.kill().unwrap();
        server.0.wait().unwrap();
        drop(ready);
        drop(server);
        let expired = count(&admin, &expired_query).await;
        let recent = count(&admin, &recent_query).await;
        assert!(
            expired <= previous_expired,
            "expired receipts came back after a kill"
        );
        assert_eq!(
            recent, RECENT,
            "a kill during cleanup touched recent receipts"
        );
        assert_eq!(
            count(&admin, &business_receipts_query).await,
            business_receipts
        );
        assert_eq!(
            protected_counts(&admin).await,
            protected_before,
            "a kill during cleanup changed a protected table"
        );
        rounds.push(
            serde_json::json!({"kill": kill, "after_ready_ms": after.as_millis() as u64,
            "expired_left": expired, "deleted_this_round": previous_expired - expired}),
        );
        previous_expired = expired;
    }

    // A clean final run finishes the job; nothing is re-applied and business data is intact.
    let mut server = spawn(&deployment, &dir, "final", &env, &url, &url);
    let client_a = client(&endpoint, TOKEN_A, &mut server).await;
    let (admin_ref, query) = (&admin, expired_query.as_str());
    let drained = wait_until(
        Duration::from_secs(120),
        "cleanup to finish after the kills",
        || async move { count(admin_ref, query).await == 0 },
    )
    .await;
    for n in 0..20u8 {
        let request = write(&namespace, &format!("base-{n}"), n);
        let record = request.record.clone();
        assert_eq!(
            client_a.save(request).await.unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            },
            "a business write kept in its receipt window was applied again"
        );
        assert_eq!(
            client_a.load(&record).await.unwrap().unwrap().payload,
            vec![n]
        );
    }
    assert_eq!(count(&admin, &recent_query).await, RECENT);
    // Each Duplicate reply queues one cache repair by design (storage lib.rs, receipt replay
    // path); run fp_c compared before the worker consumed them. Wait, then compare everything.
    wait_until(
        Duration::from_secs(60),
        "replay cache repairs to drain",
        || async move { count(admin_ref, "SELECT count(*) FROM dbproxy_cache_repairs").await == 0 },
    )
    .await;
    assert_eq!(protected_counts(&admin).await, protected_before);
    let metrics_text = metrics(&observability).await;
    std::fs::write(dir.join("metrics-final.txt"), &metrics_text).unwrap();
    let deleted_before_final = EXPIRED - previous_expired;
    let result = serde_json::json!({
        "expired_fixture": EXPIRED, "recent_fixture": RECENT, "kills": rounds,
        "deleted_across_kills": deleted_before_final,
        "final_run": {"drained_ms": drained.as_millis() as u64, "deleted": previous_expired,
            "metric_deleted_in_final_process": metric_sum(&metrics_text, "dbproxy_receipt_cleanup_deleted_total")},
        "recent_untouched": RECENT, "business_receipts_untouched": business_receipts,
        "protected_tables": protected_before, "replays_duplicate": 20,
    });
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F04_RESULT {result}");
}
