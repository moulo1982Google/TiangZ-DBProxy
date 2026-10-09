//! P01: low-rate, two-tenant TCP read matrix. Timed cold-cache eviction is kept outside RPC latency.
use super::*;
use std::io::Write;
use tiangz_dbproxy_core::SnapshotEnvelope;
use tiangz_dbproxy_storage::RedisSnapshotCache;

struct JsonPlan(String);
impl<'a> tokio_postgres::types::FromSql<'a> for JsonPlan {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.to_owned()))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::JSON
    }
}

async fn pg_snapshot(admin: &tokio_postgres::Client) -> String {
    admin.query_one("SELECT json_build_object('wal_lsn',pg_current_wal_lsn()::text,'database',(SELECT row_to_json(d) FROM pg_stat_database d WHERE datname=current_database()),'tables',(SELECT json_agg(t) FROM pg_stat_user_tables t),'client_connections',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend'))::text",&[]).await.unwrap().get(0)
}

async fn timed_read(
    client: &DbProxyClient,
    records: &[RecordKey],
    mode: &str,
) -> (f64, Vec<Option<SnapshotEnvelope>>) {
    let start = Instant::now();
    let values = tokio::time::timeout(Duration::from_secs(2), async {
        if mode == "authority" {
            client.load_multi(records).await
        } else {
            client.load_cached_multi(records, &[]).await
        }
    })
    .await
    .expect("bounded read exceeded 2s")
    .unwrap();
    (start.elapsed().as_secs_f64(), values)
}

fn check(values: &[Option<SnapshotEnvelope>], records: &[RecordKey], tenant: usize) {
    assert_eq!(values.len(), records.len());
    for (value, record) in values.iter().zip(records) {
        if record.key.starts_with("missing-") {
            assert!(value.is_none());
            continue;
        }
        let value = value.as_ref().unwrap();
        assert_eq!(&value.record, record);
        assert_eq!(value.revision, Revision(1));
        assert_eq!(value.payload, vec![10 + tenant as u8; 1024]);
    }
}

fn summary(values: &mut [f64]) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        values[((values.len() as f64 * p).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1)]
            * 1000.0
    };
    serde_json::json!({"count":values.len(),"p50_ms":percentile(0.5),"p95_ms":percentile(0.95),"p99_ms":percentile(0.99),"max_ms":values.last().unwrap()*1000.0})
}

#[tokio::test]
#[ignore = "P01: full defaults take 189 minutes; explicit short settings are only harness smoke"]
async fn p01_two_tenant_batch_read_matrix() {
    let seconds = |key, default| {
        std::env::var(key)
            .ok()
            .map(|s| s.parse::<u64>().unwrap())
            .unwrap_or(default)
    };
    let warmup = seconds("P01_WARMUP_SECONDS", 120);
    let sample = seconds("P01_SAMPLE_SECONDS", 300);
    let rounds = seconds("P01_ROUNDS", 3);
    assert!(warmup >= 1 && sample >= 1 && (1..=3).contains(&rounds));
    let env = env();
    let dir = env.artifacts.join("p01");
    std::fs::create_dir_all(&dir).unwrap();
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    let databases = [
        format!("{}_p01a", env.run_id),
        format!("{}_p01b", env.run_id),
    ];
    for db in &databases {
        create_database(&env.admin_base, db, &owner).await;
    }
    let urls = databases.map(|db| format!("{}/{db}", env.admin_base));
    let endpoint = free_port();
    let observations = [free_port(), free_port()];
    tenant_config(&dir, "A", &endpoint, &observations[0]);
    tenant_config(&dir, "B", &endpoint, &observations[1]);
    let config = deployment(&dir, &endpoint, &["A", "B"]);
    let mut server = spawn(&config, &dir, "matrix", &env, &urls[0], &urls[1]);
    let clients = [
        client(&endpoint, TOKEN_A, &mut server).await,
        client(&endpoint, TOKEN_B, &mut server).await,
    ];
    let admins = [sql(&urls[0]).await, sql(&urls[1]).await];
    let namespace = format!("p01-{}", env.run_id);
    for (tenant, client) in clients.iter().enumerate() {
        for n in 0..512 {
            let mut request = write(&namespace, &format!("key-{n:04}"), 10 + tenant as u8);
            request.payload = vec![10 + tenant as u8; 1024];
            client.save(request).await.unwrap();
        }
    }
    let rows = admins[0].query("SELECT tableoid::regclass::text,record_key FROM dbproxy_snapshots WHERE namespace=$1 ORDER BY 1,2", &[&namespace]).await.unwrap();
    let mut partitions = std::collections::BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        partitions.entry(row.get(0)).or_default().push(row.get(1));
    }
    assert_eq!(partitions.len(), 32);
    let records: Vec<_> = partitions
        .values()
        .flat_map(|keys| keys.iter().take(2))
        .map(|key| RecordKey::new(&namespace, key).unwrap())
        .collect();
    assert_eq!(records.len(), 64);
    std::fs::write(
        dir.join("partition-keys.json"),
        serde_json::to_vec_pretty(&partitions).unwrap(),
    )
    .unwrap();
    for size in [1usize, 30, 64] {
        for tenant in 0..2 {
            let names: Vec<_> = records[..size]
                .iter()
                .map(|r| r.namespace.clone())
                .collect();
            let keys: Vec<_> = records[..size].iter().map(|r| r.key.clone()).collect();
            let query = format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
                include_str!("../../../dbproxy-storage/src/snapshot_load_multi.sql")
            );
            let raw_plan: JsonPlan = admins[tenant]
                .query_one(&query, &[&names, &keys])
                .await
                .unwrap()
                .get(0);
            let plan: serde_json::Value = serde_json::from_str(&raw_plan.0).unwrap();
            std::fs::write(
                dir.join(format!("plan-{tenant}-{size}.json")),
                serde_json::to_vec_pretty(&plan).unwrap(),
            )
            .unwrap();
            let mut missing = records[..size].to_vec();
            for index in [0, size / 2, size - 1] {
                missing[index].key = format!("missing-{index}");
            }
            for mode in ["authority", "hot"] {
                let (_, values) = timed_read(&clients[tenant], &missing, mode).await;
                check(&values, &missing, tenant);
            }
        }
    }
    let mut cache = [
        redis::Client::open(env.cache[0].as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap(),
        redis::Client::open(env.cache[1].as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap(),
    ];
    let cases: Vec<_> = [1usize, 30, 64]
        .into_iter()
        .flat_map(|size| {
            ["authority", "hot", "cold"]
                .into_iter()
                .map(move |mode| (size, mode))
        })
        .collect();
    let mut results = Vec::new();
    let mut raw = std::io::BufWriter::new(std::fs::File::create(dir.join("requests.csv")).unwrap());
    writeln!(
        raw,
        "round,batch,mode,tenant,sample,elapsed_s,rpc_s,prepare_s,dispatch_delay_s"
    )
    .unwrap();
    for round in 0..rounds {
        let mut order = cases.clone();
        if round % 2 == 1 {
            order.reverse();
        }
        for (size, mode) in order {
            let phase = format!("r{round}-{size}-{mode}");
            std::fs::write(dir.join("progress.json"),serde_json::to_vec(&serde_json::json!({"phase":phase,"state":"running","warmup":warmup,"sample":sample})).unwrap()).unwrap();
            for tenant in 0..2 {
                std::fs::write(
                    dir.join(format!("{phase}-t{tenant}-before.pg.json")),
                    pg_snapshot(&admins[tenant]).await,
                )
                .unwrap();
                std::fs::write(
                    dir.join(format!("{phase}-t{tenant}-before.metrics")),
                    metrics(&observations[tenant]).await,
                )
                .unwrap();
            }
            let start = Instant::now();
            let start_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let mut ticker = tokio::time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut samples = [Vec::new(), Vec::new()];
            let mut preparation = Vec::new();
            let mut dispatch = Vec::new();
            loop {
                let scheduled = ticker.tick().await;
                if start.elapsed().as_secs_f64() >= (warmup + sample) as f64 {
                    break;
                }
                let delay = scheduled.elapsed().as_secs_f64();
                let prepare_start = Instant::now();
                if mode == "cold" {
                    let keys: Vec<_> = records[..size]
                        .iter()
                        .flat_map(|r| {
                            let key = RedisSnapshotCache::cache_key(r);
                            [
                                key.clone(),
                                format!("{key}:fresh-until"),
                                format!("{key}:negative"),
                            ]
                        })
                        .collect();
                    // Only this run's exact cache entries; retain revision fences and all other keys.
                    for connection in &mut cache {
                        let _: u64 = redis::cmd("DEL")
                            .arg(&keys)
                            .query_async(connection)
                            .await
                            .unwrap();
                    }
                }
                let prepare = prepare_start.elapsed().as_secs_f64();
                let measured = start.elapsed().as_secs() >= warmup;
                let (a, b) = tokio::join!(
                    timed_read(&clients[0], &records[..size], mode),
                    timed_read(&clients[1], &records[..size], mode)
                );
                for (tenant, (duration, values)) in [a, b].into_iter().enumerate() {
                    check(&values, &records[..size], tenant);
                    writeln!(raw,"{round},{size},{mode},{tenant},{measured},{:.6},{duration:.9},{prepare:.9},{delay:.9}",start.elapsed().as_secs_f64()).unwrap();
                    if measured {
                        samples[tenant].push(duration);
                    }
                }
                if measured {
                    preparation.push(prepare);
                    dispatch.push(delay);
                }
                assert!(server.0.try_wait().unwrap().is_none());
            }
            raw.flush().unwrap();
            let mut per_tenant = Vec::new();
            for tenant in 0..2 {
                std::fs::write(
                    dir.join(format!("{phase}-t{tenant}-after.pg.json")),
                    pg_snapshot(&admins[tenant]).await,
                )
                .unwrap();
                std::fs::write(
                    dir.join(format!("{phase}-t{tenant}-after.metrics")),
                    metrics(&observations[tenant]).await,
                )
                .unwrap();
                per_tenant.push(summary(&mut samples[tenant]));
            }
            let result = serde_json::json!({"phase":phase,"start_unix_ms":start_unix_ms,"batch":size,"mode":mode,"round":round,"tenants":per_tenant,"prepare":summary(&mut preparation),"dispatch":summary(&mut dispatch),"target_rps_per_tenant":10,"warmup_seconds":warmup,"sample_seconds":sample});
            println!("P01_PHASE_RESULT {result}");
            results.push(result);
            std::fs::write(
                dir.join("phases.json"),
                serde_json::to_vec_pretty(&results).unwrap(),
            )
            .unwrap();
        }
    }
    // One PG statement must observe one committed generation across all 32 partitions.
    // Cached multi-read has no atomic-generation contract and is not used for this assertion.
    let writer = sql(&urls[0]).await;
    let keys: Vec<_> = records.iter().map(|r| r.key.clone()).collect();
    let writer_namespace = namespace.clone();
    let updates = tokio::spawn(async move {
        for _ in 0..100 {
            assert_eq!(writer.execute("UPDATE dbproxy_snapshots SET revision=revision+1 WHERE namespace=$1 AND record_key=ANY($2)",&[&writer_namespace,&keys]).await.unwrap(),64);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    let mut observed = std::collections::BTreeSet::new();
    for _ in 0..200 {
        let (_, values) = timed_read(&clients[0], &records, "authority").await;
        let revision = values[0].as_ref().unwrap().revision;
        assert!(
            values
                .iter()
                .all(|s| s.as_ref().unwrap().revision == revision)
        );
        observed.insert(revision.0);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    updates.await.unwrap();
    assert!(
        observed.len() > 1,
        "no concurrent generation transition was observed"
    );
    let (_, values) = timed_read(&clients[0], &records, "authority").await;
    assert!(
        values
            .iter()
            .all(|s| s.as_ref().unwrap().revision == Revision(101))
    );
    let (_, values) = timed_read(&clients[1], &records, "authority").await;
    check(&values, &records, 1);
    let result = serde_json::json!({"phases":results.len(),"rounds":rounds,"warmup_seconds":warmup,"sample_seconds":sample,"partitions":32,"atomic_updates":100,"atomic_reads":200,"observed_generations":observed,"full_timing":rounds==3 && warmup>=120 && sample>=300,"scope":"fixed 10 RPC/s per tenant, one outstanding request each; no capacity claim"});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("progress.json"), "{\"state\":\"completed\"}").unwrap();
    println!("P01_MATRIX_RESULT {result}");
}
