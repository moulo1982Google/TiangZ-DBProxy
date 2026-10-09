use super::*;

#[tokio::test]
#[ignore = "A13/P09: tenant A clears 100k receipts while tenant B sustains 20 writes plus reads per second"]
async fn a13_cleanup_pressure_keeps_tenant_b_serving() {
    let env = env();
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    let dir = env.artifacts.join("a13-pressure");
    std::fs::create_dir_all(&dir).unwrap();
    let mut urls = Vec::new();
    for tenant in ["a", "b"] {
        let db = format!("{}_a13{tenant}", env.run_id);
        create_database(&env.admin_base, &db, &owner).await;
        urls.push(format!("{}/{db}", env.admin_base));
    }
    let admin_a = sql(&urls[0]).await;
    let admin_b = sql(&urls[1]).await;
    let endpoint = free_port();
    let obs_a = free_port();
    let obs_b = free_port();
    tenant_config(&dir, "A", &endpoint, &obs_a);
    tenant_config(&dir, "B", &endpoint, &obs_b);
    let deployment = deployment(&dir, &endpoint, &["A", "B"]);
    let mut seed = spawn(&deployment, &dir, "seed", &env, &urls[0], &urls[1]);
    let a = client(&endpoint, TOKEN_A, &mut seed).await;
    let b = client(&endpoint, TOKEN_B, &mut seed).await;
    for (client, payload) in [(&a, 13), (&b, 23)] {
        client
            .save(write("a13-shared", "same-key", payload))
            .await
            .unwrap();
    }
    drop(a);
    drop(b);
    drop(seed);
    // Identical receipt IDs and keys in separate databases, but A's rows are expired and
    // B's must survive. Start once without backlog for a ten-second local baseline first.
    admin_b.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'a13-fixture-'||n,'a13-fixture',n::text,'test',1,'',1,clock_timestamp() FROM generate_series(1,100000) n").await.unwrap();
    let named_a = format!("{}?application_name=a13-proxy-a", urls[0]);
    let named_b = format!("{}?application_name=a13-proxy-b", urls[1]);
    let mut server = spawn(&deployment, &dir, "run", &env, &named_a, &named_b);
    let a = client(&endpoint, TOKEN_A, &mut server).await;
    let b = client(&endpoint, TOKEN_B, &mut server).await;
    let mut phases = Vec::new();
    let mut written = 0u64;
    for phase in ["baseline", "pressure", "recovery"] {
        if phase == "pressure" {
            insert_expired(&admin_a, "a13-fixture", 100000).await;
        }
        let started = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut latencies = Vec::new();
        let mut read_latencies = Vec::new();
        let mut samples = Vec::new();
        let before_writes = written;
        loop {
            ticker.tick().await;
            let request = write("a13-load", &written.to_string(), (written % 251) as u8);
            let write_started = Instant::now();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), b.save(request.clone()))
                    .await
                    .expect("tenant B write stalled")
                    .unwrap(),
                SnapshotWriteOutcome::Applied {
                    revision: Revision(1)
                }
            );
            latencies.push(write_started.elapsed().as_secs_f64());
            let read_started = Instant::now();
            let loaded = tokio::time::timeout(Duration::from_secs(2), b.load(&request.record))
                .await
                .expect("tenant B read stalled")
                .unwrap()
                .unwrap();
            assert_eq!(loaded.payload, request.payload);
            assert_eq!(loaded.revision, Revision(1));
            read_latencies.push(read_started.elapsed().as_secs_f64());
            written += 1;
            if (written - before_writes).is_multiple_of(20) {
                let remaining = count(
                    &admin_a,
                    "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='a13-fixture'",
                )
                .await;
                let a_connections = count(
                    &admin_a,
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name='a13-proxy-a'",
                )
                .await;
                let b_connections = count(
                    &admin_a,
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name='a13-proxy-b'",
                )
                .await;
                assert_eq!(a_connections, BUDGET as i64);
                assert_eq!(b_connections, BUDGET as i64);
                samples.push(serde_json::json!({"seconds":started.elapsed().as_secs_f64(),"a_expired":remaining,"a_connections":a_connections,"b_connections":b_connections}));
                if phase == "pressure" && remaining == 0 {
                    break;
                }
            }
            if phase != "pressure" && started.elapsed() >= Duration::from_secs(10) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(280),
                "tenant A cleanup did not drain within bounded test"
            );
        }
        latencies.sort_by(f64::total_cmp);
        read_latencies.sort_by(f64::total_cmp);
        let percentile = |q: f64| latencies[((latencies.len() - 1) as f64 * q).ceil() as usize];
        phases.push(serde_json::json!({"phase":phase,"seconds":started.elapsed().as_secs_f64(),"writes":written-before_writes,"writes_per_second":(written-before_writes) as f64/started.elapsed().as_secs_f64(),"write_p50_seconds":percentile(0.50),"write_p95_seconds":percentile(0.95),"write_p99_seconds":percentile(0.99),"write_max_seconds":latencies.last(),"read_p99_seconds":read_latencies[((read_latencies.len()-1) as f64*0.99).ceil() as usize],"read_max_seconds":read_latencies.last(),"samples":samples}));
    }
    for (client, payload) in [(&a, 13), (&b, 23)] {
        let request = write("a13-shared", "same-key", payload);
        assert_eq!(
            client.save(request.clone()).await.unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        );
        assert_eq!(
            client.load(&request.record).await.unwrap().unwrap().payload,
            vec![payload]
        );
    }
    assert_eq!(
        count(
            &admin_a,
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='a13-fixture'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &admin_b,
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='a13-fixture'"
        )
        .await,
        100000
    );
    assert_eq!(
        count(
            &admin_a,
            "SELECT count(*) FROM dbproxy_snapshots WHERE namespace='a13-load'"
        )
        .await,
        0
    );
    let rows = admin_b
        .query(
            "SELECT record_key,payload,revision FROM dbproxy_snapshots WHERE namespace='a13-load'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), written as usize);
    for row in rows {
        let key: String = row.get(0);
        let n: u64 = key.parse().unwrap();
        assert!(n < written);
        assert_eq!(row.get::<_, Vec<u8>>(1), vec![(n % 251) as u8]);
        assert_eq!(row.get::<_, i64>(2), 1);
    }
    for (tenant, obs) in [("a", &obs_a), ("b", &obs_b)] {
        let response = metrics(obs).await;
        assert_eq!(
            metric_sum(&response, "dbproxy_receipt_cleanup_deleted_total"),
            if tenant == "a" { 100000.0 } else { 0.0 }
        );
        std::fs::write(dir.join(format!("metrics-{tenant}.txt")), response).unwrap();
    }
    assert!(server.0.try_wait().unwrap().is_none());
    let result = serde_json::json!({"run":env.run_id,"b_writes_verified_pg":written,"a_deleted":100000,"b_recent_kept":100000,"connection_budget_per_tenant":BUDGET,"phases":phases,"scope":"single process, two tenants, capped 20 writes and 20 reads per second; not a capacity ceiling"});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!(
        "A13_PRESSURE_RESULT run={} writes={} a_deleted=100000 b_recent_kept=100000",
        env.run_id, written
    );
}
