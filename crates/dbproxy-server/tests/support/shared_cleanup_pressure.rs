use super::*;

#[tokio::test]
#[ignore = "P09: one versus two real processes on fresh shared databases, bounded identical request pairs"]
async fn p09_shared_database_cleanup_competition() {
    let env = env();
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    let mut results = Vec::new();
    for instances in [1, 2] {
        let db = format!("{}_p09_{instances}", env.run_id);
        create_database(&env.admin_base, &db, &owner).await;
        let url = format!("{}/{db}", env.admin_base);
        let dir = env.artifacts.join("p09-shared").join(instances.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let mut deployments = Vec::new();
        for n in 0..instances {
            let child_dir = dir.join(n.to_string());
            std::fs::create_dir_all(&child_dir).unwrap();
            let endpoint = free_port();
            let obs = free_port();
            tenant_config(&child_dir, "A", &endpoint, &obs);
            let config = deployment(&child_dir, &endpoint, &["A"]);
            deployments.push((child_dir, endpoint, obs, config));
        }
        let first = &deployments[0];
        let mut seed = spawn(&first.3, &first.0, "seed", &env, &url, &url);
        let seed_client = client(&first.1, TOKEN_A, &mut seed).await;
        seed_client
            .save(write("p09", "protected", 9))
            .await
            .unwrap();
        drop(seed_client);
        drop(seed);
        let admin = sql(&url).await;
        insert_expired(&admin, "p09-expired", 20000).await;
        admin.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
            SELECT 'p09-recent-'||n,'p09-recent',n::text,'test',1,'',1,clock_timestamp() FROM generate_series(1,100) n").await.unwrap();
        let named = format!("{url}?application_name=p09-proxy-{instances}");
        let started = Instant::now();
        let mut servers = Vec::new();
        for item in &deployments {
            servers.push(spawn(&item.3, &item.0, "run", &env, &named, &named));
        }
        let left = client(&deployments[0].1, TOKEN_A, &mut servers[0]).await;
        let right_index = instances - 1;
        let right = client(
            &deployments[right_index].1,
            TOKEN_A,
            &mut servers[right_index],
        )
        .await;
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pairs = 0u64;
        let mut samples = Vec::new();
        let mut pair_latencies = Vec::new();
        let mut drained_seconds = None;
        loop {
            ticker.tick().await;
            let request = write("p09-load", &pairs.to_string(), (pairs % 251) as u8);
            let pair_started = Instant::now();
            let (one, two) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(left.save(request.clone()), right.save(request.clone()))
            })
            .await
            .expect("request pair stalled");
            match (one.unwrap(), two.unwrap()) {
                (
                    SnapshotWriteOutcome::Applied {
                        revision: Revision(1),
                    },
                    SnapshotWriteOutcome::Duplicate {
                        revision: Revision(1),
                    },
                )
                | (
                    SnapshotWriteOutcome::Duplicate {
                        revision: Revision(1),
                    },
                    SnapshotWriteOutcome::Applied {
                        revision: Revision(1),
                    },
                ) => {}
                other => panic!("same request applied incorrectly: {other:?}"),
            }
            pair_latencies.push(pair_started.elapsed().as_secs_f64());
            for client in [&left, &right] {
                let loaded =
                    tokio::time::timeout(Duration::from_secs(2), client.load(&request.record))
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                assert_eq!(loaded.payload, request.payload);
                assert_eq!(loaded.revision, Revision(1));
            }
            pairs += 1;
            if pairs.is_multiple_of(20) {
                let remaining = count(
                    &admin,
                    "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='p09-expired'",
                )
                .await;
                let connections: i64 = admin
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                        &[&format!("p09-proxy-{instances}")],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(connections, (instances * BUDGET) as i64);
                samples.push(serde_json::json!({"seconds":started.elapsed().as_secs_f64(),"expired":remaining,"connections":connections}));
                if remaining == 0 && drained_seconds.is_none() {
                    drained_seconds = Some(started.elapsed().as_secs_f64());
                }
            }
            if started.elapsed() >= Duration::from_secs(45) && drained_seconds.is_some() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(75),
                "shared cleanup did not complete"
            );
        }
        let mut deleted = 0.0;
        for item in &deployments {
            let response = metrics(&item.2).await;
            deleted += metric_sum(&response, "dbproxy_receipt_cleanup_deleted_total");
            std::fs::write(item.0.join("metrics-final.txt"), response).unwrap();
        }
        assert_eq!(
            deleted, 20000.0,
            "process deletion counts overlap or miss committed rows"
        );
        assert_eq!(
            count(
                &admin,
                "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='p09-recent'"
            )
            .await,
            100
        );
        let rows = admin.query("SELECT record_key,payload,revision FROM dbproxy_snapshots WHERE namespace='p09-load'", &[]).await.unwrap();
        assert_eq!(rows.len(), pairs as usize);
        for row in rows {
            let id: String = row.get(0);
            let n: u64 = id.parse().unwrap();
            assert!(n < pairs);
            assert_eq!(row.get::<_, Vec<u8>>(1), vec![(n % 251) as u8]);
            assert_eq!(row.get::<_, i64>(2), 1);
        }
        pair_latencies.sort_by(f64::total_cmp);
        results.push(serde_json::json!({"instances":instances,"request_pairs":pairs,"pair_p99_seconds":pair_latencies[((pair_latencies.len()-1) as f64*0.99).ceil() as usize],"pair_max_seconds":pair_latencies.last(),"cleanup_drain_seconds":drained_seconds,"deleted_total":deleted,"recent_kept":100,"samples":samples}));
        drop(left);
        drop(right);
        drop(servers);
    }
    std::fs::write(
        env.artifacts.join("p09-shared/result.json"),
        serde_json::to_vec_pretty(&results).unwrap(),
    )
    .unwrap();
    println!("P09_SHARED_RESULT run={} cases=2", env.run_id);
}
