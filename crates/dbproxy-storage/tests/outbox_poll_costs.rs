//! Bounded, fixed-rate P07 component measurements; not application capacity.
use std::{
    fs::File,
    io::{BufWriter, Write},
    time::Duration,
};
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio::time::{Instant, sleep_until, timeout};

fn seconds(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

#[tokio::test]
#[ignore = "requires a fresh isolated PG database and explicit migration opt-in"]
async fn sustained_polling_with_shared_stats() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let output = std::path::PathBuf::from(std::env::var("P07_OUTPUT").unwrap());
    std::fs::create_dir(&output).unwrap();
    let mode = std::env::var("P07_MODE").unwrap();
    assert!(matches!(mode.as_str(), "leased" | "blocked"));
    let with_stats = std::env::var("P07_STATS").unwrap() == "on";
    let warmup = seconds("P07_WARMUP_SECONDS", 120);
    let sample = seconds("P07_SAMPLE_SECONDS", 300);
    assert!(warmup <= 120 && (1..=300).contains(&sample));
    let slots = (warmup + sample) * 4;
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let queue = store.outbox_queue();
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    assert_eq!(
        sql.query_one("SELECT count(*) FROM dbproxy_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    sql.batch_execute("SET statement_timeout='5s'; INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('p07','multi')").await.unwrap();
    if mode == "leased" {
        sql.batch_execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,lease_until) SELECT 'leased-'||n,'p07','test','group-'||n,'',0,clock_timestamp()+interval '1 day' FROM generate_series(1,100000) n").await.unwrap();
    } else {
        // A dead head and 1,000 ready followers force the grouping fallback.
        sql.batch_execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,dead_lettered_at) SELECT 'blocked-'||n,'p07','test','blocked','',0,CASE WHEN n=0 THEN clock_timestamp() ELSE NULL END FROM generate_series(0,1000) n ORDER BY n").await.unwrap();
        sql.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms) SELECT 'ready-'||n,'p07','test','independent-'||(n%2),'',0 FROM generate_series(0,$1::bigint-1) n ORDER BY n", &[&(slots as i64)]).await.unwrap();
    }
    sql.batch_execute("ANALYZE dbproxy_outbox").await.unwrap();
    let start = Instant::now() + Duration::from_millis(100);
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let stats_queue = queue.clone(); // Same connection/mutex as the production queue handle.
    let stats_output = output.join("stats.csv");
    let stats_task = tokio::spawn(async move {
        let mut file = BufWriter::new(File::create(stats_output).unwrap());
        writeln!(
            file,
            "slot,sample,latency_ms,pending,processing,dead_lettered"
        )
        .unwrap();
        if !with_stats {
            return 0_u64;
        }
        let mut n = 0_u64;
        loop {
            tokio::select! {
                _ = stop_rx.changed() => break,
                _ = sleep_until(start + Duration::from_secs(n)) => {}
            }
            let now = Instant::now();
            let value = timeout(Duration::from_secs(5), stats_queue.stats())
                .await
                .unwrap()
                .unwrap();
            writeln!(
                file,
                "{n},{},{},{},{},{}",
                n >= warmup,
                now.elapsed().as_secs_f64() * 1000.0,
                value.pending,
                value.processing,
                value.dead_lettered
            )
            .unwrap();
            n += 1;
            file.flush().unwrap();
        }
        n
    });
    let mut raw = BufWriter::new(File::create(output.join("claims.csv")).unwrap());
    writeln!(raw, "slot,sample,dispatch_ms,claim_ms,ack_ms,event_id").unwrap();
    let mut groups = [0_u64; 2];
    for n in 0..slots {
        let scheduled = start + Duration::from_millis(n * 250);
        sleep_until(scheduled).await;
        let before = Instant::now();
        let lag = before.duration_since(scheduled).as_secs_f64() * 1000.0;
        let lease = timeout(Duration::from_secs(5), queue.claim("p07-worker", 30000))
            .await
            .unwrap()
            .unwrap();
        let claim_ms = before.elapsed().as_secs_f64() * 1000.0;
        let mut ack_ms = 0.0;
        let mut event_id = String::new();
        if mode == "leased" {
            assert!(lease.is_none(), "live leases must remain unclaimed");
        } else {
            let lease = lease.expect("independent group must be claimable despite blocked prefix");
            event_id = lease.event.event_id.clone();
            assert_eq!(event_id, format!("ready-{n}"));
            assert_eq!(lease.event.partition_key, format!("independent-{}", n % 2));
            let before_ack = Instant::now();
            assert!(
                timeout(Duration::from_secs(5), queue.acknowledge(&lease))
                    .await
                    .unwrap()
                    .unwrap()
            );
            ack_ms = before_ack.elapsed().as_secs_f64() * 1000.0;
            groups[(n % 2) as usize] += 1;
        }
        writeln!(
            raw,
            "{n},{},{lag},{claim_ms},{ack_ms},{event_id}",
            n >= warmup * 4
        )
        .unwrap();
        raw.flush().unwrap();
        assert!(
            lag < 5000.0,
            "fixed-rate schedule fell behind more than 5 seconds"
        );
    }
    sleep_until(start + Duration::from_secs(warmup + sample)).await;
    let _ = stop_tx.send(true);
    let stats_count = stats_task.await.unwrap();
    if with_stats {
        assert!(
            stats_count >= warmup + sample,
            "statistics loop did not sustain 1/s"
        );
    }
    if mode == "blocked" {
        assert_eq!(groups, [slots / 2, slots / 2]);
        assert_eq!(sql.query_one("SELECT count(*) FROM dbproxy_outbox WHERE event_id LIKE 'blocked-%' AND published_at IS NOT NULL", &[]).await.unwrap().get::<_, i64>(0), 0);
    }
    let result = serde_json::json!({"mode":mode,"stats":with_stats,"full_timing":warmup==120 && sample==300,"warmup_seconds":warmup,"sample_seconds":sample,"sample_count":sample*4,"total_count":slots,"stats_count":stats_count,"groups":groups,"elapsed_seconds":start.elapsed().as_secs_f64()});
    std::fs::write(
        output.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("P07_POLL_RESULT {result}");
    connection.abort();
}
