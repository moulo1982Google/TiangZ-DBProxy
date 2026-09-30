//! End-of-run publication evidence; not a timing or global ordering assertion.
use super::*;

pub async fn verify(pg: &tokio_postgres::Client, redis_url: &str, run: &str) -> Value {
    let started = tokio::time::Instant::now();
    let mut pending_samples = Vec::new();
    loop {
        let pending = count(
            pg,
            "SELECT count(*) FROM dbproxy_outbox WHERE published_at IS NULL",
        )
        .await;
        pending_samples.push(json!({"elapsed_ms":started.elapsed().as_millis(),"pending":pending}));
        if pending == 0 || started.elapsed() >= Duration::from_secs(30) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rows = pg.query("SELECT event_id,operation_id,partition_key,payload,occurred_at_unix_ms,published_at IS NOT NULL FROM dbproxy_outbox ORDER BY enqueue_order", &[]).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let stream = format!(
        "{}mix-{run}",
        tiangz_dbproxy_storage::DEFAULT_OUTBOX_STREAM_PREFIX
    );
    let messages: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut redis)
        .await
        .unwrap();
    let mut mismatches = 0_u64;
    let mut entries = Vec::new();
    for row in &rows {
        let id: String = row.get(0);
        let matches: Vec<_> = messages
            .ids
            .iter()
            .filter(|m| m.get::<String>("event_id").as_ref() == Some(&id))
            .collect();
        let valid = row.get::<_, bool>(5)
            && matches.len() == 1
            && matches.iter().all(|m| {
                m.get::<String>("operation_id") == Some(row.get(1))
                    && m.get::<String>("partition_key") == Some(row.get(2))
                    && m.get::<Vec<u8>>("payload") == Some(row.get(3))
                    && m.get::<i64>("occurred_at_unix_ms") == Some(row.get(4))
            });
        mismatches += u64::from(!valid);
        entries.push(json!({"event_id":id,"published":row.get::<_,bool>(5),"redis_ids":matches.iter().map(|m| &m.id).collect::<Vec<_>>(),"valid":valid}));
    }
    mismatches += u64::from(messages.ids.len() != rows.len());
    json!({"stream":stream,"pg_rows":rows.len(),"redis_rows":messages.ids.len(),"mismatches":mismatches,"drain_samples":pending_samples,"entries":entries,"scope":"default publisher and worker; distinct partition keys, no global order guarantee"})
}
