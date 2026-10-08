use super::*;
use std::sync::Mutex;
use tokio::{sync::Semaphore, time::advance};

type ObservedCalls = Arc<Mutex<Vec<(String, Duration)>>>;

struct DelayedSink {
    started: Arc<Semaphore>,
    calls: ObservedCalls,
    first_delay: Duration,
    next_delay: Duration,
}
#[async_trait]
impl EnqueueSink for DelayedSink {
    async fn write(
        &mut self,
        entries: &[&EnqueueEntry],
        deadline: Instant,
    ) -> Result<(), StorageError> {
        let delay = {
            let mut calls = self.calls.lock().unwrap();
            let delay = if calls.is_empty() {
                self.first_delay
            } else {
                self.next_delay
            };
            calls.push((entries[0].member.clone(), deadline - Instant::now()));
            delay
        };
        self.started.add_permits(1);
        tokio::time::sleep(delay).await;
        Ok(())
    }
}
fn entry(name: &str) -> Vec<EnqueueEntry> {
    vec![EnqueueEntry {
        entry_key: name.into(),
        encoded: vec![1],
        member: name.into(),
    }]
}
fn config(queue_ms: u64, total_ms: u64) -> EnqueueBatchConfig {
    let config = EnqueueBatchConfig {
        max_queue_wait: Duration::from_millis(queue_ms),
        total_timeout: Duration::from_millis(total_ms),
        durability: RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_millis(5),
            response_timeout: Duration::from_millis(10),
        },
        ..Default::default()
    };
    config.validate().unwrap();
    config
}
fn setup(
    config: EnqueueBatchConfig,
    first_ms: u64,
    next_ms: u64,
) -> (EnqueueBatcher, Arc<Semaphore>, ObservedCalls) {
    let started = Arc::new(Semaphore::new(0));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let batcher = EnqueueBatcher::spawn(
        DelayedSink {
            started: started.clone(),
            calls: calls.clone(),
            first_delay: Duration::from_millis(first_ms),
            next_delay: Duration::from_millis(next_ms),
        },
        config,
    );
    (batcher, started, calls)
}
fn submit(
    batcher: &EnqueueBatcher,
    name: &'static str,
) -> tokio::task::JoinHandle<Result<(), StorageError>> {
    let batcher = batcher.clone();
    tokio::spawn(async move { batcher.submit(entry(name)).await })
}
fn timeouts(batcher: &EnqueueBatcher, stage: &str) -> u64 {
    batcher
        .metrics
        .latency_snapshot()
        .iter()
        .find(|s| s.stage == stage)
        .unwrap()
        .timeouts
}

#[tokio::test(start_paused = true)]
async fn queued_time_consumes_the_original_write_budget() {
    let (batcher, started, calls) = setup(config(100, 120), 80, 100);
    let first = submit(&batcher, "first");
    started.acquire().await.unwrap().forget();
    advance(Duration::from_millis(40)).await;
    let accepted = Instant::now();
    let second = submit(&batcher, "second");
    tokio::task::yield_now().await;
    first.await.unwrap().unwrap();
    started.acquire().await.unwrap().forget();
    assert_eq!(calls.lock().unwrap()[1].1, Duration::from_millis(80));
    assert!(matches!(
        second.await.unwrap(),
        Err(StorageError::BacklogEnqueueFailed(_))
    ));
    assert!(accepted.elapsed() <= Duration::from_millis(121));
    tokio::task::yield_now().await;
    assert_eq!(timeouts(&batcher, "enqueue_total"), 1);
}

#[tokio::test(start_paused = true)]
async fn queue_expiry_returns_before_the_active_write_and_never_writes_later() {
    let (batcher, started, calls) = setup(config(30, 100), 80, 1);
    let first = submit(&batcher, "first");
    started.acquire().await.unwrap().forget();
    let queued = submit(&batcher, "expired");
    assert!(matches!(
        queued.await.unwrap(),
        Err(StorageError::BacklogEnqueueDeadlineExceeded { .. })
    ));
    assert!(!first.is_finished());
    first.await.unwrap().unwrap();
    batcher.submit(entry("fresh")).await.unwrap();
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.0.as_str())
            .collect::<Vec<_>>(),
        ["first", "fresh"]
    );
    assert_eq!(timeouts(&batcher, "enqueue_queue"), 1);
    assert_eq!(timeouts(&batcher, "enqueue_total"), 0);
}

#[tokio::test(start_paused = true)]
async fn stalled_batch_is_cancelled_and_next_batch_can_progress() {
    let (batcher, started, calls) = setup(config(30, 100), 200, 1);
    let first = submit(&batcher, "stalled");
    started.acquire().await.unwrap().forget();
    assert!(matches!(
        first.await.unwrap(),
        Err(StorageError::BacklogEnqueueFailed(_))
    ));
    batcher.submit(entry("after")).await.unwrap();
    assert_eq!(calls.lock().unwrap().len(), 2);
    assert_eq!(timeouts(&batcher, "enqueue_total"), 1);
    assert!(
        batcher
            .metrics
            .latency_snapshot()
            .iter()
            .all(|s| s.in_flight == 0)
    );
}

#[test]
fn invalid_enqueue_budgets_are_rejected_without_connecting() {
    for (queue, total) in [
        (0, 4500),
        (2000, 4000),
        (3000, 3000),
        (2000, 60001),
        (u64::MAX, 4500),
    ] {
        let config = EnqueueBatchConfig {
            max_queue_wait: Duration::from_millis(queue),
            total_timeout: Duration::from_millis(total),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}

async fn connection_case(failure: crate::redis_durability_fixture::Failure) {
    use crate::redis_durability_fixture::{Fixture, Plan};
    let fixture = Fixture::start(Plan {
        write: b"EVALSHA",
        max_ack_ms: 3000,
        failure,
        first_write_delay: Duration::ZERO,
        second_ack_delay: Duration::ZERO,
    })
    .await;
    let metrics = Arc::new(StorageMetrics::default());
    let config = EnqueueBatchConfig {
        total_timeout: Duration::from_secs(6),
        durability: RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_secs(3),
            response_timeout: Duration::from_secs(4),
        },
        ..Default::default()
    };
    let mut sink = RedisEnqueueSink::connect(&fixture.url, config, metrics.clone())
        .await
        .unwrap();
    let entries = entry("same-request");
    let entries = entries.iter().collect::<Vec<_>>();
    assert!(matches!(
        sink.write(&entries, Instant::now()).await,
        Err(StorageError::RedisDurabilityDeadlineExceeded)
    ));
    assert!(
        fixture.observed.lock().unwrap().writes.is_empty(),
        "an expired call must not start Redis writes"
    );
    let first = timeout_at(
        Instant::now() + Duration::from_millis(100),
        sink.write(&entries, Instant::now() + config.total_timeout),
    )
    .await;
    assert!(!matches!(first, Ok(Ok(()))));
    assert!(
        sink.connection.is_none(),
        "failed or cancelled connection must not be reused"
    );
    sink.write(&entries, Instant::now() + config.total_timeout)
        .await
        .unwrap();
    assert_eq!(fixture.observed.lock().unwrap().writes, [0, 1]);
    assert_eq!(
        fixture.observed.lock().unwrap().ack_ms,
        [(0, 3000), (1, 3000)]
    );
    assert!(metrics.latency_snapshot().iter().all(|s| s.in_flight == 0));
    drop(sink);
    fixture.finish().await;
}

#[tokio::test]
async fn enqueue_rewrites_on_a_new_connection_after_unconfirmed_aof() {
    connection_case(crate::redis_durability_fixture::Failure::Unconfirmed).await;
}
#[tokio::test]
async fn enqueue_rewrites_on_a_new_connection_after_timeout() {
    connection_case(crate::redis_durability_fixture::Failure::Stall).await;
}
#[tokio::test]
async fn enqueue_rewrites_on_a_new_connection_after_disconnect() {
    connection_case(crate::redis_durability_fixture::Failure::Disconnect).await;
}

#[tokio::test]
async fn write_io_timeout_is_observed_without_claiming_an_aof_timeout() {
    use crate::redis_durability_fixture::{Failure, Fixture, Plan};
    let fixture = Fixture::start(Plan {
        write: b"EVALSHA",
        max_ack_ms: 100,
        failure: Failure::Unconfirmed,
        first_write_delay: Duration::from_millis(600),
        second_ack_delay: Duration::ZERO,
    })
    .await;
    let metrics = Arc::new(StorageMetrics::default());
    let config = EnqueueBatchConfig {
        max_queue_wait: Duration::from_millis(100),
        total_timeout: Duration::from_millis(1000),
        durability: RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_millis(100),
            response_timeout: Duration::from_millis(400),
        },
        ..Default::default()
    };
    let mut sink = RedisEnqueueSink::connect(&fixture.url, config, metrics.clone())
        .await
        .unwrap();
    let entries = entry("same-request");
    let entries = entries.iter().collect::<Vec<_>>();
    assert!(
        sink.write(&entries, Instant::now() + config.total_timeout)
            .await
            .is_err()
    );
    assert!(sink.connection.is_none());
    sink.write(&entries, Instant::now() + config.total_timeout)
        .await
        .unwrap();
    let snapshots = metrics.latency_snapshot();
    assert_eq!(
        snapshots
            .iter()
            .find(|s| s.stage == "enqueue_write")
            .unwrap()
            .timeouts,
        1
    );
    assert_eq!(
        snapshots
            .iter()
            .find(|s| s.stage == "enqueue_aof")
            .unwrap()
            .timeouts,
        0
    );
    assert_eq!(fixture.observed.lock().unwrap().ack_ms, [(1, 100)]);
    drop(sink);
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn queue_gauge_releases_on_timeout_and_unstarted_receiver_drop() {
    let (batcher, started, _) = setup(config(30, 100), 80, 1);
    let first = submit(&batcher, "first");
    started.acquire().await.unwrap().forget();
    let second = submit(&batcher, "queued");
    tokio::task::yield_now().await;
    let queue = || {
        batcher
            .metrics
            .latency_snapshot()
            .into_iter()
            .find(|s| s.stage == "enqueue_queue")
            .unwrap()
    };
    assert_eq!(queue().in_flight, 1);
    assert!(second.await.unwrap().is_err());
    assert_eq!(queue().in_flight, 0);
    first.await.unwrap().unwrap();
    let metrics = Arc::new(StorageMetrics::default());
    let (sender, receiver) = mpsc::channel(1);
    let state = Arc::new(EnqueueState::new(metrics.clone()));
    let (reply, _result) = oneshot::channel();
    sender
        .try_send(EnqueueJob {
            entries: entry("discarded"),
            state,
            reply,
        })
        .unwrap();
    drop(receiver);
    assert!(metrics.latency_snapshot().iter().all(|s| s.in_flight == 0));
}

#[cfg(test)]
mod enqueue_batcher_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Semaphore;
    use tokio::time::sleep;

    /// 可阻塞、可在指定批次失败的替身写入端。 / Gated sink double that can fail a chosen batch.
    #[derive(Clone)]
    struct FakeSink {
        batches: Arc<StdMutex<Vec<Vec<String>>>>,
        gate: Arc<Semaphore>,
        fail_batch: Option<usize>,
    }

    #[async_trait]
    impl EnqueueSink for FakeSink {
        async fn write(
            &mut self,
            entries: &[&EnqueueEntry],
            _deadline: Instant,
        ) -> Result<(), StorageError> {
            self.gate.acquire().await.expect("gate open").forget();
            let index = {
                let mut batches = self.batches.lock().unwrap();
                batches.push(entries.iter().map(|entry| entry.member.clone()).collect());
                batches.len()
            };
            if self.fail_batch == Some(index) {
                return Err(StorageError::BacklogProtocol(
                    "injected write failure".to_string(),
                ));
            }
            Ok(())
        }
    }

    fn sink(fail_batch: Option<usize>) -> FakeSink {
        FakeSink {
            batches: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
            fail_batch,
        }
    }

    fn entry(name: &str) -> Vec<EnqueueEntry> {
        vec![EnqueueEntry {
            entry_key: format!("entry:{name}"),
            encoded: name.as_bytes().to_vec(),
            member: name.to_string(),
        }]
    }

    fn config(queue_capacity: usize, max_queue_wait_ms: u64) -> EnqueueBatchConfig {
        EnqueueBatchConfig {
            queue_capacity,
            max_batch_records: 512,
            max_queue_wait: Duration::from_millis(max_queue_wait_ms),
            ack: EnqueueAck::Aof,
            total_timeout: Duration::from_millis(max_queue_wait_ms + 5_000),
            ..EnqueueBatchConfig::default()
        }
    }

    fn batches(sink: &FakeSink) -> Vec<Vec<String>> {
        sink.batches.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn requests_arriving_during_a_write_share_the_next_write() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(50)).await;
        let waiting: Vec<_> = (0..50)
            .map(|i| {
                let batcher = batcher.clone();
                tokio::spawn(async move { batcher.submit(entry(&format!("p{i}"))).await })
            })
            .collect();
        sleep(Duration::from_millis(50)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in waiting {
            handle.await.unwrap().unwrap();
        }
        let written = batches(&sink);
        // 第一次写入期间到达的50个请求只用一次写入和一次AOF确认。 / The 50 requests that arrived during the first write use one write and one AOF wait.
        assert_eq!(written.len(), 2);
        assert_eq!(written[0], vec!["first"]);
        assert_eq!(written[1].len(), 50);
    }

    #[tokio::test]
    async fn expired_requests_are_rejected_without_being_written() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 50));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let stale: Vec<_> = (0..3)
            .map(|i| {
                let batcher = batcher.clone();
                tokio::spawn(async move { batcher.submit(entry(&format!("stale{i}"))).await })
            })
            .collect();
        sleep(Duration::from_millis(150)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in stale {
            assert!(matches!(
                handle.await.unwrap(),
                Err(StorageError::BacklogEnqueueDeadlineExceeded { .. })
            ));
        }
        batcher.submit(entry("fresh")).await.unwrap();
        assert_eq!(
            batches(&sink),
            vec![vec!["first".to_string()], vec!["fresh".to_string()]]
        );
    }

    #[tokio::test]
    async fn a_full_queue_rejects_immediately_instead_of_waiting() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(1, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let queued = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("queued")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let rejected = tokio::time::timeout(
            Duration::from_millis(200),
            batcher.submit(entry("rejected")),
        )
        .await
        .expect("overload must not wait");
        assert!(matches!(
            rejected,
            Err(StorageError::BacklogEnqueueOverloaded { capacity: 1 })
        ));
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        queued.await.unwrap().unwrap();
        assert!(
            !batches(&sink)
                .iter()
                .flatten()
                .any(|member| member == "rejected")
        );
    }

    #[tokio::test]
    async fn a_failed_write_fails_its_whole_batch_and_later_batches_continue() {
        let sink = sink(Some(2));
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let failing: Vec<_> = ["b", "c"]
            .iter()
            .map(|name| {
                let batcher = batcher.clone();
                let name = name.to_string();
                tokio::spawn(async move { batcher.submit(entry(&name)).await })
            })
            .collect();
        sleep(Duration::from_millis(30)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in failing {
            match handle.await.unwrap() {
                Err(StorageError::BacklogEnqueueFailed(message)) => {
                    assert!(message.contains("injected write failure"))
                }
                other => panic!("expected shared batch failure, got {other:?}"),
            }
        }
        batcher.submit(entry("after")).await.unwrap();
        assert_eq!(batches(&sink).len(), 3);
    }

    #[tokio::test]
    async fn abandoned_requests_are_not_written() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let abandoned = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("abandoned")).await }
        });
        sleep(Duration::from_millis(30)).await;
        abandoned.abort();
        let kept = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("kept")).await }
        });
        sleep(Duration::from_millis(30)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        kept.await.unwrap().unwrap();
        assert_eq!(
            batches(&sink),
            vec![vec!["first".to_string()], vec!["kept".to_string()]]
        );
    }

    #[tokio::test]
    async fn zero_limits_are_rejected_before_connecting() {
        let invalid = EnqueueBatchConfig {
            queue_capacity: 0,
            ..EnqueueBatchConfig::default()
        };
        assert!(matches!(
            RedisSnapshotBacklog::connect_with_config("redis://127.0.0.1:1/0", invalid).await,
            Err(StorageError::BacklogProtocol(_))
        ));
    }
}
