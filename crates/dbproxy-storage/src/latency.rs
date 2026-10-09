//! 存储阶段的固定桶计时；只记录边界耗时，不推断提交或业务成功。
//! Fixed-cardinality storage timing; elapsed scope time never implies commit or business success.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// 毫秒有限桶；保留超出三十秒的样本到无穷桶。
/// Millisecond bounds; samples beyond thirty seconds remain in the infinity bucket.
pub const STORAGE_LATENCY_BOUNDS_MS: [u64; 15] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 3_000, 5_000, 10_000, 15_000, 30_000,
];

#[derive(Clone, Copy, Debug)]
pub(crate) enum Stage {
    CacheLookup,
    CacheWrite,
    FallbackCapacity,
    FallbackKey,
    FallbackLease,
    PostgresQueue,
    PostgresReadQueue,
    PostgresOperation,
    PostgresReadOperation,
    PostgresWriteOperation,
    CommittedCacheSync,
    RepairAck,
    FallbackRelease,
    EnqueueQueue,
    EnqueueWrite,
    EnqueueAof,
    EnqueueTotal,
    OutboxWrite,
    OutboxAof,
    OutboxTotal,
}

impl Stage {
    const ALL: [Self; 20] = [
        Self::CacheLookup,
        Self::CacheWrite,
        Self::FallbackCapacity,
        Self::FallbackKey,
        Self::FallbackLease,
        Self::PostgresQueue,
        Self::PostgresReadQueue,
        Self::PostgresOperation,
        Self::PostgresReadOperation,
        Self::PostgresWriteOperation,
        Self::CommittedCacheSync,
        Self::RepairAck,
        Self::FallbackRelease,
        Self::EnqueueQueue,
        Self::EnqueueWrite,
        Self::EnqueueAof,
        Self::EnqueueTotal,
        Self::OutboxWrite,
        Self::OutboxAof,
        Self::OutboxTotal,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::CacheLookup => "cache_lookup",
            Self::CacheWrite => "cache_write",
            Self::FallbackCapacity => "fallback_capacity_wait",
            Self::FallbackKey => "fallback_key_wait",
            Self::FallbackLease => "fallback_distributed_lease",
            Self::PostgresReadQueue => "postgres_read_pool_wait",
            Self::PostgresQueue => "postgres_connection_wait",
            Self::PostgresReadOperation => "postgres_read_operation",
            Self::PostgresWriteOperation => "postgres_write_operation",
            Self::PostgresOperation => "postgres_operation",
            Self::CommittedCacheSync => "committed_cache_sync",
            Self::RepairAck => "cache_repair_ack",
            Self::FallbackRelease => "fallback_lease_release",
            Self::EnqueueQueue => "enqueue_queue",
            Self::EnqueueWrite => "enqueue_write",
            Self::EnqueueAof => "enqueue_aof",
            Self::EnqueueTotal => "enqueue_total",
            Self::OutboxWrite => "outbox_write",
            Self::OutboxAof => "outbox_aof",
            Self::OutboxTotal => "outbox_total",
        }
    }
}

/// 一次采集的互斥桶、微秒总和及仍在执行的阶段数；不是成功操作数。
/// Disjoint buckets, microsecond sum and active scopes at scrape time, not successful operations.
#[derive(Clone, Debug)]
pub struct StorageStageSnapshot {
    pub stage: &'static str,
    pub buckets: [u64; STORAGE_LATENCY_BOUNDS_MS.len() + 1],
    pub sum_micros: u64,
    pub in_flight: u64,
    /// 该阶段显式观察到的期限耗尽；不把取消或所有错误都当作超时。
    /// Explicitly observed deadline exhaustion; cancellation and other errors are not inferred as timeouts.
    pub timeouts: u64,
}

#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; STORAGE_LATENCY_BOUNDS_MS.len() + 1],
    sum_micros: AtomicU64,
    in_flight: AtomicU64,
    timeouts: AtomicU64,
}

impl Histogram {
    fn record(&self, duration: Duration) {
        let index = STORAGE_LATENCY_BOUNDS_MS
            .partition_point(|bound| Duration::from_millis(*bound) < duration);
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }
}

#[derive(Default)]
pub(crate) struct StorageLatency([Histogram; Stage::ALL.len()]);

impl StorageLatency {
    pub(crate) fn enter(&self, stage: Stage) {
        self.0[stage as usize]
            .in_flight
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn leave(&self, stage: Stage, duration: Duration) {
        let histogram = &self.0[stage as usize];
        histogram.record(duration);
        histogram.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn timed_out(&self, stage: Stage) {
        self.0[stage as usize]
            .timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn measure<F: std::future::Future>(
        &self,
        stage: Stage,
        future: F,
    ) -> F::Output {
        let _timer = self.start(stage);
        future.await
    }

    pub(crate) fn start(&self, stage: Stage) -> Timer<'_> {
        let histogram = &self.0[stage as usize];
        histogram.in_flight.fetch_add(1, Ordering::Relaxed);
        Timer {
            histogram,
            #[cfg(feature = "acceptance-trace")]
            stage,
            started: Instant::now(),
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<StorageStageSnapshot> {
        Stage::ALL
            .iter()
            .map(|stage| {
                let histogram = &self.0[*stage as usize];
                StorageStageSnapshot {
                    stage: stage.name(),
                    buckets: histogram
                        .buckets
                        .each_ref()
                        .map(|value| value.load(Ordering::Relaxed)),
                    sum_micros: histogram.sum_micros.load(Ordering::Relaxed),
                    in_flight: histogram.in_flight.load(Ordering::Relaxed),
                    timeouts: histogram.timeouts.load(Ordering::Relaxed),
                }
            })
            .collect()
    }
}

// Drop also runs on early errors, timeout cancellation and unwinding. Censored observations
// measure only time until cancellation, never the eventual duration of work still running in PG.
pub(crate) struct Timer<'a> {
    histogram: &'a Histogram,
    #[cfg(feature = "acceptance-trace")]
    stage: Stage,
    started: Instant,
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        #[cfg(feature = "acceptance-trace")]
        crate::acceptance_trace::record(self.stage.name(), self.started, Instant::now());
        self.histogram.record(self.started.elapsed());
        self.histogram.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_bounds_submillisecond_edges_and_overflow_are_preserved() {
        let histogram = Histogram::default();
        for elapsed in [
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_micros(1_001),
            Duration::from_secs(30),
            Duration::from_secs(31),
        ] {
            histogram.record(elapsed);
        }
        assert_eq!(histogram.buckets[0].load(Ordering::Relaxed), 2);
        assert_eq!(histogram.buckets[1].load(Ordering::Relaxed), 1);
        assert_eq!(histogram.buckets[14].load(Ordering::Relaxed), 1);
        assert_eq!(histogram.buckets[15].load(Ordering::Relaxed), 1);
        assert_eq!(histogram.sum_micros.load(Ordering::Relaxed), 61_002_001);
    }

    #[test]
    fn concurrent_scopes_aggregate_without_scrape_reset_or_dynamic_labels() {
        let metrics = StorageLatency::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        let _timer = metrics.start(Stage::CacheLookup);
                    }
                });
            }
        });
        for _ in 0..2 {
            let snapshot = metrics.snapshot();
            assert_eq!(snapshot.len(), Stage::ALL.len());
            assert_eq!(
                snapshot
                    .iter()
                    .map(|sample| sample.stage)
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                Stage::ALL.len()
            );
            assert_eq!(
                snapshot[Stage::CacheLookup as usize]
                    .buckets
                    .iter()
                    .sum::<u64>(),
                4_000
            );
            assert!(snapshot.iter().all(|sample| sample.in_flight == 0));
        }
    }

    #[tokio::test]
    async fn cancellation_and_early_error_close_scopes_without_claiming_success() {
        let metrics = StorageLatency::default();
        let result = tokio::time::timeout(Duration::from_millis(20), async {
            let _timer = metrics.start(Stage::PostgresOperation);
            assert_eq!(
                metrics.snapshot()[Stage::PostgresOperation as usize].in_flight,
                1
            );
            std::future::pending::<()>().await;
        })
        .await;
        assert!(result.is_err());
        let error: Result<(), ()> = async {
            let _timer = metrics.start(Stage::RepairAck);
            Err(())
        }
        .await;
        assert!(error.is_err());
        let snapshot = metrics.snapshot();
        for stage in [Stage::PostgresOperation, Stage::RepairAck] {
            assert_eq!(snapshot[stage as usize].buckets.iter().sum::<u64>(), 1);
            assert_eq!(snapshot[stage as usize].in_flight, 0);
        }
        assert!(snapshot[Stage::PostgresOperation as usize].sum_micros >= 20_000);
    }
}
