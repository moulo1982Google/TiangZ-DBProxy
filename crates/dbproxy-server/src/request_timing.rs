//! Fixed-cardinality completed request stage timings; never record business keys.
use crate::observability::RpcOperation;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
pub(crate) const BOUNDS_US: [u64; 9] = [
    1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000,
];
#[derive(Clone, Copy)]
pub(crate) enum RequestStage {
    Schedule,
    Order,
    Handler,
}
impl RequestStage {
    const ALL: [Self; 3] = [Self::Schedule, Self::Order, Self::Handler];
    fn name(self) -> &'static str {
        match self {
            Self::Schedule => "task_schedule",
            Self::Order => "record_order_wait",
            Self::Handler => "handler",
        }
    }
}
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; 10],
    sum_us: AtomicU64,
    max_us: AtomicU64,
}
#[derive(Default)]
pub(crate) struct RequestTimings([[Histogram; RequestStage::ALL.len()]; RpcOperation::ALL.len()]);
#[derive(serde::Serialize)]
pub struct RequestStageSnapshot {
    pub(crate) operation: &'static str,
    pub(crate) stage: &'static str,
    pub(crate) bounds_us: [u64; 9],
    pub(crate) buckets: [u64; 10],
    pub(crate) sum_us: u64,
    pub(crate) max_us: u64,
}
impl RequestTimings {
    pub(crate) fn record(&self, operation: RpcOperation, stage: RequestStage, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let h = &self.0[operation as usize][stage as usize];
        h.buckets[BOUNDS_US.partition_point(|bound| *bound < micros)]
            .fetch_add(1, Ordering::Relaxed);
        h.sum_us.fetch_add(micros, Ordering::Relaxed);
        h.max_us.fetch_max(micros, Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> Vec<RequestStageSnapshot> {
        RpcOperation::ALL
            .iter()
            .flat_map(|op| {
                RequestStage::ALL.iter().map(move |stage| {
                    let h = &self.0[*op as usize][*stage as usize];
                    RequestStageSnapshot {
                        operation: op.name(),
                        stage: stage.name(),
                        bounds_us: BOUNDS_US,
                        buckets: h.buckets.each_ref().map(|x| x.load(Ordering::Relaxed)),
                        sum_us: h.sum_us.load(Ordering::Relaxed),
                        max_us: h.max_us.load(Ordering::Relaxed),
                    }
                })
            })
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_and_stage_are_separate_and_snapshot_does_not_reset() {
        let metrics = RequestTimings::default();
        metrics.record(
            RpcOperation::LoadSnapshot,
            RequestStage::Order,
            Duration::from_millis(80),
        );
        metrics.record(
            RpcOperation::LoadSnapshot,
            RequestStage::Handler,
            Duration::from_millis(3),
        );
        metrics.record(
            RpcOperation::SaveSnapshot,
            RequestStage::Handler,
            Duration::from_millis(120),
        );
        for _ in 0..2 {
            let snapshot = metrics.snapshot();
            let read_wait = snapshot
                .iter()
                .find(|s| s.operation == "load_snapshot" && s.stage == "record_order_wait")
                .unwrap();
            assert_eq!(read_wait.buckets.iter().sum::<u64>(), 1);
            assert_eq!(read_wait.buckets[5], 1);
            assert_eq!(read_wait.max_us, 80000);
            let read_run = snapshot
                .iter()
                .find(|s| s.operation == "load_snapshot" && s.stage == "handler")
                .unwrap();
            assert_eq!(read_run.sum_us, 3000);
        }
    }
}
