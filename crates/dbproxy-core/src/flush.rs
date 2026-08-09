//! 普通快照的合并与排空协调器。
//! Coalescing and flush coordinator for ordinary snapshots.
//!
//! 该队列只适用于允许回退的 snapshot 数据，不适用于货币、背包、交易等关键事务。
//! It is only for rollback-tolerant snapshot data, never for currency, inventory, or trade.

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};

use super::{AsyncSnapshotStore, RecordKey, SnapshotWrite, SnapshotWriteOutcome, StoreError};

/// 一次 Flush 的统计结果。
/// Statistics for one flush attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotFlushReport {
    pub attempted: usize,
    pub applied: usize,
    pub duplicate: usize,
    pub remaining: usize,
}

impl SnapshotFlushReport {
    fn merge(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.applied += other.applied;
        self.duplicate += other.duplicate;
        self.remaining = other.remaining;
    }
}

/// Flush 中途失败时保留的错误和统计。
/// Error and statistics returned when a flush stops midway.
#[derive(Debug)]
pub struct SnapshotFlushError<E> {
    pub error: E,
    pub report: SnapshotFlushReport,
}

impl<E: Display> Display for SnapshotFlushError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "snapshot flush failed: {}", self.error)
    }
}

impl<E: Error + 'static> Error for SnapshotFlushError<E> {}

/// 普通快照的有界合并队列。
/// Bounded coalescing queue for ordinary snapshots.
#[derive(Default)]
pub struct SnapshotFlushQueue {
    order: VecDeque<RecordKey>,
    pending: HashMap<RecordKey, SnapshotWrite>,
}

impl SnapshotFlushQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// 加入一条允许无条件覆盖的最新快照；同一记录只保留最后一条。
    /// Enqueue an unconditional latest snapshot; only the newest request per record remains.
    ///
    /// 事务或带CAS版本的请求必须走`AsyncTransactionalStore`，不能放入这个队列。
    /// Transactional or CAS-bound requests must use `AsyncTransactionalStore` instead.
    pub fn enqueue(&mut self, request: SnapshotWrite) -> Result<bool, StoreError> {
        if request.request_id.trim().is_empty() {
            return Err(StoreError::EmptyRequestId);
        }
        if request.record.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty"));
        }
        if request.record.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty"));
        }
        if request.expected_revision.is_some() {
            return Err(StoreError::QueuedSnapshotRequiresUnconditionalWrite {
                record: request.record,
            });
        }

        let record = request.record.clone();
        let replaced = self.pending.insert(record.clone(), request).is_some();
        if !replaced {
            self.order.push_back(record);
        }
        Ok(replaced)
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// 最多排空`max_items`条；失败请求会放回队首，等待下一轮重试。
    /// Flush at most `max_items`; a failed request returns to the front for retry.
    pub async fn flush<S>(
        &mut self,
        store: &mut S,
        max_items: usize,
    ) -> Result<SnapshotFlushReport, SnapshotFlushError<S::Error>>
    where
        S: AsyncSnapshotStore,
    {
        let mut report = SnapshotFlushReport::default();
        while report.attempted < max_items {
            let Some(record) = self.order.pop_front() else {
                break;
            };
            let request = self
                .pending
                .remove(&record)
                .expect("flush order and pending map must stay in sync");
            report.attempted += 1;
            match store.save(request.clone()).await {
                Ok(SnapshotWriteOutcome::Applied { .. }) => report.applied += 1,
                Ok(SnapshotWriteOutcome::Duplicate { .. }) => report.duplicate += 1,
                Err(error) => {
                    self.pending.insert(record.clone(), request);
                    self.order.push_front(record);
                    report.remaining = self.len();
                    return Err(SnapshotFlushError { error, report });
                }
            }
        }
        report.remaining = self.len();
        Ok(report)
    }

    /// 在有限轮数内反复排空，适合进程优雅停机的最后一个 Flush 阶段。
    /// Repeatedly flush within a bounded number of rounds for graceful shutdown.
    ///
    /// 返回时`remaining > 0`表示停机窗口已耗尽，调用方不能把它记录成“全部保存成功”。
    /// If `remaining > 0`, the shutdown window was exhausted and the caller must not report full success.
    ///
    /// `max_items_per_round`或`max_rounds`为零时不执行 I/O，只返回当前积压量。
    /// A zero item limit or round limit performs no I/O and only reports the current backlog.
    pub async fn flush_until_empty<S>(
        &mut self,
        store: &mut S,
        max_items_per_round: usize,
        max_rounds: usize,
    ) -> Result<SnapshotFlushReport, SnapshotFlushError<S::Error>>
    where
        S: AsyncSnapshotStore,
    {
        let mut total = SnapshotFlushReport {
            remaining: self.len(),
            ..SnapshotFlushReport::default()
        };
        if max_items_per_round == 0 || max_rounds == 0 {
            return Ok(total);
        }

        for _ in 0..max_rounds {
            if self.is_empty() {
                break;
            }
            match self.flush(store, max_items_per_round).await {
                Ok(report) => total.merge(report),
                Err(mut error) => {
                    total.merge(error.report);
                    error.report = total;
                    return Err(error);
                }
            }
        }
        total.remaining = self.len();
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsyncSnapshotStore, InMemorySnapshotStore, SnapshotEnvelope, SnapshotStore};

    fn key(value: &str) -> RecordKey {
        RecordKey::new("player", value).unwrap()
    }

    fn request(record: RecordKey, request_id: &str, payload: &[u8]) -> SnapshotWrite {
        SnapshotWrite {
            request_id: request_id.to_string(),
            record,
            schema: "player.runtime".to_string(),
            schema_version: 1,
            payload: payload.to_vec(),
            expected_revision: None,
            updated_at_unix_ms: 1,
        }
    }

    #[derive(Default)]
    struct AsyncInMemoryStore(InMemorySnapshotStore);

    #[async_trait::async_trait]
    impl AsyncSnapshotStore for AsyncInMemoryStore {
        type Error = StoreError;

        async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
            SnapshotStore::load(&self.0, record)
        }

        async fn save(
            &mut self,
            request: SnapshotWrite,
        ) -> Result<SnapshotWriteOutcome, Self::Error> {
            SnapshotStore::save(&mut self.0, request)
        }
    }

    struct FailingStore;

    #[async_trait::async_trait]
    impl AsyncSnapshotStore for FailingStore {
        type Error = StoreError;

        async fn load(&self, _record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
            Ok(None)
        }

        async fn save(
            &mut self,
            _request: SnapshotWrite,
        ) -> Result<SnapshotWriteOutcome, Self::Error> {
            Err(StoreError::InvalidKey("injected failure"))
        }
    }

    #[tokio::test]
    async fn queue_keeps_only_the_latest_snapshot_per_record() {
        let mut queue = SnapshotFlushQueue::new();
        assert!(!queue.enqueue(request(key("1001"), "req-1", b"v1")).unwrap());
        assert!(queue.enqueue(request(key("1001"), "req-2", b"v2")).unwrap());
        assert_eq!(queue.len(), 1);

        let mut store = AsyncInMemoryStore::default();
        let report = queue.flush(&mut store, 1).await.unwrap();
        assert_eq!(report.applied, 1);
        assert_eq!(
            SnapshotStore::load(&store.0, &key("1001"))
                .unwrap()
                .unwrap()
                .payload,
            b"v2"
        );
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn failed_flush_keeps_the_request_for_a_later_retry() {
        let mut queue = SnapshotFlushQueue::new();
        queue.enqueue(request(key("1001"), "req-1", b"v1")).unwrap();
        let mut failing = FailingStore;
        let error = queue.flush(&mut failing, 1).await.unwrap_err();
        assert_eq!(error.report.remaining, 1);
        assert_eq!(queue.len(), 1);

        let mut store = AsyncInMemoryStore::default();
        let report = queue.flush(&mut store, 1).await.unwrap();
        assert_eq!(report.applied, 1);
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn flush_limit_and_shutdown_round_limit_are_reported() {
        let mut queue = SnapshotFlushQueue::new();
        for id in ["1001", "1002", "1003"] {
            queue
                .enqueue(request(key(id), &format!("req-{id}"), id.as_bytes()))
                .unwrap();
        }

        let mut store = AsyncInMemoryStore::default();
        let report = queue.flush_until_empty(&mut store, 1, 2).await.unwrap();
        assert_eq!(report.attempted, 2);
        assert_eq!(report.applied, 2);
        assert_eq!(report.remaining, 1);
        assert_eq!(queue.len(), 1);

        let report = queue.flush_until_empty(&mut store, 2, 1).await.unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.applied, 1);
        assert_eq!(report.remaining, 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn queue_rejects_compare_and_swap_requests() {
        let mut queue = SnapshotFlushQueue::new();
        let mut request = request(key("1001"), "req-1", b"v1");
        request.expected_revision = Some(crate::Revision::ZERO);
        assert!(matches!(
            queue.enqueue(request),
            Err(StoreError::QueuedSnapshotRequiresUnconditionalWrite { .. })
        ));
    }
}
