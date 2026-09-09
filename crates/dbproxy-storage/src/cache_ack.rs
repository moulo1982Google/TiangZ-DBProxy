//! 缓存写成功后的有界清理提示；PG 修复行始终是恢复依据。
//! Bounded cleanup hints after cache success; durable PG repair rows remain authoritative.

use crate::{PostgresCacheRepairQueue, StorageError, StorageMetrics, latency::Stage};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tiangz_dbproxy_core::{RecordKey, Revision};

const CAPACITY: usize = 1_024;
const BATCH_SIZE: usize = 128;

/// 同一后端共享的非持久清理提示，只保存 key/revision，不保存业务 payload。
/// Shared non-durable cleanup hints containing keys/revisions, never business payloads.
/// 提示丢失、满载或刷新失败时，原有持久修复 worker 仍会重建缓存并确认。
/// Lost/full/failed hints fall back to the existing durable repair worker.
#[derive(Clone, Default)]
pub struct CacheRepairAcknowledgements {
    pending: Arc<Mutex<HashMap<RecordKey, Revision>>>,
}

impl CacheRepairAcknowledgements {
    pub(crate) fn record(&self, key: &RecordKey, revision: Revision) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = pending.get_mut(key) {
            current.0 = current.0.max(revision.0);
        } else if pending.len() < CAPACITY {
            pending.insert(key.clone(), revision);
        }
    }

    fn take_batch(&self) -> Vec<(RecordKey, Revision)> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let keys: Vec<_> = pending.keys().take(BATCH_SIZE).cloned().collect();
        keys.into_iter()
            .map(|key| {
                let revision = pending.remove(&key).expect("key held under the same lock");
                (key, revision)
            })
            .collect()
    }

    /// 由现有维护 worker 每轮最多清理一批；失败/取消不删除尚未确认的持久目标。
    /// Flush at most one batch on the existing maintenance worker; failure/cancellation leaves
    /// unacknowledged durable targets for ordinary repair, with no per-request tasks or retries.
    pub async fn flush(
        &self,
        queue: &PostgresCacheRepairQueue,
        metrics: &StorageMetrics,
    ) -> Result<u64, StorageError> {
        let targets = self.take_batch();
        if targets.is_empty() {
            return Ok(0);
        }
        metrics
            .latency
            .measure(
                Stage::RepairAck,
                queue.acknowledge_cached_revisions(&targets),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hints_are_bounded_coalesce_revisions_and_drain_in_bounded_batches() {
        let hints = CacheRepairAcknowledgements::default();
        let key = RecordKey::new("ack", "0").unwrap();
        for i in 0..CAPACITY + 10 {
            hints.record(&RecordKey::new("ack", i.to_string()).unwrap(), Revision(1));
        }
        hints.record(&key, Revision(3));
        hints.record(&key, Revision(2));
        let mut all = HashMap::new();
        loop {
            let batch = hints.take_batch();
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= BATCH_SIZE);
            all.extend(batch);
        }
        assert_eq!(all.len(), CAPACITY);
        assert_eq!(all[&key], Revision(3));
        hints.record(&key, Revision(4));
        assert_eq!(hints.take_batch(), vec![(key, Revision(4))]);
    }
}
