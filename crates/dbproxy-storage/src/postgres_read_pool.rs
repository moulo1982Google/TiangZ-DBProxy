//! Bounded, per-tenant primary-PG read connections, shared by all write shards.
use crate::{PostgresRequestConfig, ReconnectingPostgresClient, StorageError, duration_millis};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{MutexGuard, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct PostgresReadPool(Arc<Pool>);
struct Pool {
    idle: Mutex<VecDeque<Box<ReconnectingPostgresClient>>>,
    permits: Arc<Semaphore>,
    wait: Duration,
    capacity: usize,
}
/// Instantaneous pool occupancy for gauges: `in_use` counts leased slots, including slots whose
/// holder is still reconnecting, and never counts waiters. Sampled, not transactional.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadPoolUsage {
    pub capacity: u64,
    pub in_use: u64,
}
impl PostgresReadPool {
    pub fn usage(&self) -> ReadPoolUsage {
        let capacity = self.0.capacity;
        let available = self.0.permits.available_permits().min(capacity);
        ReadPoolUsage {
            capacity: capacity as u64,
            in_use: (capacity - available) as u64,
        }
    }
    /// Connect only to the same primary URL as the write stores; performs no migrations.
    /// `size` is the pool itself and must be 1..=64; the server-level setting
    /// `storage.postgresReadConnections = 0` means "no pool" and never reaches this call.
    pub async fn connect(
        url: &str,
        size: usize,
        config: PostgresRequestConfig,
    ) -> Result<Self, StorageError> {
        if !(1..=64).contains(&size) {
            return Err(StorageError::InvalidPostgresReadConnections);
        }
        let config = config.validate()?;
        let mut idle = VecDeque::with_capacity(size);
        for _ in 0..size {
            let mut client = ReconnectingPostgresClient::connect(url).await?;
            client.reconnect_cooldown = config.reconnect_cooldown;
            idle.push_back(Box::new(client));
        }
        Ok(Self(Arc::new(Pool {
            idle: Mutex::new(idle),
            permits: Arc::new(Semaphore::new(size)),
            wait: config.connection_wait_timeout,
            capacity: size,
        })))
    }
    pub(crate) async fn acquire(&self) -> Result<ReadLease, StorageError> {
        // FIFO capacity wait is cancelled safely: no slot has been removed before it completes.
        let permit = tokio::time::timeout(self.0.wait, self.0.permits.clone().acquire_owned())
            .await
            .map_err(|_| StorageError::PostgresConnectionWaitTimeout {
                timeout_ms: duration_millis(self.0.wait),
            })?
            .expect("read pool semaphore is never closed");
        let client = self
            .0
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
            .expect("permit owns an idle connection");
        Ok(ReadLease {
            client: Some(client),
            pool: self.0.clone(),
            _permit: permit,
        })
    }
}
pub(crate) struct ReadLease {
    client: Option<Box<ReconnectingPostgresClient>>,
    pool: Arc<Pool>,
    _permit: OwnedSemaphorePermit,
}
impl Deref for ReadLease {
    type Target = ReconnectingPostgresClient;
    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}
impl DerefMut for ReadLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        // Return the slot before releasing its permit, also on error or future cancellation.
        // A poisoned mutex must not turn this drop into a second panic; the queue stays usable.
        self.pool
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(self.client.take().unwrap());
    }
}
pub(crate) enum ReadClient<'a> {
    Shared(MutexGuard<'a, ReconnectingPostgresClient>),
    Pooled(ReadLease),
}
impl Deref for ReadClient<'_> {
    type Target = ReconnectingPostgresClient;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(c) => c,
            Self::Pooled(c) => c,
        }
    }
}
impl DerefMut for ReadClient<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Shared(c) => c,
            Self::Pooled(c) => c,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PostgresSnapshotStore;
    use tiangz_dbproxy_core::{
        AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTradeStore,
        AsyncTransactionalStore, RecordKey, Revision, SnapshotWrite,
    };
    use tiangz_dbproxy_core::{
        LedgerPosting, MultiRecordTransactionalWrite, TradeState, TradeTransaction,
        TradeTransition, TransactionalRecordWrite, TransactionalWrite,
    };

    #[tokio::test]
    #[ignore = "fresh isolated PG; read-path coverage, pool saturation, cancellation and backend termination"]
    async fn real_read_pool_paths_capacity_cancellation_and_reconnect() {
        tokio::time::timeout(Duration::from_secs(30), async {
            assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
            let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
            let config = PostgresRequestConfig { connection_wait_timeout: Duration::from_millis(40), ..Default::default() };
            let pool = PostgresReadPool::connect(&url, 2, config).await.unwrap();
            let mut store = PostgresSnapshotStore::connect_with_request_config(&url, config).await.unwrap().with_read_pool(pool.clone());
            let key = RecordKey::new("read-pool", "seed").unwrap();
            store.save(SnapshotWrite { request_id: "read-pool-seed".into(), record: key.clone(), schema: "test".into(), schema_version: 1, payload: vec![42], expected_revision: Some(Revision::ZERO), updated_at_unix_ms: 1 }).await.unwrap();
            let single = RecordKey::new("read-pool", "single-receipt").unwrap();
            store.apply(TransactionalWrite { operation_id: "pool-single".into(), record: single.clone(), schema: "test".into(), schema_version: 1, expected_revision: Revision::ZERO, payload: vec![1], result: vec![11], updated_at_unix_ms: 1 }).await.unwrap();
            let multi = RecordKey::new("read-pool", "multi-receipt").unwrap();
            let record_write = |record| TransactionalRecordWrite { record, schema: "test".into(), schema_version: 1, expected_revision: Revision::ZERO, payload: vec![2], updated_at_unix_ms: 1 };
            store.apply_multi(MultiRecordTransactionalWrite { operation_id: "pool-multi".into(), writes: vec![record_write(multi.clone())], result: vec![22] }).await.unwrap();
            store.apply_trade(TradeTransaction {
                operation_id: "pool-trade-operation".into(),
                transition: TradeTransition { trade_id: "pool-trade".into(), expected_version: Revision::ZERO, expected_state: None, next_state: TradeState::Escrowed, payload: vec![3], updated_at_unix_ms: 1 },
                writes: vec![record_write(RecordKey::new("read-pool", "trade-record").unwrap())],
                ledger_postings: vec![
                    LedgerPosting { posting_id: "pool-debit".into(), account_id: "buyer".into(), asset: "gold".into(), amount: -1, metadata: vec![] },
                    LedgerPosting { posting_id: "pool-credit".into(), account_id: "escrow".into(), asset: "gold".into(), amount: 1, metadata: vec![] },
                ], outbox_events: vec![], result: vec![33],
            }).await.unwrap();
            let write_lock = store.client.lock().await;
            // All standalone authority read paths must avoid the held write connection.
            assert_eq!(store.load(&key).await.unwrap().unwrap().payload, vec![42]);
            assert_eq!(store.load_multi(std::slice::from_ref(&key)).await.unwrap()[0].as_ref().unwrap().revision, Revision(1));
            assert!(store.load_receipt("absent", &key).await.unwrap().is_none());
            assert!(store.load_multi_receipt("absent", std::slice::from_ref(&key)).await.unwrap().is_none());
            assert!(store.load_trade("absent").await.unwrap().is_none());
            assert!(store.load_trade_receipt("absent", "absent").await.unwrap().is_none());
            assert_eq!(store.load_receipt("pool-single", &single).await.unwrap().unwrap().result, vec![11]);
            assert_eq!(store.load_multi_receipt("pool-multi", &[multi]).await.unwrap().unwrap().result, vec![22]);
            assert_eq!(store.load_trade("pool-trade").await.unwrap().unwrap().payload, vec![3]);
            assert_eq!(store.load_trade_receipt("pool-trade-operation", "pool-trade").await.unwrap().unwrap().result, vec![33]);
            let first = pool.acquire().await.unwrap();
            let second = pool.acquire().await.unwrap();
            assert!(matches!(store.load(&key).await, Err(StorageError::PostgresConnectionWaitTimeout { .. })));
            // Cancelling a capacity waiter must not consume a slot.
            let mut waiter = Box::pin(pool.acquire());
            assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiter).await.is_err());
            drop(waiter);
            drop(first);
            assert_eq!(store.load(&key).await.unwrap().unwrap().payload, vec![42]);
            drop(second);
            let first = pool.acquire().await.unwrap();
            let second = pool.acquire().await.unwrap();
            let pid1: i32 = first.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
            let pid2: i32 = second.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
            assert_ne!(pid1, pid2);
            // Kill only these two owned test read backends, then reuse the same pool.
            for pid in [pid1, pid2] { write_lock.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await.unwrap(); }
            while !first.is_closed() || !second.is_closed() { tokio::task::yield_now().await; }
            drop(first); drop(second);
            // Public reads, not an explicit reconnect call, must reopen both FIFO slots.
            assert_eq!(store.load(&key).await.unwrap().unwrap().revision, Revision(1));
            assert_eq!(store.load(&key).await.unwrap().unwrap().revision, Revision(1));
            let first = pool.acquire().await.unwrap();
            let second = pool.acquire().await.unwrap();
            assert_ne!(first.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get::<_,i32>(0), pid1);
            assert_ne!(second.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get::<_,i32>(0), pid2);
            drop(first); drop(second);
            assert_eq!(store.load(&key).await.unwrap().unwrap().revision, Revision(1));
            let owned = pool.clone();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let holding = tokio::spawn(async move {
                let _lease = owned.acquire().await.unwrap();
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            ready.await.unwrap();
            holding.abort();
            assert!(holding.await.unwrap_err().is_cancelled());
            assert_eq!(pool.0.permits.available_permits(), 2);
            assert_eq!(pool.0.idle.lock().unwrap().len(), 2);
            assert_eq!(pool.usage(), ReadPoolUsage { capacity: 2, in_use: 0 });
            let occupied = pool.acquire().await.unwrap();
            assert_eq!(pool.usage(), ReadPoolUsage { capacity: 2, in_use: 1 });
            drop(occupied);
            assert_eq!(pool.usage().in_use, 0);
            assert!(store.metrics.latency.snapshot().iter().any(|s| s.stage == "postgres_read_pool_wait" && s.sum_micros > 0));

            println!("READ_POOL all_six_paths=correct write_lock=bypassed saturation=timeout cancellation=returned reconnect=correct capacity=2");
        }).await.unwrap();
    }
}
