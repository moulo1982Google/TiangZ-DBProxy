//! 有界查询与带审计的死信重试，不提供历史事件广播重放。
//! Bounded inspection and audited dead-letter retry, not historical broadcast replay.
use crate::{PostgresOutboxQueue, StorageError};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OutboxInspection {
    pub event_id: String,
    pub producer: String,
    pub publisher: String,
    pub destination: String,
    pub attempts: i64,
    pub expired_leases: i64,
    pub published: bool,
    pub dead_lettered: bool,
    pub leased: bool,
    pub last_error: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub struct OutboxSourceStats {
    pub producer: String,
    pub publisher: String,
    pub pending: u64,
    pub processing: u64,
    pub dead: u64,
    pub oldest_age_seconds: f64,
    pub expired_leases: u64,
}

impl PostgresOutboxQueue {
    /// 只返回诊断元数据，不输出游戏 payload 或连接密钥。
    /// Returns diagnostic metadata without game payloads or connection secrets.
    pub async fn inspect(&self, event_id: &str) -> Result<Option<OutboxInspection>, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        Ok(client.query_opt("SELECT event_id,producer,publisher_id,destination,attempt_count,expired_leases,published_at IS NOT NULL,dead_lettered_at IS NOT NULL,COALESCE(lease_until>clock_timestamp(),false),last_error FROM dbproxy_outbox WHERE event_id=$1",&[&event_id]).await?.map(|r|OutboxInspection{
            event_id:r.get(0),producer:r.get(1),publisher:r.get(2),destination:r.get(3),attempts:r.get(4),
            expired_leases:r.get(5),published:r.get(6),dead_lettered:r.get(7),leased:r.get(8),last_error:r.get(9)}))
    }

    /// 固定上限，避免管理查询一次读取全部积压。
    /// Bounds inspection instead of loading the whole backlog.
    pub async fn dead_letter_ids(&self) -> Result<Vec<String>, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        Ok(client.query("SELECT event_id FROM dbproxy_outbox WHERE dead_lettered_at IS NOT NULL ORDER BY dead_lettered_at,event_id LIMIT 100",&[]).await?
            .into_iter().map(|r|r.get(0)).collect())
    }

    /// 重试原事件，审计与状态改变同事务；不清除已发布标记或改变投递目标。
    /// Retries the original event with atomic audit, preserving route and published state.
    pub async fn retry_dead_letter(
        &self,
        event_id: &str,
        operator: &str,
        reason: &str,
    ) -> Result<bool, StorageError> {
        for value in [event_id, operator, reason] {
            if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
                return Err(StorageError::QueueProtocol(
                    "invalid outbox retry ID, operator or reason".into(),
                ));
            }
        }
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let tx = client.transaction().await?;
        let row=tx.query_opt("SELECT attempt_count,last_error FROM dbproxy_outbox WHERE event_id=$1 AND published_at IS NULL AND dead_lettered_at IS NOT NULL AND (lease_until IS NULL OR lease_until<=clock_timestamp()) FOR UPDATE",&[&event_id]).await?;
        let Some(row) = row else { return Ok(false) };
        tx.execute("INSERT INTO dbproxy_outbox_admin_audit(event_id,operator_name,reason,prior_attempts,prior_error) VALUES($1,$2,$3,$4,$5)",
            &[&event_id,&operator,&reason,&row.get::<_,i64>(0),&row.get::<_,Option<String>>(1)]).await?;
        tx.execute("UPDATE dbproxy_outbox SET attempt_count=0,available_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,last_error=NULL,dead_lettered_at=NULL,lease_token=lease_token+1 WHERE event_id=$1",&[&event_id]).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn source_stats(&self) -> Result<Vec<OutboxSourceStats>, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        Ok(client.query("SELECT producer,publisher_id, COUNT(*) FILTER(WHERE dead_lettered_at IS NULL AND (lease_until IS NULL OR lease_until<=clock_timestamp())), COUNT(*) FILTER(WHERE dead_lettered_at IS NULL AND lease_until>clock_timestamp()),COUNT(*) FILTER(WHERE dead_lettered_at IS NOT NULL), COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-MIN(created_at) FILTER(WHERE dead_lettered_at IS NULL)),0)::DOUBLE PRECISION, COALESCE(SUM(expired_leases),0)::BIGINT FROM dbproxy_outbox WHERE published_at IS NULL GROUP BY producer,publisher_id",&[]).await?
            .into_iter().map(|r|OutboxSourceStats { producer:r.get(0),publisher:r.get(1),pending:r.get::<_,i64>(2).max(0) as u64,
                processing:r.get::<_,i64>(3).max(0) as u64,dead:r.get::<_,i64>(4).max(0) as u64,
                oldest_age_seconds:r.get::<_,f64>(5).max(0.0),expired_leases:r.get::<_,i64>(6).max(0) as u64 }).collect())
    }
}
