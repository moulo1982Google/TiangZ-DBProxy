//! 使用已有库做只读结构巡检，不执行建表或迁移。
//! Read-only schema diagnostics; no DDL or migrations.
use tiangz_dbproxy_storage::PostgresSnapshotStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DBPROXY_POSTGRES_URL")?;
    let store = PostgresSnapshotStore::connect_existing(&url).await?;
    store.validate_schema_indexes().await?;
    println!("DBProxy schema indexes are valid (18 tables and 32 snapshot partitions)");
    Ok(())
}
