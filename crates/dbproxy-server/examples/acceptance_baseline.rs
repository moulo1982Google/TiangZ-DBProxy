//! B1 only: the same process/configuration as production, without receipt cleanup.
#[path = "../src/server_process.rs"]
mod server_process;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref() != Ok("1") {
        return Err("acceptance baseline requires explicit test opt-in".into());
    }
    server_process::main(false)
}
