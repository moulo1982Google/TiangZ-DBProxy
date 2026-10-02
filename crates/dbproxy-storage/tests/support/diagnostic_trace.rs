//! Small bounded tracing capture for real contention tests; no production dependency.
use std::sync::{Arc, Mutex};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone, Default)]
pub struct DiagnosticTrace(pub Arc<Mutex<Vec<serde_json::Value>>>);
pub struct TraceOnDrop(pub DiagnosticTrace);
impl Drop for TraceOnDrop {
    fn drop(&mut self) {
        println!(
            "PG_DIAGNOSTICS_TRACE {}",
            serde_json::json!(*self.0.0.lock().unwrap())
        );
    }
}
struct Fields(serde_json::Map<String, serde_json::Value>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_owned(),
            serde_json::Value::String(format!("{value:?}")),
        );
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(
            field.name().to_owned(),
            serde_json::Value::String(value.to_owned()),
        );
    }
}
impl Subscriber for DiagnosticTrace {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() != "tiangz_dbproxy_storage::postgres_diagnostics" {
            return;
        }
        let mut fields = Fields(serde_json::Map::new());
        event.record(&mut fields);
        let mut records = self.0.lock().unwrap();
        if records.len() == 32 {
            records.remove(0);
        }
        records.push(serde_json::Value::Object(fields.0));
    }
}
