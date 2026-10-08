//! Rolling, bounded evidence so early injected faults cannot exhaust later diagnostics.
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_storage::postgres_operation_fingerprint;

const CAPACITY: usize = 64;
pub(super) type ErrorSample = serde_json::Value;
#[derive(Default)]
struct State {
    samples: VecDeque<ErrorSample>,
    total: u64,
    exported: u64,
    last_sampled: Option<Instant>,
}
#[derive(Default)]
pub(super) struct ErrorDiagnostics {
    state: Mutex<State>,
}
impl ErrorDiagnostics {
    pub(super) fn record(
        &self,
        stage: &str,
        retryable: bool,
        correlation: Option<&str>,
        error: &str,
    ) -> Option<ErrorSample> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.total += 1;
        let sample = serde_json::json!({
            "sequence":state.total,
            "observed_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "stage":stage.chars().take(64).collect::<String>(), "retryable":retryable,
            "correlation":correlation.map(postgres_operation_fingerprint),
            "error":error.chars().take(512).collect::<String>()
        });
        if state.samples.len() == CAPACITY {
            state.samples.pop_front();
        }
        state.samples.push_back(sample.clone());
        if state.total <= 16
            || state
                .last_sampled
                .is_none_or(|t| t.elapsed() >= Duration::from_secs(10))
        {
            state.last_sampled = Some(Instant::now());
            Some(sample)
        } else {
            None
        }
    }
    pub(super) fn export(&self, only_changed: bool) -> Option<serde_json::Value> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if only_changed && state.total == state.exported {
            return None;
        }
        state.exported = state.total;
        Some(
            serde_json::json!({"capacity":CAPACITY,"totalErrors":state.total,
            "evictedErrors":state.total.saturating_sub(state.samples.len() as u64),"recentErrors":state.samples}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_errors_replace_early_faults_and_export_only_on_change() {
        let diagnostics = ErrorDiagnostics::default();
        for i in 0..200 {
            diagnostics.record(
                "apply_trade",
                true,
                Some("private-operation"),
                &format!("failure-{i}"),
            );
        }
        let evidence = diagnostics.export(true).unwrap();
        let recent = evidence["recentErrors"].as_array().unwrap();
        assert_eq!(recent.len(), 64);
        assert_eq!(evidence["evictedErrors"], 136);
        assert_eq!(recent[63]["error"], "failure-199");
        assert_eq!(
            recent[63]["correlation"],
            postgres_operation_fingerprint("private-operation")
        );
        assert!(diagnostics.export(true).is_none());
        diagnostics.record("fixture", false, None, &"错".repeat(600));
        let evidence = diagnostics.export(true).unwrap();
        assert_eq!(
            evidence["recentErrors"][63]["error"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            512
        );
    }
}
