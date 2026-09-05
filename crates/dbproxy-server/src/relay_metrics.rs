//! 指标标签只来自启动配置，未知旧路由汇总为 other，不使用事件 ID 或目标地址。
//! Labels are bounded by startup config; unknown historical routes aggregate into other.
use std::{collections::BTreeMap, fmt::Write, sync::Mutex};
use tiangz_dbproxy_storage::{OutboxRoute, OutboxSourceStats};

#[derive(Default)]
struct Entry {
    success: u64,
    error: u64,
    timeout: u64,
    retry: u64,
    dead: u64,
    lease_lost: u64,
    seconds: f64,
    depth: OutboxSourceStats,
}
pub struct RelayMetrics {
    entries: Mutex<BTreeMap<(String, String), Entry>>,
}

impl RelayMetrics {
    pub fn new(routes: &[OutboxRoute]) -> Self {
        let mut entries = BTreeMap::new();
        for key in [
            ("legacy".into(), "legacy".into()),
            ("other".into(), "other".into()),
        ]
        .into_iter()
        .chain(
            routes
                .iter()
                .map(|r| (r.producer.clone(), r.publisher.clone())),
        ) {
            entries.insert(key, Entry::default());
        }
        Self {
            entries: Mutex::new(entries),
        }
    }
    pub fn record(&self, producer: &str, publisher: &str, result: &str, seconds: f64) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let mut key = (producer.into(), publisher.into());
        if !entries.contains_key(&key) {
            key = ("other".into(), "other".into())
        }
        let entry = entries.get_mut(&key).expect("fallback metrics entry");
        match result {
            "success" => entry.success += 1,
            "error" => entry.error += 1,
            "timeout" => entry.timeout += 1,
            "retry" => entry.retry += 1,
            "dead" => entry.dead += 1,
            "lease_lost" => entry.lease_lost += 1,
            _ => {}
        }
        entry.seconds += seconds;
    }
    pub fn depths(&self, stats: Vec<OutboxSourceStats>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        for entry in entries.values_mut() {
            entry.depth = OutboxSourceStats::default()
        }
        for stats in stats {
            let mut key = (stats.producer.clone(), stats.publisher.clone());
            if !entries.contains_key(&key) {
                key = ("other".into(), "other".into())
            }
            let depth = &mut entries.get_mut(&key).expect("fallback metrics entry").depth;
            depth.pending += stats.pending;
            depth.processing += stats.processing;
            depth.dead += stats.dead;
            depth.expired_leases += stats.expired_leases;
            depth.oldest_age_seconds = depth.oldest_age_seconds.max(stats.oldest_age_seconds);
        }
    }
    pub fn render(&self) -> String {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let mut output = String::from(
            "# TYPE dbproxy_outbox_relay_publish_total counter\n# TYPE dbproxy_outbox_relay_publish_seconds_total counter\n# TYPE dbproxy_outbox_relay_state_total counter\n",
        );
        for name in [
            "pending",
            "processing",
            "dead",
            "oldest_age_seconds",
            "expired_leases",
        ] {
            let _ = writeln!(output, "# TYPE dbproxy_outbox_relay_{name} gauge");
        }
        for ((producer, publisher), e) in entries.iter() {
            let escape = |value: &str| {
                value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
            };
            let producer = escape(producer);
            let publisher = escape(publisher);
            let labels = format!(
                "producer=\"{producer}\",publisher=\"{publisher}\",backend=\"redisStream\""
            );
            for (result, value) in [
                ("success", e.success),
                ("error", e.error),
                ("timeout", e.timeout),
            ] {
                let _ = writeln!(
                    output,
                    "dbproxy_outbox_relay_publish_total{{{labels},result=\"{result}\"}} {value}"
                );
            }
            for (result, value) in [
                ("retry", e.retry),
                ("dead", e.dead),
                ("lease_lost", e.lease_lost),
            ] {
                let _ = writeln!(
                    output,
                    "dbproxy_outbox_relay_state_total{{{labels},result=\"{result}\"}} {value}"
                );
            }
            let _ = writeln!(
                output,
                "dbproxy_outbox_relay_publish_seconds_total{{{labels}}} {}",
                e.seconds
            );
            for (name, value) in [
                ("pending", e.depth.pending as f64),
                ("processing", e.depth.processing as f64),
                ("dead", e.depth.dead as f64),
                ("oldest_age_seconds", e.depth.oldest_age_seconds),
                ("expired_leases", e.depth.expired_leases as f64),
            ] {
                let _ = writeln!(output, "dbproxy_outbox_relay_{name}{{{labels}}} {value}");
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unconfigured_labels_are_bounded_and_depths_reset() {
        let metrics = RelayMetrics::new(&[]);
        for id in 0..1000 {
            metrics.record(
                &format!("player-{id}"),
                "unknown\"publisher",
                "success",
                0.01,
            );
        }
        let rendered = metrics.render();
        assert!(!rendered.contains("player-"));
        assert!(!rendered.contains("unknown"));
        assert!(rendered.contains("result=\"success\"} 1000"));
        metrics.depths(vec![OutboxSourceStats {
            producer: "x".into(),
            publisher: "y".into(),
            pending: 5,
            ..Default::default()
        }]);
        assert!(metrics.render().contains("dbproxy_outbox_relay_pending{producer=\"other\",publisher=\"other\",backend=\"redisStream\"} 5"));
        metrics.depths(vec![]);
        assert!(!metrics.render().contains("} 5\n"));
    }
}
