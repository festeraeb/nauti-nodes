use crate::node::registry::NodeRegistry;
use crate::types::{now_ms, NodeStatistics};
use std::sync::Arc;
use tracing::debug;

pub fn record_heartbeat(node: &crate::types::NodeRuntime, stats: NodeStatistics) {
    node.touch_heartbeat(stats);
}

pub fn spawn_sweeper(registry: Arc<NodeRegistry>, interval_secs: u64, stale_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tick.tick().await;
            let removed = registry.remove_stale(stale_secs * 1000, now_ms());
            if !removed.is_empty() {
                debug!(count = removed.len(), "heartbeat sweep removed nodes");
            }
        }
    });
}
