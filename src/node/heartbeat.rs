use crate::node::registry::NodeRegistry;
use crate::opsstore::{OpsEventKind, OpsStore};
use crate::types::{now_ms, NodeStatistics};
use std::sync::Arc;
use tracing::debug;

pub fn record_heartbeat(node: &crate::types::NodeRuntime, stats: NodeStatistics) {
    node.touch_heartbeat(stats);
}

pub fn spawn_sweeper(registry: Arc<NodeRegistry>, interval_secs: u64, stale_secs: u64) {
    spawn_sweeper_with_ops(registry, interval_secs, stale_secs, None);
}

/// Chunk 7: sweeper with optional operational persistence. Nodes that go
/// STALE/OFFLINE are journaled and persisted so restart recovery knows the
/// last known truth (and never assumes liveness).
pub fn spawn_sweeper_with_ops(
    registry: Arc<NodeRegistry>,
    interval_secs: u64,
    stale_secs: u64,
    ops: Option<Arc<OpsStore>>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tick.tick().await;
            let removed = registry.remove_stale(stale_secs * 1000, now_ms());
            if !removed.is_empty() {
                debug!(count = removed.len(), "heartbeat sweep removed nodes");
                if let Some(ops) = &ops {
                    for node_id in &removed {
                        let _ = ops.record(
                            OpsEventKind::NodeOffline,
                            Some(node_id),
                            None,
                            None,
                            None,
                            "heartbeat timeout: node marked OFFLINE (fail closed)",
                            "OFFLINE",
                        );
                        if let Some(rt) = registry.get_node(node_id) {
                            crate::state::persist_node(ops, &rt);
                        }
                    }
                }
            }
        }
    });
}
