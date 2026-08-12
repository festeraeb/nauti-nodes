use crate::types::{NodeId, NodeMetadata, NodeRuntime, MAILBOX_CAP};
use dashmap::DashMap;
use std::sync::Arc;
use tracing::{info, warn};

pub struct NodeRegistry {
    nodes: DashMap<NodeId, Arc<NodeRuntime>>,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
        }
    }

    pub fn register_node(&self, meta: NodeMetadata) -> Arc<NodeRuntime> {
        let (tx, _rx) = tokio::sync::mpsc::channel(MAILBOX_CAP);
        let rt = Arc::new(NodeRuntime::new(meta.clone(), tx));
        self.nodes.insert(meta.id.clone(), rt.clone());
        info!(node_id = %meta.id, "node registered");
        rt
    }

    pub fn deregister_node(&self, id: &str) -> bool {
        if self.nodes.remove(id).is_some() {
            info!(node_id = %id, "node deregistered");
            true
        } else {
            false
        }
    }

    pub fn get_node(&self, id: &str) -> Option<Arc<NodeRuntime>> {
        self.nodes.get(id).map(|e| e.clone())
    }

    pub fn list_nodes(&self) -> Vec<Arc<NodeRuntime>> {
        self.nodes.iter().map(|e| e.value().clone()).collect()
    }

    pub fn remove_stale(&self, stale_after_ms: u64, now_ms: u64) -> Vec<NodeId> {
        use crate::types::ReconciliationStatus;
        let mut changed = Vec::new();
        for mut ref_multi in self.nodes.iter_mut() {
            let rt = ref_multi.value();
            let age = now_ms.saturating_sub(rt.last_heartbeat_ms());
            if let Ok(mut md) = rt.metadata.write() {
                let prev_health = md.health_state.clone();
                if age > stale_after_ms {
                    md.health_state = "OFFLINE".to_string();
                    md.online = false;
                    md.reconciliation.status = ReconciliationStatus::OFFLINE;
                    if prev_health != "OFFLINE" {
                        changed.push(md.id.clone());
                    }
                } else if age > 15000 {
                    md.health_state = "STALE".to_string();
                    md.reconciliation.status = ReconciliationStatus::STALE;
                    if prev_health != "STALE" {
                        changed.push(md.id.clone());
                    }
                } else {
                    md.health_state = "ONLINE".to_string();
                    if prev_health != "ONLINE" {
                        changed.push(md.id.clone());
                    }
                }
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{now_ms, NodeCapabilities};

    fn sample_meta(id: &str) -> NodeMetadata {
        NodeMetadata {
            id: id.into(),
            addr: "10.0.0.1:5001".into(),
            inference_url: "http://10.0.0.1:5001".into(),
            role: "coder".into(),
            gpu_name: "test".into(),
            online: true,
            public_key: vec![],
            capabilities: NodeCapabilities {
                max_batch: 1,
                models: vec!["test".into()],
                vram_mb: 8192,
                role: "coder".into(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn register_get_list_deregister() {
        let reg = NodeRegistry::new();
        reg.register_node(sample_meta("n1"));
        assert!(reg.get_node("n1").is_some());
        assert_eq!(reg.list_nodes().len(), 1);
        assert!(reg.deregister_node("n1"));
        assert!(reg.get_node("n1").is_none());
    }

    #[test]
    fn stale_removed() {
        let reg = NodeRegistry::new();
        let rt = reg.register_node(sample_meta("stale"));
        rt.last_heartbeat.store(
            now_ms().saturating_sub(60_000),
            std::sync::atomic::Ordering::Relaxed,
        );
        let gone = reg.remove_stale(30_000, now_ms());
        assert_eq!(gone, vec!["stale".to_string()]);
    }
}
