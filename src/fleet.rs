//! Sync Forge fleet into node registry; prefer RTX 2060 for thinker workloads.

use crate::adapter::forge;
use crate::node::registry::NodeRegistry;
use crate::types::{NodeMetadata, NodeStatistics};
use std::sync::Arc;
use tracing::info;

pub async fn sync_fleet(registry: &NodeRegistry, client: &reqwest::Client, forge_url: &str) {
    let nodes = forge::fetch_fleet_from_forge(client, forge_url).await;
    for meta in nodes {
        let online = forge::probe_online(client, &meta.inference_url).await;
        let mut m = meta;
        m.online = online;
        let id = m.id.clone();
        if registry.get_node(&id).is_some() {
            if let Some(rt) = registry.get_node(&id) {
                if let Ok(mut md) = rt.metadata.write() {
                    *md = m;
                }
                rt.touch_heartbeat(NodeStatistics {
                    free_vram_mb: rt
                        .metadata
                        .read()
                        .map(|x| x.capabilities.vram_mb)
                        .unwrap_or(0),
                    ..Default::default()
                });
            }
        } else {
            registry.register_node(m);
        }
    }
}

pub fn pick_node_for_role(
    registry: &NodeRegistry,
    prefer_role: Option<&str>,
    avoid_p100: bool,
) -> Option<Arc<crate::types::NodeRuntime>> {
    let nodes = registry.list_nodes();
    let role = prefer_role.unwrap_or("thinker").to_lowercase();

    let mut candidates: Vec<_> = nodes
        .into_iter()
        .filter(|rt| {
            let md = match rt.metadata.read() {
                Ok(m) => m,
                Err(_) => return false,
            };
            if !md.online {
                return false;
            }
            if avoid_p100 && (md.gpu_name.contains("P100") || md.id.contains("P100")) {
                return false;
            }
            let r = md.role.to_lowercase();
            if role == "thinker" {
                r.contains("think") || md.id.contains("2060") || md.id == "RTX2060"
            } else if role == "coder" || role == "coding" {
                r.contains("cod") || md.id.contains("P100")
            } else if role == "reviewer" {
                r.contains("review") || md.id.contains("1070")
            } else {
                r.contains(&role) || role == "general"
            }
        })
        .collect();

    if candidates.is_empty() {
        candidates = registry
            .list_nodes()
            .into_iter()
            .filter(|rt| {
                rt.metadata
                    .read()
                    .map(|m| m.online && (!avoid_p100 || !m.gpu_name.contains("P100")))
                    .unwrap_or(false)
            })
            .collect();
    }

    // Prefer RTX2060 explicitly for thinker when P100s are busy
    if role == "thinker" {
        if let Some(rt) = candidates.iter().find(|rt| {
            rt.metadata
                .read()
                .map(|m| m.id.contains("2060") || m.id == "RTX2060")
                .unwrap_or(false)
        }) {
            return Some((*rt).clone());
        }
    }

    candidates.into_iter().max_by(|a, b| {
        let sa = a
            .stats
            .read()
            .map(|s| crate::scheduler::scoring::compute_score(&s))
            .unwrap_or(0.0);
        let sb = b
            .stats
            .read()
            .map(|s| crate::scheduler::scoring::compute_score(&s))
            .unwrap_or(0.0);
        sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
    })
}

pub fn register_local_worker(registry: &NodeRegistry, meta: NodeMetadata) {
    let id = meta.id.clone();
    registry.register_node(meta);
    info!(node_id = %id, "local worker registered");
}
