//! Sync Forge fleet into node registry; prefer RTX 2060 for thinker workloads.

use crate::adapter::forge;
use crate::node::registry::NodeRegistry;
use crate::types::{NodeMetadata, NodeStatistics};
use std::sync::Arc;
use tracing::info;

pub async fn sync_fleet(registry: &NodeRegistry, client: &reqwest::Client, forge_url: &str) {
    let nodes = forge::fetch_fleet_from_forge(client, forge_url).await;

    let mut observed_gpus = Vec::new();
    let url = format!("{}/cluster/gpus", forge_url.trim_end_matches('/'));
    if let Ok(resp) = client.get(&url).send().await {
        if resp.status().is_success() {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if let Some(gpus) = body.get("gpus").and_then(|g| g.as_array()) {
                    observed_gpus = gpus.clone();
                }
            }
        }
    }

    for meta in nodes {
        let online = forge::probe_online(client, &meta.inference_url).await;
        let mut m = meta;
        m.online = online;

        m.claim = crate::types::RegisteredHardwareClaim {
            gpu_name: m.gpu_name.clone(),
            vram_mb: m.capabilities.vram_mb,
            models: m.capabilities.models.clone(),
        };

        let mut observed = None;
        for gpu in &observed_gpus {
            let worker_name = gpu
                .get("worker_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let node = gpu.get("node").and_then(|v| v.as_str()).unwrap_or("");
            if worker_name == m.id
                || node == m.id
                || m.id.contains(worker_name)
                || m.id.contains(node)
            {
                let gpu_name = gpu
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN")
                    .to_string();
                let vram_mb = gpu.get("vram_mb").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                let backend = gpu
                    .get("backend")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let temp_c = gpu
                    .get("temperature_c")
                    .and_then(|v| v.as_f64())
                    .map(|t| t as u32);
                let src_type_str = gpu
                    .get("source_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN_PROVENANCE");
                let source_type = serde_json::from_value(serde_json::json!(src_type_str))
                    .unwrap_or(crate::types::TelemetrySourceType::UnknownProvenance);
                let observer = gpu
                    .get("observer_node_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("forge")
                    .to_string();
                let provider = gpu
                    .get("provider")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let obs_at = gpu.get("observed_at").and_then(|v| v.as_u64());

                observed = Some(crate::types::ObservedHardware {
                    gpu_name,
                    vram_mb,
                    vendor: backend,
                    device_count: 1,
                    driver_version: None,
                    temperature_c: temp_c,
                    node_id: m.id.clone(),
                    observer_node_id: observer,
                    source_type,
                    provider,
                    observed_at: obs_at,
                    received_at: Some(crate::types::now_ms()),
                    measurement_id: None,
                    device_id: None,
                    sequence: None,
                    age_ms: None,
                    source_endpoint: None,
                });
                break;
            }
        }

        if observed.is_none() {
            if let Some(existing_obs) = registry
                .get_node(&m.id)
                .and_then(|rt| rt.metadata.read().ok().and_then(|md| md.observed.clone()))
            {
                observed = Some(existing_obs);
            } else {
                let local_gpus = crate::gpu::discover_gpus();
                if !local_gpus.is_empty() {
                    if m.id == "cognitive-worker" {
                        if let Some(gpu) = local_gpus
                            .iter()
                            .find(|g| g.name.contains("2060") || g.name.contains("RTX"))
                        {
                            observed = Some(crate::types::ObservedHardware {
                                gpu_name: gpu.name.clone(),
                                vram_mb: gpu.total_memory_mb,
                                vendor: gpu.vendor.clone(),
                                device_count: local_gpus.len() as u32,
                                driver_version: gpu.driver_version.clone(),
                                temperature_c: gpu.temperature_c.map(|t| t as u32),
                                node_id: m.id.clone(),
                                observer_node_id: "local".to_string(),
                                source_type: crate::types::TelemetrySourceType::LocalLiveObservation,
                                provider: "all-smi".to_string(),
                                observed_at: Some(crate::types::now_ms()),
                                received_at: Some(crate::types::now_ms()),
                                measurement_id: None,
                                device_id: None,
                                sequence: None,
                                age_ms: None,
                                source_endpoint: None,
                            });
                        }
                    }
                }
            }
        }

        m.observed = observed;
        m.last_hardware_probe = Some(crate::types::now_ms());
        m.reconciliation = crate::types::reconcile_claim(&m.claim, &m.observed);

        if online {
            m.health_state = "ONLINE".to_string();
        } else {
            m.health_state = "OFFLINE".to_string();
            m.reconciliation.status = crate::types::ReconciliationStatus::OFFLINE;
        }

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

            // Enforce live observation and match/partial_match reconciliation policy
            let is_live_obs = if let Some(ref obs) = md.observed {
                matches!(
                    obs.source_type,
                    crate::types::TelemetrySourceType::LocalLiveObservation
                        | crate::types::TelemetrySourceType::RemoteLiveObservation
                )
            } else {
                false
            };
            let eligible = is_live_obs
                && matches!(
                    md.reconciliation.status,
                    crate::types::ReconciliationStatus::MATCH
                        | crate::types::ReconciliationStatus::PARTIAL_MATCH
                );
            if !eligible {
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
                    .map(|m| {
                        if !m.online {
                            return false;
                        }
                        let is_live_obs = if let Some(ref obs) = m.observed {
                            matches!(
                                obs.source_type,
                                crate::types::TelemetrySourceType::LocalLiveObservation
                                    | crate::types::TelemetrySourceType::RemoteLiveObservation
                            )
                        } else {
                            false
                        };
                        let eligible = is_live_obs
                            && matches!(
                                m.reconciliation.status,
                                crate::types::ReconciliationStatus::MATCH
                                    | crate::types::ReconciliationStatus::PARTIAL_MATCH
                            );
                        eligible && (!avoid_p100 || !m.gpu_name.contains("P100"))
                    })
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
