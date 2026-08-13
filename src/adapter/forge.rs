//! Discover fleet GPUs/workers from Forge cluster API.

use crate::types::{NodeCapabilities, NodeMetadata};
use serde::Deserialize;
use tracing::{debug, warn};

#[derive(Debug, Deserialize)]
struct GpuFleet {
    gpus: Vec<GpuCard>,
}

#[derive(Debug, Deserialize)]
struct GpuCard {
    id: i64,
    name: String,
    host: String,
    port: i64,
    role: String,
    model: String,
    worker_name: String,
    #[serde(default)]
    vram_mb: i64,
    #[serde(default)]
    enabled: bool,
}

pub fn inference_url_for(host: &str, port: i64) -> String {
    let host = host.trim();
    if host == "local" || host.is_empty() {
        format!("http://127.0.0.1:{}", port)
    } else if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{}:{}", host, port)
    }
}

/// Build a NodeMetadata from raw fields (test-only helper).
#[cfg(test)]
pub fn fleet_node(id: &str, url: &str, role: &str, gpu: &str, vram: u32) -> NodeMetadata {
    NodeMetadata {
        id: id.to_string(),
        addr: url.to_string(),
        inference_url: url.to_string(),
        role: role.to_string(),
        gpu_name: gpu.to_string(),
        online: true,
        public_key: vec![],
        capabilities: NodeCapabilities {
            max_batch: 1,
            models: vec![role.to_string()],
            vram_mb: vram,
            role: role.to_string(),
        },
        claim: crate::types::RegisteredHardwareClaim {
            gpu_name: gpu.to_string(),
            vram_mb: vram,
            models: vec![],
        },
        observed: Some(crate::types::ObservedHardware {
            gpu_name: gpu.to_string(),
            vram_mb: vram,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            driver_version: None,
            temperature_c: None,
            node_id: id.to_string(),
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
        }),
        reconciliation: crate::types::ReconciledHardwareState {
            status: crate::types::ReconciliationStatus::MATCH,
            fields: vec![],
        },
        last_hardware_probe: Some(crate::types::now_ms()),
        health_state: "ONLINE".to_string(),
    }
}

pub async fn fetch_fleet_from_forge(
    client: &reqwest::Client,
    forge_url: &str,
) -> Vec<NodeMetadata> {
    let url = format!("{}/cluster/gpus", forge_url.trim_end_matches('/'));
    let resp = match client
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("forge fleet fetch failed: {e}");
            return Vec::new();
        }
    };
    if !resp.status().is_success() {
        warn!("forge fleet HTTP {}", resp.status());
        return Vec::new();
    }
    let body: GpuFleet = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            warn!("forge fleet json: {e}");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for g in body.gpus {
        let id = if !g.worker_name.is_empty() {
            g.worker_name.clone()
        } else {
            format!("gpu-{}", g.id)
        };
        let url = inference_url_for(&g.host, g.port);
        let role = if g.role.is_empty() || g.role == "idle" {
            "general".to_string()
        } else {
            g.role.clone()
        };
        let model_label = if g.model.is_empty() {
            role.clone()
        } else {
            g.model
                .split('/')
                .next_back()
                .unwrap_or(&g.model)
                .to_string()
        };
        out.push(NodeMetadata {
            id,
            addr: url.clone(),
            inference_url: url,
            role: role.clone(),
            gpu_name: g.name,
            online: g.enabled,
            public_key: vec![],
            capabilities: NodeCapabilities {
                max_batch: 1,
                models: vec![model_label],
                vram_mb: if g.vram_mb > 0 {
                    g.vram_mb as u32
                } else {
                    8192
                },
                role,
            },
            ..Default::default()
        });
    }
    if out.is_empty() {
        debug!("forge returned empty GPU list; federation has no known nodes");
        Vec::new()
    } else {
        debug!(count = out.len(), "forge fleet synced");
        out
    }
}

pub async fn probe_online(client: &reqwest::Client, base_url: &str) -> bool {
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inference_url_for_local() {
        assert_eq!(inference_url_for("local", 5200), "http://127.0.0.1:5200");
        assert_eq!(inference_url_for("", 8080), "http://127.0.0.1:8080");
    }

    #[test]
    fn inference_url_for_http_prefix() {
        assert_eq!(
            inference_url_for("http://10.0.0.1:5200", 0),
            "http://10.0.0.1:5200"
        );
        assert_eq!(
            inference_url_for("https://example.com:443", 0),
            "https://example.com:443"
        );
    }

    #[test]
    fn inference_url_for_hostname() {
        assert_eq!(inference_url_for("myhost", 5571), "http://myhost:5571");
    }

    #[tokio::test]
    async fn unreachable_forge_returns_empty_fleet() {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        // Forge at an unreachable address returns empty, not fake nodes
        let nodes = fetch_fleet_from_forge(&client, "http://127.0.0.1:1").await;
        assert!(
            nodes.is_empty(),
            "unreachable Forge must return empty fleet, got {} nodes",
            nodes.len()
        );
    }
}
