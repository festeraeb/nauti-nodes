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

/// Static fallback when Forge is offline (cesarops2 local first; T440/P100 when not isolated).
pub fn fallback_fleet() -> Vec<NodeMetadata> {
    let isolated = std::env::var("CESAROPS2_ISOLATED").ok().as_deref() == Some("1")
        || std::env::var("NAUTI_AVOID_P100").ok().as_deref() == Some("1");
    let mut nodes = vec![
        fleet_node(
            "RTX2060",
            "http://127.0.0.1:5200",
            "thinker",
            "RTX 2060 SUPER",
            8192,
        ),
        fleet_node(
            "GTX1070",
            "http://127.0.0.1:5571",
            "reviewer",
            "GTX 1070",
            8192,
        ),
    ];
    if isolated {
        return nodes;
    }
    nodes.extend([
        fleet_node(
            "RTX2060-LAN",
            "http://10.0.0.201:5200",
            "thinker",
            "RTX 2060 SUPER",
            8192,
        ),
        fleet_node(
            "GTX1070-LAN",
            "http://10.0.0.201:5571",
            "reviewer",
            "GTX 1070",
            8192,
        ),
        fleet_node(
            "M2200",
            "http://100.110.214.86:5571",
            "validator",
            "Quadro M2200",
            4096,
        ),
        fleet_node(
            "P100-Coder",
            "http://127.0.0.1:5001",
            "coder",
            "P100 #1",
            16384,
        ),
        fleet_node(
            "P100-Reviewer",
            "http://127.0.0.1:5002",
            "reviewer",
            "P100 #2",
            16384,
        ),
    ]);
    nodes
}

fn fleet_node(id: &str, url: &str, role: &str, gpu: &str, vram: u32) -> NodeMetadata {
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
            return fallback_fleet();
        }
    };
    if !resp.status().is_success() {
        warn!("forge fleet HTTP {}", resp.status());
        return fallback_fleet();
    }
    let body: GpuFleet = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            warn!("forge fleet json: {e}");
            return fallback_fleet();
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
        });
    }
    if out.is_empty() {
        fallback_fleet()
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
