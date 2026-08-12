//! Worker mode: register RTX 2060 (or local GPU) with coordinator + heartbeat.

use crate::adapter::llama;
use crate::types::{NodeCapabilities, NodeMetadata, Result};
use crate::Config;
use tracing::{info, warn};

pub async fn run_worker(config: Config) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init()
        .ok();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| crate::types::Error::Internal(e.to_string()))?;

    let inference_url = config.local_inference_url.clone();
    if !llama::health_ok(&client, &inference_url).await {
        warn!(
            url = %inference_url,
            "local llama-server not responding — start llama on this GPU first"
        );
    }

    // Discover real GPU info using all-smi (vendor-agnostic)
    let gpus = crate::gpu::discover_gpus();
    let (gpu_name, vram_mb) = if let Some(gpu) = gpus.first() {
        info!(
            gpu_name = %gpu.name,
            vendor = %gpu.vendor,
            vram_mb = gpu.total_memory_mb,
            uuid = %gpu.uuid,
            "discovered GPU via all-smi"
        );
        (gpu.name.clone(), gpu.total_memory_mb)
    } else {
        warn!("no GPU discovered; using env-var fallback");
        (config.worker_gpu.clone(), 8192)
    };

    let register_url = format!(
        "{}/internal/worker/register",
        config.coordinator_url.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "id": config.worker_id,
        "inference_url": inference_url,
        "role": config.worker_role,
        "gpu_name": gpu_name,
        "vram_mb": vram_mb,
        "models": [config.worker_role.clone()],
    });
    let _ = client.post(&register_url).json(&body).send().await;

    info!(
        worker_id = %config.worker_id,
        role = %config.worker_role,
        inference = %inference_url,
        coordinator = %config.coordinator_url,
        "worker registered"
    );

    let hb_url = format!(
        "{}/internal/worker/heartbeat",
        config.coordinator_url.trim_end_matches('/')
    );
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
    loop {
        tick.tick().await;
        let online = llama::health_ok(&client, &inference_url).await;
        // Refresh GPU metrics from all-smi on each heartbeat
        let current_gpus = crate::gpu::discover_gpus();
        let (free_vram, tokens_per_sec) = if online {
            if let Some(gpu) = current_gpus.first() {
                (gpu.free_memory_mb, 25.0)
            } else {
                (7000, 25.0)
            }
        } else {
            (0, 0.0)
        };
        let body = serde_json::json!({
            "node_id": config.worker_id,
            "active_jobs": 0,
            "free_vram_mb": free_vram,
            "tokens_per_sec": tokens_per_sec,
        });
        if let Err(e) = client.post(&hb_url).json(&body).send().await {
            warn!("heartbeat failed: {e}");
        }
    }
}

#[allow(dead_code)]
pub fn local_worker_meta(config: &Config) -> NodeMetadata {
    NodeMetadata {
        id: config.worker_id.clone(),
        addr: config.local_inference_url.clone(),
        inference_url: config.local_inference_url.clone(),
        role: config.worker_role.clone(),
        gpu_name: config.worker_gpu.clone(),
        online: true,
        public_key: vec![],
        capabilities: NodeCapabilities {
            max_batch: 1,
            models: vec![config.worker_role.clone()],
            vram_mb: 8192,
            role: config.worker_role.clone(),
        },
        ..Default::default()
    }
}
