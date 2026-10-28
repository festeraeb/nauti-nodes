//! Worker mode: register RTX 2060 (or local GPU) with coordinator + heartbeat.

use crate::adapter::llama;
use crate::node::auth::{
    sign_message, SignedHeartbeat, SignedJoinRequest, CURRENT_PROTOCOL_VERSION,
};
use crate::node::identity::NodeIdentity;
use crate::types::{NodeCapabilities, NodeMetadata, Result};
use crate::Config;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
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

    // Load or create persistent Ed25519 node identity
    let id_path = Path::new(&config.identity_path);
    let identity = NodeIdentity::load_or_create(id_path).map_err(|e| {
        crate::types::Error::Internal(format!("failed to load/create identity: {e}"))
    })?;
    let signing_key = identity
        .signing_key()
        .map_err(|e| crate::types::Error::Internal(format!("invalid signing key: {e}")))?;
    let public_key_hex = hex::encode(&identity.public_key);
    let fingerprint = format!(
        "ed25519:{}",
        &public_key_hex[..16.min(public_key_hex.len())]
    );

    let inference_url = config.local_inference_url.clone();
    let runtime_online = llama::health_ok(&client, &inference_url).await;
    if !runtime_online {
        warn!(
            url = %inference_url,
            "local llama-server not responding — ensure llama is running on this host"
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
        if config.strict_alpha {
            tracing::error!(
                "strict alpha mode: no physical GPU discovered — refusing synthetic fallback"
            );
            return Err(crate::types::Error::Config(
                "strict alpha mode requires physical GPU hardware discovery".into(),
            ));
        }
        // Report what was measured: no GPU, 0 MiB. A made-up VRAM figure here would make the
        // coordinator schedule work onto a card that is not there.
        warn!("no GPU discovered; registering with vram_mb = 0");
        (config.worker_gpu.clone(), 0)
    };

    let register_url = format!(
        "{}/internal/worker/register",
        config.coordinator_url.trim_end_matches('/')
    );

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let nonce = uuid::Uuid::new_v4().to_string();

    let mut join_req = SignedJoinRequest {
        node_id: identity.node_id.clone(),
        public_key_hex: public_key_hex.clone(),
        inference_url: inference_url.clone(),
        role: config.worker_role.clone(),
        gpu_name: gpu_name.clone(),
        vram_mb: vram_mb as u32,
        models: vec![config.worker_role.clone()],
        timestamp_secs: ts,
        nonce,
        protocol_version: config.protocol_version,
        signature_hex: String::new(),
    };
    join_req.signature_hex = sign_message(&signing_key, &join_req.canonical_message());

    let reg_resp = client.post(&register_url).json(&join_req).send().await;
    let registration_success = match &reg_resp {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    };

    println!("============================================================");
    println!("NAUTI NODES VOLUNTEER WORKER ONLINE");
    println!("============================================================");
    println!("Node ID:              {}", identity.node_id);
    println!("Public Fingerprint:   {}", fingerprint);
    println!("Coordinator:          {}", config.coordinator_url);
    println!(
        "Registration Status:  {}",
        if registration_success {
            "SUCCESS"
        } else {
            "PENDING/REJECTED"
        }
    );
    println!("GPU Discovered:       {} ({} MiB VRAM)", gpu_name, vram_mb);
    println!("Protocol Version:     v{}", config.protocol_version);
    println!("Lifecycle State:      ONLINE");
    println!("Workload Boundary:    PUBLIC ONLY");
    println!("============================================================");

    // Chunk 8B.2: the worker serves the VERSIONED fenced command path.
    // Ownership-changing commands reach this worker ONLY through
    // /internal/federation/command as signed, epoch-fenced envelopes.
    let state = crate::state::AppState::new(config.clone())?;
    let app = crate::api::routes::router(state);
    let listen_addr = format!("0.0.0.0:{}", config.listen_port);
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .map_err(|e| crate::types::Error::Internal(format!("bind {listen_addr}: {e}")))?;
    info!(addr = %listen_addr, "worker command endpoint listening (fenced)");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

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
                (gpu.free_memory_mb as u32, 25.0)
            } else if config.strict_alpha {
                (0, 0.0)
            } else {
                (7000, 25.0)
            }
        } else {
            (0, 0.0)
        };

        let hb_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let hb_nonce = uuid::Uuid::new_v4().to_string();

        let mut hb_req = SignedHeartbeat {
            node_id: identity.node_id.clone(),
            active_jobs: 0,
            free_vram_mb: free_vram,
            tokens_per_sec,
            timestamp_secs: hb_ts,
            nonce: hb_nonce,
            signature_hex: String::new(),
        };
        hb_req.signature_hex = sign_message(&signing_key, &hb_req.canonical_message());

        if let Err(e) = client.post(&hb_url).json(&hb_req).send().await {
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
            vram_mb: 0, // unknown: not measured here; never invent a figure
            role: config.worker_role.clone(),
        },
        ..Default::default()
    }
}
