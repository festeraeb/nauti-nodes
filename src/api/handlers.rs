use crate::adapter::llama::ChatMessage;
use crate::fleet;
use crate::job_runner;
use crate::jobs::JobRecord;
use crate::node::auth::{SignedHeartbeat, SignedJoinRequest};
use crate::state::AppState;
use crate::types::{NodeMetadata, NodeStatistics};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json,
    },
};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct InferenceRequest {
    pub model: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub prefer_role: Option<String>,
    #[serde(default = "default_avoid_p100")]
    pub avoid_p100: bool,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub privacy_class: Option<crate::capabilities::PrivacyClass>,
}

fn default_avoid_p100() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct QuotaSetRequest {
    pub api_key: String,
    pub balance_tokens: u64,
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    // Preferred: `X-API-Key: <key>`
    if let Some(v) = headers.get("x-api-key") {
        if let Ok(s) = v.to_str() {
            let s = s.trim();
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }

    // Fallback: `Authorization: Bearer <key>`
    if let Some(v) = headers.get("authorization") {
        if let Ok(s) = v.to_str() {
            let s = s.trim();
            let lower = s.to_lowercase();
            if lower.starts_with("bearer ") {
                let key = s[7..].trim();
                if !key.is_empty() {
                    return Some(key.to_string());
                }
            }
        }
    }

    None
}

// Removed WorkerRegisterRequest and WorkerHeartbeatRequest in favor of Signed equivalents

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "nauti-nodes",
        "version": "4.0.0",
    }))
}

pub async fn list_nodes(State(state): State<AppState>) -> Json<serde_json::Value> {
    let nodes: Vec<serde_json::Value> = state
        .registry
        .list_nodes()
        .into_iter()
        .filter_map(|rt| {
            let md = rt.metadata.read().ok()?;
            let stats = rt.stats.read().ok()?;
            let src_type = md
                .observed
                .as_ref()
                .map(|o| format!("{:?}", o.source_type))
                .unwrap_or_else(|| "UNKNOWN_PROVENANCE".to_string());
            let obs_at = md.observed.as_ref().and_then(|o| o.observed_at);
            Some(serde_json::json!({
                "id": md.id,
                "role": md.role,
                "gpu_name": md.gpu_name,
                "inference_url": md.inference_url,
                "online": md.online,
                "models": md.capabilities.models,
                "vram_mb": md.capabilities.vram_mb,
                "active_jobs": stats.active_jobs,
                "tokens_per_sec": stats.tokens_per_sec,
                "reconciliation_status": format!("{:?}", md.reconciliation.status),
                "telemetry_source": src_type,
                "observed_at": obs_at,
            }))
        })
        .collect();
    Json(serde_json::json!({ "nodes": nodes }))
}

pub async fn list_models(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut models = Vec::new();
    for rt in state.registry.list_nodes() {
        if let Ok(md) = rt.metadata.read() {
            for m in &md.capabilities.models {
                if !models.contains(m) {
                    models.push(m.clone());
                }
            }
            if !models.iter().any(|x| x == &md.role) {
                models.push(md.role.clone());
            }
        }
    }
    Json(serde_json::json!({ "models": models }))
}

pub async fn inference(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<InferenceRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    fleet::sync_fleet(&state.registry, &state.http, &state.config.forge_url).await;

    let prefer = req.prefer_role.as_deref().or(req.model.as_deref());
    let rt = fleet::pick_node_for_role(&state.registry, prefer, req.avoid_p100).ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "no eligible nodes (try starting llama on cesarops2 :5200)".to_string(),
    ))?;

    // Enforce privacy boundary: external alpha workers accept ONLY Public workloads
    let target_privacy = req
        .privacy_class
        .unwrap_or(crate::capabilities::PrivacyClass::Public);
    if target_privacy != crate::capabilities::PrivacyClass::Public {
        let md = rt
            .metadata
            .read()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        if md.capabilities.role == "external_alpha"
            || md.role == "external_alpha"
            || md.id.contains("external")
        {
            return Err((
                StatusCode::FORBIDDEN,
                "non-public workload privacy class unauthorized for external alpha worker"
                    .to_string(),
            ));
        }
    }

    // Phase 4: API key + credit ledger reservation.
    let api_key = extract_api_key(&headers).ok_or((
        StatusCode::UNAUTHORIZED,
        "missing API key (set X-API-Key header or Authorization: Bearer <key>)".to_string(),
    ))?;

    let reserve_tokens = req.max_tokens.unwrap_or(512) as u64;
    if reserve_tokens == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "max_tokens must be > 0".to_string(),
        ));
    }
    state
        .scheduler
        .check_quota(&api_key, reserve_tokens)
        .map_err(|e| (StatusCode::PAYMENT_REQUIRED, e.to_string()))?;
    state
        .scheduler
        .reserve_tokens(&api_key, reserve_tokens)
        .map_err(|e| (StatusCode::PAYMENT_REQUIRED, e.to_string()))?;

    let (node_id, inference_url, model, role) = {
        let md = rt
            .metadata
            .read()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let model = req
            .model
            .clone()
            .filter(|m| m != "thinker" && m != "coder" && m != "reviewer")
            .unwrap_or_else(|| {
                md.capabilities
                    .models
                    .first()
                    .cloned()
                    .unwrap_or_else(|| md.role.clone())
            });
        (
            md.id.clone(),
            md.inference_url.clone(),
            model,
            md.role.clone(),
        )
    };

    let job_id = Uuid::new_v4().to_string();
    let (tx, _rx) = tokio::sync::broadcast::channel(256);
    let record = Arc::new(JobRecord {
        node_id: node_id.clone(),
        inference_url: inference_url.clone(),
        model: model.clone(),
        messages: req.messages,
        api_key: Some(api_key.clone()),
        reserved_tokens: reserve_tokens,
        cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tx: tx.clone(),
        done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    state.jobs.insert(job_id.clone(), record.clone());
    job_runner::spawn_job(
        state.http.clone(),
        state.jobs.clone(),
        job_id.clone(),
        record.clone(),
        state.scheduler.clone(),
    );

    if !req.stream {
        let mut rx = tx.subscribe();
        let mut full = String::new();
        loop {
            if record.done.load(Ordering::Relaxed) {
                while let Ok(chunk) = rx.try_recv() {
                    full.push_str(&chunk);
                }
                break;
            }
            match tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await {
                Ok(Ok(chunk)) => {
                    full.push_str(&chunk);
                }
                Ok(Err(_)) => break,
                Err(_) => {
                    // Timeout slice — check record.done on next loop iteration
                }
            }
        }
        return Ok(Json(serde_json::json!({
            "job_id": job_id,
            "node_id": node_id,
            "role": role,
            "inference_url": inference_url,
            "content": full,
        }))
        .into_response());
    }

    let job_id_sse = job_id.clone();
    let node_id_sse = node_id.clone();
    let stream = BroadcastStream::new(tx.subscribe()).filter_map(move |msg| match msg {
        Ok(delta) => Some(Ok::<Event, std::convert::Infallible>(
            Event::default().data(
                serde_json::json!({
                    "job_id": job_id_sse,
                    "node_id": node_id_sse,
                    "delta": delta
                })
                .to_string(),
            ),
        )),
        Err(_) => None,
    });

    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

pub async fn cancel_inference(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .jobs
        .cancel(&job_id)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    Ok(Json(serde_json::json!({ "cancelled": job_id })))
}

pub async fn set_quota_balance(
    State(state): State<AppState>,
    Json(req): Json<QuotaSetRequest>,
) -> Json<serde_json::Value> {
    match state
        .scheduler
        .set_balance(&req.api_key, req.balance_tokens)
    {
        Ok(()) => Json(serde_json::json!({
            "api_key": req.api_key,
            "balance_tokens": req.balance_tokens,
            "ok": true
        })),
        Err(e) => Json(serde_json::json!({
            "api_key": req.api_key,
            "balance_tokens": req.balance_tokens,
            "ok": false,
            "error": e.to_string()
        })),
    }
}

pub async fn landing_page() -> Html<&'static str> {
    Html(include_str!("../../landing.html"))
}

pub async fn fetch_observed_hardware(
    client: &reqwest::Client,
    forge_url: &str,
    node_id: &str,
) -> Option<crate::types::ObservedHardware> {
    let url = format!("{}/cluster/gpus", forge_url.trim_end_matches('/'));
    if let Ok(resp) = client.get(&url).send().await {
        if resp.status().is_success() {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if let Some(gpus) = body.get("gpus").and_then(|g| g.as_array()) {
                    for gpu in gpus {
                        let worker_name = gpu
                            .get("worker_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let node = gpu.get("node").and_then(|v| v.as_str()).unwrap_or("");
                        if worker_name == node_id
                            || node == node_id
                            || node_id.contains(worker_name)
                            || node_id.contains(node)
                        {
                            let gpu_name = gpu
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("UNKNOWN")
                                .to_string();
                            let vram_mb =
                                gpu.get("vram_mb").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
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
                            let source_type = serde_json::from_value(serde_json::json!(
                                src_type_str
                            ))
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

                            return Some(crate::types::ObservedHardware {
                                gpu_name,
                                vram_mb,
                                vendor: backend,
                                device_count: 1,
                                driver_version: None,
                                temperature_c: temp_c,
                                node_id: node_id.to_string(),
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
                        }
                    }
                }
            }
        }
    }
    let local_gpus = crate::gpu::discover_gpus();
    if !local_gpus.is_empty() {
        if node_id == "cognitive-worker" {
            if let Some(gpu) = local_gpus
                .iter()
                .find(|g| g.name.contains("2060") || g.name.contains("RTX"))
            {
                return Some(crate::types::ObservedHardware {
                    gpu_name: gpu.name.clone(),
                    vram_mb: gpu.total_memory_mb,
                    vendor: gpu.vendor.clone(),
                    device_count: local_gpus.len() as u32,
                    driver_version: gpu.driver_version.clone(),
                    temperature_c: gpu.temperature_c.map(|t| t as u32),
                    node_id: node_id.to_string(),
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
            if let Some(gpu) = local_gpus.first() {
                return Some(crate::types::ObservedHardware {
                    gpu_name: gpu.name.clone(),
                    vram_mb: gpu.total_memory_mb,
                    vendor: gpu.vendor.clone(),
                    device_count: local_gpus.len() as u32,
                    driver_version: gpu.driver_version.clone(),
                    temperature_c: gpu.temperature_c.map(|t| t as u32),
                    node_id: node_id.to_string(),
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
    None
}

pub async fn reconcile_node(
    client: &reqwest::Client,
    forge_url: &str,
    meta: &mut crate::types::NodeMetadata,
) {
    let observed = fetch_observed_hardware(client, forge_url, &meta.id).await;
    meta.observed = observed.clone();
    meta.last_hardware_probe = Some(crate::types::now_ms());
    meta.reconciliation = crate::types::reconcile_claim(&meta.claim, &meta.observed);
}

pub async fn worker_register(
    State(state): State<AppState>,
    Json(req): Json<SignedJoinRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    use crate::node::auth::AuthError;
    req.verify(&state.nonce_tracker).map_err(|e| match &e {
        AuthError::StaleRequest { .. } => (StatusCode::FORBIDDEN, e.to_string()),
        AuthError::ReplayedNonce(_) => (StatusCode::CONFLICT, e.to_string()),
        _ => (StatusCode::UNAUTHORIZED, e.to_string()),
    })?;

    // Check revocation store
    if state.revocations.is_revoked(&req.node_id)
        || state.revocations.is_revoked(&req.public_key_hex)
    {
        return Err((StatusCode::FORBIDDEN, "node identity is revoked".to_string()));
    }

    // Check duplicate node ID with different public key
    if let Some(existing) = state.registry.get_node(&req.node_id) {
        if let Ok(md) = existing.metadata.read() {
            let existing_pub_hex = hex::encode(&md.public_key);
            if !existing_pub_hex.is_empty() && existing_pub_hex != req.public_key_hex {
                return Err((
                    StatusCode::CONFLICT,
                    "node_id already registered under a different public key".to_string(),
                ));
            }
        }
    }

    state.nonce_tracker.mark_used(req.nonce.clone());

    let role = req.role.clone();
    let claim = crate::types::RegisteredHardwareClaim {
        gpu_name: req.gpu_name.clone(),
        vram_mb: req.vram_mb,
        models: if req.models.is_empty() {
            vec!["default".into()]
        } else {
            req.models.clone()
        },
    };

    let mut meta = NodeMetadata {
        id: req.node_id.clone(),
        addr: req.inference_url.clone(),
        inference_url: req.inference_url,
        role: role.clone(),
        gpu_name: req.gpu_name,
        online: true,
        public_key: hex::decode(&req.public_key_hex).unwrap_or_default(),
        capabilities: crate::types::NodeCapabilities {
            max_batch: 1,
            models: if req.models.is_empty() {
                vec!["default".into()]
            } else {
                req.models
            },
            vram_mb: req.vram_mb,
            role,
        },
        claim,
        observed: None,
        reconciliation: crate::types::ReconciledHardwareState::default(),
        last_hardware_probe: None,
        health_state: "ONLINE".to_string(),
        lifecycle_state: crate::types::NodeLifecycleState::Online,
    };

    reconcile_node(&state.http, &state.config.forge_url, &mut meta).await;

    if state.registry.get_node(&req.node_id).is_some() {
        if let Some(rt) = state.registry.get_node(&req.node_id) {
            if let Ok(mut md) = rt.metadata.write() {
                *md = meta;
            }
        }
    } else {
        state.registry.register_node(meta);
    }
    Ok(Json(serde_json::json!({ "registered": req.node_id })))
}

pub async fn worker_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<SignedHeartbeat>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rt = state
        .registry
        .get_node(&req.node_id)
        .ok_or((StatusCode::NOT_FOUND, "node not found".into()))?;
    let pub_key_hex = {
        let md = rt
            .metadata
            .read()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        hex::encode(&md.public_key)
    };

    if state.revocations.is_revoked(&req.node_id) || state.revocations.is_revoked(&pub_key_hex) {
        if let Ok(mut md) = rt.metadata.write() {
            md.online = false;
            md.health_state = "REVOKED".to_string();
            md.lifecycle_state = crate::types::NodeLifecycleState::Revoked;
        }
        return Err((StatusCode::FORBIDDEN, "node identity is revoked".to_string()));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if now.abs_diff(req.timestamp_secs) > 300 {
        return Err((StatusCode::UNAUTHORIZED, "stale request".into()));
    }
    if state.nonce_tracker.is_used(&req.nonce) {
        return Err((StatusCode::UNAUTHORIZED, "replayed nonce".into()));
    }

    crate::node::auth::verify_signature(&pub_key_hex, &req.canonical_message(), &req.signature_hex)
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;

    state.nonce_tracker.mark_used(req.nonce.clone());

    rt.touch_heartbeat(NodeStatistics {
        active_jobs: req.active_jobs,
        free_vram_mb: req.free_vram_mb,
        tokens_per_sec: req.tokens_per_sec,
        ..Default::default()
    });

    let needs_reconcile = {
        if let Ok(md) = rt.metadata.read() {
            let elapsed = md
                .last_hardware_probe
                .map(|t| crate::types::now_ms().saturating_sub(t))
                .unwrap_or(u64::MAX);
            elapsed > 10000
        } else {
            false
        }
    };

    if needs_reconcile {
        let observed =
            fetch_observed_hardware(&state.http, &state.config.forge_url, &req.node_id).await;
        if let Ok(mut md) = rt.metadata.write() {
            if md.health_state != "WITHDRAWN" && md.health_state != "DRAINING" && md.health_state != "REVOKED" {
                md.online = true;
                md.health_state = "ONLINE".to_string();
                md.lifecycle_state = crate::types::NodeLifecycleState::Online;
            }
            md.observed = observed;
            md.last_hardware_probe = Some(crate::types::now_ms());
            md.reconciliation = crate::types::reconcile_claim(&md.claim, &md.observed);
        }
    } else {
        if let Ok(mut md) = rt.metadata.write() {
            if md.health_state != "WITHDRAWN" && md.health_state != "DRAINING" && md.health_state != "REVOKED" {
                md.online = true;
                md.health_state = "ONLINE".to_string();
                md.lifecycle_state = crate::types::NodeLifecycleState::Online;
            }
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct RevokeRequest {
    pub identifier: String,
    #[serde(default)]
    pub reason: String,
}

pub async fn admin_revoke(
    State(state): State<AppState>,
    Json(req): Json<RevokeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .revocations
        .revoke(&req.identifier, &req.reason)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    for rt in state.registry.list_nodes() {
        if let Ok(mut md) = rt.metadata.write() {
            let pub_hex = hex::encode(&md.public_key);
            if md.id == req.identifier || pub_hex == req.identifier {
                md.online = false;
                md.health_state = "REVOKED".to_string();
                md.lifecycle_state = crate::types::NodeLifecycleState::Revoked;
                md.reconciliation.status = crate::types::ReconciliationStatus::OFFLINE;
            }
        }
    }

    Ok(Json(serde_json::json!({
        "revoked": req.identifier,
        "reason": req.reason
    })))
}

pub async fn admin_unrevoke(
    State(state): State<AppState>,
    Json(req): Json<RevokeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let unrevoked = state
        .revocations
        .unrevoke(&req.identifier)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "unrevoked": req.identifier,
        "found": unrevoked
    })))
}

pub async fn list_revocations(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let list = state
        .revocations
        .list_revoked()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!(list)))
}

pub async fn worker_withdraw(
    State(state): State<AppState>,
    Json(req): Json<crate::node::auth::SignedWithdrawRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rt = state
        .registry
        .get_node(&req.node_id)
        .ok_or((StatusCode::NOT_FOUND, "node not found".into()))?;

    let pub_key_hex = {
        let md = rt
            .metadata
            .read()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        hex::encode(&md.public_key)
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if now.abs_diff(req.timestamp_secs) > 300 {
        return Err((StatusCode::UNAUTHORIZED, "stale request".into()));
    }
    if state.nonce_tracker.is_used(&req.nonce) {
        return Err((StatusCode::UNAUTHORIZED, "replayed nonce".into()));
    }

    crate::node::auth::verify_signature(&pub_key_hex, &req.canonical_message(), &req.signature_hex)
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;

    state.nonce_tracker.mark_used(req.nonce.clone());

    if let Ok(mut md) = rt.metadata.write() {
        md.online = false;
        md.health_state = "WITHDRAWN".to_string();
        md.lifecycle_state = crate::types::NodeLifecycleState::Withdrawn;
        md.reconciliation.status = crate::types::ReconciliationStatus::OFFLINE;
    }

    info!(node_id = %req.node_id, "node gracefully withdrawn");
    Ok(Json(serde_json::json!({ "withdrawn": req.node_id })))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminReconciliationResponse {
    pub node_id: String,
    pub status: String,
    pub claim: serde_json::Value,
    pub observed: serde_json::Value,
    pub reconciliation: serde_json::Value,
}

pub async fn admin_reconciliation(State(state): State<AppState>) -> impl IntoResponse {
    let nodes = state.registry.list_nodes();
    let mut out = Vec::new();
    for rt in nodes {
        if let Ok(md) = rt.metadata.read() {
            let observed_val = if let Some(obs) = &md.observed {
                serde_json::json!({
                    "gpu": obs.gpu_name,
                    "vram_mb": obs.vram_mb,
                    "observed_at": obs.observed_at,
                    "source_type": format!("{:?}", obs.source_type),
                    "observer_node_id": obs.observer_node_id,
                    "provider": obs.provider,
                    "received_at": obs.received_at,
                })
            } else {
                serde_json::Value::Null
            };
            out.push(AdminReconciliationResponse {
                node_id: md.id.clone(),
                status: md.health_state.clone(),
                claim: serde_json::json!({
                    "gpu": md.claim.gpu_name,
                    "vram_mb": md.claim.vram_mb
                }),
                observed: observed_val,
                reconciliation: serde_json::json!({
                    "status": format!("{:?}", md.reconciliation.status),
                    "fields": md.reconciliation.fields
                }),
            });
        }
    }
    Json(out)
}

pub async fn sync_fleet_now(State(state): State<AppState>) -> Json<serde_json::Value> {
    fleet::sync_fleet(&state.registry, &state.http, &state.config.forge_url).await;
    let n = state.registry.list_nodes().len();
    Json(serde_json::json!({ "synced": n }))
}

pub async fn node_identity(State(state): State<AppState>) -> Json<serde_json::Value> {
    match &state.identity {
        Some(id) => {
            let pub_hex: String = id.public_key.iter().map(|b| format!("{b:02x}")).collect();
            Json(serde_json::json!({
                "node_id": id.node_id,
                "public_key_hex": pub_hex,
                "created_at_ms": id.created_at_ms,
                "software_version": id.software_version,
            }))
        }
        None => Json(serde_json::json!({
            "node_id": null,
            "error": "identity not loaded",
        })),
    }
}

#[derive(Debug, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub parent_node_id: String,
    pub public_key_hex: String,
    pub software_version: String,
    pub capabilities: Option<serde_json::Value>,
}

pub async fn node_join(
    State(state): State<AppState>,
    Json(req): Json<JoinRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let lineage = state.lineage.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "lineage not initialized".to_string(),
    ))?;

    let rel = lineage
        .add_child(&req.node_id, &req.parent_node_id)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;

    info!(child = %req.node_id, parent = %req.parent_node_id, depth = rel.depth, "node joined lineage");

    Ok(Json(serde_json::json!({
        "node_id": req.node_id,
        "parent_node_id": rel.parent_node_id,
        "root_node_id": rel.root_node_id,
        "branch_id": rel.branch_id,
        "depth": rel.depth,
        "status": "active",
    })))
}

pub async fn tree_snapshot(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let lineage = state.lineage.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "lineage not initialized".to_string(),
    ))?;

    let relationships = lineage
        .all_active()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let root = lineage
        .root_node_id()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let nodes: Vec<serde_json::Value> = relationships
        .iter()
        .map(|r| {
            serde_json::json!({
                "node_id": r.node_id,
                "parent_node_id": r.parent_node_id,
                "root_node_id": r.root_node_id,
                "branch_id": r.branch_id,
                "depth": r.depth,
                "status": r.status.as_str(),
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "root_node_id": root,
        "nodes": nodes,
        "count": nodes.len(),
    })))
}

use tracing::info;

pub async fn prometheus_metrics(State(state): State<AppState>) -> String {
    let m = &state.metrics;
    m.federation_nodes_total
        .store(state.registry.list_nodes().len() as u64, Ordering::Relaxed);
    m.render_prometheus()
}

pub async fn metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let m = &state.metrics;
    m.federation_nodes_total
        .store(state.registry.list_nodes().len() as u64, Ordering::Relaxed);
    let mut json = m.snapshot_json();
    json["active_jobs"] = serde_json::json!(state.jobs.active_count());
    Json(json)
}

#[derive(Debug, Deserialize)]
pub struct ModelStartRequest {
    pub model_id: String,
    pub port: Option<u16>,
    pub node_id: Option<String>,
}

pub async fn model_start(
    State(state): State<AppState>,
    Json(req): Json<ModelStartRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if let Some(node_id) = &req.node_id {
        if state.revocations.is_revoked(node_id) {
            return Err((StatusCode::FORBIDDEN, "node identity is revoked".into()));
        }
        if let Some(rt) = state.registry.get_node(node_id) {
            if let Ok(md) = rt.metadata.read() {
                if matches!(
                    md.lifecycle_state,
                    crate::types::NodeLifecycleState::Withdrawn
                        | crate::types::NodeLifecycleState::Draining
                        | crate::types::NodeLifecycleState::Revoked
                        | crate::types::NodeLifecycleState::Offline
                ) {
                    return Err((
                        StatusCode::FORBIDDEN,
                        format!("node lifecycle state '{:?}' prohibits starting model", md.lifecycle_state),
                    ));
                }
            }
        }
    }

    let proc = state
        .model_manager
        .request_model(&req.model_id, req.port.unwrap_or(5200), 16384)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    if let Some(node_id) = &req.node_id {
        if let Some(rt) = state.registry.get_node(node_id) {
            if let Ok(mut md) = rt.metadata.write() {
                md.capabilities.models = vec![req.model_id.clone()];
                md.inference_url = proc.base_url.clone();
                md.addr = proc.base_url.clone();
            }
        }
    }

    Ok(Json(serde_json::json!({
        "started": true,
        "model_id": req.model_id,
        "port": proc.port,
        "base_url": proc.base_url,
        "pid": proc.pid
    })))
}

pub async fn model_stop(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .model_manager
        .stop_model()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({ "stopped": true })))
}

pub async fn model_status(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let status = state.model_manager.get_status();
    Json(serde_json::to_value(status).unwrap_or_default())
}
