use crate::adapter::llama::ChatMessage;
use crate::config::Config;
use crate::eligibility::RuntimeState;
use crate::fleet;
use crate::job_runner;
use crate::jobs::JobRecord;
use crate::node::auth::{SignedHeartbeat, SignedJoinRequest};
use crate::opsstore::OpsEventKind;
use crate::preemption::PreemptibleJob;
use crate::state::AppState;
use crate::types::{now_ms, NodeMetadata, NodeStatistics, ReconciliationStatus};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json,
    },
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
                "health_state": md.health_state,
                "lifecycle_state": format!("{:?}", md.lifecycle_state),
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

    state
        .metrics
        .inference_requests_total
        .fetch_add(1, Ordering::Relaxed);

    // Chunk 7 work-vs-lease race guard: confirm the assignment atomically
    // against the authoritative eligibility state. If a Forge lease was
    // granted (or the node otherwise became ineligible) between selection
    // and this call, the assignment is REFUSED. Fail closed.
    if !rt.try_assign() {
        state
            .metrics
            .ownership_conflicts_blocked_total
            .fetch_add(1, Ordering::Relaxed);
        let blocked_node = rt.metadata.read().map(|m| m.id.clone()).unwrap_or_default();
        let _ = state.ops.record(
            OpsEventKind::OwnershipConflictBlocked,
            Some(&blocked_node),
            None,
            None,
            None,
            "shared assignment refused at confirm time (ownership fail closed)",
            "BLOCKED",
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "node became ineligible during assignment (ownership fail closed)".to_string(),
        ));
    }

    // Phase 4: API key + credit ledger reservation.
    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => {
            rt.release_assignment();
            return Err((
                StatusCode::UNAUTHORIZED,
                "missing API key (set X-API-Key header or Authorization: Bearer <key>)".to_string(),
            ));
        }
    };

    let reserve_tokens = req.max_tokens.unwrap_or(512) as u64;
    if reserve_tokens == 0 {
        rt.release_assignment();
        return Err((
            StatusCode::BAD_REQUEST,
            "max_tokens must be > 0".to_string(),
        ));
    }
    if let Err(e) = state.scheduler.check_quota(&api_key, reserve_tokens) {
        rt.release_assignment();
        return Err((StatusCode::PAYMENT_REQUIRED, e.to_string()));
    }
    if let Err(e) = state.scheduler.reserve_tokens(&api_key, reserve_tokens) {
        rt.release_assignment();
        return Err((StatusCode::PAYMENT_REQUIRED, e.to_string()));
    }

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
    // Chunk 7: register the live job as preemptible and open its accounting
    // ledger row (duplicate-open guard keyed by job_id).
    state
        .preemption
        .register_job(PreemptibleJob::new(job_id.clone(), node_id.clone(), true));
    let _ = state
        .ops
        .open_reservation(&job_id, &api_key, reserve_tokens);
    job_runner::spawn_job(
        state.http.clone(),
        state.jobs.clone(),
        job_id.clone(),
        record.clone(),
        state.scheduler.clone(),
        Some(rt.clone()),
        state.ops.clone(),
        state.preemption.clone(),
        state.metrics.clone(),
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

    // Check operator kill switch
    if !state.external_alpha_enabled.load(Ordering::Relaxed) {
        return Err((
            StatusCode::FORBIDDEN,
            "external alpha joins disabled by operator kill switch".into(),
        ));
    }

    req.verify(&state.nonce_tracker).map_err(|e| match &e {
        AuthError::StaleRequest { .. } => (StatusCode::FORBIDDEN, e.to_string()),
        AuthError::ReplayedNonce(_) => (StatusCode::CONFLICT, e.to_string()),
        AuthError::IncompatibleProtocolVersion { .. } => (StatusCode::BAD_REQUEST, e.to_string()),
        _ => (StatusCode::UNAUTHORIZED, e.to_string()),
    })?;

    // Check revocation store
    if state.revocations.is_revoked(&req.node_id)
        || state.revocations.is_revoked(&req.public_key_hex)
    {
        return Err((
            StatusCode::FORBIDDEN,
            "node identity is revoked".to_string(),
        ));
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
        // Signed volunteer workers assert their own runtime truth via
        // heartbeats (RC1 protocol). Chunk 7 default: Ready.
        ..Default::default()
    };

    reconcile_node(&state.http, &state.config.forge_url, &mut meta).await;

    let existed = state.registry.get_node(&req.node_id).is_some();
    if existed {
        if let Some(rt) = state.registry.get_node(&req.node_id) {
            if let Ok(mut md) = rt.metadata.write() {
                *md = meta;
            }
        }
    } else {
        state.registry.register_node(meta);
    }

    // Chunk 7: hardware reconciliation event (fresh observation on join).
    let rec_status = state
        .registry
        .get_node(&req.node_id)
        .and_then(|rt| rt.metadata.read().ok().map(|m| m.reconciliation.status));
    let rec_result = rec_status
        .map(|s| format!("{s:?}"))
        .unwrap_or_else(|| "UNVERIFIED".into());
    let _ = state.ops.record(
        if rec_status == Some(ReconciliationStatus::MISMATCH) {
            OpsEventKind::HardwareMismatch
        } else {
            OpsEventKind::HardwareReconciled
        },
        Some(&req.node_id),
        None,
        None,
        None,
        &format!(
            "join reconciliation: {:?}",
            rec_status.unwrap_or(ReconciliationStatus::UNVERIFIED)
        ),
        &rec_result,
    );

    // Chunk 7: active Forge lease truth overrides rejoin. A worker
    // reconnecting while its GPU is leased must NOT re-enter the shared pool.
    let leased = state
        .leases
        .apply_overlay_to_node(&state.registry, &req.node_id);

    if let Some(rt) = state.registry.get_node(&req.node_id) {
        let _ = state.ops.record(
            if existed {
                OpsEventKind::NodeReconnected
            } else {
                OpsEventKind::NodeJoined
            },
            Some(&req.node_id),
            None,
            None,
            None,
            &format!("signed join protocol v{}", req.protocol_version),
            if leased { "RESERVED_FORGE" } else { "ONLINE" },
        );
        if existed {
            state
                .metrics
                .node_reconnects_total
                .fetch_add(1, Ordering::Relaxed);
        }
        crate::state::persist_node(&state.ops, &rt);
    }

    Ok(Json(serde_json::json!({
        "registered": req.node_id,
        "leased": leased
    })))
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
        return Err((
            StatusCode::FORBIDDEN,
            "node identity is revoked".to_string(),
        ));
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

    // Chunk 7: worker-reported runtime truth. An RC1 worker whose local
    // inference runtime is down heartbeats free_vram_mb == 0 and
    // tokens_per_sec == 0. An open heartbeat is NOT proof of runtime health.
    let runtime_down = req.free_vram_mb == 0 && req.tokens_per_sec == 0.0;

    // Chunk 7 invariant: a heartbeat must NEVER resurrect a leased GPU into
    // the shared pool. Lease truth wins over liveness.
    if state.leases.has_active_lease(&req.node_id) {
        if let Ok(mut md) = rt.metadata.write() {
            if runtime_down {
                md.runtime_state = RuntimeState::Unavailable;
            }
        }
        let _ = state.ops.record(
            OpsEventKind::OwnershipConflictBlocked,
            Some(&req.node_id),
            None,
            None,
            None,
            "heartbeat while Forge lease active: node kept excluded from shared pool",
            "RESERVED_FORGE",
        );
        return Ok(Json(serde_json::json!({ "ok": true, "leased": true })));
    }

    let prev_health = rt
        .metadata
        .read()
        .map(|m| m.health_state.clone())
        .unwrap_or_default();

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
            if md.health_state != "WITHDRAWN"
                && md.health_state != "DRAINING"
                && md.health_state != "REVOKED"
                && md.health_state != "RESERVED_FORGE"
                && md.health_state != "PREEMPTING"
            {
                md.online = true;
                md.health_state = "ONLINE".to_string();
                md.lifecycle_state = crate::types::NodeLifecycleState::Online;
            }
            md.observed = observed;
            md.last_hardware_probe = Some(crate::types::now_ms());
            md.reconciliation = crate::types::reconcile_claim(&md.claim, &md.observed);
        }
    } else if let Ok(mut md) = rt.metadata.write() {
        if md.health_state != "WITHDRAWN"
            && md.health_state != "DRAINING"
            && md.health_state != "REVOKED"
            && md.health_state != "RESERVED_FORGE"
            && md.health_state != "PREEMPTING"
        {
            md.online = true;
            md.health_state = "ONLINE".to_string();
            md.lifecycle_state = crate::types::NodeLifecycleState::Online;
        }
    }

    // Chunk 7: runtime truth, reconnect detection, persistence.
    let mut reconnected = false;
    if let Ok(mut md) = rt.metadata.write() {
        if runtime_down {
            if md.runtime_state != RuntimeState::Unavailable {
                state
                    .metrics
                    .runtime_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                let _ = state.ops.record(
                    OpsEventKind::RuntimeFailed,
                    Some(&req.node_id),
                    None,
                    None,
                    None,
                    "worker heartbeat reports runtime down",
                    "RUNTIME_UNAVAILABLE",
                );
            }
            md.runtime_state = RuntimeState::Unavailable;
            md.failure_reason = "runtime down (worker-reported)".to_string();
        } else {
            md.runtime_state = RuntimeState::Ready;
            md.failure_reason = String::new();
        }
        if prev_health != md.health_state {
            md.last_state_change_ms = now_ms();
            if md.health_state == "ONLINE" && (prev_health == "OFFLINE" || prev_health == "STALE") {
                reconnected = true;
            }
        }
    }
    if reconnected {
        state
            .metrics
            .node_reconnects_total
            .fetch_add(1, Ordering::Relaxed);
        let _ = state.ops.record(
            OpsEventKind::NodeReconnected,
            Some(&req.node_id),
            None,
            None,
            None,
            "worker reconnected via signed heartbeat",
            "ONLINE",
        );
    }
    crate::state::persist_node(&state.ops, &rt);
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn check_admin_auth(
    headers: &axum::http::HeaderMap,
    config: &Config,
) -> Result<(), (StatusCode, String)> {
    if let Some(expected_key) = &config.admin_key {
        let provided_key = headers
            .get("X-Admin-Key")
            .and_then(|h| h.to_str().ok())
            .or_else(|| {
                headers
                    .get("Authorization")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.strip_prefix("Bearer "))
            });

        match provided_key {
            Some(k) if k == expected_key => Ok(()),
            _ => Err((
                StatusCode::UNAUTHORIZED,
                "admin authentication required".into(),
            )),
        }
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct RevokeRequest {
    pub identifier: String,
    #[serde(default)]
    pub reason: String,
}

pub async fn admin_revoke(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<RevokeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    state
        .revocations
        .revoke(&req.identifier, &req.reason)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    for rt in state.registry.list_nodes() {
        let matched = {
            let mut matched = false;
            if let Ok(mut md) = rt.metadata.write() {
                let pub_hex = hex::encode(&md.public_key);
                if md.id == req.identifier || pub_hex == req.identifier {
                    md.online = false;
                    md.health_state = "REVOKED".to_string();
                    md.lifecycle_state = crate::types::NodeLifecycleState::Revoked;
                    md.reconciliation.status = crate::types::ReconciliationStatus::OFFLINE;
                    md.last_state_change_ms = now_ms();
                    md.failure_reason = format!("revoked: {}", req.reason);
                    matched = true;
                }
            }
            matched
        };
        if matched {
            let _ = state.ops.record(
                OpsEventKind::RevocationApplied,
                Some(&req.identifier),
                None,
                None,
                None,
                &req.reason,
                "REVOKED",
            );
            crate::state::persist_node(&state.ops, &rt);
        }
    }
    let _ = state.ops.record(
        OpsEventKind::NodeRevoked,
        Some(&req.identifier),
        None,
        None,
        None,
        &req.reason,
        "REVOKED",
    );

    Ok(Json(serde_json::json!({
        "revoked": req.identifier,
        "reason": req.reason
    })))
}

pub async fn admin_unrevoke(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<RevokeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let unrevoked = state
        .revocations
        .unrevoke(&req.identifier)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let _ = state.ops.record(
        OpsEventKind::NodeUnrevoked,
        Some(&req.identifier),
        None,
        None,
        None,
        "revocation lifted by operator",
        "UNREVOKED",
    );

    Ok(Json(serde_json::json!({
        "unrevoked": req.identifier,
        "found": unrevoked
    })))
}

pub async fn list_revocations(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
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
        md.last_state_change_ms = now_ms();
    }
    let _ = state.ops.record(
        OpsEventKind::NodeWithdrawn,
        Some(&req.node_id),
        None,
        None,
        None,
        "graceful signed withdrawal",
        "WITHDRAWN",
    );
    crate::state::persist_node(&state.ops, &rt);

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
    match crate::state::identity_snapshot(&state) {
        Some(id) => {
            let pub_hex: String = id
                .node_identity
                .public_key
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            Json(serde_json::json!({
                "node_id": id.node_identity.node_id,
                "public_key_hex": pub_hex,
                "created_at_ms": id.node_identity.created_at_ms,
                "software_version": id.node_identity.software_version,
                "federation_state": format!("{:?}", id.federation_state),
                "authority_epoch": id.authority_epoch.value(),
                "is_coordinator": id.is_coordinator,
                "fingerprint": id.fingerprint(),
            }))
        }
        None => Json(serde_json::json!({
            "node_id": null,
            "error": "identity not loaded",
        })),
    }
}

/// Chunk 8B.14: federation status. Reports only real local state
/// (identity, authority view, peers, lineage, lease truth, fence
/// counters). Never synthesizes peer or authority data.
pub async fn federation_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let peers: Vec<serde_json::Value> = state
        .peers
        .list()
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "coordinator_id": p.coordinator_id,
                "endpoint": p.endpoint,
                "role": p.role.as_str(),
                "public_key_hex": p.public_key_hex,
                "parent_id": p.parent_id,
                "last_seen_ms": p.last_seen_ms,
            })
        })
        .collect();

    // Lineage parent/children for the local coordinator id (real store).
    let (lineage_parent, lineage_children) = match crate::state::identity_snapshot(&state) {
        Some(id) => match state.lineage.as_ref() {
            Some(lineage) => {
                let me = &id.node_identity.node_id;
                let parent = lineage.parent(me).unwrap_or(None);
                let children = lineage.children(me).unwrap_or_default();
                (parent, children)
            }
            None => (None, Vec::new()),
        },
        None => (None, Vec::new()),
    };

    let counters = &state.worker_gate.counters;
    let fence_counters = serde_json::json!({
        "commands_accepted_total": counters.commands_accepted_total.load(Ordering::SeqCst),
        "commands_rejected_auth_total": counters.commands_rejected_auth_total.load(Ordering::SeqCst),
        "commands_rejected_stale_epoch_total": counters
            .commands_rejected_stale_epoch_total
            .load(Ordering::SeqCst),
        "commands_rejected_authority_conflict_total": counters
            .commands_rejected_authority_conflict_total
            .load(Ordering::SeqCst),
        "duplicate_commands_blocked_total": counters
            .duplicate_commands_blocked_total
            .load(Ordering::SeqCst),
        "checkpoint_resumes_total": counters.checkpoint_resumes_total.load(Ordering::SeqCst),
        "checkpoint_double_resume_blocked_total": counters
            .checkpoint_double_resume_blocked_total
            .load(Ordering::SeqCst),
    });

    match crate::state::identity_snapshot(&state) {
        Some(id) => {
            let role = crate::federation::CoordinatorRole::from_local_identity(&id);
            // Current authority view from the election over known records.
            let local_record = crate::federation::AuthorityRecord {
                coordinator_id: id.node_identity.node_id.clone(),
                epoch: id.authority_epoch.value(),
                acquired_at_ms: id.epoch_acquired_at_ms as i64,
            };
            let elected = state.authority.elect(&local_record);
            Json(serde_json::json!({
                "coordinator_id": id.node_identity.node_id,
                "is_coordinator": id.is_coordinator,
                "federation_state": format!("{:?}", id.federation_state),
                "local_role": role.as_str(),
                "authority_epoch": id.authority_epoch.value(),
                "epoch_acquired_at_ms": id.epoch_acquired_at_ms,
                "can_issue_mutations": id.can_issue_mutations(),
                "fingerprint": id.fingerprint(),
                "trusted_peers": id.trusted_peers,
                "replication_position": id.replication_position,
                "last_synced_at_ms": id.last_synced_at_ms,
                "current_authority": {
                    "coordinator_id": elected.coordinator_id,
                    "epoch": elected.epoch,
                    "acquired_at_ms": elected.acquired_at_ms,
                },
                "lease_authority_epoch": state.leases.current_authority_epoch(),
                "active_leases_known": state.leases.all_active_leases().len(),
                "lineage_parent": lineage_parent,
                "lineage_children": lineage_children,
                "fence_counters": fence_counters,
                "peers": peers,
            }))
        }
        None => Json(serde_json::json!({
            "coordinator_id": null,
            "error": "identity not loaded",
            "peers": peers,
        })),
    }
}

/// Chunk 8B.4: this coordinator's authority record (probe target).
/// Real local values only.
pub async fn federation_authority(State(state): State<AppState>) -> Json<serde_json::Value> {
    match crate::state::identity_snapshot(&state) {
        Some(id) => Json(serde_json::json!({
            "coordinator_id": id.node_identity.node_id,
            "epoch": id.authority_epoch.value(),
            "acquired_at_ms": id.epoch_acquired_at_ms as i64,
        })),
        None => Json(serde_json::json!({
            "coordinator_id": null,
            "epoch": 0,
            "acquired_at_ms": 0,
        })),
    }
}

/// Chunk 8B.13: snapshot of current federation truth (admin-authenticated).
pub async fn federation_snapshot_get(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let snap = crate::federation::snapshot::capture(&state)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    serde_json::to_value(&snap)
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Debug, Deserialize)]
pub struct FederationCatchupRequest {
    /// Base URL of the peer coordinator to catch up from.
    pub peer_url: String,
}

/// Chunk 8B.13: catch up from a live peer over REAL HTTP. The peer's
/// snapshot is fetched, then applied; this coordinator becomes a FOLLOWER
/// at the peer epoch (never an authority). Admin-authenticated.
pub async fn federation_catchup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<FederationCatchupRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let url = format!(
        "{}/v1/federation/snapshot",
        req.peer_url.trim_end_matches('/')
    );
    // Admin key forwarding: the peer requires admin auth for snapshots.
    let mut request = state.http.get(&url);
    if let Some(key) = state.config.admin_key.as_deref() {
        request = request.header("X-Admin-Key", key);
    }
    let resp = request.send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("catch-up fetch failed: {e}"),
        )
    })?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("catch-up fetch status {}", resp.status()),
        ));
    }
    let snap: crate::federation::snapshot::FederationSnapshot = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("bad snapshot from peer: {e}"),
        )
    })?;
    let report = crate::federation::snapshot::apply(&state, &snap)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Record the catch-up in the real journal.
    let _ = state.ops.record(
        OpsEventKind::StateRecovered,
        None,
        None,
        None,
        None,
        &format!(
            "federation catch-up from {} (epoch {}): leases+{} revocations+{} lineage+{} fences+{}",
            snap.authority_id,
            snap.epoch,
            report.leases_imported,
            report.revocations_imported,
            report.lineage_imported,
            report.fences_imported
        ),
        "CATCHUP",
    );
    Ok(Json(serde_json::json!({
        "caught_up_from": snap.authority_id,
        "epoch": snap.epoch,
        "report": {
            "leases_imported": report.leases_imported,
            "leases_skipped_existing": report.leases_skipped_existing,
            "revocations_imported": report.revocations_imported,
            "lineage_imported": report.lineage_imported,
            "lineage_skipped": report.lineage_skipped,
            "fences_imported": report.fences_imported,
            "fences_kept_newer_local": report.fences_kept_newer_local,
            "adopted_follower_epoch": report.adopted_follower_epoch,
        },
    })))
}

#[derive(Debug, Deserialize)]
pub struct FederationSendRequest {
    /// Worker (or peer) base URL that will receive the fenced command.
    pub target_url: String,
    /// The command to sign and send.
    pub command: crate::federation::envelope::FederationCommand,
}

/// Chunk 8B.2/8B.5: coordinator-side command sender. Signs the command
/// with THIS coordinator's durable Ed25519 identity and delivers it over
/// real HTTP to the target's fenced command endpoint. Admin-authenticated.
pub async fn federation_send_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<FederationSendRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let id = crate::state::identity_snapshot(&state).ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "identity not loaded".to_string(),
    ))?;
    let signing_key = id
        .node_identity
        .signing_key()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let pub_hex = hex::encode(&id.node_identity.public_key);
    let envelope = crate::federation::envelope::SignedFederationCommand::build_signed(
        &id.node_identity.node_id,
        &pub_hex,
        &signing_key,
        req.command,
    );
    let url = format!(
        "{}/internal/federation/command",
        req.target_url.trim_end_matches('/')
    );
    let resp = state
        .http
        .post(&url)
        .json(&envelope)
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("command delivery failed: {e}"),
            )
        })?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_else(|_| serde_json::json!({}));
    Ok(Json(serde_json::json!({
        "delivered_status": status.as_u16(),
        "accepted": status.is_success(),
        "response": body,
    })))
}

/// Chunk 8B.2: versioned fenced command endpoint (worker side).
///
/// This is the ONLY command surface that changes ownership state. Every
/// envelope is authenticated (durable coordinator Ed25519 identity) and
/// fenced (durable per-resource epoch) before any mutation.
pub async fn federation_command(
    State(state): State<AppState>,
    Json(env): Json<crate::federation::envelope::SignedFederationCommand>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match state.worker_gate.handle(&env) {
        Ok(outcome) => Ok(Json(serde_json::json!({
            "accepted": true,
            "outcome": format!("{outcome:?}"),
        }))),
        Err(reject) => {
            let status = match &reject {
                crate::federation::worker_gate::CommandReject::Auth(_) => StatusCode::UNAUTHORIZED,
                crate::federation::worker_gate::CommandReject::Fence(_) => StatusCode::CONFLICT,
                crate::federation::worker_gate::CommandReject::Execution(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            Err((status, reject.to_string()))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DelegatedJobSubmit {
    pub job_id: uuid::Uuid,
    pub origin_coordinator_id: String,
}

/// Chunk 9: receive a delegated job. Records the pending job durably; a
/// duplicate job_id returns the current authoritative status instead of
/// executing twice. Admin-authenticated.
pub async fn delegated_job_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<DelegatedJobSubmit>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let epoch = state.leases.current_authority_epoch();
    match state.delegation.insert_pending(
        &req.job_id.to_string(),
        &req.origin_coordinator_id,
        epoch,
    ) {
        Ok(()) => {
            let _ = state.ops.record(
                OpsEventKind::JobCompleted,
                Some(&req.job_id.to_string()),
                None,
                Some(&req.job_id.to_string()),
                Some(&req.origin_coordinator_id),
                "delegated job accepted (exactly-once)",
                "PENDING",
            );
            Ok(Json(serde_json::json!({
                "ack": "Accepted",
                "job_id": req.job_id,
            })))
        }
        Err(_) => {
            // Duplicate: return current authoritative status.
            let status = state
                .delegation
                .get(&req.job_id.to_string())
                .map(|row| row.map(|r| r.state).unwrap_or_else(|| "UNKNOWN".into()))
                .unwrap_or_else(|_| "UNKNOWN".into());
            Ok(Json(serde_json::json!({
                "ack": "Duplicate",
                "job_id": req.job_id,
                "status": status,
            })))
        }
    }
}

/// Chunk 9: authoritative status of a delegated job. Admin-authenticated.
pub async fn delegated_job_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let row = state
        .delegation
        .get(&job_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    match row {
        Some(row) => Ok(Json(serde_json::json!({
            "job_id": row.job_id,
            "origin_coordinator_id": row.origin_coordinator_id,
            "executing_coordinator_id": row.executing_coordinator_id,
            "state": row.state,
            "ownership_epoch": row.ownership_epoch,
            "created_at_ms": row.created_at_ms,
            "completed_at_ms": row.completed_at_ms,
        }))),
        None => Err((StatusCode::NOT_FOUND, "delegated job not found".into())),
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
                        format!(
                            "node lifecycle state '{:?}' prohibits starting model",
                            md.lifecycle_state
                        ),
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

pub async fn model_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let status = state.model_manager.get_status();
    Json(serde_json::to_value(status).unwrap_or_default())
}

#[derive(Debug, Deserialize)]
pub struct ForgeReleaseRequest {
    pub mission_id: String,
    pub node_id: String,
}

pub async fn forge_reserve(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<crate::preemption::ForgeReservationRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    state
        .metrics
        .lease_requests_total
        .fetch_add(1, Ordering::Relaxed);

    // 1. Authoritative lease decision (Chunk 7 state machine).
    //    Idempotent for the same (mission_id, node_id); a DIFFERENT mission
    //    attempting to claim an already-leased GPU fails explicitly (409).
    let outcome = state
        .leases
        .begin_reserve(
            &req.mission_id,
            &req.node_id,
            req.lease_id.as_deref(),
            req.device_id.as_deref(),
            req.vram_mb,
            req.priority,
            req.allow_preemption,
        )
        .map_err(|e| match &e {
            crate::lease::LeaseError::Conflict(holder) => {
                state
                    .metrics
                    .lease_conflicts_total
                    .fetch_add(1, Ordering::Relaxed);
                (
                    StatusCode::CONFLICT,
                    format!(
                        "GPU already leased: lease_id={} mission_id={} state={}",
                        holder.lease_id,
                        holder.mission_id,
                        holder.state.as_str()
                    ),
                )
            }
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;

    let (lease, is_new) = match outcome {
        crate::lease::ReserveOutcome::Created(l) => (l, true),
        crate::lease::ReserveOutcome::Idempotent(l) => (l, false),
    };
    let lease_id = lease.lease_id.clone();

    if !is_new {
        // Duplicate reserve for the same lease: idempotent 200, no re-drain.
        return Ok(Json(serde_json::json!({
            "released": false,
            "jobs_preempted": 0,
            "jobs_checkpointed": 0,
            "jobs_killed": 0,
            "vram_released_mb": 0,
            "preempted_jobs": [],
            "idempotent": true,
            "lease_id": lease.lease_id,
            "mission_id": lease.mission_id,
            "node_id": lease.node_id,
            "lease_state": lease.state.as_str(),
        })));
    }

    // 2. SCHEDULER EXCLUSION: atomically remove the node from the shared
    //    pool BEFORE drain completes. From this instant no shared work can
    //    be confirmed on the GPU (try_assign fails closed).
    if let Some(rt) = state.registry.get_node(&req.node_id) {
        if let Ok(mut md) = rt.metadata.write() {
            md.online = false;
            md.health_state = "RESERVED_FORGE".to_string();
            md.last_state_change_ms = now_ms();
        }
    }
    let lease = state
        .leases
        .mark_scheduler_excluded(&lease_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state.leases.sync_lease_ref_on_node(&state.registry, &lease);

    // 3. DRAIN: checkpoint/preempt registered shared jobs on the node.
    let result = state.preemption.handle_reservation(&req);
    for job_id in &result.preempted_jobs {
        state
            .metrics
            .jobs_preempted_total
            .fetch_add(1, Ordering::Relaxed);
        // Stop live inference jobs and refund their reservations exactly once.
        if let Some(record) = state.jobs.get(job_id) {
            record.cancelled.store(true, Ordering::Relaxed);
            record.done.store(true, Ordering::Relaxed);
            let refund = record.reserved_tokens;
            if let Some(key) = record.api_key.as_deref() {
                let settled = state
                    .ops
                    .settle_reservation(job_id, 0, refund, "PREEMPTED")
                    .unwrap_or(false);
                if settled && refund > 0 {
                    let _ = state.scheduler.release_tokens(key, refund);
                    let _ = state.ops.record(
                        OpsEventKind::AccountingRefunded,
                        Some(&req.node_id),
                        Some(&lease_id),
                        Some(job_id),
                        Some(&req.mission_id),
                        &format!("preempted job refunded {refund} tokens"),
                        "PREEMPTED",
                    );
                }
            }
            state.jobs.remove(job_id);
            let _ = state.ops.record(
                OpsEventKind::JobPreempted,
                Some(&req.node_id),
                Some(&lease_id),
                Some(job_id),
                Some(&req.mission_id),
                "live shared job stopped for Forge lease",
                "PREEMPTED",
            );
        }
        // Checkpoint registry row (job / node / lease / mission association).
        let cp_ref = format!("checkpoint-{job_id}");
        let _ = state.ops.record_checkpoint(
            job_id,
            &req.node_id,
            Some(&lease_id),
            Some(&req.mission_id),
            &cp_ref,
            "{}",
        );
        let _ = state.ops.record(
            OpsEventKind::JobCheckpointed,
            Some(&req.node_id),
            Some(&lease_id),
            Some(job_id),
            Some(&req.mission_id),
            &cp_ref,
            "CHECKPOINTED",
        );
    }

    // 4. RUNTIME EXIT: yield the local model runtime process cleanly.
    let _ = state.model_manager.stop_model().await;
    let exited_lease = state
        .leases
        .mark_runtime_exited(&lease_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state
        .leases
        .sync_lease_ref_on_node(&state.registry, &exited_lease);
    if let Some(rt) = state.registry.get_node(&req.node_id) {
        if let Ok(mut md) = rt.metadata.write() {
            md.runtime_state = RuntimeState::Unavailable;
        }
    }
    let _lease = state
        .leases
        .mark_gpu_released(&lease_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 5. LEASE GRANTED — only now, because Nauti has ACTUALLY yielded
    //    ownership (scheduler excluded, shared work stopped, runtime out).
    let lease = state
        .leases
        .grant(&lease_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state.leases.sync_lease_ref_on_node(&state.registry, &lease);
    state
        .metrics
        .leases_granted_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .preemptions_total
        .fetch_add(result.jobs_preempted as u64, Ordering::Relaxed);

    if let Some(rt) = state.registry.get_node(&req.node_id) {
        crate::state::persist_node(&state.ops, &rt);
    }

    info!(
        lease_id = %lease_id,
        mission_id = %req.mission_id,
        node_id = %req.node_id,
        jobs_preempted = result.jobs_preempted,
        "Forge GPU lease granted (ownership yielded)"
    );
    Ok(Json(serde_json::json!({
        "released": result.released,
        "jobs_preempted": result.jobs_preempted,
        "jobs_checkpointed": result.jobs_checkpointed,
        "jobs_killed": result.jobs_killed,
        "vram_released_mb": result.vram_released_mb,
        "preempted_jobs": result.preempted_jobs,
        "lease_id": lease.lease_id,
        "mission_id": lease.mission_id,
        "node_id": lease.node_id,
        "lease_state": lease.state.as_str(),
        "granted_at_ms": lease.granted_at_ms,
    })))
}

pub async fn forge_release(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ForgeReleaseRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;

    // Chunk 7 lease state machine: RELEASING. Duplicate releases of an
    // already-terminal lease are idempotent (Ok(None) -> 200 released=false).
    let begin = state
        .leases
        .begin_release(&req.mission_id, &req.node_id)
        .map_err(|e| match e {
            crate::lease::LeaseError::Mismatch { expected_mission } => (
                StatusCode::CONFLICT,
                format!(
                    "release does not match active lease (expected mission {expected_mission})"
                ),
            ),
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;

    let Some(lease) = begin else {
        // Duplicate release or no lease known: idempotent success.
        let _ = state.preemption.release_reservation(&req.mission_id);
        return Ok(Json(serde_json::json!({
            "released": false,
            "restored": false,
            "resumed_jobs": 0,
            "reason": "no active lease (duplicate release)",
        })));
    };
    let lease_id = lease.lease_id.clone();

    // RC1 compatibility mirror (is_node_reserved semantics).
    let _ = state.preemption.release_reservation(&req.mission_id);

    let mut restored = false;
    let mut resumed_count = 0;
    let mut reason = String::new();

    if let Some(rt) = state.registry.get_node(&req.node_id) {
        let (prohibited, current_model) = {
            let md = rt.metadata.read().unwrap();
            let prohibited = matches!(
                md.lifecycle_state,
                crate::types::NodeLifecycleState::Withdrawn
                    | crate::types::NodeLifecycleState::Draining
                    | crate::types::NodeLifecycleState::Revoked
                    | crate::types::NodeLifecycleState::Offline
            ) || md.health_state == "WITHDRAWN"
                || md.health_state == "REVOKED"
                || md.health_state == "DRAINING"
                || state.revocations.is_revoked(&req.node_id);
            (prohibited, md.capabilities.role.clone())
        };

        if prohibited {
            // GPU returns from Forge's perspective, but the node must NOT be
            // restored to the shared pool (revoked/withdrawn/draining).
            reason = "node lifecycle state prohibits shared compute recovery".to_string();
            let lease = state
                .leases
                .complete_release(&lease_id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            state.leases.sync_lease_ref_on_node(&state.registry, &lease);
            let _ = state.ops.record(
                OpsEventKind::LeaseReleased,
                Some(&req.node_id),
                Some(&lease_id),
                None,
                Some(&req.mission_id),
                &reason,
                "RELEASED",
            );
        } else {
            // RECOVERING: shared compute restoration.
            let lease = state
                .leases
                .mark_recovery_started(&lease_id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            state.leases.sync_lease_ref_on_node(&state.registry, &lease);
            if let Ok(mut md) = rt.metadata.write() {
                md.health_state = "RECOVERING".to_string();
                md.last_state_change_ms = now_ms();
            }

            // Resume checkpointed jobs (idempotent checkpoint consumption).
            resumed_count = state.preemption.resume_checkpointed(&req.node_id);
            if resumed_count > 0 {
                state
                    .metrics
                    .jobs_requeued_total
                    .fetch_add(resumed_count as u64, Ordering::Relaxed);
            }
            for cp in state
                .ops
                .checkpoints_for_node(&req.node_id)
                .unwrap_or_default()
            {
                if state.ops.consume_checkpoint(&cp.job_id).unwrap_or(false) {
                    let _ = state.ops.record(
                        OpsEventKind::JobResumed,
                        Some(&req.node_id),
                        Some(&lease_id),
                        Some(&cp.job_id),
                        Some(&req.mission_id),
                        "checkpointed job resumed after lease release",
                        "RESUMED",
                    );
                }
            }

            // Attempt to restore the shared model runtime. For signed
            // volunteer workers the worker itself is the runtime authority
            // (RC1 protocol), so a failed local restart does not keep the
            // node excluded; the truth is re-established via heartbeats.
            let model_to_start = if current_model.is_empty() {
                "thinker".to_string()
            } else {
                current_model
            };
            match state
                .model_manager
                .request_model(&model_to_start, 5200, 16384)
                .await
            {
                Ok(_) => {
                    let lease = state
                        .leases
                        .mark_runtime_ready(&lease_id)
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    state.leases.sync_lease_ref_on_node(&state.registry, &lease);
                }
                Err(e) => {
                    state
                        .metrics
                        .runtime_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = state.ops.record(
                        OpsEventKind::RuntimeFailed,
                        Some(&req.node_id),
                        Some(&lease_id),
                        None,
                        Some(&req.mission_id),
                        &format!("shared runtime restart failed: {e}"),
                        "RUNTIME_FAILED",
                    );
                }
            }

            // Restore the node to the shared pool (RC1 semantics).
            if let Ok(mut md) = rt.metadata.write() {
                md.online = true;
                md.health_state = "ONLINE".to_string();
                md.reconciliation.status = crate::types::ReconciliationStatus::MATCH;
                md.runtime_state = RuntimeState::Ready;
                md.last_state_change_ms = now_ms();
                md.failure_reason = String::new();
            }
            let lease = state
                .leases
                .complete_release(&lease_id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            state.leases.sync_lease_ref_on_node(&state.registry, &lease);
            restored = true;
        }
        crate::state::persist_node(&state.ops, &rt);
    } else {
        // Node unknown to the registry: release the lease anyway.
        let lease = state
            .leases
            .complete_release(&lease_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        state.leases.sync_lease_ref_on_node(&state.registry, &lease);
        if reason.is_empty() {
            reason = "node not in registry; lease released".to_string();
        }
    }

    state
        .metrics
        .leases_released_total
        .fetch_add(1, Ordering::Relaxed);

    info!(
        lease_id = %lease_id,
        mission_id = %req.mission_id,
        node_id = %req.node_id,
        restored = restored,
        "Forge GPU lease released"
    );

    Ok(Json(serde_json::json!({
        "released": true,
        "restored": restored,
        "resumed_jobs": resumed_count,
        "reason": reason,
        "lease_id": lease_id,
        "lease_state": "RELEASED",
    })))
}

pub async fn list_forge_reservations(State(state): State<AppState>) -> Json<serde_json::Value> {
    let res = state.preemption.list_reservations();
    Json(serde_json::to_value(res).unwrap_or_default())
}

pub async fn admin_disable_external_alpha(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    state.external_alpha_enabled.store(false, Ordering::Relaxed);
    info!("operator kill switch engaged: external alpha disabled");
    Ok(Json(serde_json::json!({
        "external_alpha_enabled": false,
        "status": "disabled"
    })))
}

pub async fn admin_enable_external_alpha(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    state.external_alpha_enabled.store(true, Ordering::Relaxed);
    info!("operator kill switch disengaged: external alpha enabled");
    Ok(Json(serde_json::json!({
        "external_alpha_enabled": true,
        "status": "enabled"
    })))
}

pub async fn admin_kill_switch_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let enabled = state.external_alpha_enabled.load(Ordering::Relaxed);
    Json(serde_json::json!({
        "external_alpha_enabled": enabled
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk 7: operator fleet observability
// ─────────────────────────────────────────────────────────────────────────────

/// Machine-readable operator fleet view. Admin-authenticated.
/// Exposes safe operational information only: no private keys, admin keys,
/// bearer tokens, or volunteer-local secrets.
pub async fn operator_fleet_status(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let now = now_ms();

    let mut nodes = Vec::new();
    let mut eligible_count = 0u64;
    let mut ownership_counts: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();

    for rt in state.registry.list_nodes() {
        let md = match rt.metadata.read() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let ownership = crate::eligibility::derive_ownership(&md, md.assigned_jobs);
        *ownership_counts
            .entry(ownership.as_str().to_string())
            .or_insert(0) += 1;
        let eligibility = crate::eligibility::evaluate_metadata(&md);
        if eligibility.is_ok() {
            eligible_count += 1;
        }
        let fingerprint = if md.public_key.is_empty() {
            serde_json::Value::Null
        } else {
            let hexed = hex::encode(&md.public_key);
            serde_json::json!(format!("ed25519:{}", &hexed[..16.min(hexed.len())]))
        };
        let last_hb = rt.last_heartbeat_ms();
        nodes.push(serde_json::json!({
            "node_id": md.id,
            "public_fingerprint": fingerprint,
            "role": md.role,
            "gpu_name": md.gpu_name,
            "vram_mb": md.capabilities.vram_mb,
            "models": md.capabilities.models,
            "lifecycle_state": md.lifecycle_state.as_str(),
            "health_state": md.health_state,
            "online": md.online,
            "runtime_state": md.runtime_state.as_str(),
            "reconciliation_status": format!("{:?}", md.reconciliation.status),
            "ownership": ownership.as_str(),
            "eligible": eligibility.is_ok(),
            "ineligibility_reason": eligibility.err().map(|r| r.as_str()),
            "active_lease": md.active_lease,
            "assigned_jobs": md.assigned_jobs,
            "last_heartbeat_ms": last_hb,
            "heartbeat_age_ms": now.saturating_sub(last_hb),
            "last_state_change_ms": md.last_state_change_ms,
            "failure_reason": md.failure_reason,
        }));
    }

    let leases: Vec<serde_json::Value> = state
        .leases
        .all_leases(100)
        .into_iter()
        .map(|l| {
            serde_json::json!({
                "lease_id": l.lease_id,
                "mission_id": l.mission_id,
                "node_id": l.node_id,
                "device_id": l.device_id,
                "state": l.state.as_str(),
                "vram_mb": l.vram_mb,
                "priority": l.priority,
                "requested_at_ms": l.requested_at_ms,
                "scheduler_excluded_at_ms": l.scheduler_excluded_at_ms,
                "runtime_exited_at_ms": l.runtime_exited_at_ms,
                "gpu_released_at_ms": l.gpu_released_at_ms,
                "granted_at_ms": l.granted_at_ms,
                "releasing_at_ms": l.releasing_at_ms,
                "released_at_ms": l.released_at_ms,
                "recovery_started_at_ms": l.recovery_started_at_ms,
                "runtime_ready_at_ms": l.runtime_ready_at_ms,
                "scheduler_eligible_at_ms": l.scheduler_eligible_at_ms,
                "first_inference_at_ms": l.first_inference_at_ms,
                "failure_reason": l.failure_reason,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "generated_at_ms": now,
        "nodes": nodes,
        "leases": leases,
        "summary": {
            "nodes_total": nodes.len(),
            "eligible_for_shared_work": eligible_count,
            "ownership": ownership_counts,
            "active_leases": state.leases.all_active_leases().len(),
        },
    })))
}

#[derive(Debug, Deserialize)]
pub struct OperatorEventsQuery {
    pub node: Option<String>,
    pub lease: Option<String>,
    pub job: Option<String>,
    pub kind: Option<String>,
    pub limit: Option<u64>,
}

/// Recent operational event history ("why isn't this GPU available?").
/// Admin-authenticated.
pub async fn operator_events(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(q): Query<OperatorEventsQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    check_admin_auth(&headers, &state.config)?;
    let events = state
        .ops
        .events(
            q.node.as_deref(),
            q.lease.as_deref(),
            q.job.as_deref(),
            q.kind.as_deref(),
            q.limit.unwrap_or(100),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "count": events.len(),
        "events": events,
    })))
}

// ── Chunk 10: Provider observability ────────────────────────────────────

use crate::providers::select_provider;
use crate::providers::ProviderPolicy;
use crate::providers::ProviderRequest;

/// GET /v1/providers — list all registered providers with health + capabilities.
pub async fn list_providers(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut providers: Vec<serde_json::Value> = Vec::new();
    for provider in state.providers.all() {
        let health = provider
            .health()
            .await
            .unwrap_or_else(|e| crate::providers::ProviderHealth {
                provider_id: provider.id().to_string(),
                healthy: false,
                available: false,
                last_checked_ms: crate::types::now_ms() as i64,
                reason: Some(format!("health check failed: {e}")),
            });
        let caps = provider.capabilities().await.unwrap_or_else(|e| {
            crate::providers::ProviderCapabilities {
                provider_id: provider.id().to_string(),
                kind: provider.kind(),
                models: Vec::new(),
                gpu_vendors: Vec::new(),
                total_vram_mb: None,
                privacy_classes: Vec::new(),
                checkpointable: false,
                queue_depth: 0,
                expected_latency_ms: None,
                cost_units_per_1k_tokens: None,
            }
        });
        providers.push(serde_json::json!({
            "id": provider.id(),
            "kind": serde_json::to_string(&caps.kind).unwrap_or_default(),
            "healthy": health.healthy,
            "available": health.available,
            "models": caps.models,
            "gpu_vendors": caps.gpu_vendors,
            "total_vram_mb": caps.total_vram_mb,
            "queue_depth": caps.queue_depth,
            "reason": health.reason,
        }));
    }
    Ok(Json(serde_json::json!({ "providers": providers })))
}
