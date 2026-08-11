use crate::adapter::llama::ChatMessage;
use crate::fleet;
use crate::job_runner;
use crate::jobs::JobRecord;
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

#[derive(Debug, Deserialize)]
pub struct WorkerRegisterRequest {
    pub id: String,
    pub inference_url: String,
    pub role: String,
    pub gpu_name: String,
    pub vram_mb: u32,
    #[serde(default)]
    pub models: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub node_id: String,
    pub active_jobs: u32,
    pub free_vram_mb: u32,
    pub tokens_per_sec: f64,
}

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
        while let Ok(chunk) = rx.recv().await {
            full.push_str(&chunk);
            if record.done.load(Ordering::Relaxed) {
                break;
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

pub async fn worker_register(
    State(state): State<AppState>,
    Json(req): Json<WorkerRegisterRequest>,
) -> Json<serde_json::Value> {
    let role = req.role.clone();
    let meta = NodeMetadata {
        id: req.id.clone(),
        addr: req.inference_url.clone(),
        inference_url: req.inference_url,
        role: role.clone(),
        gpu_name: req.gpu_name,
        online: true,
        public_key: vec![],
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
    };
    if state.registry.get_node(&req.id).is_some() {
        if let Some(rt) = state.registry.get_node(&req.id) {
            if let Ok(mut md) = rt.metadata.write() {
                *md = meta;
            }
        }
    } else {
        state.registry.register_node(meta);
    }
    Json(serde_json::json!({ "registered": req.id }))
}

pub async fn worker_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<WorkerHeartbeatRequest>,
) -> Json<serde_json::Value> {
    if let Some(rt) = state.registry.get_node(&req.node_id) {
        rt.touch_heartbeat(NodeStatistics {
            active_jobs: req.active_jobs,
            free_vram_mb: req.free_vram_mb,
            tokens_per_sec: req.tokens_per_sec,
            ..Default::default()
        });
        if let Ok(mut md) = rt.metadata.write() {
            md.online = true;
        }
    }
    Json(serde_json::json!({ "ok": true }))
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

pub async fn metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "nodes": state.registry.list_nodes().len(),
        "active_jobs": state.jobs.active_count(),
    }))
}
