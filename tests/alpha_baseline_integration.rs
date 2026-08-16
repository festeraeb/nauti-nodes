//! Chunk 1 Integration Tests — Alpha Baseline & Failure Bounds
//!
//! Verifies strict alpha invariants, required dependency failure cases,
//! and real GPU inference execution path using real HTTP loopback connections.

use ed25519_dalek::SigningKey;
use nauti_nodes::{
    api::routes::router,
    node::auth::{sign_message, SignedJoinRequest},
    state::AppState,
    types::{
        NodeCapabilities, NodeMetadata, ObservedHardware, ReconciledHardwareState,
        ReconciliationStatus, RegisteredHardwareClaim, TelemetrySourceType,
    },
    Config,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn make_key() -> SigningKey {
    SigningKey::generate(&mut rand::rngs::OsRng)
}

fn pub_hex(key: &SigningKey) -> String {
    hex::encode(key.verifying_key().to_bytes())
}

fn test_config() -> Config {
    Config {
        listen_port: 0,
        forge_url: "http://127.0.0.1:1".to_string(), // Unreachable Forge by default
        coordinator_url: "http://127.0.0.1:8099".to_string(),
        local_inference_url: "http://127.0.0.1:5200".to_string(),
        db_url: ":memory:".to_string(),
        strict_alpha: true,
        ..Default::default()
    }
}

async fn start_server_with_state(state: AppState) -> String {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind :0 failed");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

async fn start_server() -> (String, AppState) {
    let config = test_config();
    let state = AppState::new(config).expect("AppState::new failed");
    let url = start_server_with_state(state.clone()).await;
    (url, state)
}

// -----------------------------------------------------------------------------
// Test 1: Forge Unavailable -> Visibly fails with empty fleet
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_1_forge_unavailable() {
    let client = reqwest::Client::new();
    let nodes = nauti_nodes::adapter::forge::fetch_fleet_from_forge(&client, "http://127.0.0.1:1").await;
    assert!(nodes.is_empty(), "Unreachable Forge must return empty fleet, not fake nodes");
}

// -----------------------------------------------------------------------------
// Test 2: Paddler / Requested Model Unavailable -> 503 Service Unavailable
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_2_6_model_unavailable() {
    let (base, _) = start_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "test-key")
        .json(&serde_json::json!({
            "model": "non-existent-model-xyz",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 503);
}

// -----------------------------------------------------------------------------
// Test 3: Worker Unavailable -> 503 Service Unavailable
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_3_worker_unavailable() {
    let (base, _) = start_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "test-key")
        .json(&serde_json::json!({
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 503);
}

// -----------------------------------------------------------------------------
// Test 4: Worker Heartbeat Lost -> Stale Node Eviction
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_4_heartbeat_eviction() {
    let registry = Arc::new(nauti_nodes::node::registry::NodeRegistry::new());

    let mut meta = nauti_nodes::adapter::forge::fleet_node(
        "stale-worker",
        "http://127.0.0.1:5200",
        "thinker",
        "RTX 2060",
        8192,
    );
    meta.health_state = "ONLINE".to_string();
    registry.register_node(meta);

    assert_eq!(registry.list_nodes().len(), 1);

    // Evict stale nodes exceeding 1 second
    let stale = registry.remove_stale(1, nauti_nodes::types::now_ms() + 10_000);
    assert_eq!(stale, vec!["stale-worker".to_string()], "Stale worker must be marked stale/offline");
}

// -----------------------------------------------------------------------------
// Test 5: Inference Runtime Unavailable
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_5_runtime_unavailable() {
    let client = reqwest::Client::new();
    let ok = nauti_nodes::adapter::llama::health_ok(&client, "http://127.0.0.1:1").await;
    assert!(!ok, "Unreachable inference runtime must report health_ok = false");
}

// -----------------------------------------------------------------------------
// Test 7: Invalid Node Signature -> 401 Unauthorized
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_7_invalid_signature() {
    let (base, _) = start_server().await;
    let key = make_key();

    let mut req = SignedJoinRequest {
        node_id: "bad-sig-node".into(),
        public_key_hex: pub_hex(&key),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 2060".into(),
        vram_mb: 8192,
        models: vec!["thinker".into()],
        timestamp_secs: now_secs(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: "00".repeat(64), // Invalid signature
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 401);
}

// -----------------------------------------------------------------------------
// Test 8: Replayed Nonce & Stale Registration Attempt -> 409 & 403
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_8_replayed_nonce_and_stale() {
    let (base, _) = start_server().await;
    let key = make_key();

    let mut req = SignedJoinRequest {
        node_id: "node-replay".into(),
        public_key_hex: pub_hex(&key),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 2060".into(),
        vram_mb: 8192,
        models: vec!["thinker".into()],
        timestamp_secs: now_secs(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let client = reqwest::Client::new();
    let resp1 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status().as_u16(), 200);

    // Replay same nonce
    let resp2 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status().as_u16(), 409);

    // Stale timestamp request (>300s)
    let mut stale_req = req.clone();
    stale_req.node_id = "node-stale".into();
    stale_req.nonce = uuid::Uuid::new_v4().to_string();
    stale_req.timestamp_secs = now_secs() - 600;
    stale_req.signature_hex = sign_message(&key, &stale_req.canonical_message());

    let resp3 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&stale_req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp3.status().as_u16(), 403);
}

// -----------------------------------------------------------------------------
// Test 9: Hardware Claim Mismatch -> Node not eligible for inference selection
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_9_hardware_mismatch() {
    let registry = nauti_nodes::node::registry::NodeRegistry::new();

    let meta = NodeMetadata {
        id: "mismatch-node".to_string(),
        addr: "http://127.0.0.1:5200".to_string(),
        inference_url: "http://127.0.0.1:5200".to_string(),
        role: "thinker".to_string(),
        gpu_name: "RTX 4090".to_string(),
        online: true,
        public_key: vec![],
        capabilities: NodeCapabilities {
            max_batch: 1,
            models: vec!["thinker".to_string()],
            vram_mb: 24576,
            role: "thinker".to_string(),
        },
        claim: RegisteredHardwareClaim {
            gpu_name: "RTX 4090".to_string(),
            vram_mb: 24576,
            models: vec!["thinker".to_string()],
        },
        observed: Some(ObservedHardware {
            gpu_name: "Quadro M2200".to_string(),
            vram_mb: 4096,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            driver_version: None,
            temperature_c: None,
            node_id: "mismatch-node".to_string(),
            observer_node_id: "local".to_string(),
            source_type: TelemetrySourceType::LocalLiveObservation,
            provider: "all-smi".to_string(),
            observed_at: Some(nauti_nodes::types::now_ms()),
            received_at: Some(nauti_nodes::types::now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
        }),
        reconciliation: ReconciledHardwareState {
            status: ReconciliationStatus::MISMATCH,
            fields: vec!["model mismatch".into(), "VRAM mismatch".into()],
        },
        last_hardware_probe: Some(nauti_nodes::types::now_ms()),
        health_state: "ONLINE".to_string(),
        ..Default::default()
    };

    registry.register_node(meta);

    let selected = nauti_nodes::fleet::pick_node_for_role(&registry, Some("thinker"), false);
    assert!(selected.is_none(), "Hardware mismatch node MUST NOT be selected for inference");
}

// -----------------------------------------------------------------------------
// Test 10: Unauthorized Non-Public Workload -> 403 Forbidden
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_failure_case_10_non_public_workload_unauthorized() {
    let config = test_config();
    let state = AppState::new(config).unwrap();

    let mut meta = nauti_nodes::adapter::forge::fleet_node(
        "external-worker-1",
        "http://127.0.0.1:5200",
        "external_alpha",
        "RTX 2060",
        8192,
    );
    meta.capabilities.role = "external_alpha".to_string();
    state.registry.register_node(meta);

    let base = start_server_with_state(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "test-key")
        .json(&serde_json::json!({
            "messages": [{"role": "user", "content": "confidential prompt"}],
            "privacy_class": "LocalOnly"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 403);
}

// -----------------------------------------------------------------------------
// Real GPU Inference Success Path
// CLIENT -> COORDINATOR -> AUTHENTICATED WORKER -> REAL GPU INFERENCE -> STREAM
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_real_gpu_inference_end_to_end() {
    let client = reqwest::Client::new();
    if !nauti_nodes::adapter::llama::health_ok(&client, "http://127.0.0.1:5200").await {
        eprintln!("Skipping live GPU test: local llama-server on port 5200 is not responding");
        return;
    }

    let mut config = test_config();
    config.local_inference_url = "http://127.0.0.1:5200".to_string();
    let state = AppState::new(config).unwrap();

    // Register authenticated worker
    let key = make_key();
    let mut req = SignedJoinRequest {
        node_id: "alpha-test-worker-1".into(),
        public_key_hex: pub_hex(&key),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100-PCIE-16GB".into(),
        vram_mb: 16384,
        models: vec!["thinker".into()],
        timestamp_secs: now_secs(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let base = start_server_with_state(state.clone()).await;

    let reg_resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(reg_resp.status().as_u16(), 200);

    // Give worker observed hardware & MATCH reconciliation state
    if let Some(rt) = state.registry.get_node("alpha-test-worker-1") {
        if let Ok(mut md) = rt.metadata.write() {
            md.observed = Some(ObservedHardware {
                gpu_name: "Tesla P100-PCIE-16GB".to_string(),
                vram_mb: 16384,
                vendor: "NVIDIA".to_string(),
                device_count: 1,
                driver_version: Some("580.178.04".to_string()),
                temperature_c: Some(40),
                node_id: "alpha-test-worker-1".to_string(),
                observer_node_id: "local".to_string(),
                source_type: TelemetrySourceType::LocalLiveObservation,
                provider: "all-smi".to_string(),
                observed_at: Some(nauti_nodes::types::now_ms()),
                received_at: Some(nauti_nodes::types::now_ms()),
                measurement_id: None,
                device_id: None,
                sequence: None,
                age_ms: None,
                source_endpoint: None,
            });
            md.reconciliation = ReconciledHardwareState {
                status: ReconciliationStatus::MATCH,
                fields: vec![],
            };
        }
    }

    // Set token quota for API key
    let _ = state.scheduler.set_balance("alpha-test-key", 10_000);

    // Send real inference request
    let response = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "alpha-test-key")
        .json(&serde_json::json!({
            "model": "thinker",
            "messages": [{"role": "user", "content": "Reply with one word: 'READY'."}],
            "stream": false,
            "avoid_p100": false,
            "max_tokens": 16
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 200);
    let json: serde_json::Value = response.json().await.unwrap();
    assert!(json.get("content").is_some());
    let content = json["content"].as_str().unwrap_or("");
    assert!(!content.is_empty(), "Inference content must not be empty");
    assert!(json.get("job_id").is_some());
    assert_eq!(json["node_id"], "alpha-test-worker-1");
}
