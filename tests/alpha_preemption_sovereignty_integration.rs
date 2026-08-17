//! Chunk 4: Forge Preemption & Mission Sovereignty Integration Test Suite.

use axum::Router;
use nauti_nodes::{
    api::routes,
    capabilities::PrivacyClass,
    config::Config,
    node::auth::{sign_message, SignedJoinRequest},
    preemption::{CheckpointData, CheckpointStore, ForgeReservationRequest, PreemptibleJob},
    runtime::{ModelManager, ModelState, ProcessInfo, RuntimeBackend},
    state::AppState,
    types::{
        now_ms, NodeLifecycleState, ObservedHardware, ReconciliationStatus, TelemetrySourceType,
    },
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn test_config() -> Config {
    let mut config = Config::default();
    config.db_url = "sqlite::memory:".to_string();
    config.forge_url = "http://127.0.0.1:1".to_string();
    config.local_inference_url = "http://127.0.0.1:5200".to_string();
    config.strict_alpha = true;
    config
}

fn make_key() -> (ed25519_dalek::SigningKey, String) {
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());
    (key, pub_hex)
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn spawn_test_server() -> (String, AppState) {
    let config = test_config();
    let state = AppState::new(config).unwrap();
    let app: Router = routes::router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (base, state)
}

pub struct MockRuntimeBackend {
    pub should_fail_start: bool,
}

impl Default for MockRuntimeBackend {
    fn default() -> Self {
        Self {
            should_fail_start: false,
        }
    }
}

#[async_trait::async_trait]
impl RuntimeBackend for MockRuntimeBackend {
    fn name(&self) -> &'static str {
        "mock-runtime"
    }

    fn detect(&self) -> bool {
        true
    }

    async fn start(
        &self,
        config: &nauti_nodes::runtime::LocalModelConfig,
        port: u16,
    ) -> Result<ProcessInfo, String> {
        if self.should_fail_start {
            return Err("mock start failed".to_string());
        }
        Ok(ProcessInfo {
            pid: 8888,
            port,
            base_url: format!("http://127.0.0.1:{port}"),
            model_id: config.model_id.clone(),
            started_at_ms: now_ms(),
        })
    }

    async fn check_readiness(
        &self,
        _base_url: &str,
        _expected_model: &str,
        _timeout_secs: u64,
    ) -> Result<bool, String> {
        Ok(true)
    }

    async fn stop(&self, _pid: u32) -> Result<(), String> {
        Ok(())
    }

    async fn current_model(&self, _base_url: &str) -> Option<String> {
        Some("thinker".to_string())
    }
}

// -----------------------------------------------------------------------------
// Test 1: Idle Preemption -> Reservation excludes node from shared picking
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_1_idle_preemption_excludes_node() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Register node
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    // Mark live observation & MATCH reconciliation
    if let Some(rt) = state.registry.get_node(&node_id) {
        if let Ok(mut md) = rt.metadata.write() {
            md.observed = Some(ObservedHardware {
                gpu_name: "RTX 4090".into(),
                vram_mb: 24576,
                vendor: "NVIDIA".into(),
                device_count: 1,
                driver_version: None,
                temperature_c: None,
                node_id: node_id.clone(),
                observer_node_id: "local".into(),
                source_type: TelemetrySourceType::LocalLiveObservation,
                provider: "all-smi".into(),
                observed_at: Some(now_ms()),
                received_at: Some(now_ms()),
                measurement_id: None,
                device_id: None,
                sequence: None,
                age_ms: None,
                source_endpoint: None,
            });
            md.reconciliation.status = ReconciliationStatus::MATCH;
        }
    }

    assert!(
        nauti_nodes::fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some()
    );

    // Trigger Forge reservation
    let res_req = ForgeReservationRequest {
        mission_id: "mission-alpha".into(),
        node_id: node_id.clone(),
        vram_mb: 24576,
        priority: 100,
        allow_preemption: true,
    };
    let res_resp = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    assert_eq!(res_resp.status().as_u16(), 200);

    // Verify node is now ineligible
    assert!(
        nauti_nodes::fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none()
    );
}

// -----------------------------------------------------------------------------
// Test 2: Ready Runtime Preemption -> Runtime process stopped, GPU yielded
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_2_ready_runtime_preemption() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Start model runtime
    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);
    let _ = manager
        .request_model("thinker", 18300, 16384)
        .await
        .unwrap();
    assert_eq!(manager.get_status().state, ModelState::Ready);

    // Reserve node
    let res_req = ForgeReservationRequest {
        mission_id: "mission-beta".into(),
        node_id: "node-2".into(),
        vram_mb: 16384,
        priority: 100,
        allow_preemption: true,
    };
    let _ = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();

    // Stop model via manager
    let _ = manager.stop_model().await;
    assert_eq!(manager.get_status().state, ModelState::Available);
    assert_eq!(manager.get_status().process, None);
}

// -----------------------------------------------------------------------------
// Test 3: Active Job Preemption & Checkpointing
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_3_active_job_checkpointed() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let node_id = "node-3";

    let job = PreemptibleJob::new("job-cp-test", node_id, true);
    state.preemption.register_job(job);

    let res_req = ForgeReservationRequest {
        mission_id: "mission-gamma".into(),
        node_id: node_id.into(),
        vram_mb: 16384,
        priority: 100,
        allow_preemption: true,
    };

    let resp = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    let result_json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result_json["jobs_preempted"].as_u64().unwrap(), 1);
    assert_eq!(result_json["jobs_checkpointed"].as_u64().unwrap(), 1);
}

// -----------------------------------------------------------------------------
// Test 4: GPU Release & Recovery -> Shared compute restored
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_4_release_and_recovery() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Register node
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    if let Some(rt) = state.registry.get_node(&node_id) {
        if let Ok(mut md) = rt.metadata.write() {
            md.observed = Some(ObservedHardware {
                gpu_name: "RTX 4090".into(),
                vram_mb: 24576,
                vendor: "NVIDIA".into(),
                device_count: 1,
                driver_version: None,
                temperature_c: None,
                node_id: node_id.clone(),
                observer_node_id: "local".into(),
                source_type: TelemetrySourceType::LocalLiveObservation,
                provider: "all-smi".into(),
                observed_at: Some(now_ms()),
                received_at: Some(now_ms()),
                measurement_id: None,
                device_id: None,
                sequence: None,
                age_ms: None,
                source_endpoint: None,
            });
            md.reconciliation.status = ReconciliationStatus::MATCH;
        }
    }

    // 1. Reserve
    let res_req = ForgeReservationRequest {
        mission_id: "mission-delta".into(),
        node_id: node_id.clone(),
        vram_mb: 24576,
        priority: 100,
        allow_preemption: true,
    };
    let _ = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    assert!(
        nauti_nodes::fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none()
    );

    // 2. Release
    let rel_resp = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({
            "mission_id": "mission-delta",
            "node_id": node_id
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(rel_resp.status().as_u16(), 200);
    let rel_json: serde_json::Value = rel_resp.json().await.unwrap();
    assert!(rel_json["released"].as_bool().unwrap());
    assert!(rel_json["restored"].as_bool().unwrap());

    // Node is eligible again
    assert!(
        nauti_nodes::fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some()
    );
}

// -----------------------------------------------------------------------------
// Test 5: Revoked Node Does NOT Recover on Release
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_5_revoked_node_no_recovery_on_release() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let node_id = "revoked-preempt-node";

    // Reserve node
    let res_req = ForgeReservationRequest {
        mission_id: "mission-epsilon".into(),
        node_id: node_id.into(),
        vram_mb: 16384,
        priority: 100,
        allow_preemption: true,
    };
    let _ = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();

    // Revoke node during reservation
    state
        .revocations
        .revoke(node_id, "banned during mission")
        .unwrap();

    // Release GPU
    let rel_resp = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({
            "mission_id": "mission-epsilon",
            "node_id": node_id
        }))
        .send()
        .await
        .unwrap();

    let json: serde_json::Value = rel_resp.json().await.unwrap();
    assert!(json["released"].as_bool().unwrap());
    assert!(
        !json["restored"].as_bool().unwrap(),
        "Revoked node MUST NOT be restored on GPU release"
    );
}

// -----------------------------------------------------------------------------
// Test 6: Withdrawn Node Does NOT Recover on Release
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_6_withdrawn_node_no_recovery_on_release() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    // Reserve
    let res_req = ForgeReservationRequest {
        mission_id: "mission-zeta".into(),
        node_id: node_id.clone(),
        vram_mb: 24576,
        priority: 100,
        allow_preemption: true,
    };
    let _ = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();

    // Withdraw node
    let mut wreq = nauti_nodes::node::auth::SignedWithdrawRequest {
        node_id: node_id.clone(),
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    wreq.signature_hex = sign_message(&key, &wreq.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/withdraw"))
        .json(&wreq)
        .send()
        .await
        .unwrap();

    // Release
    let rel_resp = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({
            "mission_id": "mission-zeta",
            "node_id": node_id
        }))
        .send()
        .await
        .unwrap();

    let json: serde_json::Value = rel_resp.json().await.unwrap();
    assert!(json["released"].as_bool().unwrap());
    assert!(
        !json["restored"].as_bool().unwrap(),
        "Withdrawn node MUST NOT be restored on GPU release"
    );
}

// -----------------------------------------------------------------------------
// Test 7: Duplicate Reservation and Release Idempotency
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_7_idempotent_reservation_and_release() {
    let (base, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let node_id = "node-idempotent";

    let res_req = ForgeReservationRequest {
        mission_id: "mission-idem".into(),
        node_id: node_id.into(),
        vram_mb: 16384,
        priority: 100,
        allow_preemption: true,
    };

    // Duplicate reservations
    let r1 = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    let r2 = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status().as_u16(), 200);
    assert_eq!(r2.status().as_u16(), 200);

    // Duplicate releases
    let rel1 = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({ "mission_id": "mission-idem", "node_id": node_id }))
        .send()
        .await
        .unwrap();
    let rel2 = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({ "mission_id": "mission-idem", "node_id": node_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(rel1.status().as_u16(), 200);
    assert_eq!(rel2.status().as_u16(), 200);
}

// -----------------------------------------------------------------------------
// Test 8: End-to-End Preemption & Recovery Timings
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_8_end_to_end_preemption_timings() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let node_id = "timing-node";

    let start_time = Instant::now();

    // 1. Reserve
    let res_req = ForgeReservationRequest {
        mission_id: "mission-timing".into(),
        node_id: node_id.into(),
        vram_mb: 16384,
        priority: 100,
        allow_preemption: true,
    };
    let t0 = Instant::now();
    let _ = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&res_req)
        .send()
        .await
        .unwrap();
    let reserve_latency = t0.elapsed();

    assert!(state.preemption.is_node_reserved(node_id));

    // 2. Release
    let t1 = Instant::now();
    let _ = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({ "mission_id": "mission-timing", "node_id": node_id }))
        .send()
        .await
        .unwrap();
    let release_latency = t1.elapsed();

    assert!(!state.preemption.is_node_reserved(node_id));
    println!("Preemption Reserve Latency: {:?}", reserve_latency);
    println!("Preemption Release Latency: {:?}", release_latency);
    println!("Total Preemption Cycle Time: {:?}", start_time.elapsed());
}
