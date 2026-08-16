//! Chunk 2 Volunteer Node Lifecycle Integration Test Suite

use axum::Router;
use ed25519_dalek::SigningKey;
use nauti_nodes::{
    api::routes,
    config::Config,
    node::auth::{sign_message, SignedHeartbeat, SignedJoinRequest, SignedWithdrawRequest},
    node::identity::NodeIdentity,
    state::AppState,
    types::{
        now_ms, NodeLifecycleState, ObservedHardware,
        ReconciliationStatus, TelemetrySourceType,
    },
};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;

async fn spawn_test_server() -> (String, AppState) {
    let mut config = Config::default();
    config.db_url = "sqlite::memory:".to_string();
    config.forge_url = "http://127.0.0.1:1".to_string();
    config.strict_alpha = true;

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

fn make_key() -> (SigningKey, String) {
    let key = SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());
    (key, pub_hex)
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn test_1_valid_first_join() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100-PCIE-16GB".into(),
        vram_mb: 16384,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200);

    let rt = state.registry.get_node(&node_id).expect("node registered");
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.id, node_id);
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Online);
    assert_eq!(md.health_state, "ONLINE");
}

#[tokio::test]
async fn test_2_restart_with_same_identity() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().with_extension("json");
    let _ = std::fs::remove_file(&path);

    // Initial creation
    let id1 = NodeIdentity::load_or_create(&path).unwrap();
    let original_id = id1.node_id.clone();
    let original_pub = id1.public_key.clone();

    // Reload after simulated restart
    let id2 = NodeIdentity::load_or_create(&path).unwrap();
    assert_eq!(id2.node_id, original_id);
    assert_eq!(id2.public_key, original_pub);
    assert!(id2.validate().is_ok());
}

#[tokio::test]
async fn test_3_invalid_identity_state() {
    let (base, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();

    let mut req = SignedJoinRequest {
        node_id: uuid::Uuid::new_v4().to_string(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: "00".repeat(64), // Invalid signature
    };

    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn test_4_duplicate_identity_protection() {
    let (base, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key1, pub_hex1) = make_key();
    let (key2, pub_hex2) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // 1. First worker registers
    let mut req1 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex1.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req1.signature_hex = sign_message(&key1, &req1.canonical_message());

    let resp1 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status().as_u16(), 200);

    // 2. Second worker attempts to register with SAME node_id but DIFFERENT public key
    let mut req2 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex2,
        inference_url: "http://127.0.0.1:5201".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req2.signature_hex = sign_message(&key2, &req2.canonical_message());

    let resp2 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req2)
        .send()
        .await
        .unwrap();

    // Must be rejected with 409 CONFLICT
    assert_eq!(resp2.status().as_u16(), 409);
}

#[tokio::test]
async fn test_5_stale_heartbeat_eviction() {
    let (_base, state) = spawn_test_server().await;
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let meta = nauti_nodes::types::NodeMetadata {
        id: node_id.clone(),
        addr: "http://127.0.0.1:5200".into(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        online: true,
        public_key: hex::decode(&pub_hex).unwrap(),
        health_state: "ONLINE".into(),
        lifecycle_state: NodeLifecycleState::Online,
        ..Default::default()
    };
    let rt = state.registry.register_node(meta);

    // Age heartbeat by 60s
    rt.last_heartbeat.store(
        now_ms().saturating_sub(60_000),
        std::sync::atomic::Ordering::Relaxed,
    );

    let changed = state.registry.remove_stale(30_000, now_ms());
    assert_eq!(changed, vec![node_id.clone()]);

    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "OFFLINE");
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Offline);
    assert!(!md.online);
}

#[tokio::test]
async fn test_6_reconnect_after_timeout() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Register node
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    // Simulate timeout / offline
    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.online = false;
        md.health_state = "OFFLINE".into();
        md.lifecycle_state = NodeLifecycleState::Offline;
    }

    // Worker reconnects with heartbeat
    let mut hb = SignedHeartbeat {
        node_id: node_id.clone(),
        active_jobs: 0,
        free_vram_mb: 16384,
        tokens_per_sec: 25.0,
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    hb.signature_hex = sign_message(&key, &hb.canonical_message());

    let hb_resp = client
        .post(format!("{base}/internal/worker/heartbeat"))
        .json(&hb)
        .send()
        .await
        .unwrap();

    assert_eq!(hb_resp.status().as_u16(), 200);

    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "ONLINE");
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Online);
    assert!(md.online);
}

#[tokio::test]
async fn test_7_graceful_withdrawal() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    // Send withdraw request
    let mut wreq = SignedWithdrawRequest {
        node_id: node_id.clone(),
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    wreq.signature_hex = sign_message(&key, &wreq.canonical_message());

    let wresp = client
        .post(format!("{base}/internal/worker/withdraw"))
        .json(&wreq)
        .send()
        .await
        .unwrap();

    assert_eq!(wresp.status().as_u16(), 200);

    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "WITHDRAWN");
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Withdrawn);
    assert!(!md.online);
}

#[tokio::test]
async fn test_8_rejoin_after_withdrawal() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // 1. Join
    let mut req1 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req1.signature_hex = sign_message(&key, &req1.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req1).send().await;

    // 2. Withdraw
    let mut wreq = SignedWithdrawRequest {
        node_id: node_id.clone(),
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    wreq.signature_hex = sign_message(&key, &wreq.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/withdraw")).json(&wreq).send().await;

    // 3. Rejoin using same identity
    let mut req2 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req2.signature_hex = sign_message(&key, &req2.canonical_message());

    let rresp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req2)
        .send()
        .await
        .unwrap();

    assert_eq!(rresp.status().as_u16(), 200);

    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "ONLINE");
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Online);
    assert!(md.online);
}

#[tokio::test]
async fn test_9_revoked_identity() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Register node
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    // Admin revokes node
    let rev_resp = client
        .post(format!("{base}/v1/admin/revoke"))
        .json(&serde_json::json!({
            "identifier": node_id,
            "reason": "malicious behavior"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(rev_resp.status().as_u16(), 200);

    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "REVOKED");
    assert_eq!(md.lifecycle_state, NodeLifecycleState::Revoked);
    assert!(!md.online);
}

#[tokio::test]
async fn test_10_revoked_identity_attempting_restart() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Admin pre-revokes node_id
    state.revocations.revoke(&node_id, "banned node").unwrap();

    // Revoked node attempts to register
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn test_11_hardware_changed_on_rejoin() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // 1. Initial Join with 16GB P100
    let mut req1 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req1.signature_hex = sign_message(&key, &req1.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req1).send().await;

    // 2. Rejoin with updated hardware claim (e.g. 24GB RTX 3090)
    let mut req2 = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "NVIDIA RTX 3090".into(),
        vram_mb: 24576,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req2.signature_hex = sign_message(&key, &req2.canonical_message());

    let _ = client.post(format!("{base}/internal/worker/register")).json(&req2).send().await;

    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.claim.gpu_name, "NVIDIA RTX 3090");
    assert_eq!(md.claim.vram_mb, 24576);
}

#[tokio::test]
async fn test_12_coordinator_restart_recovery() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // 1. Worker sends heartbeat before registering (simulating coordinator restart)
    let mut hb = SignedHeartbeat {
        node_id: node_id.clone(),
        active_jobs: 0,
        free_vram_mb: 16384,
        tokens_per_sec: 25.0,
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    hb.signature_hex = sign_message(&key, &hb.canonical_message());

    let resp1 = client
        .post(format!("{base}/internal/worker/heartbeat"))
        .json(&hb)
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.status().as_u16(), 404);

    // 2. Worker re-registers
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp2 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp2.status().as_u16(), 200);
    assert!(state.registry.get_node(&node_id).is_some());
}

#[tokio::test]
async fn test_13_withdrawn_node_cannot_receive_work() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    // Register node and give MATCH observed state
    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.observed = Some(ObservedHardware {
            gpu_name: "Tesla P100".into(),
            vram_mb: 16384,
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
        md.health_state = "WITHDRAWN".into();
        md.lifecycle_state = NodeLifecycleState::Withdrawn;
        md.online = false;
    }

    // Try sending inference job
    let inf_resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "alpha-test-key")
        .json(&serde_json::json!({
            "model": "thinker",
            "messages": [{"role": "user", "content": "hi"}],
            "avoid_p100": false
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(inf_resp.status().as_u16(), 503);
}

#[tokio::test]
async fn test_14_draining_node_cannot_receive_work() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.observed = Some(ObservedHardware {
            gpu_name: "Tesla P100".into(),
            vram_mb: 16384,
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
        md.health_state = "DRAINING".into();
        md.lifecycle_state = NodeLifecycleState::Draining;
    }

    let inf_resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "alpha-test-key")
        .json(&serde_json::json!({
            "model": "thinker",
            "messages": [{"role": "user", "content": "hi"}],
            "avoid_p100": false
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(inf_resp.status().as_u16(), 503);
}

#[tokio::test]
async fn test_15_public_privacy_boundary_intact() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let mut req = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "external_alpha".into(),
        gpu_name: "Tesla P100".into(),
        vram_mb: 16384,
        models: vec![],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());
    let _ = client.post(format!("{base}/internal/worker/register")).json(&req).send().await;

    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.observed = Some(ObservedHardware {
            gpu_name: "Tesla P100".into(),
            vram_mb: 16384,
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

    // Non-public workload (LocalOnly) to external worker must return 403 FORBIDDEN
    let inf_resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", "alpha-test-key")
        .json(&serde_json::json!({
            "model": "external_alpha",
            "messages": [{"role": "user", "content": "secret work"}],
            "privacy_class": "LocalOnly",
            "avoid_p100": false
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(inf_resp.status().as_u16(), 403);
}
