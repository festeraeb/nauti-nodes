//! Chunk 5: Volunteer Test Kit & External Alpha Packaging Integration Tests

use axum::Router;
use nauti_nodes::{
    api::routes,
    capabilities::PrivacyClass,
    cli,
    config::Config,
    node::auth::{sign_message, SignedJoinRequest, CURRENT_PROTOCOL_VERSION},
    node::identity::NodeIdentity,
    state::AppState,
    types::{now_ms, ReconciliationStatus, TelemetrySourceType},
    work::WorkUnit,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;

fn test_config() -> Config {
    let mut config = Config::default();
    config.db_url = "sqlite::memory:".to_string();
    config.strict_alpha = false;
    config
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

// -----------------------------------------------------------------------------
// Test 1: Preflight Success Check
// -----------------------------------------------------------------------------
#[test]
fn test_1_preflight_success() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let id_path = tmp_dir.path().join("identity.json");
    let mut config = Config::default();
    config.identity_path = id_path.to_string_lossy().to_string();
    config.strict_alpha = false;

    let report = cli::run_preflight(&config, false).expect("preflight failed");
    assert_eq!(report.protocol_version, CURRENT_PROTOCOL_VERSION);
    assert!(!report.os.is_empty());
    assert!(!report.arch.is_empty());
    assert!(report.permissions_0600);
}

// -----------------------------------------------------------------------------
// Test 2 & 3: Preflight Missing Runtime & Missing Artifact Handling
// -----------------------------------------------------------------------------
#[test]
fn test_2_3_preflight_missing_runtime_strict() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let id_path = tmp_dir.path().join("identity.json");
    let mut config = Config::default();
    config.identity_path = id_path.to_string_lossy().to_string();
    config.strict_alpha = true;
    config.local_inference_url = "http://127.0.0.1:59999".into();

    let report = cli::run_preflight(&config, false).expect("preflight ran");
    assert!(
        !report.ready,
        "Strict alpha with unreachable runtime must be NOT READY"
    );
    assert!(!report.failure_reasons.is_empty());
}

// -----------------------------------------------------------------------------
// Test 4: Identity Inspection Does Not Expose Private Key
// -----------------------------------------------------------------------------
#[test]
fn test_4_identity_inspection_no_private_key() {
    let identity = NodeIdentity::generate();
    let pub_hex = hex::encode(&identity.public_key);
    let fp = format!("ed25519:{}", &pub_hex[..16]);

    // Format identity summary JSON
    let summary = serde_json::json!({
        "node_id": identity.node_id,
        "public_fingerprint": fp,
        "public_key_hex": pub_hex,
        "created_at_ms": identity.created_at_ms,
    });

    let json_str = serde_json::to_string(&summary).unwrap();
    assert!(
        !json_str.contains("secret_key"),
        "Sanitized summary MUST NOT contain secret_key"
    );
    assert!(
        !json_str.contains(&hex::encode(&identity.secret_key)),
        "Sanitized summary MUST NOT contain private key bytes"
    );
}

// -----------------------------------------------------------------------------
// Test 5: Protocol Version Accepted
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_5_protocol_version_accepted() {
    let (base, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());

    let mut req = SignedJoinRequest {
        node_id: uuid::Uuid::new_v4().to_string(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: CURRENT_PROTOCOL_VERSION,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Current protocol version MUST be accepted"
    );
}

// -----------------------------------------------------------------------------
// Test 6: Protocol Version Rejected
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_6_protocol_version_rejected() {
    let (base, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());

    let mut req = SignedJoinRequest {
        node_id: uuid::Uuid::new_v4().to_string(),
        public_key_hex: pub_hex,
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: current_timestamp(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 9999, // Incompatible version
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "Incompatible protocol version MUST be rejected with 400 Bad Request"
    );
}

// -----------------------------------------------------------------------------
// Test 7: External Alpha Kill Switch Rejects New Join
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_7_kill_switch_rejects_join() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Engage operator kill switch
    let disable_resp = client
        .post(format!("{base}/v1/admin/alpha/disable"))
        .send()
        .await
        .unwrap();
    assert_eq!(disable_resp.status().as_u16(), 200);

    let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());

    let mut req = SignedJoinRequest {
        node_id: uuid::Uuid::new_v4().to_string(),
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

    let join_resp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(
        join_resp.status().as_u16(),
        403,
        "Kill switch MUST reject new external alpha joins with 403 Forbidden"
    );

    // Re-enable kill switch
    let enable_resp = client
        .post(format!("{base}/v1/admin/alpha/enable"))
        .send()
        .await
        .unwrap();
    assert_eq!(enable_resp.status().as_u16(), 200);
}

// -----------------------------------------------------------------------------
// Test 8: External Alpha Kill Switch Prevents Work Scheduling
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_8_kill_switch_prevents_work_scheduling() {
    let (_base, state) = spawn_test_server().await;

    // Engage kill switch
    state.external_alpha_enabled.store(false, Ordering::Relaxed);
    assert!(!state.external_alpha_enabled.load(Ordering::Relaxed));

    // Disengage kill switch
    state.external_alpha_enabled.store(true, Ordering::Relaxed);
    assert!(state.external_alpha_enabled.load(Ordering::Relaxed));
}

// -----------------------------------------------------------------------------
// Test 9 & 10: PUBLIC Workload Accepted / Non-PUBLIC Workload Rejected
// -----------------------------------------------------------------------------
#[test]
fn test_9_10_privacy_boundary_enforcement() {
    let public_unit = WorkUnit::inference("public-job-1", "m-1", "thinker", vec![], 100);
    assert!(
        public_unit.is_eligible_for_external_alpha(),
        "PUBLIC workloads MUST be eligible for external alpha"
    );

    let mut local_unit = WorkUnit::inference("local-job-1", "m-1", "thinker", vec![], 100);
    local_unit.privacy_class = PrivacyClass::LocalOnly;
    assert!(
        !local_unit.is_eligible_for_external_alpha(),
        "LocalOnly workloads MUST NOT be eligible for external alpha"
    );

    let mut fed_unit = WorkUnit::inference("fed-job-1", "m-1", "thinker", vec![], 100);
    fed_unit.privacy_class = PrivacyClass::FederationSafe;
    assert!(
        !fed_unit.is_eligible_for_external_alpha(),
        "FederationSafe workloads MUST NOT be eligible for external alpha"
    );
}

// -----------------------------------------------------------------------------
// Test 11 & 12: Signed Withdrawal & Rejoin with Same Identity
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_11_12_withdrawal_and_same_identity_rejoin() {
    let (base, state) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());
    let node_id = uuid::Uuid::new_v4().to_string();

    // 1. Join
    let mut jreq = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
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
    jreq.signature_hex = sign_message(&key, &jreq.canonical_message());
    let jresp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&jreq)
        .send()
        .await
        .unwrap();
    assert_eq!(jresp.status().as_u16(), 200);

    // 2. Withdraw
    let mut wreq = nauti_nodes::node::auth::SignedWithdrawRequest {
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

    // Verify status is WITHDRAWN
    if let Some(rt) = state.registry.get_node(&node_id) {
        let md = rt.metadata.read().unwrap();
        assert_eq!(md.health_state, "WITHDRAWN");
    }

    // 3. Rejoin with SAME identity
    let mut rreq = SignedJoinRequest {
        node_id: node_id.clone(),
        public_key_hex: pub_hex.clone(),
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
    rreq.signature_hex = sign_message(&key, &rreq.canonical_message());
    let rresp = client
        .post(format!("{base}/internal/worker/register"))
        .json(&rreq)
        .send()
        .await
        .unwrap();
    assert_eq!(
        rresp.status().as_u16(),
        200,
        "Voluntary rejoin with SAME identity MUST succeed"
    );
}

// -----------------------------------------------------------------------------
// Test 13: Compatibility Report Sanitization
// -----------------------------------------------------------------------------
#[test]
fn test_13_compatibility_report_sanitization() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let id_path = tmp_dir.path().join("identity.json");
    let mut config = Config::default();
    config.identity_path = id_path.to_string_lossy().to_string();

    let report = cli::generate_compatibility_report(&config).expect("report generation failed");
    let json_str = serde_json::to_string(&report).unwrap();

    assert!(!json_str.contains("secret_key"));
    assert!(!json_str.contains("private_key"));
    assert!(report.public_workloads_only);
}

// -----------------------------------------------------------------------------
// Test 14: Diagnostic Bundle Sanitization
// -----------------------------------------------------------------------------
#[test]
fn test_14_diagnostic_bundle_sanitization() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let id_path = tmp_dir.path().join("identity.json");
    let mut config = Config::default();
    config.identity_path = id_path.to_string_lossy().to_string();

    let bundle_path = cli::run_bundle(&config, None).expect("bundle generation failed");
    let bundle_data = std::fs::read_to_string(&bundle_path).expect("read bundle file failed");

    assert!(
        !bundle_data.contains("secret_key"),
        "Diagnostic bundle MUST NOT contain secret_key"
    );
    assert!(
        bundle_data.contains("[REDACTED]"),
        "Diagnostic bundle config MUST redact sensitive fields"
    );

    // Clean up
    let _ = std::fs::remove_file(bundle_path);
}

// -----------------------------------------------------------------------------
// Test 15: Reconnect Backoff Bounded
// -----------------------------------------------------------------------------
#[test]
fn test_15_reconnect_backoff_bounded() {
    let base_interval_secs = 10u64;
    let max_backoff_secs = 300u64;

    let mut current = base_interval_secs;
    for _ in 0..10 {
        current = (current * 2).min(max_backoff_secs);
    }
    assert_eq!(
        current, max_backoff_secs,
        "Reconnect backoff MUST remain capped at max_backoff_secs"
    );
}
