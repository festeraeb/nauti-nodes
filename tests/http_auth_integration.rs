//! HTTP boundary integration tests for Nauti node auth.
//!
//! Each test binds a full Axum server to port 0 (randomly assigned) via
//! `tokio::net::TcpListener` and exercises the `/internal/worker/register`
//! endpoint with real `reqwest` HTTP calls over a loopback TCP connection.
//!
//! Covered boundaries:
//! - Valid signed request          → 200 OK + registration receipt
//! - Tampered payload after sign   → 401 Unauthorized (SignatureMismatch)
//! - Stale timestamp (>5 min old)  → 403 Forbidden
//! - Replayed nonce                → 409 Conflict
//! - GET /health                   → 200 OK

use ed25519_dalek::SigningKey;
use nauti_nodes::{
    api::routes::router,
    node::auth::{sign_message, SignedJoinRequest},
    state::AppState,
    Config,
};
use std::time::{SystemTime, UNIX_EPOCH};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

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

/// Build a freshly signed, valid join request.
fn valid_join(key: &SigningKey) -> SignedJoinRequest {
    let mut req = SignedJoinRequest {
        node_id: format!("test-{}", uuid::Uuid::new_v4()),
        public_key_hex: pub_hex(key),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 4090".into(),
        vram_mb: 24576,
        models: vec!["thinker".into()],
        timestamp_secs: now_secs(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(key, &req.canonical_message());
    req
}

// ─────────────────────────────────────────────────────────────────────────────
// Test server factory
// ─────────────────────────────────────────────────────────────────────────────

/// Spawn the full Nauti router on a random OS-assigned port.
/// Returns the base URL: `http://127.0.0.1:<port>`.
async fn start_server() -> String {
    let config = Config::default();
    let state = AppState::new(config).expect("AppState::new failed");

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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_health_over_tcp() {
    let base = start_server().await;
    let resp = reqwest::get(format!("{base}/health"))
        .await
        .expect("GET /health over TCP failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok", "health body must contain status:ok");
}

#[tokio::test]
async fn test_valid_register_200() {
    let base = start_server().await;
    let key = make_key();
    let req = valid_join(&key);

    let resp = reqwest::Client::new()
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .expect("POST failed");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "valid signed register must be 200; body={}",
        resp.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn test_tampered_register_401() {
    let base = start_server().await;
    let key = make_key();
    let mut req = valid_join(&key);

    // Mutate role after signing — signature now covers the original role
    req.role = "superadmin".into();

    let resp = reqwest::Client::new()
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .expect("POST failed");

    assert_eq!(
        resp.status().as_u16(),
        401,
        "tampered payload must return 401 Unauthorized"
    );
}

#[tokio::test]
async fn test_stale_register_403() {
    let base = start_server().await;
    let key = make_key();
    let mut req = valid_join(&key);

    // Backdate timestamp by 10 minutes
    req.timestamp_secs = now_secs() - 600;
    req.signature_hex = sign_message(&key, &req.canonical_message());

    let resp = reqwest::Client::new()
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .expect("POST failed");

    assert_eq!(
        resp.status().as_u16(),
        403,
        "stale request must return 403 Forbidden"
    );
}

#[tokio::test]
async fn test_replayed_nonce_409() {
    let base = start_server().await;
    let key = make_key();
    let req = valid_join(&key);
    let client = reqwest::Client::new();

    // First request — must succeed
    let r1 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .expect("first POST failed");
    assert_eq!(r1.status().as_u16(), 200, "first request must succeed");

    // Second request with identical nonce — must be rejected
    let r2 = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .expect("second POST failed");
    assert_eq!(
        r2.status().as_u16(),
        409,
        "replayed nonce must return 409 Conflict"
    );
}
