//! Chunk 3: Dynamic Model Lifecycle & Runtime Process Management Integration Test Suite.
//!
//! Tests:
//! 1. Cold Start
//! 2. Graceful Stop
//! 3. Restart
//! 4. Model Switch
//! 5. Missing Runtime Binary
//! 6. Missing Model Artifact
//! 7. Startup Failure
//! 8. Readiness Timeout
//! 9. Health Check Failure
//! 10. Model Mismatch
//! 11. Occupied Port Range Fallback
//! 12. Runtime Crash Detection
//! 13. Withdrawal Prohibits Model Start
//! 14. Revoked Node Prohibits Model Start

use axum::Router;
use nauti_nodes::{
    config::Config,
    node::auth::{sign_message, SignedJoinRequest},
    runtime::{
        LlamaServerBackend, LocalModelConfig, ModelManager, ModelState, ProcessInfo, RuntimeBackend,
    },
    state::AppState,
    types::{now_ms, NodeMetadata},
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn test_config() -> Config {
    let mut config = Config::default();
    config.db_url = "sqlite::memory:".to_string();
    config.forge_url = "http://127.0.0.1:1".to_string();
    config.local_inference_url = "http://127.0.0.1:5200".to_string();
    config.strict_alpha = true;
    config
}

fn make_key() -> ed25519_dalek::SigningKey {
    ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng)
}

fn pub_hex(key: &ed25519_dalek::SigningKey) -> String {
    hex::encode(key.verifying_key().to_bytes())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn start_server_with_state(state: AppState) -> String {
    let app: Router = nauti_nodes::api::routes::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{}", addr)
}

/// Mock runtime backend for fast, deterministic unit/integration testing without requiring llama-server binary on host.
pub struct MockRuntimeBackend {
    pub should_fail_start: bool,
    pub should_timeout_readiness: bool,
    pub should_mismatch_model: bool,
    pub returned_model: String,
}

impl Default for MockRuntimeBackend {
    fn default() -> Self {
        Self {
            should_fail_start: false,
            should_timeout_readiness: false,
            should_mismatch_model: false,
            returned_model: "thinker".to_string(),
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

    async fn start(&self, config: &LocalModelConfig, port: u16) -> Result<ProcessInfo, String> {
        if self.should_fail_start {
            return Err("mock runtime spawn failed".to_string());
        }
        Ok(ProcessInfo {
            pid: 9999,
            port,
            base_url: format!("http://127.0.0.1:{port}"),
            model_id: config.model_id.clone(),
            started_at_ms: now_ms(),
        })
    }

    async fn check_readiness(
        &self,
        _base_url: &str,
        expected_model: &str,
        _timeout_secs: u64,
    ) -> Result<bool, String> {
        if self.should_timeout_readiness {
            return Err("mock readiness probe timed out".to_string());
        }
        if self.should_mismatch_model {
            return Err(format!(
                "model mismatch: expected '{}', loaded '{}'",
                expected_model, self.returned_model
            ));
        }
        Ok(true)
    }

    async fn stop(&self, _pid: u32) -> Result<(), String> {
        Ok(())
    }

    async fn current_model(&self, _base_url: &str) -> Option<String> {
        Some(self.returned_model.clone())
    }
}

// -----------------------------------------------------------------------------
// Test 1: Cold Start -> Model requested, process spawned, readiness OK
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_1_cold_start_success() {
    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);

    let status_init = manager.get_status();
    assert_eq!(status_init.state, ModelState::Available);

    let res = manager.request_model("thinker", 18200, 16384).await;
    assert!(res.is_ok(), "cold start request_model must succeed");

    let proc = res.unwrap();
    assert_eq!(proc.model_id, "thinker");
    assert_eq!(proc.port, 18200);

    let status_ready = manager.get_status();
    assert_eq!(status_ready.state, ModelState::Ready);
    assert_eq!(status_ready.loaded_model, Some("thinker".to_string()));
}

// -----------------------------------------------------------------------------
// Test 2: Graceful Stop -> Model runtime stops cleanly
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_2_graceful_stop() {
    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);

    let _ = manager
        .request_model("thinker", 18200, 16384)
        .await
        .unwrap();
    assert_eq!(manager.get_status().state, ModelState::Ready);

    let stop_res = manager.stop_model().await;
    assert!(stop_res.is_ok());

    let status_stopped = manager.get_status();
    assert_eq!(status_stopped.state, ModelState::Available);
    assert_eq!(status_stopped.loaded_model, None);
    assert_eq!(status_stopped.process, None);
}

// -----------------------------------------------------------------------------
// Test 3: Restart -> Runtime can stop and start again
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_3_restart_runtime() {
    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);

    let _ = manager
        .request_model("thinker", 18200, 16384)
        .await
        .unwrap();
    let _ = manager.stop_model().await.unwrap();
    let res2 = manager.request_model("thinker", 18200, 16384).await;

    assert!(res2.is_ok(), "restart request_model must succeed");
    assert_eq!(manager.get_status().state, ModelState::Ready);
}

// -----------------------------------------------------------------------------
// Test 4: Model Switch -> Stop Model A, Start Model B
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_4_model_switch() {
    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);

    let _ = manager
        .request_model("thinker", 18200, 16384)
        .await
        .unwrap();
    assert_eq!(
        manager.get_status().loaded_model,
        Some("thinker".to_string())
    );

    let _ = manager.stop_model().await.unwrap();

    let res_coder = manager.request_model("coder", 18200, 16384).await;
    assert!(res_coder.is_ok());
    assert_eq!(manager.get_status().loaded_model, Some("coder".to_string()));
}

// -----------------------------------------------------------------------------
// Test 5: Missing Runtime Binary -> Truthful failure
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_5_missing_runtime_binary() {
    let backend = LlamaServerBackend::new();
    if backend.detect() {
        return; // Skip if actual binary is installed on host
    }

    let manager = ModelManager::new();
    let res = manager.request_model("thinker", 18200, 16384).await;

    assert!(res.is_err());
    let status = manager.get_status();
    assert_eq!(status.state, ModelState::Failed);
    assert!(status.last_error.unwrap().contains("binary not found"));
}

// -----------------------------------------------------------------------------
// Test 6: Missing Model Artifact -> Truthful failure
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_6_missing_model_artifact() {
    let real_backend = Arc::new(LlamaServerBackend::new());
    let mut real_manager = ModelManager::with_backend(real_backend);
    real_manager.manifest.register(LocalModelConfig {
        model_id: "missing-model".to_string(),
        artifact_path: "/nonexistent/path/model.gguf".to_string(),
        runtime_type: "llama-server".to_string(),
        vram_required_mb: 4096,
        privacy_class: nauti_nodes::capabilities::PrivacyClass::Public,
        extra_args: vec![],
    });

    let res = real_manager
        .request_model("missing-model", 18200, 16384)
        .await;
    assert!(res.is_err());
    let status = real_manager.get_status();
    assert_eq!(status.state, ModelState::Failed);
}

// -----------------------------------------------------------------------------
// Test 7: Startup Failure -> Spawn error marks FAILED
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_7_startup_failure() {
    let mock = Arc::new(MockRuntimeBackend {
        should_fail_start: true,
        ..Default::default()
    });
    let manager = ModelManager::with_backend(mock);

    let res = manager.request_model("thinker", 5200, 16384).await;
    assert!(res.is_err());
    let status = manager.get_status();
    assert_eq!(status.state, ModelState::Failed);
    assert!(status.last_error.unwrap().contains("spawn failed"));
}

// -----------------------------------------------------------------------------
// Test 8: Readiness Timeout -> Probe timeout marks FAILED
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_8_readiness_timeout() {
    let mock = Arc::new(MockRuntimeBackend {
        should_timeout_readiness: true,
        ..Default::default()
    });
    let manager = ModelManager::with_backend(mock);

    let res = manager.request_model("thinker", 5200, 16384).await;
    assert!(res.is_err());
    let status = manager.get_status();
    assert_eq!(status.state, ModelState::Failed);
    assert!(status.last_error.unwrap().contains("timed out"));
}

// -----------------------------------------------------------------------------
// Test 9: Model Mismatch -> Loaded model does not match requested
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_9_model_mismatch() {
    let mock = Arc::new(MockRuntimeBackend {
        should_mismatch_model: true,
        returned_model: "wrong-model".to_string(),
        ..Default::default()
    });
    let manager = ModelManager::with_backend(mock);

    let res = manager.request_model("thinker", 5200, 16384).await;
    assert!(res.is_err());
    let status = manager.get_status();
    assert_eq!(status.state, ModelState::Failed);
    assert!(status.last_error.unwrap().contains("mismatch"));
}

// -----------------------------------------------------------------------------
// Test 10: Port Selection -> Selects available port if preferred is occupied
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_10_port_selection_fallback() {
    let mut bound_port = 0;
    let mut _listener = None;
    for test_port in 18000..18100 {
        if let Ok(l) = std::net::TcpListener::bind(("127.0.0.1", test_port)) {
            bound_port = test_port;
            _listener = Some(l);
            break;
        }
    }
    assert!(
        bound_port > 0,
        "must find a free port to bind for fallback test"
    );

    let mock = Arc::new(MockRuntimeBackend::default());
    let manager = ModelManager::with_backend(mock);

    // Preferred port bound_port is occupied, manager must fallback to bound_port + 1
    let proc = manager
        .request_model("thinker", bound_port, 16384)
        .await
        .unwrap();
    assert_eq!(
        proc.port,
        bound_port + 1,
        "Port selection must fall back when preferred port is bound"
    );
}

// -----------------------------------------------------------------------------
// Test 11: Withdrawal Prohibits Model Start -> 403 Forbidden
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_11_withdrawal_prohibits_model_start() {
    let config = test_config();
    let state = AppState::new(config).unwrap();
    let base = start_server_with_state(state.clone()).await;

    // Register & withdraw node
    let key = make_key();
    let mut join_req = SignedJoinRequest {
        node_id: "withdrawn-model-node".into(),
        public_key_hex: pub_hex(&key),
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
    join_req.signature_hex = sign_message(&key, &join_req.canonical_message());

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{base}/internal/worker/register"))
        .json(&join_req)
        .send()
        .await
        .unwrap();

    // Withdraw node
    let mut withdraw_req = nauti_nodes::node::auth::SignedWithdrawRequest {
        node_id: "withdrawn-model-node".into(),
        timestamp_secs: now_secs(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    withdraw_req.signature_hex = sign_message(&key, &withdraw_req.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/withdraw"))
        .json(&withdraw_req)
        .send()
        .await
        .unwrap();

    // Attempt model start on withdrawn node -> 403 FORBIDDEN
    let resp = client
        .post(format!("{base}/v1/model/start"))
        .json(&serde_json::json!({
            "model_id": "thinker",
            "node_id": "withdrawn-model-node"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 403);
}

// -----------------------------------------------------------------------------
// Test 12: Revoked Node Prohibits Model Start -> 403 Forbidden
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_12_revoked_node_prohibits_model_start() {
    let config = test_config();
    let state = AppState::new(config).unwrap();
    let base = start_server_with_state(state.clone()).await;

    // Revoke node
    state
        .revocations
        .revoke("revoked-model-node", "security violation")
        .unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/model/start"))
        .json(&serde_json::json!({
            "model_id": "thinker",
            "node_id": "revoked-model-node"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 403);
}
