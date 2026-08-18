//! Chunk 10: execution provider integration tests.

use nauti_nodes::{
    api::routes::router,
    providers::{
        select_provider, ExecutionProvider, LocalWorkerProvider, ProviderPolicy, ProviderRegistry,
    },
    state::AppState,
    Config,
};
use std::sync::Arc;

fn temp_config(dir: &std::path::Path) -> Config {
    Config {
        db_url: format!("sqlite:{}/ops.db", dir.display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: format!("{}/node-identity.json", dir.display()),
        coordinator_identity_path: None,
        federation_peers: Vec::new(),
        trusted_coordinator_keys: Vec::new(),
        ..Default::default()
    }
}

async fn start_server(config: Config) -> String {
    let state = AppState::new(config).expect("AppState::new failed");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.ok();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn providers_registry_empty_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = AppState::new(config).expect("AppState::new failed");
    assert!(state.providers.is_empty());
    assert_eq!(state.providers.len(), 0);
}

#[tokio::test]
async fn providers_endpoint_returns_empty_list() {
    let tmp = tempfile::tempdir().unwrap();
    let base = start_server(temp_config(tmp.path())).await;
    let resp = reqwest::get(format!("{base}/v1/providers"))
        .await
        .expect("real HTTP GET failed");
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v["providers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn local_provider_health_no_nodes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let provider = LocalWorkerProvider::new("local", state);
    let health = provider.health().await.unwrap();
    assert!(!health.healthy);
    assert!(!health.available);
    assert!(health.reason.is_some());
}

#[tokio::test]
async fn local_provider_capabilities_empty_when_no_nodes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let provider = LocalWorkerProvider::new("local", state);
    let caps = provider.capabilities().await.unwrap();
    assert!(caps.models.is_empty());
    assert!(caps.gpu_vendors.is_empty());
    assert_eq!(caps.kind, nauti_nodes::providers::ProviderKind::LocalWorker);
}

#[tokio::test]
async fn provider_registry_register_and_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let provider = Arc::new(LocalWorkerProvider::new("local", state));
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    assert_eq!(registry.len(), 1);
    assert!(!registry.is_empty());
    let fetched = registry.get("local").unwrap();
    assert_eq!(fetched.id(), "local");
    assert_eq!(
        fetched.kind(),
        nauti_nodes::providers::ProviderKind::LocalWorker
    );
}

#[tokio::test]
async fn select_provider_returns_none_when_no_providers() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let _state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let registry = ProviderRegistry::new();
    let req = nauti_nodes::providers::ProviderRequest {
        job_id: uuid::Uuid::new_v4(),
        model_id: "thinker".into(),
        privacy: nauti_nodes::capabilities::PrivacyClass::Public,
        minimum_vram_mb: 8192,
        gpu_vendor: None,
        checkpointable: true,
        request: nauti_nodes::api::handlers::InferenceRequest {
            model: Some("thinker".into()),
            messages: vec![],
            stream: false,
            prefer_role: None,
            avoid_p100: true,
            max_tokens: None,
            temperature: None,
            privacy_class: None,
        },
    };
    let policy = ProviderPolicy::default();
    let selected = select_provider(&registry, &req, &policy).await.unwrap();
    assert!(selected.is_none());
}

#[tokio::test]
async fn provider_status_unknown_job_returns_failed() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let provider = LocalWorkerProvider::new("local", state);
    let status = provider.status(uuid::Uuid::new_v4()).await.unwrap();
    assert_eq!(
        status.state,
        nauti_nodes::providers::ProviderJobState::Failed
    );
    assert_eq!(status.detail.as_deref(), Some("job not found"));
}

#[tokio::test]
async fn provider_kind_serde_roundtrip() {
    let kind = nauti_nodes::providers::ProviderKind::LocalWorker;
    let s = serde_json::to_string(&kind).unwrap();
    assert_eq!(s, "\"LocalWorker\"");
    let kind2 = nauti_nodes::providers::ProviderKind::FederatedWorker;
    let s2 = serde_json::to_string(&kind2).unwrap();
    assert_eq!(s2, "\"FederatedWorker\"");
    let kind3: nauti_nodes::providers::ProviderKind =
        serde_json::from_str("\"RemoteInferenceApi\"").unwrap();
    assert_eq!(
        kind3,
        nauti_nodes::providers::ProviderKind::RemoteInferenceApi
    );
}

#[tokio::test]
async fn provider_job_state_display() {
    let state = nauti_nodes::providers::ProviderJobState::Accepted;
    assert_eq!(state.to_string(), "ACCEPTED");
    let state2 = nauti_nodes::providers::ProviderJobState::Running;
    assert_eq!(state2.to_string(), "RUNNING");
    let state3 = nauti_nodes::providers::ProviderJobState::Cancelled;
    assert_eq!(state3.to_string(), "CANCELLED");
}

#[tokio::test]
async fn provider_policy_rejects_model_not_in_capabilities() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    let provider = LocalWorkerProvider::new("local", state);
    let caps = provider.capabilities().await.unwrap();
    let req = nauti_nodes::providers::ProviderRequest {
        job_id: uuid::Uuid::new_v4(),
        model_id: "any".into(),
        privacy: nauti_nodes::providers::PrivacyClass::Public,
        minimum_vram_mb: 0,
        gpu_vendor: None,
        checkpointable: true,
        request: nauti_nodes::api::handlers::InferenceRequest {
            model: None,
            messages: vec![],
            stream: false,
            prefer_role: None,
            avoid_p100: true,
            max_tokens: None,
            temperature: None,
            privacy_class: None,
        },
    };
    let policy = ProviderPolicy::default();
    // No nodes => no models => model mismatch correctly rejected.
    assert!(!nauti_nodes::providers::provider_is_allowed(
        &caps, &req, &policy
    ));
}
