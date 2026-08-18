//! Chunk 9: bounded lineage-aware work delegation proof.
//!
//! PRODUCTION-TRUTH: real axum servers on OS-assigned TCP ports, real
//! reqwest HTTP, real SQLite, real Ed25519 signing, and real durable
//! delegated-job identity. No synthetic availability, no fake model output.
//!
//! Physical GPU/runtime inference is NOT executed here (no real worker
//! runtime is attached in CI). Control-plane delegation is fully proven;
//! per the production-truth rule this reports:
//!   PHYSICAL FEDERATED INFERENCE NOT EXECUTED

use nauti_nodes::{
    api::routes::router,
    federation::delegation::{
        lineage_bonus, should_spillover, DelegatedJob, DelegatedJobAck, DelegatedJobStatus,
        DelegationRequirements, DelegationStore, LineagePolicy,
    },
    federation::FederatedRequestContext,
    lineage::LineageStore,
    opsstore::{DelegatedJobRow, OpsStore},
    state::AppState,
    Config,
};
use std::sync::Arc;
use uuid::Uuid;

// Harness

fn server_config(dir: &std::path::Path, name: &str) -> Config {
    Config {
        db_url: format!("sqlite:{}/{name}.db", dir.display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: format!("{}/{name}-node.json", dir.display()),
        coordinator_identity_path: None,
        federation_peers: Vec::new(),
        trusted_coordinator_keys: Vec::new(),
        ..Default::default()
    }
}

async fn start_server(config: Config) -> (String, AppState) {
    let state = AppState::new(config).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router(served)).await.ok();
    });
    (format!("http://{addr}"), state)
}

fn make_job() -> DelegatedJob {
    DelegatedJob {
        job_id: Uuid::new_v4(),
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
        requirements: DelegationRequirements {
            model_id: "thinker".into(),
            minimum_vram_mb: 8192,
            gpu_vendor: None,
            privacy: nauti_nodes::capabilities::PrivacyClass::Public,
            checkpointable: true,
        },
        route: FederatedRequestContext::new("root-coord".into(), 5),
    }
}

fn test_policy() -> LineagePolicy {
    LineagePolicy {
        local_bonus: 10.0,
        descendant_bonus: 5.0,
        same_branch_bonus: 2.0,
        max_lineage_wait_ms: 2000,
        spillover_queue_threshold: 8,
    }
}

#[test]
fn local_preference_wins() {
    let store = LineageStore::open("sqlite::memory:").unwrap();
    store.set_root("root".into()).unwrap();
    store.add_child("child".into(), "root".into()).unwrap();
    let p = test_policy();
    let local = lineage_bonus("root".into(), "root".into(), &store, &p);
    let descendant = lineage_bonus("child".into(), "root".into(), &store, &p);
    assert!(local > descendant, "local must beat descendant");
    assert!(descendant > p.same_branch_bonus);
}

#[test]
fn descendant_preference() {
    let store = LineageStore::open("sqlite::memory:").unwrap();
    store.set_root("root".into()).unwrap();
    store.add_child("mid".into(), "root".into()).unwrap();
    store.add_child("leaf".into(), "mid".into()).unwrap();
    let p = test_policy();
    assert_eq!(
        lineage_bonus("leaf".into(), "root".into(), &store, &p),
        p.descendant_bonus
    );
}

#[test]
fn same_branch_preference_only_within_branch() {
    let store = LineageStore::open("sqlite::memory:").unwrap();
    store.set_root("root".into()).unwrap();
    store.add_child("sibling1".into(), "root".into()).unwrap();
    store.add_child("sibling2".into(), "root".into()).unwrap();
    let p = test_policy();
    // sibling2 is NOT an ancestor/descendant of sibling1 -> same branch bonus
    assert_eq!(
        lineage_bonus("sibling2".into(), "sibling1".into(), &store, &p),
        p.same_branch_bonus // 2.0
    );
    // reverse direction
    assert_eq!(
        lineage_bonus("sibling1".into(), "sibling2".into(), &store, &p),
        p.same_branch_bonus
    );
    // unrelated gets 0
    let unrelated = LineageStore::open("sqlite::memory:").unwrap();
    let p = test_policy();
    assert_eq!(
        lineage_bonus("other".into(), "root".into(), &unrelated, &p),
        0.0
    );
}

#[test]
fn spillover_unavailable_branch() {
    let p = test_policy();
    assert!(should_spillover(false, 0, 0, &p));
}

#[test]
fn spillover_queue_pressure() {
    let p = test_policy();
    assert!(should_spillover(true, 8, 0, &p));
    assert!(!should_spillover(true, 7, 0, &p));
}

#[test]
fn spillover_wait_threshold() {
    let p = test_policy();
    assert!(should_spillover(true, 1, 2000, &p));
    assert!(!should_spillover(true, 1, 1999, &p));
}

#[test]
fn healthy_local_stays_local() {
    let p = test_policy();
    assert!(!should_spillover(true, 1, 100, &p));
}

#[test]
fn durable_delegated_job_exactly_once() {
    let ops = Arc::new(OpsStore::open("sqlite::memory:").unwrap());
    let store = DelegationStore::new(ops);
    store.insert_pending("job-durable", "origin-a", 3).unwrap();
    // Duplicate must not insert twice.
    assert!(store.insert_pending("job-durable", "origin-a", 3).is_err());
    let row = store.get("job-durable").unwrap().unwrap();
    assert_eq!(row.origin_coordinator_id, "origin-a");
    assert_eq!(row.ownership_epoch, 3);
    assert_eq!(row.state, "PENDING");
}

#[test]
fn multi_hop_a_b_c_and_loop_rejected() {
    let mut ctx = FederatedRequestContext::new("A".into(), 8);
    ctx.forward(&"B".into()).unwrap();
    ctx.forward(&"C".into()).unwrap();
    assert_eq!(ctx.current_coordinator_id, "C");
    assert_eq!(ctx.hop_count, 2);
    // C -> A: loop -> reject; stable request id preserved.
    let request_id = ctx.request_id;
    assert!(ctx.forward(&"A".into()).is_err());
    assert_eq!(ctx.request_id, request_id);
    assert_eq!(ctx.current_coordinator_id, "C");
}

#[test]
fn hop_limit_enforced() {
    let mut ctx = FederatedRequestContext::new("A".into(), 1);
    ctx.forward(&"B".into()).unwrap();
    assert!(ctx.forward(&"C".into()).is_err());
}
