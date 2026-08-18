//! Chunk 8B: federation behavioral proof.
//!
//! PRODUCTION-TRUTH: every test here exercises real execution paths —
//! real TCP loopback servers (axum on OS-assigned ports), real reqwest
//! HTTP calls, real SQLite files, real identity files on disk with real
//! OS permission checks. No mocks, no synthetic telemetry, no simulated
//! federation peers. Values asserted are deterministic control-plane
//! state, never generated scientific measurements.

use nauti_nodes::{
    api::routes::router,
    lease::{LeaseError, LeaseManager, ReserveOutcome},
    node::coordinator_identity::CoordinatorIdentity,
    node::identity::NodeIdentity,
    node::registry::NodeRegistry,
    opsstore::OpsStore,
    state::{coordinator_identity_for, evaluate_node_with_fencing, AppState},
    types::{
        NodeCapabilities, NodeLifecycleState, NodeMetadata, ReconciledHardwareState,
        ReconciliationStatus, RegisteredHardwareClaim,
    },
    Config,
};
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

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

/// Deterministic eligible node metadata (control-plane state only; not
/// hardware telemetry).
fn eligible_meta(id: &str) -> NodeMetadata {
    NodeMetadata {
        id: id.into(),
        addr: "127.0.0.1:1".into(),
        inference_url: "http://127.0.0.1:5200".into(),
        role: "thinker".into(),
        gpu_name: "RTX 2060 SUPER".into(),
        online: true,
        public_key: vec![1; 32],
        capabilities: NodeCapabilities {
            max_batch: 1,
            models: vec!["thinker".into()],
            vram_mb: 16384,
            role: "thinker".into(),
        },
        claim: RegisteredHardwareClaim::default(),
        observed: None,
        reconciliation: ReconciledHardwareState {
            status: ReconciliationStatus::MATCH,
            fields: vec![],
        },
        last_hardware_probe: None,
        health_state: "ONLINE".into(),
        lifecycle_state: NodeLifecycleState::Online,
        ..Default::default()
    }
}

/// Spawn the full Nauti router on a random OS-assigned port (real TCP).
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

#[cfg(unix)]
fn file_mode(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Federation status endpoint over real HTTP
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn federation_status_reports_real_local_state() {
    let tmp = tempfile::tempdir().unwrap();
    let base = start_server(temp_config(tmp.path())).await;

    let resp = reqwest::get(format!("{base}/v1/federation/status"))
        .await
        .expect("real HTTP GET failed");
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();

    // Fresh in-memory identity: real UUID, Joining state, epoch 0.
    let cid = v["coordinator_id"]
        .as_str()
        .expect("coordinator_id present");
    assert!(
        uuid::Uuid::parse_str(cid).is_ok(),
        "coordinator_id is a UUID"
    );
    assert_eq!(v["federation_state"], "Joining");
    assert_eq!(v["local_role"], "JOINING");
    assert_eq!(v["authority_epoch"], 0);
    assert_eq!(v["lease_authority_epoch"], 0);
    assert_eq!(v["can_issue_mutations"], false);
    assert_eq!(v["is_coordinator"], true);
    assert!(v["peers"].as_array().unwrap().is_empty());
    assert!(v["fingerprint"].as_str().unwrap().starts_with("ed25519:"));
}

#[tokio::test]
async fn federation_status_seeds_configured_peers() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());
    config.federation_peers = vec!["http://10.9.9.9:8081".to_string()];
    let base = start_server(config).await;

    let resp = reqwest::get(format!("{base}/v1/federation/status"))
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    let peers = v["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0]["coordinator_id"], "http://10.9.9.9:8081");
    assert_eq!(peers[0]["endpoint"], "http://10.9.9.9:8081");
    assert_eq!(peers[0]["role"], "JOINING");
    assert_eq!(peers[0]["public_key_hex"], "");
    assert!(peers[0]["last_seen_ms"].is_null());
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Identity persistence contract (real files)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn coordinator_identity_persists_across_restart_real_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());
    config.coordinator_identity_path = Some(format!(
        "{}/coordinator-identity.json",
        tmp.path().display()
    ));

    let node_path = std::path::PathBuf::from(&config.identity_path);
    let coord_path = std::path::PathBuf::from(config.coordinator_identity_path.as_ref().unwrap());

    let s1 = AppState::new(config.clone()).unwrap();
    let id1 = s1.identity.clone().expect("identity loaded");
    let first_node_id = id1.node_identity.node_id.clone();
    drop(s1);

    // Real files must exist on disk.
    assert!(node_path.exists(), "durable node identity file written");
    assert!(coord_path.exists(), "coordinator identity file written");
    #[cfg(unix)]
    {
        assert_eq!(file_mode(&node_path), 0o600);
        assert_eq!(file_mode(&coord_path), 0o600);
    }

    // Restart: same identity recovered from disk.
    let s2 = AppState::new(config.clone()).unwrap();
    let id2 = s2.identity.clone().expect("identity loaded");
    assert_eq!(id2.node_identity.node_id, first_node_id);
    assert_eq!(
        id2.node_identity.public_key, id1.node_identity.public_key,
        "public key survives restart"
    );
}

#[tokio::test]
async fn in_memory_contract_writes_no_file_and_is_independent() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path()); // coordinator_identity_path = None

    let s1 = AppState::new(config.clone()).unwrap();
    let s2 = AppState::new(config.clone()).unwrap();
    let id1 = s1.identity.as_ref().unwrap();
    let id2 = s2.identity.as_ref().unwrap();
    assert_ne!(
        id1.node_identity.node_id, id2.node_identity.node_id,
        "without a configured path each start is an independent in-memory identity"
    );
    // The directory must contain only the SQLite db — no identity files.
    let entries: Vec<String> = std::fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        !entries.iter().any(|n| n.contains("identity")),
        "no identity file may be written in in-memory mode, found: {entries:?}"
    );
}

#[tokio::test]
async fn identity_continuity_and_split_brain_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());

    // Durable identity anchors the coordinator identity.
    let node = NodeIdentity::load_or_create(std::path::Path::new(&config.identity_path)).unwrap();

    // In-memory wrap preserves node_id.
    let cid = coordinator_identity_for(&config, &node).unwrap();
    assert_eq!(cid.node_identity.node_id, node.node_id);

    // Persisted wrap preserves node_id across reloads.
    config.coordinator_identity_path = Some(format!(
        "{}/coordinator-identity.json",
        tmp.path().display()
    ));
    let cid2 = coordinator_identity_for(&config, &node).unwrap();
    assert_eq!(cid2.node_identity.node_id, node.node_id);
    let cid3 = coordinator_identity_for(&config, &node).unwrap();
    assert_eq!(cid3.node_identity.node_id, node.node_id);

    // Split-brain: a DIFFERENT durable identity must be refused (fail closed).
    let other = NodeIdentity::generate();
    let err = coordinator_identity_for(&config, &other).unwrap_err();
    assert!(
        err.to_string()
            .contains("does not wrap durable node identity"),
        "identity split must fail closed, got: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Epoch sync from persisted identity into lease authority (real SQLite)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lease_authority_epoch_syncs_from_persisted_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());
    config.coordinator_identity_path = Some(format!(
        "{}/coordinator-identity.json",
        tmp.path().display()
    ));
    let coord_path = std::path::PathBuf::from(config.coordinator_identity_path.as_ref().unwrap());

    // First start: epoch 0 everywhere.
    let s1 = AppState::new(config.clone()).unwrap();
    assert_eq!(s1.leases.current_authority_epoch(), 0);
    drop(s1);

    // A real federation join persisted to disk advances the epoch.
    let mut cid: CoordinatorIdentity =
        serde_json::from_str(&std::fs::read_to_string(&coord_path).unwrap()).unwrap();
    cid.join_federation(vec!["peer-public-key-hex".to_string()]);
    std::fs::write(&coord_path, serde_json::to_string_pretty(&cid).unwrap()).unwrap();

    // Restart: the lease authority must carry the persisted epoch.
    let s2 = AppState::new(config.clone()).unwrap();
    assert_eq!(s2.leases.current_authority_epoch(), 1);
    let id = s2.identity.as_ref().unwrap();
    assert_eq!(id.authority_epoch.value(), 1);
    assert!(
        id.can_issue_mutations(),
        "Active member with epoch>0 may mutate"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Lease overlay epoch stamping + eligibility fencing (real SQLite)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn lease_overlays_stamped_with_live_authority_epoch_and_fenced() {
    let store = Arc::new(OpsStore::open("sqlite::memory:").unwrap());
    let leases = LeaseManager::new(store.clone());
    let registry = NodeRegistry::new();

    registry.register_node(eligible_meta("node-fence"));
    registry.register_node(eligible_meta("node-free"));

    leases.set_authority_epoch(7);

    // Drive the real lease state machine to LEASED.
    match leases
        .begin_reserve("mission-fence", "node-fence", None, None, 8192, 10, true)
        .unwrap()
    {
        ReserveOutcome::Created(_) => {}
        _ => panic!("expected created"),
    }
    let rec = leases
        .lease_by_mission_node("mission-fence", "node-fence")
        .unwrap();
    leases.mark_scheduler_excluded(&rec.lease_id).unwrap();
    leases.mark_runtime_exited(&rec.lease_id).unwrap();
    leases.mark_gpu_released(&rec.lease_id).unwrap();
    leases.grant(&rec.lease_id).unwrap();
    assert!(leases.apply_overlay_to_node(&registry, "node-fence"));

    // The overlay must carry the live authority epoch, not a hardcoded 0.
    let rt = registry.get_node("node-fence").unwrap();
    let lease_epoch = rt
        .metadata
        .read()
        .unwrap()
        .active_lease
        .as_ref()
        .expect("overlay applied")
        .epoch;
    assert_eq!(lease_epoch, 7, "overlay stamped with live authority epoch");

    // Fencing: a newer local epoch (9) exposes the lease as granted by a
    // stale coordinator -> explicit fencing failure.
    let err = evaluate_node_with_fencing(&rt, 9).unwrap_err();
    assert!(
        err.to_string().contains("stale epoch"),
        "stale-epoch lease must fail with a fencing error, got: {err}"
    );

    // Current epoch (7): fencing passes, standard eligibility still fails
    // closed because the GPU is Forge-leased.
    let err = evaluate_node_with_fencing(&rt, 7).unwrap_err();
    assert!(
        err.to_string().contains("node ineligible"),
        "current-epoch leased node fails standard eligibility, got: {err}"
    );

    // An unleased eligible node passes fencing and eligibility.
    let rt_free = registry.get_node("node-free").unwrap();
    assert!(evaluate_node_with_fencing(&rt_free, 7).is_ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Fenced reserve gate: stale coordinators cannot create leases
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fenced_reserve_rejects_stale_authority_and_journals_it() {
    let store = Arc::new(OpsStore::open("sqlite::memory:").unwrap());
    let leases = LeaseManager::new(store.clone());
    leases.set_authority_epoch(7);

    // Stale epoch 5 < 7: rejected before any lease state is created.
    let err = leases
        .begin_reserve_fenced(5, "mission-old", "node-a", None, None, 1, 1, true)
        .unwrap_err();
    match err {
        LeaseError::StaleEpoch { provided, current } => {
            assert_eq!(provided, 5);
            assert_eq!(current, 7);
        }
        other => panic!("expected StaleEpoch, got: {other:?}"),
    }
    assert!(
        leases
            .lease_by_mission_node("mission-old", "node-a")
            .is_none(),
        "fenced request must not create lease state"
    );

    // The fenced attempt is journaled in the real ops event store.
    let events = store
        .events(Some("node-a"), None, None, Some("LEASE_CONFLICT"), 10)
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.detail.contains("fenced stale-epoch")),
        "fenced attempt must be journaled"
    );

    // Current epoch proceeds through normal reserve rules.
    assert!(matches!(
        leases
            .begin_reserve_fenced(7, "mission-cur", "node-a", None, None, 1, 1, true)
            .unwrap(),
        ReserveOutcome::Created(_)
    ));
    // Newer epoch (we are behind the requester) is also accepted.
    assert!(matches!(
        leases
            .begin_reserve_fenced(9, "mission-new", "node-b", None, None, 1, 1, true)
            .unwrap(),
        ReserveOutcome::Created(_)
    ));

    // Monotonic floor: a stale writer cannot lower the fencing bar.
    leases.set_authority_epoch(2);
    assert_eq!(leases.current_authority_epoch(), 7);
}
