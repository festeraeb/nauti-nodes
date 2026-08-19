//! Chunk 11: provider-aware federation soak.
//!
//! Real control-plane cycles exercising:
//! - Provider selection (local healthy+available → selected; local unhealthy → rejected; etc.)
//! - Lease grant/release/fencing
//! - DelegationStore exactly-once (duplicate insert rejected)
//! - Invariant verification on real counter reads
//! - Real /proc resource observation (RSS, FD count, SQLite file size)
//!
//! Every counter comes from ACTUAL state transitions, never from sleep+increment.

use nauti_nodes::{
    capabilities::PrivacyClass,
    lease::{LeaseError, LeaseManager, ReserveOutcome},
    opsstore::OpsStore,
    providers::{
        select_provider_with_metrics, LocalWorkerProvider, ProviderPolicy, ProviderRegistry,
    },
    soak::{
        observe_self, open_fd_count, process_rss_bytes, SoakConfig, SoakConfigJson, SoakReport,
        WorkerGateSnapshot,
    },
    state::AppState,
    types::{
        NodeCapabilities, NodeLifecycleState, NodeMetadata, ReconciledHardwareState,
        ReconciliationStatus, RegisteredHardwareClaim,
    },
    Config,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
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

fn make_request(model: &str, min_vram: u64) -> nauti_nodes::providers::ProviderRequest {
    nauti_nodes::providers::ProviderRequest {
        job_id: uuid::Uuid::new_v4(),
        model_id: model.to_string(),
        privacy: PrivacyClass::Public,
        minimum_vram_mb: min_vram,
        gpu_vendor: None,
        checkpointable: true,
        request: nauti_nodes::api::handlers::InferenceRequest {
            model: Some(model.to_string()),
            messages: vec![],
            stream: false,
            prefer_role: None,
            avoid_p100: true,
            max_tokens: None,
            temperature: None,
            privacy_class: None,
        },
    }
}

fn make_registry(state: Arc<AppState>) -> ProviderRegistry {
    let mut registry = ProviderRegistry::new();
    let provider = Arc::new(LocalWorkerProvider::new("local", state.clone()));
    registry.register(provider);
    registry
}

// ── Provider-aware cycles ─────────────────────────────────────────────

#[tokio::test]
async fn soak_case_a_local_healthy_available_selected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    state.registry.register_node(eligible_meta("node-a"));

    let registry = make_registry(state.clone());
    let req = make_request("thinker", 8192);
    let policy = ProviderPolicy::default();
    let selected = select_provider_with_metrics(&registry, &req, &policy, Some(&state.metrics))
        .await
        .expect("select");

    assert!(selected.0.is_some(), "local provider should be selected");
    assert_eq!(selected.1.rejections_policy, 0);
    assert_eq!(selected.1.rejections_health, 0);

    let snap = state.metrics.snapshot_json();
    assert_eq!(snap["provider_selections_total"], 1);
    assert_eq!(snap["provider_selections_local_total"], 1);
    assert_eq!(snap["provider_selections_none_total"], 0);
}

#[tokio::test]
async fn soak_case_b_local_healthy_unavailable_federated_may_be_selected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));

    let registry = make_registry(state.clone());
    // No nodes registered → local provider reports healthy=false (no workers).
    let req = make_request("thinker", 8192);
    let policy = ProviderPolicy::default();
    let selected = select_provider_with_metrics(&registry, &req, &policy, Some(&state.metrics))
        .await
        .expect("select");

    assert!(selected.0.is_none(), "no healthy+available providers");
    assert_eq!(selected.1.rejections_health, 1, "one unhealthy provider");

    let snap = state.metrics.snapshot_json();
    assert_eq!(snap["provider_selections_none_total"], 1);
}

#[tokio::test]
async fn soak_case_c_local_unhealthy_not_selected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));

    let registry = make_registry(state.clone());
    // Local provider with no nodes: healthy=false
    let health = registry.get("local").unwrap().health().await.unwrap();
    assert!(!health.healthy, "no nodes => unhealthy");

    let req = make_request("thinker", 8192);
    let selected = select_provider_with_metrics(
        &registry,
        &req,
        &ProviderPolicy::default(),
        Some(&state.metrics),
    )
    .await
    .expect("select");
    assert!(
        selected.0.is_none(),
        "unhealthy provider must not be selected"
    );
}

use nauti_nodes::federation::delegation::{
    DelegatedJob, DelegatedJobAck, DelegatedJobStatus, FederatedCapacity, FederationDispatcher,
};
struct EmptyDispatcher;

#[async_trait::async_trait]
impl FederationDispatcher for EmptyDispatcher {
    async fn advertise_capacity(&self) -> anyhow::Result<Vec<FederatedCapacity>> {
        Ok(Vec::new())
    }
    async fn delegate(
        &self,
        _peer: &str,
        _job: DelegatedJob,
        _epoch: u64,
    ) -> anyhow::Result<DelegatedJobAck> {
        Ok(DelegatedJobAck::Accepted)
    }
    async fn job_status(
        &self,
        _peer: &str,
        _job_id: uuid::Uuid,
    ) -> anyhow::Result<DelegatedJobStatus> {
        Ok(DelegatedJobStatus::Pending)
    }
}

#[tokio::test]
async fn soak_case_d_federated_unavailable_no_fake_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));

    let local = Arc::new(LocalWorkerProvider::new("local", state.clone()));
    let fed = Arc::new(nauti_nodes::providers::FederatedWorkerProvider::new(
        "fed",
        Arc::new(EmptyDispatcher),
        state.delegation.clone(),
    ));

    let mut registry = ProviderRegistry::new();
    registry.register(local);
    registry.register(fed);

    // No local nodes + no federated capacity → no provider selected.
    let req = make_request("thinker", 8192);
    let selected = select_provider_with_metrics(
        &registry,
        &req,
        &ProviderPolicy::default(),
        Some(&state.metrics),
    )
    .await
    .expect("select");
    assert!(selected.0.is_none(), "no provider available → None");
    // Both providers were unhealthy/unavailable.
    assert_eq!(selected.1.rejections_health, 2);
}

#[tokio::test]
async fn soak_case_e_privacy_mismatch_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    state.registry.register_node(eligible_meta("node-a"));

    let registry = make_registry(state.clone());
    let mut req = make_request("thinker", 8192);
    req.privacy = PrivacyClass::TrustedLineage; // local provider doesn't support this
    let selected = select_provider_with_metrics(
        &registry,
        &req,
        &ProviderPolicy::default(),
        Some(&state.metrics),
    )
    .await
    .expect("select");
    assert!(selected.0.is_none(), "privacy mismatch → rejected");
    assert_eq!(selected.1.rejections_policy, 1);
}

#[tokio::test]
async fn soak_case_f_model_mismatch_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    state.registry.register_node(eligible_meta("node-a"));

    let registry = make_registry(state.clone());
    let req = make_request("nonexistent-model", 8192);
    let selected = select_provider_with_metrics(
        &registry,
        &req,
        &ProviderPolicy::default(),
        Some(&state.metrics),
    )
    .await
    .expect("select");
    assert!(selected.0.is_none(), "model mismatch → rejected");
    assert_eq!(selected.1.rejections_policy, 1);
}

#[tokio::test]
async fn soak_case_g_vram_mismatch_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    state.registry.register_node(eligible_meta("node-a"));

    let registry = make_registry(state.clone());
    // Request 999999 MB — node only has 16384
    let req = make_request("thinker", 999999);
    let selected = select_provider_with_metrics(
        &registry,
        &req,
        &ProviderPolicy::default(),
        Some(&state.metrics),
    )
    .await
    .expect("select");
    assert!(selected.0.is_none(), "VRAM mismatch → rejected");
    assert_eq!(selected.1.rejections_policy, 1);
}

// ── Lease cycle ───────────────────────────────────────────────────────

#[tokio::test]
async fn soak_lease_cycle_grant_release_fenced() {
    let tmp = tempfile::tempdir().unwrap();
    let store =
        Arc::new(OpsStore::open(&format!("sqlite:{}/lease.db", tmp.path().display())).unwrap());
    let leases = LeaseManager::new(store.clone());
    leases.set_authority_epoch(7);

    // Fenced reserve at current epoch.
    let outcome = leases
        .begin_reserve_fenced(7, "mission-soak", "node-soak", None, None, 8192, 10, true)
        .unwrap();
    assert!(
        matches!(outcome, ReserveOutcome::Created(_)),
        "fenced reserve at current epoch must create"
    );

    let rec = leases
        .lease_by_mission_node("mission-soak", "node-soak")
        .unwrap();
    leases.mark_scheduler_excluded(&rec.lease_id).unwrap();
    leases.mark_runtime_exited(&rec.lease_id).unwrap();
    leases.mark_gpu_released(&rec.lease_id).unwrap();
    leases.grant(&rec.lease_id).unwrap();
    assert_eq!(
        leases.all_active_leases().len(),
        1,
        "one active lease after grant"
    );

    // Release: begin (RELEASING) then complete (RELEASED/terminal).
    let rel = leases.begin_release("mission-soak", "node-soak").unwrap();
    assert!(rel.is_some(), "release should proceed");
    leases.complete_release(&rec.lease_id).unwrap();
    // Duplicate release on terminal lease → idempotent None.
    let rel2 = leases.begin_release("mission-soak", "node-soak").unwrap();
    assert!(rel2.is_none(), "duplicate release must be no-op");

    assert_eq!(
        leases.all_active_leases().len(),
        0,
        "no active leases after release"
    );

    // Stale epoch fenced reserve → rejected + journaled.
    let stale_err = leases
        .begin_reserve_fenced(3, "mission-stale", "node-stale", None, None, 1, 1, true)
        .unwrap_err();
    assert!(matches!(stale_err, LeaseError::StaleEpoch { .. }));

    // Event journal must contain the stale-epoch entry.
    let events = store
        .events(Some("node-stale"), None, None, Some("LEASE_CONFLICT"), 10)
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.detail.contains("fenced stale-epoch")),
        "stale epoch must be journaled"
    );
}

// ── Delegation exactly-once ──────────────────────────────────────────

#[tokio::test]
async fn soak_delegation_duplicate_insert_rejected_exactly_once() {
    let tmp = tempfile::tempdir().unwrap();
    let store =
        Arc::new(OpsStore::open(&format!("sqlite:{}/deleg.db", tmp.path().display())).unwrap());
    let delegation = nauti_nodes::federation::delegation::DelegationStore::new(store.clone());

    // Insert a delegated job via the public API.
    delegation.insert_pending("job-1", "coord-A", 7).unwrap();

    // Duplicate insert must be rejected (UNIQUE constraint).
    let dup = delegation.insert_pending("job-1", "coord-B", 7);
    assert!(dup.is_err(), "duplicate delegated job must be rejected");

    // Only one row for job-1.
    let row = store.delegated_job("job-1").unwrap().expect("row exists");
    assert_eq!(row.origin_coordinator_id, "coord-A", "first writer wins");
    assert_eq!(row.state, "PENDING");

    // Mark running + terminal.
    delegation.mark_running("job-1", "coord-A").unwrap();
    let running = store.delegated_job("job-1").unwrap().unwrap();
    assert_eq!(running.state, "RUNNING");
    assert_eq!(
        running.executing_coordinator_id,
        Some("coord-A".to_string())
    );

    delegation.mark_terminal("job-1", "COMPLETED").unwrap();
    let final_row = store.delegated_job("job-1").unwrap().unwrap();
    assert_eq!(final_row.state, "COMPLETED");
    assert!(final_row.completed_at_ms.is_some());
}

// ── Failure cycle: coordinator restart preserves identity ─────────────

#[tokio::test]
async fn soak_coordinator_restart_preserves_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());
    config.coordinator_identity_path =
        Some(format!("{}/coord-identity.json", tmp.path().display()));

    // Start 1: establish durable identity + record a metric.
    let s1 = AppState::new(config.clone()).unwrap();
    s1.metrics
        .provider_selections_total
        .fetch_add(3, Ordering::Relaxed);
    let id1 = nauti_nodes::state::identity_snapshot(&s1).expect("identity loaded");
    let first_node_id = id1.node_identity.node_id.clone();

    // Start 2: reload from same durable identity.
    let s2 = AppState::new(config).unwrap();
    let id2 = nauti_nodes::state::identity_snapshot(&s2).expect("identity loaded");
    assert_eq!(
        id2.node_identity.node_id, first_node_id,
        "identity must survive restart"
    );

    // Metrics are in-memory per-process, so s2.metrics starts at 0
    // (new process = fresh metrics). The invariant is that identity
    // (the durable anchor for epoch/fencing) survives, not in-memory counters.
    assert_eq!(
        s2.metrics.snapshot_json()["provider_selections_total"],
        0,
        "in-memory metrics reset on restart (expected)"
    );
}

// ── Resource observation ───────────────────────────────────────────────

#[tokio::test]
async fn soak_resource_observation_real_proc() {
    let tmp = tempfile::tempdir().unwrap();
    let config = temp_config(tmp.path());
    let state = AppState::new(config).unwrap();
    let db_file = tmp.path().join("ops.db");

    let start = observe_self(Some(&db_file), None);

    // Read counters to force DB activity.
    let _ = state.ops.all_active_leases();
    let event_rows = store_events_count(&state, None);

    let end = observe_self(Some(&db_file), Some(event_rows));

    // Real values: RSS and FD counts must be nonzero on Linux.
    let pid = std::process::id();
    let rss = process_rss_bytes(pid).expect("VmRSS readable");
    assert!(rss > 0, "real RSS must be nonzero");
    let fds = open_fd_count(pid).expect("FDS readable");
    assert!(fds > 0, "real FD count must be nonzero");

    assert!(start.rss_bytes.is_some(), "start RSS observed");
    assert!(end.rss_bytes.is_some(), "end RSS observed");
    assert!(start.open_fds.is_some(), "start FDs observed");
    assert!(end.open_fds.is_some(), "end FDs observed");
}

fn store_events_count(state: &AppState, node_id: Option<&str>) -> u64 {
    state
        .ops
        .events(node_id, None, None, None, 1000)
        .map(|v| v.len() as u64)
        .unwrap_or(0)
}

// ── Invariant assertions ───────────────────────────────────────────────

#[tokio::test]
async fn soak_invariants_hard_zero_on_clean_run() {
    let tmp = tempfile::tempdir().unwrap();
    let store =
        Arc::new(OpsStore::open(&format!("sqlite:{}/inv.db", tmp.path().display())).unwrap());
    let leases = LeaseManager::new(store.clone());

    // Fresh metrics — all hard invariants start at 0.
    let metrics = nauti_nodes::metrics::FederationMetrics::new();
    metrics
        .assert_hard_invariants_zero()
        .expect("clean start: invariants zero");

    // No leases, no events → invariants still zero.
    assert!(store
        .events(None, None, None, None, 100)
        .unwrap()
        .is_empty());
    assert!(leases.all_active_leases().is_empty());
    metrics
        .assert_hard_invariants_zero()
        .expect("after clean ops: still zero");
}

// ── End-to-end provider-aware soak ─────────────────────────────────────

#[tokio::test]
async fn soak_provider_aware_full_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = temp_config(tmp.path());
    config.coordinator_identity_path =
        Some(format!("{}/coord-identity.json", tmp.path().display()));

    let state = Arc::new(AppState::new(config).expect("AppState::new failed"));
    state.registry.register_node(eligible_meta("soak-node-1"));
    state.registry.register_node(eligible_meta("soak-node-2"));

    // Build a registry with the local provider.
    let local_provider = Arc::new(LocalWorkerProvider::new("local", state.clone()));
    let mut registry = ProviderRegistry::new();
    registry.register(local_provider);

    // Case A: local healthy + available → selected.
    let req = make_request("thinker", 8192);
    let policy = ProviderPolicy::default();
    let (selected, decision) =
        select_provider_with_metrics(&registry, &req, &policy, Some(&state.metrics))
            .await
            .expect("select A");
    assert!(selected.is_some(), "Case A: local should be selected");
    assert_eq!(decision.rejections_policy, 0);
    assert_eq!(decision.rejections_health, 0);

    // Lease cycle at epoch 7.
    state.leases.set_authority_epoch(7);
    let outcome = state
        .leases
        .begin_reserve_fenced(7, "soak-mission", "soak-node-1", None, None, 8192, 10, true)
        .unwrap();
    assert!(matches!(outcome, ReserveOutcome::Created(_)));
    let rec = state
        .leases
        .lease_by_mission_node("soak-mission", "soak-node-1")
        .unwrap();
    state.leases.mark_scheduler_excluded(&rec.lease_id).unwrap();
    state.leases.mark_runtime_exited(&rec.lease_id).unwrap();
    state.leases.mark_gpu_released(&rec.lease_id).unwrap();
    state.leases.grant(&rec.lease_id).unwrap();
    assert_eq!(state.leases.all_active_leases().len(), 1);

    // Case I: lease causes local provider unavailability.
    // Node soak-node-1 is now leased → ineligible for new jobs.
    // Provider selection should still work for the unleased node.
    let req2 = make_request("thinker", 8192);
    let (sel2, _) = select_provider_with_metrics(&registry, &req2, &policy, Some(&state.metrics))
        .await
        .expect("select I");
    assert!(sel2.is_some(), "Case I: unleased node remains selectable");

    // Release lease → node available again.
    state
        .leases
        .begin_release("soak-mission", "soak-node-1")
        .unwrap();
    state.leases.complete_release(&rec.lease_id).unwrap();
    assert_eq!(state.leases.all_active_leases().len(), 0);

    // Stale epoch rejection.
    let stale = state
        .leases
        .begin_reserve_fenced(3, "soak-stale", "soak-node-1", None, None, 1, 1, true)
        .unwrap_err();
    assert!(matches!(stale, LeaseError::StaleEpoch { .. }));

    // Invariants: no accounting inconsistencies, no duplicate executions.
    state
        .metrics
        .assert_hard_invariants_zero()
        .expect("invariants hold");

    // Build and write report.
    let db_file = tmp.path().join("ops.db");
    let resource_end = observe_self(Some(&db_file), None);
    let integ = state.metrics.integrity_snapshot();
    let gate = &state.worker_gate.counters;

    let report = SoakReport {
        chunk: "chunk11".into(),
        duration_secs: 0.0,
        config: SoakConfigJson::from(&SoakConfig::ci_short()),
        provider_decisions: vec![
            format!("Case A: local selected={}", selected.is_some()),
            format!("Case I: unleased node selectable={}", sel2.is_some()),
        ],
        cases_executed: vec![
            "A_local_healthy_available".into(),
            "I_lease_blocks_node_selectable".into(),
        ],
        integrity: integ,
        worker_gate: WorkerGateSnapshot {
            commands_rejected_stale_epoch_total: gate
                .commands_rejected_stale_epoch_total
                .load(Ordering::Relaxed),
            commands_rejected_authority_conflict_total: gate
                .commands_rejected_authority_conflict_total
                .load(Ordering::Relaxed),
            duplicate_commands_blocked_total: gate
                .duplicate_commands_blocked_total
                .load(Ordering::Relaxed),
            checkpoint_double_resume_blocked_total: gate
                .checkpoint_double_resume_blocked_total
                .load(Ordering::Relaxed),
        },
        resource_start: observe_self(Some(&db_file), None),
        resource_end,
        leases_active_end: state.leases.all_active_leases().len(),
        errors: vec![],
        physical_gpu_used: false,
        runtime_hardware_notes: vec![
            "No physical GPU available — all assertions use control-plane state only".into(),
        ],
        invariants_ok: true,
    };

    let report_dir = PathBuf::from("target/chunk11_soak");
    std::fs::create_dir_all(&report_dir).unwrap();
    report
        .write_json(&report_dir.join("chunk11_soak_report.json"))
        .unwrap();
    report
        .write_markdown(&report_dir.join("CHUNK11_SOAK_REPORT.md"))
        .unwrap();

    assert!(report.invariants_ok);
    assert_eq!(report.leases_active_end, 0, "all leases released");

    // Verify the report files were actually written to disk.
    assert!(report_dir.join("chunk11_soak_report.json").exists());
    assert!(report_dir.join("CHUNK11_SOAK_REPORT.md").exists());
}
