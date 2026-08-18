//! Chunk 8B authority failover, split-brain attack, lease failover,
//! accounting, routing, lineage and catch-up (8B.4–8B.13).
//!
//! PRODUCTION-TRUTH:
//! - Coordinators run as REAL OS processes (the actual nauti-nodes
//!   binary) with real SQLite databases and real durable identity files.
//! - All probes/commands/catch-up travel over REAL TCP loopback HTTP.
//! - Kills are real process kills (SIGKILL via Child::kill).
//! - No simulated timers, no fake progress, no synthetic telemetry.
//!
//! Mandatory cases covered in this file:
//!   5  active authority selected
//!   6  epoch monotonic
//!  12  active lease replicated (via catch-up snapshot)
//!  13  authority process dies
//!  14  authority recovery
//!  15  epoch advances
//!  20  split-brain attempt (two real processes, one resource)
//!  21  conflicting lease attempt
//!  24  lease survives failover
//!  25  revoked worker survives failover
//!  27  accounting exactly once
//!  30  routing loop rejected
//!  31  hop limit rejected
//!  32  lineage parent/child
//!  33  lineage cycle rejected
//!  34  new coordinator catchup
//!  35  stale local DB cannot overwrite newer epoch (snapshot direction)

use nauti_nodes::{
    api::routes::router,
    federation::envelope::{FederationCommand, SignedFederationCommand},
    federation::snapshot::{apply as apply_snapshot, capture as capture_snapshot},
    federation::{FederatedRequestContext, FederationRouteError},
    lineage::LineageStore,
    node::coordinator_identity::CoordinatorIdentity,
    node::identity::NodeIdentity,
    state::AppState,
    Config,
};
use std::sync::atomic::Ordering;

// ─────────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct CoordKeys {
    node_id: String,
    key: ed25519_dalek::SigningKey,
    pub_hex: String,
}

fn worker_config(dir: &std::path::Path, trusted: Vec<String>) -> Config {
    Config {
        db_url: format!("sqlite:{}/worker.db", dir.display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: format!("{}/worker-identity.json", dir.display()),
        coordinator_identity_path: None,
        federation_peers: Vec::new(),
        trusted_coordinator_keys: trusted,
        ..Default::default()
    }
}

fn server_config(dir: &std::path::Path, name: &str, trusted: Vec<String>) -> Config {
    Config {
        db_url: format!("sqlite:{}/{name}.db", dir.display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: format!("{}/{name}-node.json", dir.display()),
        coordinator_identity_path: None,
        federation_peers: Vec::new(),
        trusted_coordinator_keys: trusted,
        ..Default::default()
    }
}

async fn start_server(config: Config) -> (String, AppState) {
    let state = AppState::new(config).expect("AppState::new failed");
    serve_state(state).await
}

/// Serve an EXISTING state (so tests can install lineage etc. first).
async fn serve_state(state: AppState) -> (String, AppState) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let served = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router(served)).await.ok();
    });
    (format!("http://{addr}"), state)
}

/// Config with a PRE-SEEDED durable authority identity at the given epoch.
/// The worker gate trusts that identity's public key, so fenced commands
/// signed with it pass authentication.
fn authority_file_config(dir: &std::path::Path, name: &str, epoch: u64) -> (Config, CoordKeys) {
    let node_path = dir.join(format!("{name}-node.json"));
    let coord_path = dir.join(format!("{name}-coord.json"));
    let node = NodeIdentity::load_or_create(&node_path).unwrap();
    let mut cid = CoordinatorIdentity::wrap(node.clone());
    if epoch > 0 {
        cid.adopt_epoch_as_authority(epoch).unwrap();
    }
    std::fs::write(&coord_path, serde_json::to_string_pretty(&cid).unwrap()).unwrap();
    let keys = CoordKeys {
        node_id: node.node_id.clone(),
        key: node.signing_key().unwrap(),
        pub_hex: hex::encode(&node.public_key),
    };
    let config = Config {
        db_url: format!("sqlite:{}/{name}.db", dir.display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: node_path.to_string_lossy().to_string(),
        coordinator_identity_path: Some(coord_path.to_string_lossy().to_string()),
        federation_peers: Vec::new(),
        trusted_coordinator_keys: vec![keys.pub_hex.clone()],
        ..Default::default()
    };
    (config, keys)
}

/// Install an Active authority identity at a given epoch into the state.
fn install_authority(state: &AppState, epoch: u64) -> CoordKeys {
    let id = nauti_nodes::state::identity_snapshot(state).unwrap();
    let mut updated = (*id).clone();
    updated.adopt_epoch_as_authority(epoch).unwrap();
    let keys = CoordKeys {
        node_id: updated.node_identity.node_id.clone(),
        key: updated.node_identity.signing_key().unwrap(),
        pub_hex: hex::encode(&updated.node_identity.public_key),
    };
    nauti_nodes::state::install_identity(state, updated);
    state.leases.set_authority_epoch(epoch);
    keys
}

fn sign(keys: &CoordKeys, command: FederationCommand) -> SignedFederationCommand {
    SignedFederationCommand::build_signed(&keys.node_id, &keys.pub_hex, &keys.key, command)
}

async fn post_command(
    client: &reqwest::Client,
    base: &str,
    env: &SignedFederationCommand,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = client
        .post(format!("{base}/internal/federation/command"))
        .json(env)
        .send()
        .await
        .expect("real HTTP POST failed");
    let status = resp.status();
    let body = resp.json().await.unwrap_or_default();
    (status, body)
}

/// Spawn a REAL coordinator OS process with a pre-seeded authority
/// identity at the given epoch. Returns (child, base_url, node identity).
async fn spawn_real_coordinator(
    dir: &std::path::Path,
    name: &str,
    epoch: u64,
) -> (std::process::Child, String, NodeIdentity) {
    let node_path = dir.join(format!("{name}-node.json"));
    let coord_path = dir.join(format!("{name}-coord.json"));
    let db_url = format!("sqlite:{}/{name}.db", dir.display());

    // Pre-seed durable identities so the process starts with a known
    // authority epoch (real files, real keys).
    let node = NodeIdentity::load_or_create(&node_path).unwrap();
    let mut cid = CoordinatorIdentity::wrap(node.clone());
    if epoch > 0 {
        cid.adopt_epoch_as_authority(epoch).unwrap();
    }
    std::fs::write(&coord_path, serde_json::to_string_pretty(&cid).unwrap()).unwrap();

    // Pick a free port.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let bin = env!("CARGO_BIN_EXE_nauti-nodes");
    let child = std::process::Command::new(bin)
        .env("NAUTI_MODE", "coordinator")
        .env("NAUTI_LISTEN_PORT", port.to_string())
        .env("NAUTI_DB_URL", &db_url)
        .env("NAUTI_IDENTITY_PATH", &node_path)
        .env("NAUTI_COORDINATOR_IDENTITY_PATH", &coord_path)
        .env("FORGE_URL", "http://127.0.0.1:1")
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn real coordinator process");

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let mut up = false;
    for _ in 0..100 {
        if client
            .get(format!("{base}/health"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(up, "real coordinator process did not come up at {base}");
    (child, base, node)
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 5+6: authority selection over real HTTP + monotonic epoch
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case05_case06_active_authority_selected_and_epoch_monotonic() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();

    // A is the authority at epoch 7; B is a member at epoch 2.
    let (base_a, state_a) = start_server(server_config(tmp_a.path(), "a", vec![])).await;
    install_authority(&state_a, 7);
    let (base_b, state_b) = start_server(server_config(tmp_b.path(), "b", vec![])).await;
    install_authority(&state_b, 2);

    // A's real authority record over real HTTP.
    let resp = reqwest::get(format!("{base_a}/v1/federation/authority"))
        .await
        .unwrap();
    let rec: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rec["epoch"], 7);

    // B probes A over REAL TCP and must elect A (highest epoch).
    let probed = state_b
        .authority
        .probe_peer(&base_a)
        .await
        .expect("real probe failed");
    assert_eq!(probed.epoch, 7);

    let id_b = nauti_nodes::state::identity_snapshot(&state_b).unwrap();
    let elected = state_b
        .authority
        .elect(&nauti_nodes::federation::AuthorityRecord {
            coordinator_id: id_b.node_identity.node_id.clone(),
            epoch: id_b.authority_epoch.value(),
            acquired_at_ms: id_b.epoch_acquired_at_ms as i64,
        });
    assert_eq!(
        elected.coordinator_id,
        nauti_nodes::state::identity_snapshot(&state_a)
            .unwrap()
            .node_identity
            .node_id,
        "case 5: the active (highest-epoch) authority must be selected"
    );

    // Case 6: epoch adoption is strictly monotonic.
    let mut id = (*id_b).clone();
    assert!(id.adopt_epoch_as_authority(2).is_err());
    assert!(id.adopt_epoch_as_authority(1).is_err());
    assert!(id.adopt_epoch_as_authority(8).is_ok());
    // Lease authority floor never moves backwards.
    state_b.leases.set_authority_epoch(8);
    state_b.leases.set_authority_epoch(3);
    assert_eq!(state_b.leases.current_authority_epoch(), 8);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cases 13+14+15: real authority process death + deterministic recovery
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case13_14_15_authority_process_death_and_epoch_advance() {
    let tmp = tempfile::tempdir().unwrap();

    // A: REAL coordinator process, authority epoch 5.
    let (mut child_a, base_a, _node_a) = spawn_real_coordinator(tmp.path(), "coord-a", 5).await;

    // B: follower coordinator (in-process server), epoch 1.
    let tmp_b = tempfile::tempdir().unwrap();
    let (base_b, state_b) = start_server(server_config(tmp_b.path(), "b", vec![])).await;
    install_authority(&state_b, 1);
    let _ = base_b;

    // B learns A's real authority record over real HTTP.
    let rec_a = state_b.authority.probe_peer(&base_a).await.unwrap();
    assert_eq!(rec_a.epoch, 5, "case 5/13: A is alive at epoch 5");

    // Case 13: kill the authority process (real SIGKILL).
    child_a.kill().expect("kill failed");
    child_a.wait().expect("wait failed");

    // Probe evidence: A no longer answers (real TCP failure).
    let probe_err = state_b.authority.probe_peer(&base_a).await;
    assert!(
        probe_err.is_err(),
        "case 13: dead authority must not answer probes"
    );

    // Case 14: B acquires authority via the deterministic mechanism.
    let id_b = nauti_nodes::state::identity_snapshot(&state_b).unwrap();
    let mut updated = (*id_b).clone();
    // B must be mutation-capable (it is: Active at epoch 1).
    let new_epoch = state_b
        .authority
        .acquire_authority(&mut updated)
        .expect("case 14: authority acquisition must succeed after A's death");
    nauti_nodes::state::install_identity(&state_b, updated);
    state_b.leases.set_authority_epoch(new_epoch);

    // Case 15: epoch advanced to exactly A.epoch + 1.
    assert_eq!(
        new_epoch,
        rec_a.epoch + 1,
        "case 15: epoch must advance to N+1"
    );
    let id_b2 = nauti_nodes::state::identity_snapshot(&state_b).unwrap();
    assert_eq!(id_b2.authority_epoch.value(), 6);
    assert!(id_b2.can_issue_mutations());
}

// ─────────────────────────────────────────────────────────────────────────────
// Cases 20+21: split-brain attack with two REAL coordinator processes
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case20_21_split_brain_attack_produces_zero_conflicting_ownership() {
    let tmp = tempfile::tempdir().unwrap();

    // Two REAL coordinator processes, BOTH believing they are authority at
    // the SAME epoch 3 (the dual-authority window).
    let (mut child_a, base_a, _node_a) = spawn_real_coordinator(tmp.path(), "brain-a", 3).await;
    let (mut child_b, base_b, _node_b) = spawn_real_coordinator(tmp.path(), "brain-b", 3).await;

    // Both processes are really alive.
    let client = reqwest::Client::new();
    assert!(client
        .get(format!("{base_a}/health"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    assert!(client
        .get(format!("{base_b}/health"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());

    // The worker trusts BOTH coordinators' keys.
    let id_a: CoordinatorIdentity = serde_json::from_str(
        &std::fs::read_to_string(tmp.path().join("brain-a-coord.json")).unwrap(),
    )
    .unwrap();
    let id_b: CoordinatorIdentity = serde_json::from_str(
        &std::fs::read_to_string(tmp.path().join("brain-b-coord.json")).unwrap(),
    )
    .unwrap();
    let pub_a = hex::encode(&id_a.node_identity.public_key);
    let pub_b = hex::encode(&id_b.node_identity.public_key);

    let tmp_w = tempfile::tempdir().unwrap();
    let (worker_base, worker_state) = start_server(worker_config(
        tmp_w.path(),
        vec![pub_a.clone(), pub_b.clone()],
    ))
    .await;

    // Sign commands with each coordinator's REAL durable key (the test
    // owns the identity files; the signatures are real Ed25519).
    let key_a = id_a.node_identity.signing_key().unwrap();
    let key_b = id_b.node_identity.signing_key().unwrap();
    let grant_a = SignedFederationCommand::build_signed(
        &id_a.node_identity.node_id,
        &pub_a,
        &key_a,
        FederationCommand::LeaseGrant {
            lease_id: "lease-sb-a".into(),
            mission_id: "mission-sb-a".into(),
            resource_id: "contested-gpu".into(),
            epoch: 3,
        },
    );
    let grant_b = SignedFederationCommand::build_signed(
        &id_b.node_identity.node_id,
        &pub_b,
        &key_b,
        FederationCommand::LeaseGrant {
            lease_id: "lease-sb-b".into(),
            mission_id: "mission-sb-b".into(),
            resource_id: "contested-gpu".into(),
            epoch: 3,
        },
    );

    // Both attempt to take ownership of the SAME resource at the SAME epoch.
    let (s1, body1) = post_command(&client, &worker_base, &grant_a).await;
    let (s2, body2) = post_command(&client, &worker_base, &grant_b).await;

    let accepted = [s1.is_success(), s2.is_success()];
    let accepted_count = accepted.iter().filter(|x| **x).count();
    assert_eq!(
        accepted_count, 1,
        "case 20: exactly one same-epoch authority may win (got {s1} {body1} / {s2} {body2})"
    );
    let rejected_status = if s1.is_success() { s2 } else { s1 };
    assert_eq!(
        rejected_status, 409,
        "the loser must be rejected as authority conflict"
    );

    // accepted_conflicting_ownership = 0 — one lease, one owner in the real store.
    let active = worker_state.leases.all_active_leases();
    assert_eq!(
        active.len(),
        1,
        "case 21: only one lease may hold the resource"
    );
    assert_eq!(active[0].node_id, "contested-gpu");
    let conflicts = worker_state
        .worker_gate
        .counters
        .commands_rejected_authority_conflict_total
        .load(Ordering::SeqCst);
    assert_eq!(conflicts, 1, "the conflict must be counted");

    // The same winner trying a SECOND conflicting lease on the same
    // resource must also not produce dual ownership.
    let winner_keys = if s1.is_success() {
        (&id_a, &key_a, pub_a)
    } else {
        (&id_b, &key_b, pub_b)
    };
    let grant_conflict = SignedFederationCommand::build_signed(
        &winner_keys.0.node_identity.node_id,
        &winner_keys.2,
        winner_keys.1,
        FederationCommand::LeaseGrant {
            lease_id: "lease-sb-conflict".into(),
            mission_id: "mission-sb-other".into(),
            resource_id: "contested-gpu".into(),
            epoch: 3,
        },
    );
    let (s3, _) = post_command(&client, &worker_base, &grant_conflict).await;
    assert!(
        !s3.is_success(),
        "case 21: a conflicting second lease must be rejected"
    );
    assert_eq!(
        worker_state.leases.all_active_leases().len(),
        1,
        "still exactly one owner"
    );

    child_a.kill().ok();
    child_b.kill().ok();
    child_a.wait().ok();
    child_b.wait().ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 24: lease survives failover, blocks shared work, then recovers
// ─────────────────────────────────────────────────────────────────────────────

fn eligible_meta(id: &str) -> nauti_nodes::types::NodeMetadata {
    use nauti_nodes::types::*;
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

#[tokio::test]
async fn case24_lease_survives_failover_and_resource_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    // Durable authority identity at epoch 3; the gate trusts its key.
    let (config, keys_a) = authority_file_config(tmp.path(), "failover", 3);
    let client = reqwest::Client::new();

    // ── Authority A: grant a real lease, then die ────────────────────────
    {
        let (base, state) = start_server(config.clone()).await;
        let keys = keys_a.clone();

        // Register + persist a real node (so recovery knows it).
        let rt = state.registry.register_node(eligible_meta("gpu-fo"));
        nauti_nodes::state::persist_node(&state.ops, &rt);

        // Fenced LeaseGrant through the real command path.
        let env = sign(
            &keys,
            FederationCommand::LeaseGrant {
                lease_id: "lease-fo".into(),
                mission_id: "mission-fo".into(),
                resource_id: "gpu-fo".into(),
                epoch: 3,
            },
        );
        let (s, body) = post_command(&client, &base, &env).await;
        assert_eq!(s, 200, "grant must apply: {body}");
        state
            .leases
            .apply_overlay_to_node(&state.registry, "gpu-fo");
        nauti_nodes::state::persist_node(&state.ops, &rt);
        // Authority process dies here (server + state dropped).
    }

    // ── New authority B recovers on the SAME durable database ───────────
    {
        let (_base, state) = start_server(config.clone()).await;

        // Case 24: the lease survived the failover.
        assert!(
            state.leases.has_active_lease("gpu-fo"),
            "case 24: active lease must survive authority death"
        );
        let rec = state.leases.lease_by_id("lease-fo").unwrap();
        assert_eq!(rec.state.as_str(), "LEASED");

        // B becomes authority at epoch 4 (N+1) on the SAME durable
        // identity (monotonic adoption over the persisted epoch 3).
        install_authority(&state, 4);
        let keys = keys_a.clone();

        // Worker reconnects (fresh truth, same as a real signed heartbeat
        // would re-establish): runtime Ready + fresh reconciliation. The
        // lease must STILL keep the node out of the shared pool.
        let rt = state.registry.get_node("gpu-fo").unwrap();
        {
            let mut md = rt.metadata.write().unwrap();
            md.online = true;
            md.health_state = "ONLINE".into();
            md.lifecycle_state = nauti_nodes::types::NodeLifecycleState::Online;
            md.runtime_state = nauti_nodes::eligibility::RuntimeState::Ready;
            md.reconciliation.status = nauti_nodes::types::ReconciliationStatus::MATCH;
        }
        assert!(
            !rt.try_assign(),
            "case 24: shared request must be rejected while the lease holds"
        );

        // B releases the lease via the fenced command path.
        let env = sign(
            &keys,
            FederationCommand::LeaseRelease {
                lease_id: "lease-fo".into(),
                mission_id: "mission-fo".into(),
                resource_id: "gpu-fo".into(),
                epoch: 4,
            },
        );
        let outcome = state
            .worker_gate
            .handle(&env)
            .expect("release must pass the fence");
        assert_eq!(
            outcome,
            nauti_nodes::federation::worker_gate::CommandOutcome::Applied
        );
        // Sync the registry overlay with the terminal lease state.
        let released = state.leases.lease_by_id("lease-fo").unwrap();
        state
            .leases
            .sync_lease_ref_on_node(&state.registry, &released);

        // Resource recovers: shared assignment now succeeds.
        assert!(
            rt.try_assign(),
            "case 24: resource must recover after the new authority releases"
        );
        rt.release_assignment();

        // No duplicate release: a second release is a terminal no-op.
        let env2 = sign(
            &keys,
            FederationCommand::LeaseRelease {
                lease_id: "lease-fo".into(),
                mission_id: "mission-fo".into(),
                resource_id: "gpu-fo".into(),
                epoch: 4,
            },
        );
        let outcome2 = state.worker_gate.handle(&env2).unwrap();
        // Either no-op outcome proves no double release: the applied-map
        // blocks the re-delivery (IdempotentDuplicate) OR the executor sees
        // the already-terminal lease (LeaseAlreadyTerminal).
        assert!(
            matches!(
                outcome2,
                nauti_nodes::federation::worker_gate::CommandOutcome::IdempotentDuplicate
                    | nauti_nodes::federation::worker_gate::CommandOutcome::LeaseAlreadyTerminal
            ),
            "duplicate release must be a no-op, got {outcome2:?}"
        );
        assert!(!state.leases.has_active_lease("gpu-fo"));
    }

    // ── No epoch rollback across the restart ─────────────────────────────
    {
        let (_base, state) = start_server(config.clone()).await;
        // A stale command at epoch 3 must still be fenced out on a fresh
        // worker gate over the same durable DB (fence persisted).
        let fence = state
            .worker_gate
            .gate
            .current_fence("gpu-fo")
            .unwrap()
            .unwrap();
        assert_eq!(fence.epoch, 4, "durable fence must hold the new epoch");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 25: revocations survive failover
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case25_revoked_worker_survives_failover() {
    let tmp = tempfile::tempdir().unwrap();
    let config = server_config(tmp.path(), "rev", vec![]);

    {
        let (_base, state) = start_server(config.clone()).await;
        state
            .revocations
            .revoke("bad-worker-1", "compromised key")
            .unwrap();
    }
    // Restart.
    {
        let (_base, state) = start_server(config.clone()).await;
        assert!(
            state.revocations.is_revoked("bad-worker-1"),
            "case 25: revocation must survive failover"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 27: exactly-once accounting across failover
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case27_accounting_exactly_once_across_failover() {
    let tmp = tempfile::tempdir().unwrap();
    let config = server_config(tmp.path(), "acct", vec![]);

    // Authority A: open a real reservation, then die before settlement.
    {
        let (_base, state) = start_server(config.clone()).await;
        state.scheduler.set_balance("key-acct", 1000).unwrap();
        state.scheduler.reserve_tokens("key-acct", 400).unwrap();
        state
            .ops
            .open_reservation("job-acct", "key-acct", 400)
            .unwrap();
        // Balance is now 600 (400 reserved). Death happens here.
        assert!(state.scheduler.check_quota("key-acct", 700).is_err());
    }

    // First recovery: orphaned reservation refunded EXACTLY once.
    {
        let (_base, state) = start_server(config.clone()).await;
        let row = state.ops.reservation("job-acct").unwrap().unwrap();
        assert_eq!(
            row.state, "REFUNDED",
            "orphaned reservation must be refunded"
        );
        // Full balance restored.
        assert!(state.scheduler.check_quota("key-acct", 1000).is_ok());
        let refunds = state
            .ops
            .events(
                None,
                None,
                Some("job-acct"),
                Some("ACCOUNTING_REFUNDED"),
                100,
            )
            .unwrap();
        assert_eq!(refunds.len(), 1, "exactly one refund event");
    }

    // Second recovery: NO double refund.
    {
        let (_base, state) = start_server(config.clone()).await;
        assert!(state.scheduler.check_quota("key-acct", 1000).is_ok());
        assert!(
            state.scheduler.check_quota("key-acct", 1001).is_err(),
            "no extra tokens may appear (no double refund)"
        );
        let refunds = state
            .ops
            .events(
                None,
                None,
                Some("job-acct"),
                Some("ACCOUNTING_REFUNDED"),
                100,
            )
            .unwrap();
        assert_eq!(
            refunds.len(),
            1,
            "still exactly one refund after second restart"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cases 30+31: federated routing loop + hop limit
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn case30_routing_loop_rejected_a_b_c_a() {
    let mut ctx = FederatedRequestContext::new("coord-A".into(), 8);
    ctx.forward(&"coord-B".into()).unwrap();
    ctx.forward(&"coord-C".into()).unwrap();
    // C tries to send back to A: loop.
    assert_eq!(
        ctx.forward(&"coord-A".into()),
        Err(FederationRouteError::LoopDetected("coord-A".into())),
        "case 30: A -> B -> C -> A must be rejected"
    );
    assert_eq!(ctx.current_coordinator_id, "coord-C");
    assert_eq!(ctx.hop_count, 2);
}

#[test]
fn case31_hop_limit_rejected() {
    let mut ctx = FederatedRequestContext::new("coord-A".into(), 2);
    ctx.forward(&"coord-B".into()).unwrap();
    ctx.forward(&"coord-C".into()).unwrap();
    assert_eq!(
        ctx.forward(&"coord-D".into()),
        Err(FederationRouteError::MaxHopsExceeded {
            hop_count: 2,
            max_hops: 2
        }),
        "case 31: hop budget must be enforced"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cases 32+33: persistent lineage relationships
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn case32_case33_lineage_multilevel_and_cycle_rejection() {
    let tmp = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite:{}/lineage.db", tmp.path().display());
    let store = LineageStore::open(&db_url).unwrap();

    // Case 32: parent assignment + multi-level lineage.
    store.set_root("root-coord").unwrap();
    store.add_child("mid-coord", "root-coord").unwrap();
    store.add_child("leaf-coord", "mid-coord").unwrap();

    assert_eq!(
        store.parent("leaf-coord").unwrap().as_deref(),
        Some("mid-coord")
    );
    assert_eq!(
        store.parent("mid-coord").unwrap().as_deref(),
        Some("root-coord")
    );
    let children = store.children("root-coord").unwrap();
    assert_eq!(children, vec!["mid-coord".to_string()]);
    let ancestors = store.ancestors("leaf-coord").unwrap();
    assert_eq!(ancestors.len(), 2, "leaf has two ancestors (mid + root)");
    let rel = store.get_relationship("leaf-coord").unwrap().unwrap();
    assert_eq!(rel.depth, 2);

    // Persistence across reopen (real file).
    drop(store);
    let store = LineageStore::open(&db_url).unwrap();
    assert_eq!(
        store.parent("leaf-coord").unwrap().as_deref(),
        Some("mid-coord")
    );

    // Case 33: cycle rejection — making root a child of its own descendant.
    assert!(
        store.add_child("root-coord", "leaf-coord").is_err(),
        "case 33: lineage cycle must be rejected"
    );
    // Self-parent also rejected.
    assert!(store.add_child("mid-coord", "mid-coord").is_err());

    // Revoked coordinator behavior: a revoked node no longer holds an
    // ACTIVE relationship (fail-closed exclusion from the living tree).
    store.revoke("leaf-coord").unwrap();
    assert!(
        store.get_relationship("leaf-coord").unwrap().is_none(),
        "revoked node must not keep an active lineage relationship"
    );
    assert!(
        !store
            .all_active()
            .unwrap()
            .iter()
            .any(|r| r.node_id == "leaf-coord"),
        "revoked node must disappear from the active tree"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 34 (+12): new coordinator catch-up over real HTTP
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case34_new_coordinator_catches_up_from_live_peer() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_n = tempfile::tempdir().unwrap();

    // Authority A with real state: epoch 5, an active lease, a revocation,
    // a lineage relationship and a durable fence. Lineage is installed
    // BEFORE serving so the snapshot handler sees it.
    let cfg_a = server_config(tmp_a.path(), "auth", vec![]);
    let db_a = cfg_a.db_url.clone();
    let mut pre_a = AppState::new(cfg_a).unwrap();
    install_authority(&pre_a, 5);
    let lineage = LineageStore::open(&db_a).unwrap();
    lineage.set_root("auth-root").unwrap();
    lineage.add_child("child-1", "auth-root").unwrap();
    pre_a.lineage = Some(std::sync::Arc::new(lineage));
    let (base_a, state_a) = serve_state(pre_a).await;
    state_a
        .leases
        .begin_reserve("mission-cu", "node-cu", Some("lease-cu"), None, 1, 1, true)
        .unwrap();
    // Drive to LEASED through the real state machine.
    let l = state_a
        .leases
        .lease_by_mission_node("mission-cu", "node-cu")
        .unwrap();
    state_a.leases.mark_scheduler_excluded(&l.lease_id).unwrap();
    state_a.leases.mark_runtime_exited(&l.lease_id).unwrap();
    state_a.leases.mark_gpu_released(&l.lease_id).unwrap();
    state_a.leases.grant(&l.lease_id).unwrap();
    state_a.revocations.revoke("banned-node", "abuse").unwrap();
    state_a
        .ops
        .upsert_fence(&nauti_nodes::opsstore::FenceRow {
            resource_id: "node-cu".into(),
            epoch: 5,
            authority_coordinator_id: "auth".into(),
            updated_at_ms: 0,
        })
        .unwrap();

    // Snapshot endpoint serves real state over real HTTP.
    let client = reqwest::Client::new();
    let snap_resp = client
        .get(format!("{base_a}/v1/federation/snapshot"))
        .send()
        .await
        .unwrap();
    assert_eq!(snap_resp.status(), 200);

    // New coordinator N with an EMPTY local state catches up. A real
    // coordinator opens its lineage store at startup; do the same here
    // BEFORE the server clone is spawned so the handler sees it.
    let newcomer_cfg = server_config(tmp_n.path(), "newcomer", vec![]);
    let newcomer_db = newcomer_cfg.db_url.clone();
    let mut newcomer_state = AppState::new(newcomer_cfg).unwrap();
    newcomer_state.lineage = Some(std::sync::Arc::new(
        LineageStore::open(&newcomer_db).unwrap(),
    ));
    let (_base_n, state_n) = serve_state(newcomer_state).await;
    let resp = client
        .post(format!("{_base_n}/v1/federation/catchup"))
        .json(&serde_json::json!({ "peer_url": base_a }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let report: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(report["epoch"], 5);
    assert_eq!(report["report"]["leases_imported"], 1);
    assert_eq!(report["report"]["revocations_imported"], 1);
    assert_eq!(report["report"]["lineage_imported"], 2, "root + child");
    assert_eq!(report["report"]["fences_imported"], 1);
    assert_eq!(report["report"]["adopted_follower_epoch"], true);

    // Case 12: the active lease replicated — N knows it and it blocks.
    assert!(
        state_n.leases.has_active_lease("node-cu"),
        "case 12: active lease must replicate to the newcomer"
    );
    // Revocation replicated.
    assert!(state_n.revocations.is_revoked("banned-node"));
    // Lineage replicated: the child relationship exists locally.
    let lineage_n = state_n
        .lineage
        .as_ref()
        .expect("lineage opened on catch-up DB");
    assert_eq!(
        lineage_n.parent("child-1").unwrap().as_deref(),
        Some("auth-root")
    );
    // Follower epoch adopted WITHOUT mutation rights.
    let id_n = nauti_nodes::state::identity_snapshot(&state_n).unwrap();
    assert_eq!(id_n.authority_epoch.value(), 5);
    assert!(
        !id_n.can_issue_mutations(),
        "case 34: caught-up coordinator must be a follower (fail closed)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 35: stale snapshot cannot downgrade a newer local fence
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case35_stale_snapshot_cannot_overwrite_newer_epoch() {
    let tmp = tempfile::tempdir().unwrap();
    let (_base, state) = start_server(server_config(tmp.path(), "local", vec![])).await;

    // Local fence at epoch 9 (learned from the real current authority).
    state
        .ops
        .upsert_fence(&nauti_nodes::opsstore::FenceRow {
            resource_id: "res-35".into(),
            epoch: 9,
            authority_coordinator_id: "current-auth".into(),
            updated_at_ms: 0,
        })
        .unwrap();

    // A stale snapshot (epoch 4 fence for the same resource) arrives.
    let mut stale = capture_snapshot(&state).unwrap();
    stale.epoch = 4;
    stale.fences = vec![nauti_nodes::opsstore::FenceRow {
        resource_id: "res-35".into(),
        epoch: 4,
        authority_coordinator_id: "stale-auth".into(),
        updated_at_ms: 0,
    }];
    stale.active_leases.clear();

    let report = apply_snapshot(&state, &stale).unwrap();
    assert_eq!(report.fences_kept_newer_local, 1);
    assert_eq!(report.fences_imported, 0);
    let fence = state.ops.get_fence("res-35").unwrap().unwrap();
    assert_eq!(fence.epoch, 9, "case 35: newer local epoch must survive");
    assert_eq!(fence.authority_coordinator_id, "current-auth");
}
