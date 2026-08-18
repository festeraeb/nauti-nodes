//! Chunk 8B worker-side fencing + federation authentication (8B.2 / 8B.3).
//!
//! PRODUCTION-TRUTH: all traffic here is real — real axum servers on
//! OS-assigned TCP ports, real reqwest HTTP calls, real Ed25519 signing
//! with durable identities, real SQLite fence/lease stores. No mocks.
//!
//! Mandatory cases covered in this file:
//!   1  unknown peer rejected
//!   2  bad signature rejected
//!   3  replay rejected
//!   4  stale timestamp rejected
//!   7  worker accepts current epoch
//!   8  worker accepts newer epoch
//!   9  worker rejects older epoch
//!  10  same epoch wrong authority rejected
//!  11  worker restart preserves fence (durable)
//!  16  stale authority reconnect rejected
//!  17  stale work rejected
//!  18  stale lease release rejected
//!  19  partition fail-closed (policy gate)
//!  22  duplicate lease idempotent
//!  23  duplicate release idempotent
//!  26  revoked coordinator rejected
//!  28  checkpoint resume once
//!  29  request duplicate does not execute twice

use nauti_nodes::{
    api::routes::router,
    federation::authority::{assert_may_mutate, PartitionDenial},
    federation::envelope::{FederationCommand, SignedFederationCommand},
    federation::AuthorityRecord,
    node::identity::NodeIdentity,
    state::AppState,
    Config,
};
use std::sync::atomic::Ordering;

// ─────────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────────

struct Coordinator {
    id: NodeIdentity,
    key: ed25519_dalek::SigningKey,
    pub_hex: String,
}

fn make_coordinator() -> Coordinator {
    let id = NodeIdentity::generate();
    let key = id.signing_key().unwrap();
    let pub_hex = hex::encode(&id.public_key);
    Coordinator { id, key, pub_hex }
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

/// Start a real worker server (full router incl. the fenced command
/// endpoint) on an OS-assigned port. Returns (base_url, state handle).
async fn start_worker(config: Config) -> (String, AppState) {
    let state = AppState::new(config).expect("worker AppState::new failed");
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

async fn send_command(
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
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    (status, body)
}

fn sign(coord: &Coordinator, command: FederationCommand) -> SignedFederationCommand {
    SignedFederationCommand::build_signed(&coord.id.node_id, &coord.pub_hex, &coord.key, command)
}

fn grant(resource: &str, lease: &str, mission: &str, epoch: u64) -> FederationCommand {
    FederationCommand::LeaseGrant {
        lease_id: lease.into(),
        mission_id: mission.into(),
        resource_id: resource.into(),
        epoch,
    }
}

fn release(resource: &str, lease: &str, mission: &str, epoch: u64) -> FederationCommand {
    FederationCommand::LeaseRelease {
        lease_id: lease.into(),
        mission_id: mission.into(),
        resource_id: resource.into(),
        epoch,
    }
}

fn work(resource: &str, job: &str, epoch: u64) -> FederationCommand {
    FederationCommand::WorkAssign {
        job_id: job.into(),
        resource_id: resource.into(),
        epoch,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Authentication rejections (cases 1-4, 26)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case01_unknown_peer_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let trusted = make_coordinator();
    let stranger = make_coordinator();
    let (base, _state) = start_worker(worker_config(tmp.path(), vec![trusted.pub_hex])).await;
    let client = reqwest::Client::new();

    let env = sign(&stranger, grant("r1", "l1", "m1", 1));
    let (status, _) = send_command(&client, &base, &env).await;
    assert_eq!(status, 401, "unknown coordinator must be rejected");
}

#[tokio::test]
async fn case02_bad_signature_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let coord = make_coordinator();
    let (base, _state) = start_worker(worker_config(tmp.path(), vec![coord.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    let mut env = sign(&coord, grant("r1", "l1", "m1", 1));
    // Tamper with the payload AFTER signing.
    env.command = grant("r1", "l1", "m1", 99);
    let (status, _) = send_command(&client, &base, &env).await;
    assert_eq!(status, 401, "tampered envelope must fail signature check");
}

#[tokio::test]
async fn case03_replay_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let coord = make_coordinator();
    let (base, _state) = start_worker(worker_config(tmp.path(), vec![coord.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    let env = sign(&coord, grant("r-replay", "l-replay", "m1", 1));
    let (s1, _) = send_command(&client, &base, &env).await;
    assert_eq!(s1, 200);
    // Exact same envelope again: replayed nonce must be rejected.
    let (s2, _) = send_command(&client, &base, &env).await;
    assert_eq!(s2, 401, "replayed envelope must be rejected");
}

#[tokio::test]
async fn case04_stale_timestamp_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let coord = make_coordinator();
    let (base, _state) = start_worker(worker_config(tmp.path(), vec![coord.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    let mut env = sign(&coord, grant("r1", "l1", "m1", 1));
    env.timestamp_secs = env.timestamp_secs.saturating_sub(10_000);
    env.sign(&coord.key); // re-sign: only freshness is wrong
    let (status, _) = send_command(&client, &base, &env).await;
    assert_eq!(status, 401, "stale timestamp must be rejected");
}

#[tokio::test]
async fn case26_revoked_coordinator_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let coord = make_coordinator();
    let (base, state) = start_worker(worker_config(tmp.path(), vec![coord.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    // Operator revokes the coordinator identity in the durable store.
    state
        .revocations
        .revoke(&coord.id.node_id, "compromised in test")
        .unwrap();

    let env = sign(&coord, grant("r1", "l1", "m1", 1));
    let (status, _) = send_command(&client, &base, &env).await;
    assert_eq!(status, 401, "revoked coordinator must be rejected");
}

// ─────────────────────────────────────────────────────────────────────────────
// Fencing decisions (cases 7-10, 16-18, 22-23)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cases07_09_10_epoch_fencing_matrix() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator();
    let b = make_coordinator();
    let c = make_coordinator();
    let (base, state) = start_worker(worker_config(
        tmp.path(),
        vec![a.pub_hex.clone(), b.pub_hex.clone(), c.pub_hex.clone()],
    ))
    .await;
    let client = reqwest::Client::new();

    // Case 7: current epoch accepted (first authority on the resource).
    let (s, body) = send_command(&client, &base, &sign(&a, grant("res-x", "l-x", "m-a", 5))).await;
    assert_eq!(s, 200, "case 7: current epoch must be accepted: {body}");
    assert!(body["outcome"].as_str().unwrap().contains("Applied"));

    // Case 9: older epoch rejected (stale coordinator).
    let (s, _) = send_command(&client, &base, &sign(&a, work("res-x", "job-old", 4))).await;
    assert_eq!(s, 409, "case 9: older epoch must be rejected");

    // Case 10: same epoch, DIFFERENT authority -> authority conflict.
    let (s, _) = send_command(&client, &base, &sign(&b, work("res-x", "job-b", 5))).await;
    assert_eq!(
        s, 409,
        "case 10: same epoch different authority must conflict"
    );

    // Case 8: newer epoch accepted and persists the new authority.
    let (s, body) = send_command(&client, &base, &sign(&b, work("res-x", "job-new", 6))).await;
    assert_eq!(s, 200, "case 8: newer epoch must be accepted: {body}");
    let fence = state
        .worker_gate
        .gate
        .current_fence("res-x")
        .unwrap()
        .unwrap();
    assert_eq!(fence.epoch, 6);
    assert_eq!(fence.authority_coordinator_id, b.id.node_id);

    // The deposed authority (epoch 5) is now stale on this resource.
    let (s, _) = send_command(&client, &base, &sign(&a, work("res-x", "job-a-late", 5))).await;
    assert_eq!(s, 409, "deposed authority must be fenced out");
}

#[tokio::test]
async fn case11_worker_restart_preserves_durable_fence() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator();
    let b = make_coordinator();
    let config = worker_config(tmp.path(), vec![a.pub_hex.clone(), b.pub_hex.clone()]);
    let client = reqwest::Client::new();

    // Phase 1: worker accepts authority A at epoch 7.
    {
        let (base, _state) = start_worker(config.clone()).await;
        let (s, _) = send_command(&client, &base, &sign(&a, grant("res-d", "l-d", "m-a", 7))).await;
        assert_eq!(s, 200);
    }
    // Worker process is gone here (server dropped).

    // Phase 2: worker restarts on the SAME durable database.
    {
        let (base, state) = start_worker(config.clone()).await;
        // The fence must have survived the restart.
        let fence = state
            .worker_gate
            .gate
            .current_fence("res-d")
            .unwrap()
            .unwrap();
        assert_eq!(fence.epoch, 7, "fence must survive worker restart");
        assert_eq!(fence.authority_coordinator_id, a.id.node_id);

        // Stale command after restart: still rejected.
        let (s, _) = send_command(&client, &base, &sign(&b, work("res-d", "job-b", 6))).await;
        assert_eq!(s, 409, "stale epoch must still be rejected after restart");
        // Same-epoch different authority after restart: still conflict.
        let (s, _) = send_command(&client, &base, &sign(&b, work("res-d", "job-b2", 7))).await;
        assert_eq!(s, 409, "authority conflict must survive restart");
    }
}

#[tokio::test]
async fn cases16_17_18_stale_authority_reconnect_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator(); // old authority
    let b = make_coordinator(); // new authority
    let (base, state) = start_worker(worker_config(
        tmp.path(),
        vec![a.pub_hex.clone(), b.pub_hex.clone()],
    ))
    .await;
    let client = reqwest::Client::new();

    // A is authority at epoch 4, then B takes over at epoch 5.
    send_command(&client, &base, &sign(&a, grant("res-s", "l-s", "m-a", 4))).await;
    let (s, _) = send_command(&client, &base, &sign(&b, work("res-s", "job-b", 5))).await;
    assert_eq!(s, 200, "new authority must be accepted");

    // Case 16/17: A reconnects and tries stale WORK at epoch 4.
    let (s, _) = send_command(&client, &base, &sign(&a, work("res-s", "job-a-stale", 4))).await;
    assert_eq!(s, 409, "case 17: stale work must be rejected");

    // Case 18: A cannot release B's lease state either.
    let (s, _) = send_command(&client, &base, &sign(&a, release("res-s", "l-s", "m-a", 4))).await;
    assert_eq!(s, 409, "case 18: stale lease release must be rejected");

    // A cannot grant a lease on an ALREADY-fenced resource at the stale
    // epoch — the worker must not honor a deposed authority's grant.
    let (s, _) = send_command(&client, &base, &sign(&a, grant("res-s", "l-s2", "m-a", 4))).await;
    assert_eq!(s, 409, "case 16: stale authority grant must be rejected");

    // Counters reflect the rejections (observational truth).
    let stale = state
        .worker_gate
        .counters
        .commands_rejected_stale_epoch_total
        .load(Ordering::SeqCst);
    assert!(stale >= 3, "stale rejections must be counted, got {stale}");
}

#[tokio::test]
async fn case22_duplicate_lease_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator();
    let (base, state) = start_worker(worker_config(tmp.path(), vec![a.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    let (s1, body1) = send_command(
        &client,
        &base,
        &sign(&a, grant("res-i", "lease-i", "m-i", 3)),
    )
    .await;
    assert_eq!(s1, 200);
    assert!(body1["outcome"].as_str().unwrap().contains("Applied"));

    // Re-delivery of the SAME grant (fresh envelope, same command).
    let (s2, body2) = send_command(
        &client,
        &base,
        &sign(&a, grant("res-i", "lease-i", "m-i", 3)),
    )
    .await;
    assert_eq!(s2, 200);
    assert!(
        body2["outcome"]
            .as_str()
            .unwrap()
            .contains("IdempotentDuplicate"),
        "duplicate grant must be idempotent, got {body2}"
    );

    // Exactly one lease row exists in the real SQLite store.
    assert!(state.leases.lease_by_id("lease-i").is_some());
    assert_eq!(state.leases.all_active_leases().len(), 1);
}

#[tokio::test]
async fn case23_duplicate_release_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator();
    let (base, state) = start_worker(worker_config(tmp.path(), vec![a.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    // Grant then release.
    let (s, _) = send_command(
        &client,
        &base,
        &sign(&a, grant("res-r", "lease-r", "m-r", 3)),
    )
    .await;
    assert_eq!(s, 200);
    let (s, body) = send_command(
        &client,
        &base,
        &sign(&a, release("res-r", "lease-r", "m-r", 3)),
    )
    .await;
    assert_eq!(s, 200, "release must succeed: {body}");

    // Duplicate release: no error, no state change. Either the applied-map
    // blocks the re-delivery (IdempotentDuplicate) or the executor sees the
    // already-terminal lease (LeaseAlreadyTerminal) — both are no-ops.
    let (s, body) = send_command(
        &client,
        &base,
        &sign(&a, release("res-r", "lease-r", "m-r", 3)),
    )
    .await;
    assert_eq!(s, 200);
    let outcome = body["outcome"].as_str().unwrap();
    assert!(
        outcome.contains("LeaseAlreadyTerminal") || outcome.contains("IdempotentDuplicate"),
        "duplicate release must be a no-op, got {body}"
    );

    // Lease is terminal in the real store and no longer active.
    let rec = state.leases.lease_by_id("lease-r").unwrap();
    assert_eq!(rec.state.as_str(), "RELEASED");
    assert!(state.leases.all_active_leases().is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Exactly-once behaviors (cases 28, 29)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case28_checkpoint_resumes_exactly_once() {
    let tmp = tempfile::tempdir().unwrap();
    let old_auth = make_coordinator(); // epoch 5
    let new_auth = make_coordinator(); // epoch 6
    let (base, state) = start_worker(worker_config(
        tmp.path(),
        vec![old_auth.pub_hex.clone(), new_auth.pub_hex.clone()],
    ))
    .await;
    let client = reqwest::Client::new();

    // Real checkpoint row in the worker's SQLite store.
    state
        .ops
        .record_checkpoint(
            "job-cp",
            "res-cp",
            Some("lease-cp"),
            Some("m-cp"),
            "ref",
            "{}",
        )
        .unwrap();

    // Old authority resumes at epoch 5: applied exactly once.
    let resume5 = FederationCommand::CheckpointResume {
        job_id: "job-cp".into(),
        resource_id: "res-cp".into(),
        epoch: 5,
    };
    let (s, body) = send_command(&client, &base, &sign(&old_auth, resume5.clone())).await;
    assert_eq!(s, 200, "first resume must apply: {body}");
    assert!(body["outcome"].as_str().unwrap().contains("Applied"));

    // Same authority re-delivers: idempotent, not executed twice.
    let (s, body) = send_command(&client, &base, &sign(&old_auth, resume5)).await;
    assert_eq!(s, 200);
    assert!(body["outcome"]
        .as_str()
        .unwrap()
        .contains("IdempotentDuplicate"));

    // New authority (epoch 6) takes over and also tries to resume.
    let resume6 = FederationCommand::CheckpointResume {
        job_id: "job-cp".into(),
        resource_id: "res-cp".into(),
        epoch: 6,
    };
    let (s, body) = send_command(&client, &base, &sign(&new_auth, resume6.clone())).await;
    assert_eq!(s, 200);
    assert!(
        body["outcome"]
            .as_str()
            .unwrap()
            .contains("CheckpointAlreadyConsumed"),
        "new authority must see the checkpoint already consumed: {body}"
    );

    // Old authority after takeover: fenced out entirely.
    let resume5b = FederationCommand::CheckpointResume {
        job_id: "job-cp".into(),
        resource_id: "res-cp".into(),
        epoch: 5,
    };
    let (s, _) = send_command(&client, &base, &sign(&old_auth, resume5b)).await;
    assert_eq!(s, 409, "old authority must be fenced out after takeover");

    // Exactly one resume happened in the real store.
    let row = state.ops.checkpoint("job-cp").unwrap().unwrap();
    assert!(row.consumed_at_ms.is_some());
    let resumes = state
        .worker_gate
        .counters
        .checkpoint_resumes_total
        .load(Ordering::SeqCst);
    let blocked = state
        .worker_gate
        .counters
        .checkpoint_double_resume_blocked_total
        .load(Ordering::SeqCst);
    assert_eq!(resumes, 1, "exactly one checkpoint resume");
    assert_eq!(
        blocked, 1,
        "the second (new-authority) resume must be blocked"
    );
}

#[tokio::test]
async fn case29_duplicate_work_assign_executes_once() {
    let tmp = tempfile::tempdir().unwrap();
    let a = make_coordinator();
    let (base, state) = start_worker(worker_config(tmp.path(), vec![a.pub_hex.clone()])).await;
    let client = reqwest::Client::new();

    let (s, _) = send_command(&client, &base, &sign(&a, work("res-w", "job-w", 2))).await;
    assert_eq!(s, 200);
    let (s, body) = send_command(&client, &base, &sign(&a, work("res-w", "job-w", 2))).await;
    assert_eq!(s, 200);
    assert!(body["outcome"]
        .as_str()
        .unwrap()
        .contains("IdempotentDuplicate"));

    // Real job registry holds exactly one assignment.
    assert_eq!(
        state.jobs.active_count(),
        1,
        "job must be assigned exactly once"
    );
    let blocked = state
        .worker_gate
        .counters
        .duplicate_commands_blocked_total
        .load(Ordering::SeqCst);
    assert!(blocked >= 1, "duplicate must be counted as blocked");
}

// ─────────────────────────────────────────────────────────────────────────────
// Partition policy gate (case 19)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn case19_partition_policy_fails_closed_for_non_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let config = worker_config(tmp.path(), vec![]);
    let state = AppState::new(config).unwrap();

    // Become an Active member with epoch 1 (single-coordinator join).
    {
        let id = nauti_nodes::state::identity_snapshot(&state).unwrap();
        let mut updated = (*id).clone();
        updated.join_federation(vec![]);
        nauti_nodes::state::install_identity(&state, updated);
    }

    // Alone: this coordinator IS the authority -> mutations allowed.
    let id = nauti_nodes::state::identity_snapshot(&state).unwrap();
    assert!(assert_may_mutate(&id, &state.authority).is_ok());

    // A peer with a HIGHER epoch is learned (real probe record). The local
    // coordinator is now partitioned away from authority and must fail
    // closed for all new ownership-changing operations.
    state.authority.record_peer_authority(AuthorityRecord {
        coordinator_id: "peer-authority".into(),
        epoch: 9,
        acquired_at_ms: 100,
    });
    let denial = assert_may_mutate(&id, &state.authority).unwrap_err();
    assert_eq!(
        denial,
        PartitionDenial::NotElectedAuthority {
            elected: "peer-authority".into()
        },
        "partitioned side must fail closed"
    );

    // A Joining (not yet Active) coordinator is denied outright.
    let tmp2 = tempfile::tempdir().unwrap();
    let state2 = AppState::new(worker_config(tmp2.path(), vec![])).unwrap();
    let id2 = nauti_nodes::state::identity_snapshot(&state2).unwrap();
    assert_eq!(
        assert_may_mutate(&id2, &state2.authority),
        Err(PartitionDenial::NotMutationCapable)
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Coordinator-side signed delivery over real HTTP (send-command path)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn coordinator_send_command_signs_and_delivers_over_real_http() {
    let tmp_w = tempfile::tempdir().unwrap();
    let tmp_c = tempfile::tempdir().unwrap();

    // Coordinator with a durable identity (persisted so the same key signs).
    let mut coord_config = Config {
        db_url: format!("sqlite:{}/coord.db", tmp_c.path().display()),
        forge_url: "http://127.0.0.1:1".to_string(),
        identity_path: format!("{}/coord-node.json", tmp_c.path().display()),
        coordinator_identity_path: Some(format!("{}/coord-id.json", tmp_c.path().display())),
        federation_peers: Vec::new(),
        trusted_coordinator_keys: Vec::new(),
        ..Default::default()
    };
    coord_config.admin_key = None;

    // Worker that trusts the coordinator's public key.
    let coord_node =
        NodeIdentity::load_or_create(std::path::Path::new(&coord_config.identity_path)).unwrap();
    let coord_pub = hex::encode(&coord_node.public_key);
    let (worker_base, _worker_state) =
        start_worker(worker_config(tmp_w.path(), vec![coord_pub])).await;

    // Coordinator server (real process-level router).
    let coord_state = AppState::new(coord_config.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = listener.local_addr().unwrap();
    let served = coord_state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router(served)).await.ok();
    });
    let coord_base = format!("http://{coord_addr}");

    // Ask the coordinator to sign+deliver a LeaseGrant to the worker.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{coord_base}/v1/federation/send-command"))
        .json(&serde_json::json!({
            "target_url": worker_base,
            "command": {
                "LeaseGrant": {
                    "lease_id": "lease-send",
                    "mission_id": "mission-send",
                    "resource_id": "res-send",
                    "epoch": 1u64,
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["accepted"], true,
        "worker must accept coordinator's signed command: {v}"
    );
    assert_eq!(v["delivered_status"], 200);
}
