//! Chunk 7: persistent fleet operations failure-injection suite.
//!
//! Real Tokio concurrency, real SQLite (file-backed), real HTTP over
//! loopback. Covers the 20 mandated failure/recovery cases:
//!
//!  1. coordinator restart with idle fleet
//!  2. coordinator restart during shared work (orphaned reservation refund)
//!  3. coordinator restart during active Forge lease (CRITICAL invariant)
//!  4. coordinator restart during lease recovery
//!  5. worker SIGKILL (abrupt heartbeat stop)
//!  6. heartbeat timeout
//!  7. worker reconnect with same identity
//!  8. runtime crash (worker-reported runtime down)
//!  9. runtime recovery
//! 10. lease-vs-work race (invariant proof)
//! 11. duplicate lease request (idempotent)
//! 12. conflicting lease request (explicit 409)
//! 13. duplicate release (idempotent)
//! 14. revoked node during lease (no restore)
//! 15. withdrawn node during lease (no restore)
//! 16. hardware changed after reconnect (mismatch fails closed)
//! 17. hardware mismatch after restart (STALE until fresh reconcile)
//! 18. accounting survives preemption + restart (no double refund)
//! 19. checkpoint survives coordinator restart
//! 20. stale state fails closed

use axum::Router;
use ed25519_dalek::SigningKey;
use nauti_nodes::{
    api::routes,
    config::Config,
    eligibility::LeaseState,
    fleet,
    node::auth::{sign_message, SignedHeartbeat, SignedJoinRequest},
    preemption::PreemptibleJob,
    state::AppState,
    types::now_ms,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

struct Env {
    _dir: tempfile::TempDir,
    config: Config,
}

fn file_env() -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("nauti.db");
    let mut config = Config::default();
    config.db_url = format!("sqlite:{}", db_path.display());
    config.forge_url = "http://127.0.0.1:1".to_string();
    config.strict_alpha = false;
    Env { _dir: dir, config }
}

async fn spawn_server(state: AppState) -> String {
    let app: Router = routes::router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    base
}

fn make_key() -> (SigningKey, String) {
    let key = SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());
    (key, pub_hex)
}

fn ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn register_worker(
    client: &reqwest::Client,
    base: &str,
    key: &SigningKey,
    pub_hex: &str,
    node_id: &str,
    gpu: &str,
    vram_mb: u32,
) -> reqwest::StatusCode {
    let mut req = SignedJoinRequest {
        node_id: node_id.to_string(),
        public_key_hex: pub_hex.to_string(),
        inference_url: "http://127.0.0.1:5999".into(),
        role: "thinker".into(),
        gpu_name: gpu.to_string(),
        vram_mb,
        models: vec!["thinker".into()],
        timestamp_secs: ts(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(key, &req.canonical_message());
    client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await
        .unwrap()
        .status()
}

async fn send_heartbeat(
    client: &reqwest::Client,
    base: &str,
    key: &SigningKey,
    node_id: &str,
    free_vram_mb: u32,
    tokens_per_sec: f64,
) -> serde_json::Value {
    let mut hb = SignedHeartbeat {
        node_id: node_id.to_string(),
        active_jobs: 0,
        free_vram_mb,
        tokens_per_sec,
        timestamp_secs: ts(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    hb.signature_hex = sign_message(key, &hb.canonical_message());
    client
        .post(format!("{base}/internal/worker/heartbeat"))
        .json(&hb)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn reserve(
    client: &reqwest::Client,
    base: &str,
    mission: &str,
    node: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&serde_json::json!({
            "mission_id": mission,
            "node_id": node,
            "vram_mb": 16384,
            "priority": 100,
            "allow_preemption": true
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.json().await.unwrap())
}

async fn release(
    client: &reqwest::Client,
    base: &str,
    mission: &str,
    node: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = client
        .post(format!("{base}/v1/forge/release"))
        .json(&serde_json::json!({ "mission_id": mission, "node_id": node }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.json().await.unwrap())
}

async fn inference_status(
    client: &reqwest::Client,
    base: &str,
    api_key: &str,
) -> reqwest::StatusCode {
    client
        .post(format!("{base}/v1/chat/completions"))
        .header("X-API-Key", api_key)
        .json(&serde_json::json!({
            "model": "thinker",
            "messages": [{"role": "user", "content": "chunk7 probe"}],
            "avoid_p100": false,
            "max_tokens": 64
        }))
        .send()
        .await
        .unwrap()
        .status()
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 1: coordinator restart with idle fleet
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_01_restart_idle_fleet() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some());

    // Restart: fresh AppState on the same DB. Workers are NOT assumed online.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let base2 = spawn_server(state2.clone()).await;
    let rt = state2.registry.get_node(&node_id).expect("node recovered");
    {
        let md = rt.metadata.read().unwrap();
        assert!(!md.online, "recovered node must start offline");
        assert_eq!(md.health_state, "OFFLINE");
        assert_eq!(
            md.runtime_state,
            nauti_nodes::eligibility::RuntimeState::Unknown
        );
    }
    assert!(
        fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none(),
        "recovered node must be ineligible until fresh truth"
    );

    // Fresh signed heartbeat re-establishes truth.
    send_heartbeat(&client, &base2, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 2: restart during shared work — orphaned reservation refunded once
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_02_restart_during_shared_work_refunds_once() {
    let env = file_env();
    let state = AppState::new(env.config.clone()).unwrap();
    let api_key = "chunk7-orphan-key";
    state.scheduler.set_balance(api_key, 10_000).unwrap();

    // Simulate an in-flight job at crash time: quota reserved + ledger ACTIVE.
    state.scheduler.reserve_tokens(api_key, 1_500).unwrap();
    state
        .ops
        .open_reservation("job-orphan", api_key, 1_500)
        .unwrap();
    assert_eq!(state.scheduler.check_quota(api_key, 8_501).is_err(), true);

    // Crash + restart.
    let state2 = AppState::new(env.config.clone()).unwrap();
    // Orphan refunded exactly once.
    assert!(state2.scheduler.check_quota(api_key, 10_000).is_ok());
    let row = state2.ops.reservation("job-orphan").unwrap().unwrap();
    assert_eq!(row.state, "REFUNDED");

    // Second restart must NOT double-refund.
    let state3 = AppState::new(env.config.clone()).unwrap();
    assert!(state3.scheduler.check_quota(api_key, 10_000).is_ok());
    assert!(
        state3.scheduler.check_quota(api_key, 10_001).is_err(),
        "balance must not exceed the original after double restart"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 3 (CRITICAL): restart during active Forge lease
// lease exists -> coordinator dies -> restart -> worker reconnects ->
// shared workload arrives -> MUST NOT be scheduled onto the leased GPU.
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_03_restart_during_active_lease_blocks_shared_work() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    let (st, res) = reserve(&client, &base, "mission-survive", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(res["lease_state"], "LEASED");
    assert!(state.leases.has_active_lease(&node_id));

    // Coordinator dies; restart on the same DB.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let base2 = spawn_server(state2.clone()).await;

    // Lease ownership truth survived.
    let lease = state2
        .leases
        .active_lease_for_node(&node_id)
        .expect("lease preserved");
    assert_eq!(lease.mission_id, "mission-survive");
    assert!(matches!(lease.state, LeaseState::Leased));

    // Worker reconnects (re-register + heartbeat) — must stay excluded.
    assert_eq!(
        register_worker(
            &client,
            &base2,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    let hb = send_heartbeat(&client, &base2, &key, &node_id, 16000, 25.0).await;
    assert_eq!(hb["leased"], true, "heartbeat must report lease truth");

    // Shared workload arrives: MUST NOT schedule.
    assert!(
        fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none(),
        "leased GPU must never be picked for shared work after restart"
    );
    state2.scheduler.set_balance("chunk7-key", 100_000).unwrap();
    assert_eq!(
        inference_status(&client, &base2, "chunk7-key").await,
        503,
        "shared inference must be refused while lease survives restart"
    );

    // Release completes recovery and the node returns to the shared pool.
    let (st, rel) = release(&client, &base2, "mission-survive", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(rel["released"], true);
    assert_eq!(rel["restored"], true);
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 4: restart during lease recovery (RELEASING interrupted)
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_04_restart_during_lease_recovery() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    let (_, res) = reserve(&client, &base, "mission-interrupted", &node_id).await;
    let lease_id = res["lease_id"].as_str().unwrap().to_string();

    // Simulate crash mid-release: lease stuck in RELEASING.
    {
        let mut rec = state.leases.lease_by_id(&lease_id).unwrap();
        rec.state = LeaseState::Releasing;
        rec.releasing_at_ms = Some(now_ms());
        state.ops.update_lease(&rec).unwrap();
    }

    // Restart: recovery not verified -> fail closed, still excluded.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let base2 = spawn_server(state2.clone()).await;
    assert!(state2.leases.has_active_lease(&node_id));
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none());

    // Operator re-drives the release. The node has not re-established truth
    // since the restart, so it must NOT be auto-restored (fail closed):
    // the lease clears, but the node stays excluded until it rejoins.
    let (st, rel) = release(&client, &base2, "mission-interrupted", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(rel["released"], true);
    assert_eq!(
        rel["restored"], false,
        "release must not restore a node whose truth was not re-established"
    );
    assert!(!state2.leases.has_active_lease(&node_id));
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none());

    // Worker re-establishes truth -> eligible again.
    assert_eq!(
        register_worker(
            &client,
            &base2,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base2, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 5: worker SIGKILL (abrupt heartbeat stop) with sweeper
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_05_worker_sigkill_sweeper_marks_offline() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;

    // Real sweeper with aggressive timing.
    nauti_nodes::node::heartbeat::spawn_sweeper_with_ops(
        state.registry.clone(),
        1,
        2,
        Some(state.ops.clone()),
    );

    // "SIGKILL" the worker: an aborted task sends no further heartbeats.
    let hb_client = client.clone();
    let hb_base = base.clone();
    let hb_key = key.clone();
    let hb_node = node_id.clone();
    let worker = tokio::spawn(async move {
        loop {
            send_heartbeat(&hb_client, &hb_base, &hb_key, &hb_node, 16000, 25.0).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.abort(); // abrupt death — no graceful withdraw

    // Wait for heartbeat silence to exceed the stale window.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(
        md.health_state, "OFFLINE",
        "sweeper must mark killed worker offline"
    );
    assert!(!md.online);
    drop(md);
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none());

    // Journal recorded the offline transition.
    let events = state
        .ops
        .events(Some(&node_id), None, None, Some("NODE_OFFLINE"), 10)
        .unwrap();
    assert!(!events.is_empty(), "NODE_OFFLINE event must be journaled");
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 6: heartbeat timeout (direct stale transition)
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_06_heartbeat_timeout() {
    let env = file_env();
    let state = AppState::new(env.config.clone()).unwrap();
    let node_id = "stale-node";
    let meta = nauti_nodes::types::NodeMetadata {
        id: node_id.into(),
        health_state: "ONLINE".into(),
        online: true,
        ..Default::default()
    };
    let rt = state.registry.register_node(meta);
    rt.last_heartbeat.store(
        now_ms().saturating_sub(120_000),
        std::sync::atomic::Ordering::Relaxed,
    );

    let changed = state.registry.remove_stale(30_000, now_ms());
    assert_eq!(changed, vec![node_id.to_string()]);
    let md = rt.metadata.read().unwrap();
    assert_eq!(md.health_state, "OFFLINE");
    assert!(!md.online);
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 7: worker reconnect with same identity
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_07_worker_reconnect_same_identity() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );

    // Silence -> offline.
    state
        .registry
        .get_node(&node_id)
        .unwrap()
        .last_heartbeat
        .store(
            now_ms().saturating_sub(120_000),
            std::sync::atomic::Ordering::Relaxed,
        );
    state.registry.remove_stale(30_000, now_ms());

    // Reconnect with the SAME identity.
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );

    // Identity continuity: exactly one registry record, same public key.
    assert_eq!(state.registry.list_nodes().len(), 1);
    let rt = state.registry.get_node(&node_id).unwrap();
    let md = rt.metadata.read().unwrap();
    assert_eq!(hex::encode(&md.public_key), pub_hex);
    assert_eq!(md.health_state, "ONLINE");
    drop(md);

    // Reconnect journaled.
    let events = state
        .ops
        .events(Some(&node_id), None, None, Some("NODE_RECONNECTED"), 10)
        .unwrap();
    assert!(!events.is_empty(), "NODE_RECONNECTED must be journaled");
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 8: runtime crash (worker heartbeats runtime-down truth)
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_08_runtime_crash_removes_eligibility() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some());

    // Runtime dies; worker stays up and truthfully reports zero stats.
    send_heartbeat(&client, &base, &key, &node_id, 0, 0.0).await;

    let rt = state.registry.get_node(&node_id).unwrap();
    assert_eq!(
        rt.metadata.read().unwrap().runtime_state,
        nauti_nodes::eligibility::RuntimeState::Unavailable
    );
    assert!(
        fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none(),
        "runtime-down node must not receive new work even though heartbeat is live"
    );
    let events = state
        .ops
        .events(Some(&node_id), None, None, Some("RUNTIME_FAILED"), 10)
        .unwrap();
    assert!(!events.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 9: runtime recovery re-verifies truth before eligibility
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_09_runtime_recovery_restores_eligibility() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 0, 0.0).await; // down
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none());

    // Runtime recovers; worker reports live stats again.
    send_heartbeat(&client, &base, &key, &node_id, 15000, 24.0).await;
    let rt = state.registry.get_node(&node_id).unwrap();
    assert_eq!(
        rt.metadata.read().unwrap().runtime_state,
        nauti_nodes::eligibility::RuntimeState::Ready
    );
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 10: lease-vs-work race — invariant proof under real concurrency
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_10_lease_work_race_invariant() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    state.scheduler.set_balance("race-key", 10_000_000).unwrap();

    let mut blocked_assignments = 0u64;
    let mut granted_leases = 0u64;

    for i in 0..20 {
        let mission = format!("race-mission-{i}");

        // Concurrent shared-inference attempts during the reserve window.
        let mut inflight = Vec::new();
        for _ in 0..6 {
            let c = client.clone();
            let b = base.clone();
            inflight.push(tokio::spawn(async move {
                inference_status(&c, &b, "race-key").await
            }));
        }
        // Concurrent direct assignment confirmations.
        let rt = state.registry.get_node(&node_id).unwrap();
        let rt_clone = rt.clone();
        let assign_handle = tokio::task::spawn_blocking(move || {
            let mut confirmed = 0;
            for _ in 0..200 {
                if rt_clone.try_assign() {
                    confirmed += 1;
                    rt_clone.release_assignment();
                }
            }
            confirmed
        });

        let (st, _res) = reserve(&client, &base, &mission, &node_id).await;
        assert_eq!(st, 200);
        granted_leases += 1;

        // INVARIANT: with an active lease, no shared assignment can confirm.
        assert!(state.leases.has_active_lease(&node_id));
        assert!(
            fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none(),
            "iteration {i}: leased GPU picked for shared work — INVARIANT VIOLATION"
        );
        assert!(
            !rt.try_assign(),
            "iteration {i}: assignment confirmed on leased GPU — INVARIANT VIOLATION"
        );

        for h in inflight {
            let status = h.await.unwrap();
            assert!(
                status == 503 || status == 200,
                "unexpected inference status {status}"
            );
        }
        let _confirmed_before_grant = assign_handle.await.unwrap();

        // Release and verify the pool returns.
        let (st, rel) = release(&client, &base, &mission, &node_id).await;
        assert_eq!(st, 200);
        assert_eq!(rel["released"], true);
        assert!(!state.leases.has_active_lease(&node_id));
        send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
        assert!(
            fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some(),
            "iteration {i}: node must return to the shared pool after release"
        );
    }

    let conflicts = state
        .metrics
        .ownership_conflicts_blocked_total
        .load(std::sync::atomic::Ordering::Relaxed);
    blocked_assignments += conflicts;
    assert_eq!(granted_leases, 20);
    // Blocked assignments are the race guard working; double-ownership never
    // occurred because every post-grant probe above failed closed.
    let events = state
        .ops
        .events(None, None, None, Some("OWNERSHIP_CONFLICT_BLOCKED"), 1000)
        .unwrap();
    assert_eq!(events.len() as u64, blocked_assignments);
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 11: duplicate lease request is idempotent
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_11_duplicate_lease_request_idempotent() {
    let env = file_env();
    let client = reqwest::Client::new();
    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;

    let (st1, r1) = reserve(&client, &base, "mission-dup", "node-dup").await;
    let (st2, r2) = reserve(&client, &base, "mission-dup", "node-dup").await;
    assert_eq!(st1, 200);
    assert_eq!(st2, 200);
    assert_eq!(r1["lease_id"], r2["lease_id"], "same lease id on duplicate");
    assert_eq!(r2["idempotent"], true);
    // No double drain: only one lease row for the mission.
    assert_eq!(state.leases.all_active_leases().len(), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 12: conflicting lease request fails explicitly
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_12_conflicting_lease_request_409() {
    let env = file_env();
    let client = reqwest::Client::new();
    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;

    let (st1, _) = reserve(&client, &base, "mission-a", "node-conflict").await;
    assert_eq!(st1, 200);
    let resp = client
        .post(format!("{base}/v1/forge/reserve"))
        .json(&serde_json::json!({
            "mission_id": "mission-b",
            "node_id": "node-conflict",
            "vram_mb": 16384,
            "priority": 100,
            "allow_preemption": true
        }))
        .send()
        .await
        .unwrap();
    let st2 = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(
        st2, 409,
        "different mission on leased GPU must fail explicitly"
    );
    assert!(
        body.contains("mission-a"),
        "conflict must name the holding mission: {body}"
    );
    let _ = (st2, body);
    // Conflict journaled; original lease untouched.
    let events = state
        .ops
        .events(None, None, None, Some("LEASE_CONFLICT"), 10)
        .unwrap();
    assert!(!events.is_empty());
    let lease = state.leases.active_lease_for_node("node-conflict").unwrap();
    assert_eq!(lease.mission_id, "mission-a");
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 13: duplicate release is idempotent
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_13_duplicate_release_idempotent() {
    let env = file_env();
    let client = reqwest::Client::new();
    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;

    let (_, _r) = reserve(&client, &base, "mission-rel", "node-rel").await;
    let (st1, rel1) = release(&client, &base, "mission-rel", "node-rel").await;
    let (st2, rel2) = release(&client, &base, "mission-rel", "node-rel").await;
    assert_eq!(st1, 200);
    assert_eq!(st2, 200);
    assert_eq!(rel1["released"], true);
    assert_eq!(rel2["released"], false, "second release is a no-op");
    assert!(!state.leases.has_active_lease("node-rel"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 14: revoked node during lease — no restore on release
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_14_revoked_node_during_lease_no_restore() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    let (st, _) = reserve(&client, &base, "mission-rev", &node_id).await;
    assert_eq!(st, 200);

    // Operator revokes mid-lease.
    let rev = client
        .post(format!("{base}/v1/admin/revoke"))
        .json(&serde_json::json!({"identifier": node_id, "reason": "abuse during lease"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rev.status(), 200);

    let (st, rel) = release(&client, &base, "mission-rev", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(rel["released"], true);
    assert_eq!(
        rel["restored"], false,
        "revoked node must NOT return to the pool"
    );
    let md = state.registry.get_node(&node_id).unwrap();
    assert_eq!(md.metadata.read().unwrap().health_state, "REVOKED");
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 15: withdrawn node during lease — no restore on release
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_15_withdrawn_node_during_lease_no_restore() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    let (st, _) = reserve(&client, &base, "mission-wd", &node_id).await;
    assert_eq!(st, 200);

    // Worker withdraws mid-lease (signed).
    let mut wreq = nauti_nodes::node::auth::SignedWithdrawRequest {
        node_id: node_id.clone(),
        timestamp_secs: ts(),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    wreq.signature_hex = sign_message(&key, &wreq.canonical_message());
    let w = client
        .post(format!("{base}/internal/worker/withdraw"))
        .json(&wreq)
        .send()
        .await
        .unwrap();
    assert_eq!(w.status(), 200);

    let (st, rel) = release(&client, &base, "mission-wd", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(rel["released"], true);
    assert_eq!(
        rel["restored"], false,
        "withdrawn node must NOT return to the pool"
    );
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 16: hardware changed after reconnect -> mismatch fails closed
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_16_hardware_changed_after_reconnect() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "Tesla P100-PCIE-16GB",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_some());

    // Reconnect with a DIFFERENT hardware claim.
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "NVIDIA RTX 3090",
            24576
        )
        .await,
        200
    );

    // Observer evidence still reports the original GPU -> MISMATCH.
    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.observed = Some(nauti_nodes::types::ObservedHardware {
            gpu_name: "Tesla P100-PCIE-16GB".into(),
            vram_mb: 16384,
            vendor: "NVIDIA".into(),
            device_count: 1,
            driver_version: None,
            temperature_c: None,
            node_id: node_id.clone(),
            observer_node_id: "forge".into(),
            source_type: nauti_nodes::types::TelemetrySourceType::RemoteLiveObservation,
            provider: "all-smi".into(),
            observed_at: Some(now_ms()),
            received_at: Some(now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
        });
        md.reconciliation = nauti_nodes::types::reconcile_claim(&md.claim, &md.observed);
    }
    assert_eq!(
        rt.metadata.read().unwrap().reconciliation.status,
        nauti_nodes::types::ReconciliationStatus::MISMATCH
    );
    assert!(
        fleet::pick_node_for_role(&state.registry, Some("thinker"), false).is_none(),
        "hardware mismatch must fail closed"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 17: restart degrades MATCH reconciliation to STALE until fresh truth
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_17_hardware_stale_after_restart() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    // Establish MATCH truth and persist it.
    let rt = state.registry.get_node(&node_id).unwrap();
    {
        let mut md = rt.metadata.write().unwrap();
        md.reconciliation.status = nauti_nodes::types::ReconciliationStatus::MATCH;
    }
    nauti_nodes::state::persist_node(&state.ops, &rt);

    // Restart.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let base2 = spawn_server(state2.clone()).await;
    let rt2 = state2.registry.get_node(&node_id).unwrap();
    assert_eq!(
        rt2.metadata.read().unwrap().reconciliation.status,
        nauti_nodes::types::ReconciliationStatus::STALE,
        "MATCH must degrade to STALE across restart (hardware not assumed unchanged)"
    );
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none());

    // Fresh heartbeat re-reconciles and restores eligibility.
    send_heartbeat(&client, &base2, &key, &node_id, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 18: accounting survives preemption + restart (no double refund)
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_18_accounting_survives_preemption_and_restart() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;

    let api_key = "chunk7-accounting";
    state.scheduler.set_balance(api_key, 50_000).unwrap();

    // In-flight shared job: quota reserved, ledger ACTIVE, job registered.
    state.scheduler.reserve_tokens(api_key, 2_000).unwrap();
    state
        .ops
        .open_reservation("job-preempted", api_key, 2_000)
        .unwrap();
    state
        .preemption
        .register_job(PreemptibleJob::new("job-preempted", node_id.clone(), true));
    let (tx, _rx) = tokio::sync::broadcast::channel(8);
    let record = Arc::new(nauti_nodes::jobs::JobRecord {
        node_id: node_id.clone(),
        inference_url: "http://127.0.0.1:5999".into(),
        model: "thinker".into(),
        messages: vec![],
        api_key: Some(api_key.to_string()),
        reserved_tokens: 2_000,
        cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tx,
        done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    state.jobs.insert("job-preempted".into(), record);

    // Forge lease preempts the job: refund exactly once.
    let (st, res) = reserve(&client, &base, "mission-accounting", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(res["jobs_preempted"], 1);
    assert!(state.scheduler.check_quota(api_key, 50_000).is_ok());
    let row = state.ops.reservation("job-preempted").unwrap().unwrap();
    assert_eq!(row.state, "PREEMPTED");
    assert_eq!(row.released_tokens, 2_000);

    // Restart: PREEMPTED rows are NOT refunded again.
    let state2 = AppState::new(env.config.clone()).unwrap();
    assert!(state2.scheduler.check_quota(api_key, 50_000).is_ok());
    assert!(
        state2.scheduler.check_quota(api_key, 50_001).is_err(),
        "double refund detected"
    );
    let row2 = state2.ops.reservation("job-preempted").unwrap().unwrap();
    assert_eq!(row2.state, "PREEMPTED");
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 19: checkpoint survives coordinator restart
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_19_checkpoint_survives_restart() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    state
        .preemption
        .register_job(PreemptibleJob::new("job-cp7", node_id.clone(), true));
    let (st, res) = reserve(&client, &base, "mission-cp7", &node_id).await;
    assert_eq!(st, 200);
    assert_eq!(res["jobs_checkpointed"], 1);

    // Restart.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let cp = state2
        .ops
        .checkpoint("job-cp7")
        .unwrap()
        .expect("checkpoint must survive restart");
    assert_eq!(cp.node_id, node_id);
    assert_eq!(cp.mission_id.as_deref(), Some("mission-cp7"));
    assert!(
        cp.lease_id.is_some(),
        "checkpoint must keep lease association"
    );
    assert!(cp.consumed_at_ms.is_none());

    // Resume consumes exactly once.
    assert!(state2.ops.consume_checkpoint("job-cp7").unwrap());
    assert!(!state2.ops.consume_checkpoint("job-cp7").unwrap());
}

// ─────────────────────────────────────────────────────────────────────────────
// Case 20: stale state fails closed (recovered node + stale lease guard)
// ─────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn case_20_stale_state_fails_closed() {
    let env = file_env();
    let client = reqwest::Client::new();
    let (key, pub_hex) = make_key();
    let node_id = uuid::Uuid::new_v4().to_string();

    let state = AppState::new(env.config.clone()).unwrap();
    let base = spawn_server(state.clone()).await;
    assert_eq!(
        register_worker(
            &client,
            &base,
            &key,
            &pub_hex,
            &node_id,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base, &key, &node_id, 16000, 25.0).await;
    let (_, _r) = reserve(&client, &base, "mission-stale", &node_id).await;

    // Restart with an active lease: recovered node record + lease overlay.
    let state2 = AppState::new(env.config.clone()).unwrap();
    let base2 = spawn_server(state2.clone()).await;

    // Before ANY fresh heartbeat: fail closed.
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none());
    let rt = state2.registry.get_node(&node_id).unwrap();
    assert!(
        !rt.try_assign(),
        "stale recovered node must refuse assignment"
    );

    // Even a fresh heartbeat cannot break the lease exclusion.
    let hb = send_heartbeat(&client, &base2, &key, &node_id, 16000, 25.0).await;
    assert_eq!(hb["leased"], true);
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none());

    // Stale heartbeat on a shared node also fails closed.
    let (key2, pub2) = make_key();
    let node2 = uuid::Uuid::new_v4().to_string();
    assert_eq!(
        register_worker(
            &client,
            &base2,
            &key2,
            &pub2,
            &node2,
            "RTX 2060 SUPER",
            16384
        )
        .await,
        200
    );
    send_heartbeat(&client, &base2, &key2, &node2, 16000, 25.0).await;
    assert!(fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_some());
    state2
        .registry
        .get_node(&node2)
        .unwrap()
        .last_heartbeat
        .store(
            now_ms().saturating_sub(120_000),
            std::sync::atomic::Ordering::Relaxed,
        );
    assert!(
        fleet::pick_node_for_role(&state2.registry, Some("thinker"), false).is_none(),
        "stale-heartbeat node must fail closed"
    );
}
