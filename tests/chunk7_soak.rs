//! Chunk 7 soak harness (Rust-first).
//!
//! Duration is configurable via `NAUTI_SOAK_DURATION_SECS`:
//!   - default 5s  (CI smoke)
//!   - 3600        (1-hour local validation)
//!   - 28800       (overnight operator soak)
//!
//! The soak repeatedly exercises: worker heartbeats, inference jobs,
//! runtime up/down, worker disconnect/reconnect, Forge lease/release,
//! coordinator restart, accounting and the event journal.
//!
//! End-of-run invariant summary MUST report:
//!   ownership_conflicts = 0
//!   accounting_inconsistencies = 0

use axum::{routing::get, routing::post, Router};
use ed25519_dalek::SigningKey;
use nauti_nodes::{
    api::routes,
    config::Config,
    eligibility, fleet,
    node::auth::{sign_message, SignedHeartbeat, SignedJoinRequest},
    state::AppState,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

// ── Mock inference runtime (real HTTP server, OpenAI-compatible SSE) ────────

async fn mock_health() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

async fn mock_chat() -> (
    axum::http::StatusCode,
    [(&'static str, &'static str); 1],
    String,
) {
    let body = "data: {\"choices\":[{\"delta\":{\"content\":\"soak-token \"}}]}\n\n\
                data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n\
                data: [DONE]\n\n"
        .to_string();
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/event-stream")],
        body,
    )
}

async fn spawn_mock_runtime() -> String {
    let app = Router::new()
        .route("/health", get(mock_health))
        .route("/v1/chat/completions", post(mock_chat));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn make_key() -> (SigningKey, String) {
    let key = SigningKey::generate(&mut rand::rngs::OsRng);
    let pub_hex = hex::encode(key.verifying_key().to_bytes());
    (key, pub_hex)
}

fn soak_config(db_url: &str) -> Config {
    let mut config = Config::default();
    config.db_url = db_url.to_string();
    config.forge_url = "http://127.0.0.1:1".to_string();
    config.strict_alpha = false;
    config
}

async fn spawn_coordinator(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
    let app: Router = routes::router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base, handle)
}

async fn register(
    client: &reqwest::Client,
    base: &str,
    key: &SigningKey,
    pub_hex: &str,
    node_id: &str,
    runtime_url: &str,
) {
    let mut req = SignedJoinRequest {
        node_id: node_id.to_string(),
        public_key_hex: pub_hex.to_string(),
        inference_url: runtime_url.to_string(),
        role: "thinker".into(),
        gpu_name: "SOAK-GPU".to_string(),
        vram_mb: 16384,
        models: vec!["thinker".into()],
        timestamp_secs: ts(),
        nonce: uuid::Uuid::new_v4().to_string(),
        protocol_version: 1,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(key, &req.canonical_message());
    let _ = client
        .post(format!("{base}/internal/worker/register"))
        .json(&req)
        .send()
        .await;
}

// ── Soak test ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunk7_soak_invariants() {
    let duration_secs: u64 = std::env::var("NAUTI_SOAK_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        .max(3);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);

    let dir = tempfile::tempdir().expect("tempdir");
    let db_url = format!("sqlite:{}", dir.path().join("soak.db").display());

    let runtime_url = spawn_mock_runtime().await;
    let client = reqwest::Client::new();

    // Counters for the final summary.
    let jobs_submitted = Arc::new(AtomicU64::new(0));
    let jobs_refused = Arc::new(AtomicU64::new(0));
    let lease_cycles = Arc::new(AtomicU64::new(0));
    let worker_reconnects = Arc::new(AtomicU64::new(0));
    let runtime_crashes = Arc::new(AtomicU64::new(0));
    let coordinator_restarts = Arc::new(AtomicU64::new(0));
    let ownership_conflicts = Arc::new(AtomicU64::new(0));

    // Shared coordinator base URL (changes across restarts).
    let state1 = AppState::new(soak_config(&db_url)).unwrap();
    let metrics = state1.metrics.clone();
    let (base, server_handle) = spawn_coordinator(state1.clone()).await;
    let base_url = Arc::new(RwLock::new(base));

    const WORKERS: usize = 3;
    let mut worker_ids = Vec::new();
    let mut worker_keys = Vec::new();
    let alive: Vec<Arc<AtomicBool>> = (0..WORKERS)
        .map(|_| Arc::new(AtomicBool::new(true)))
        .collect();
    let runtime_down: Vec<Arc<AtomicBool>> = (0..WORKERS)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect();

    // Register workers.
    for i in 0..WORKERS {
        let (key, pub_hex) = make_key();
        let node_id = format!("soak-worker-{i}");
        register(
            &client,
            &base_url.read().await,
            &key,
            &pub_hex,
            &node_id,
            &runtime_url,
        )
        .await;
        worker_ids.push(node_id);
        worker_keys.push((key, pub_hex));
    }
    let registry = state1.registry.clone();
    let leases = state1.leases.clone();

    // Heartbeat tasks.
    let mut hb_handles = Vec::new();
    for i in 0..WORKERS {
        let client = client.clone();
        let base_url = base_url.clone();
        let key = worker_keys[i].0.clone();
        let node_id = worker_ids[i].clone();
        let alive = alive[i].clone();
        let down = runtime_down[i].clone();
        hb_handles.push(tokio::spawn(async move {
            loop {
                if alive.load(Ordering::Relaxed) {
                    let base = base_url.read().await.clone();
                    let (free_vram, tps) = if down.load(Ordering::Relaxed) {
                        (0, 0.0)
                    } else {
                        (15000, 25.0)
                    };
                    let mut hb = SignedHeartbeat {
                        node_id: node_id.clone(),
                        active_jobs: 0,
                        free_vram_mb: free_vram,
                        tokens_per_sec: tps,
                        timestamp_secs: ts(),
                        nonce: uuid::Uuid::new_v4().to_string(),
                        signature_hex: String::new(),
                    };
                    hb.signature_hex = sign_message(&key, &hb.canonical_message());
                    let _ = client
                        .post(format!("{base}/internal/worker/heartbeat"))
                        .json(&hb)
                        .send()
                        .await;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }));
    }

    // Shared API key balance for accounting audit.
    const API_KEY: &str = "soak-key";
    const INITIAL_BALANCE: u64 = 100_000_000;
    state1
        .scheduler
        .set_balance(API_KEY, INITIAL_BALANCE)
        .unwrap();

    // Deterministic lease cycle up front: guarantees at least one real
    // grant/release is exercised regardless of chaos timing.
    {
        let base = base_url.read().await.clone();
        let resp = client
            .post(format!("{base}/v1/forge/reserve"))
            .json(&serde_json::json!({
                "mission_id": "soak-baseline-mission",
                "node_id": worker_ids[0],
                "vram_mb": 16384,
                "priority": 50,
                "allow_preemption": true
            }))
            .send()
            .await
            .expect("baseline reserve");
        assert_eq!(resp.status(), 200, "baseline lease reserve must succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rel = client
            .post(format!("{base}/v1/forge/release"))
            .json(&serde_json::json!({
                "mission_id": "soak-baseline-mission",
                "node_id": worker_ids[0]
            }))
            .send()
            .await
            .expect("baseline release");
        assert_eq!(rel.status(), 200, "baseline lease release must succeed");
        lease_cycles.fetch_add(1, Ordering::Relaxed);
    }

    // Job submission task.
    let job_client = client.clone();
    let job_base = base_url.clone();
    let jobs_submitted_c = jobs_submitted.clone();
    let jobs_refused_c = jobs_refused.clone();
    let job_handle = tokio::spawn(async move {
        loop {
            let base = job_base.read().await.clone();
            let resp = job_client
                .post(format!("{base}/v1/chat/completions"))
                .header("X-API-Key", API_KEY)
                .json(&serde_json::json!({
                    "model": "thinker",
                    "messages": [{"role": "user", "content": "soak"}],
                    "avoid_p100": false,
                    "max_tokens": 32,
                    "stream": false
                }))
                .send()
                .await;
            match resp {
                Ok(r) => {
                    if r.status() == 200 {
                        jobs_submitted_c.fetch_add(1, Ordering::Relaxed);
                    } else {
                        jobs_refused_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    jobs_refused_c.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    });

    // Chaos controller: disconnects, runtime crashes, lease cycles, restart.
    let mut restart_done = false;
    let mut iteration = 0u64;
    let mut current_server = Some(server_handle);
    let mut current_state = state1.clone();
    let first_metrics = metrics.clone();

    while Instant::now() < deadline {
        iteration += 1;
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Mid-soak coordinator restart (crash + recover on same DB).
        let restart_at = Duration::from_secs(duration_secs.max(6) * 2 / 5);
        if !restart_done && start.elapsed() > restart_at {
            if let Some(h) = current_server.take() {
                h.abort(); // crash
            }
            let new_state = AppState::new(soak_config(&db_url)).unwrap();
            let (new_base, new_handle) = spawn_coordinator(new_state.clone()).await;
            *base_url.write().await = new_base;
            current_server = Some(new_handle);
            current_state = new_state;
            coordinator_restarts.fetch_add(1, Ordering::Relaxed);
            restart_done = true;
            // Workers re-register against the new coordinator.
            for i in 0..WORKERS {
                let base = base_url.read().await.clone();
                register(
                    &client,
                    &base,
                    &worker_keys[i].0,
                    &worker_keys[i].1,
                    &worker_ids[i],
                    &runtime_url,
                )
                .await;
                worker_reconnects.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }

        let action = iteration % 4;
        match action {
            0 => {
                // Worker disconnect/reconnect.
                let w = (iteration as usize / 4) % WORKERS;
                alive[w].store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(300)).await;
                alive[w].store(true, Ordering::Relaxed);
                let base = base_url.read().await.clone();
                register(
                    &client,
                    &base,
                    &worker_keys[w].0,
                    &worker_keys[w].1,
                    &worker_ids[w],
                    &runtime_url,
                )
                .await;
                worker_reconnects.fetch_add(1, Ordering::Relaxed);
            }
            1 => {
                // Runtime crash + recovery on one worker.
                let w = (iteration as usize / 4) % WORKERS;
                runtime_down[w].store(true, Ordering::Relaxed);
                runtime_crashes.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(300)).await;
                runtime_down[w].store(false, Ordering::Relaxed);
            }
            2 => {
                // Full lease cycle on one worker.
                let w = (iteration as usize / 4) % WORKERS;
                let base = base_url.read().await.clone();
                let mission = format!("soak-mission-{iteration}");
                let reserve = client
                    .post(format!("{base}/v1/forge/reserve"))
                    .json(&serde_json::json!({
                        "mission_id": mission,
                        "node_id": worker_ids[w],
                        "vram_mb": 16384,
                        "priority": 50,
                        "allow_preemption": true
                    }))
                    .send()
                    .await;
                if let Ok(r) = reserve {
                    if r.status() == 200 {
                        lease_cycles.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let _ = client
                            .post(format!("{base}/v1/forge/release"))
                            .json(&serde_json::json!({
                                "mission_id": mission,
                                "node_id": worker_ids[w]
                            }))
                            .send()
                            .await;
                    }
                }
            }
            _ => {}
        }

        // CONTINUOUS INVARIANT CHECK: a node with an active lease must never
        // be eligible for shared work.
        let reg = current_state.registry.clone();
        for rt in reg.list_nodes() {
            let node_id = rt.metadata.read().map(|m| m.id.clone()).unwrap_or_default();
            if current_state.leases.has_active_lease(&node_id) {
                let eligible =
                    eligibility::evaluate_node(&rt, nauti_nodes::types::now_ms(), 45_000).is_ok();
                if eligible {
                    ownership_conflicts.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        let _ = (registry.clone(), leases.clone());
    }

    // Shutdown workers + job task, then let in-flight jobs quiesce so the
    // accounting audit does not race a settlement.
    for h in hb_handles {
        h.abort();
    }
    job_handle.abort();
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Final invariant: pick must agree with lease truth.
    for rt in current_state.registry.list_nodes() {
        let node_id = rt.metadata.read().map(|m| m.id.clone()).unwrap_or_default();
        if current_state.leases.has_active_lease(&node_id)
            && fleet::pick_node_for_role(&current_state.registry, Some("thinker"), false)
                .map(|p| p.metadata.read().map(|m| m.id.clone()).unwrap_or_default())
                == Some(node_id)
        {
            ownership_conflicts.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ACCOUNTING AUDIT: balance == initial - reserved + released (ledger).
    // The ledger is read from the FINAL coordinator's store (same DB file).
    let final_ops = current_state.ops.clone();
    let mut reserved_total = 0u64;
    let mut released_total = 0u64;
    for row in final_ops.all_reservations().unwrap() {
        reserved_total += row.reserved_tokens;
        released_total += row.released_tokens;
    }
    let expected_balance = INITIAL_BALANCE - reserved_total + released_total;
    let ok_at_expected = current_state
        .scheduler
        .check_quota(API_KEY, expected_balance)
        .is_ok();
    let ok_above_expected = current_state
        .scheduler
        .check_quota(API_KEY, expected_balance + 1)
        .is_ok();
    let final_metrics = current_state.metrics.clone();
    let accounting_inconsistencies = first_metrics
        .accounting_inconsistencies_total
        .load(Ordering::Relaxed)
        + final_metrics
            .accounting_inconsistencies_total
            .load(Ordering::Relaxed)
        + if ok_at_expected && !ok_above_expected {
            0
        } else {
            1
        };
    // Lease grant/release totals across coordinator incarnations.
    let total_leases_granted = if Arc::ptr_eq(&first_metrics, &final_metrics) {
        final_metrics.leases_granted_total.load(Ordering::Relaxed)
    } else {
        first_metrics.leases_granted_total.load(Ordering::Relaxed)
            + final_metrics.leases_granted_total.load(Ordering::Relaxed)
    };
    let total_leases_released = if Arc::ptr_eq(&first_metrics, &final_metrics) {
        final_metrics.leases_released_total.load(Ordering::Relaxed)
    } else {
        first_metrics.leases_released_total.load(Ordering::Relaxed)
            + final_metrics.leases_released_total.load(Ordering::Relaxed)
    };

    let summary = serde_json::json!({
        "duration_secs": start.elapsed().as_secs(),
        "jobs_submitted": jobs_submitted.load(Ordering::Relaxed),
        "jobs_refused": jobs_refused.load(Ordering::Relaxed),
        "jobs_completed": first_metrics.completed_work_units.load(Ordering::Relaxed)
            + final_metrics.completed_work_units.load(Ordering::Relaxed),
        "jobs_failed": first_metrics.failed_work_units.load(Ordering::Relaxed)
            + final_metrics.failed_work_units.load(Ordering::Relaxed),
        "jobs_preempted": first_metrics.jobs_preempted_total.load(Ordering::Relaxed)
            + final_metrics.jobs_preempted_total.load(Ordering::Relaxed),
        "jobs_requeued": first_metrics.jobs_requeued_total.load(Ordering::Relaxed)
            + final_metrics.jobs_requeued_total.load(Ordering::Relaxed),
        "lease_cycles": lease_cycles.load(Ordering::Relaxed),
        "leases_granted": total_leases_granted,
        "leases_released": total_leases_released,
        "worker_reconnects": worker_reconnects.load(Ordering::Relaxed),
        "runtime_crashes": runtime_crashes.load(Ordering::Relaxed),
        "runtime_failures_observed": first_metrics.runtime_failures_total.load(Ordering::Relaxed)
            + final_metrics.runtime_failures_total.load(Ordering::Relaxed),
        "coordinator_restarts": coordinator_restarts.load(Ordering::Relaxed),
        "ownership_conflicts": ownership_conflicts.load(Ordering::Relaxed),
        "ownership_assignments_blocked": first_metrics
            .ownership_conflicts_blocked_total
            .load(Ordering::Relaxed)
            + final_metrics
                .ownership_conflicts_blocked_total
                .load(Ordering::Relaxed),
        "accounting_inconsistencies": accounting_inconsistencies,
        "accounting_expected_balance": expected_balance,
        "stale_nodes": current_state
            .registry
            .list_nodes()
            .iter()
            .filter(|rt| rt.metadata.read().map(|m| m.health_state == "STALE").unwrap_or(false))
            .count(),
    });
    println!(
        "CHUNK7 SOAK SUMMARY:\n{}",
        serde_json::to_string_pretty(&summary).unwrap()
    );

    assert_eq!(
        ownership_conflicts.load(Ordering::Relaxed),
        0,
        "OWNERSHIP INVARIANT VIOLATED: leased GPU was eligible for shared work"
    );
    assert_eq!(
        accounting_inconsistencies, 0,
        "ACCOUNTING INVARIANT VIOLATED: ledger and quota balance disagree"
    );
    assert!(
        jobs_submitted.load(Ordering::Relaxed) > 0,
        "soak must actually submit shared work"
    );
    assert!(
        total_leases_granted > 0,
        "soak must actually exercise lease grants"
    );
    assert_eq!(
        total_leases_released, total_leases_granted,
        "every granted lease must be released by soak end"
    );
    assert!(
        coordinator_restarts.load(Ordering::Relaxed) >= 1,
        "soak must exercise a coordinator restart"
    );
}
