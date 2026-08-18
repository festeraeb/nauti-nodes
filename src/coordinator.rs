//! HTTP coordinator: Forge fleet sync + public inference API.

use crate::api;
use crate::fleet;
use crate::node::heartbeat;
use crate::state::AppState;
use crate::types::Result;
use crate::Config;
use tracing::info;

pub async fn run_coordinator(config: Config) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init()
        .ok();

    // Load or create durable node identity
    let identity = crate::node::identity::NodeIdentity::load_or_create(std::path::Path::new(
        &config.identity_path,
    ))?;
    info!(node_id = %identity.node_id, "node identity established");

    // Initialize lineage store and register self as root
    let lineage = crate::lineage::LineageStore::open(&config.db_url)?;
    lineage.set_root(&identity.node_id)?;

    let mut state = AppState::new(config.clone())?;
    // AppState::new already loads/generates the CoordinatorIdentity
    // (Chunk 8). The NodeIdentity loaded above is used only for lineage
    // root registration.
    state.lineage = Some(std::sync::Arc::new(lineage));

    // Chunk 7: count coordinator restarts observationally (every
    // COORDINATOR_STARTED journal entry beyond the first is a restart).
    let starts = state
        .ops
        .events(None, None, None, Some("COORDINATOR_STARTED"), 1000)
        .map(|e| e.len())
        .unwrap_or(1);
    if starts > 1 {
        state
            .metrics
            .coordinator_restarts_total
            .store((starts - 1) as u64, std::sync::atomic::Ordering::Relaxed);
    }

    fleet::sync_fleet(&state.registry, &state.http, &config.forge_url).await;

    heartbeat::spawn_sweeper_with_ops(state.registry.clone(), 15, 30, Some(state.ops.clone()));

    let sync_state = state.clone();
    let forge_url = config.forge_url.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tick.tick().await;
            fleet::sync_fleet(&sync_state.registry, &sync_state.http, &forge_url).await;
        }
    });

    let app = api::routes::router(state);
    let addr = format!("0.0.0.0:{}", config.listen_port);
    info!(%addr, forge = %config.forge_url, "NautiInferer coordinator listening");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| crate::types::Error::Internal(format!("bind {addr}: {e}")))?;
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::types::Error::Internal(e.to_string()))?;
    Ok(())
}
