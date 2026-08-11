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

    let state = AppState::new(config.clone())?;
    fleet::sync_fleet(&state.registry, &state.http, &config.forge_url).await;

    heartbeat::spawn_sweeper(state.registry.clone(), 15, 30);

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
