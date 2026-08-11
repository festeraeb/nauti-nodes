use crate::api::handlers;
use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(handlers::landing_page))
        .route("/health", get(handlers::health))
        .route("/metrics", get(handlers::metrics))
        .route("/v1/inference", post(handlers::inference))
        .route(
            "/v1/inference/cancel/:job_id",
            post(handlers::cancel_inference),
        )
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/nodes", get(handlers::list_nodes))
        .route("/internal/worker/register", post(handlers::worker_register))
        .route(
            "/internal/worker/heartbeat",
            post(handlers::worker_heartbeat),
        )
        .route("/internal/quotas/set", post(handlers::set_quota_balance))
        .route("/internal/fleet/sync", post(handlers::sync_fleet_now))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
