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
        .route("/v1/chat/completions", post(handlers::inference))
        .route(
            "/v1/inference/cancel/:job_id",
            post(handlers::cancel_inference),
        )
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/nodes", get(handlers::list_nodes))
        .route("/v1/node/identity", get(handlers::node_identity))
        .route("/v1/node/join", post(handlers::node_join))
        .route(
            "/v1/admin/reconciliation",
            get(handlers::admin_reconciliation),
        )
        .route("/v1/tree/snapshot", get(handlers::tree_snapshot))
        .route("/internal/worker/register", post(handlers::worker_register))
        .route(
            "/internal/worker/heartbeat",
            post(handlers::worker_heartbeat),
        )
        .route("/internal/worker/withdraw", post(handlers::worker_withdraw))
        .route("/v1/admin/revoke", post(handlers::admin_revoke))
        .route("/v1/admin/unrevoke", post(handlers::admin_unrevoke))
        .route("/v1/admin/revocations", get(handlers::list_revocations))
        .route("/internal/quotas/set", post(handlers::set_quota_balance))
        .route("/internal/fleet/sync", post(handlers::sync_fleet_now))
        .route("/metrics/prometheus", get(handlers::prometheus_metrics))
        .route("/v1/model/start", post(handlers::model_start))
        .route("/v1/model/stop", post(handlers::model_stop))
        .route("/v1/model/status", get(handlers::model_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
