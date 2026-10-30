//! Nexus-attributed join over real HTTP.
//!
//! Each test binds the full Axum router on a loopback port and posts to
//! `/v1/node/join` with and without the `X-Nexus-Peer-*` headers the Lemonade
//! Nexus service ingress sets. With `nexus_ingress_only` on, the attributed
//! peer IS the worker identity; a body that disagrees, a missing attribution
//! or a missing permission is refused with 403 before the lineage changes.
//! With it off, the pre-Nexus behaviour is unchanged.

use nauti_nodes::{api::routes::router, lineage::LineageStore, state::AppState, Config};
use std::sync::Arc;

const ROOT: &str = "coordinator-root";

async fn start_server(nexus_ingress_only: bool) -> String {
    let mut config = Config::default();
    config.nexus_ingress_only = nexus_ingress_only;
    let mut state = AppState::new(config).expect("AppState::new failed");
    let lineage = LineageStore::open(":memory:").expect("lineage open");
    lineage.set_root(ROOT).expect("set_root");
    state.lineage = Some(Arc::new(lineage));

    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

fn join_body(node_id: &str) -> serde_json::Value {
    serde_json::json!({
        "node_id": node_id,
        "parent_node_id": ROOT,
        "public_key_hex": "00".repeat(32),
        "software_version": "test",
        "capabilities": null,
    })
}

async fn join(
    base: &str,
    node_id: &str,
    headers: &[(&str, &str)],
) -> (u16, serde_json::Value) {
    let mut req = reqwest::Client::new()
        .post(format!("{base}/v1/node/join"))
        .json(&join_body(node_id));
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = req.send().await.expect("POST /v1/node/join");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let body = serde_json::from_str(&text).unwrap_or(serde_json::json!({ "raw": text }));
    (status, body)
}

const ATTRIBUTED: &[(&str, &str)] = &[
    ("X-Nexus-Peer-Node", "infer-c4-001"),
    ("X-Nexus-Peer-Key", "mgmt-key-c4"),
    ("X-Nexus-Peer-Permissions", "svc:nauti-worker,connect_private"),
    ("X-Nexus-Family", "nauti-worker"),
];

#[tokio::test]
async fn gate_on_attributed_peer_joins_and_is_recorded() {
    let base = start_server(true).await;
    let (status, body) = join(&base, "infer-c4-001", ATTRIBUTED).await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "active");
    assert_eq!(body["identity_source"], "nexus-ingress");
    assert_eq!(body["nexus_peer"]["node_id"], "infer-c4-001");
    assert_eq!(body["nexus_peer"]["key"], "mgmt-key-c4");
    assert_eq!(body["nexus_peer"]["family"], "nauti-worker");
}

#[tokio::test]
async fn gate_on_unattributed_join_is_403_and_touches_no_lineage() {
    let base = start_server(true).await;
    let (status, body) = join(&base, "infer-c4-001", &[]).await;
    assert_eq!(status, 403, "body={body}");

    // The same node can still join once attributed: nothing was recorded.
    let (status, _) = join(&base, "infer-c4-001", ATTRIBUTED).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn gate_on_forged_body_node_id_is_403() {
    let base = start_server(true).await;
    let (status, body) = join(&base, "someone-else", ATTRIBUTED).await;
    assert_eq!(status, 403, "body={body}");
    assert!(body["raw"].as_str().unwrap_or("").contains("not the Nexus-attributed peer"));
}

#[tokio::test]
async fn gate_on_missing_permission_is_403_but_admin_passes() {
    let base = start_server(true).await;
    let lacking: &[(&str, &str)] = &[
        ("X-Nexus-Peer-Node", "infer-c4-002"),
        ("X-Nexus-Peer-Key", "k"),
        ("X-Nexus-Peer-Permissions", "connect_private"),
    ];
    let (status, _) = join(&base, "infer-c4-002", lacking).await;
    assert_eq!(status, 403);

    let admin: &[(&str, &str)] = &[
        ("X-Nexus-Peer-Node", "infer-c4-002"),
        ("X-Nexus-Peer-Key", "k"),
        ("X-Nexus-Peer-Permissions", "admin"),
    ];
    let (status, body) = join(&base, "infer-c4-002", admin).await;
    assert_eq!(status, 200, "body={body}");
}

#[tokio::test]
async fn gate_on_malformed_header_is_403() {
    let base = start_server(true).await;
    let bad: &[(&str, &str)] = &[
        ("X-Nexus-Peer-Node", "infer c4"),
        ("X-Nexus-Peer-Key", "k"),
        ("X-Nexus-Peer-Permissions", "admin"),
    ];
    let (status, _) = join(&base, "infer c4", bad).await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn gate_off_keeps_the_old_behaviour_and_still_records_attribution() {
    let base = start_server(false).await;
    let (status, body) = join(&base, "lan-node-1", &[]).await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["identity_source"], "body");
    assert!(body["nexus_peer"].is_null());

    let (status, body) = join(&base, "infer-c4-001", ATTRIBUTED).await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["nexus_peer"]["node_id"], "infer-c4-001");
}
