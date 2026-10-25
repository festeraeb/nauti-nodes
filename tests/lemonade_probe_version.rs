//! NN2: LemonadeRuntime::probe reports the server version from
//! GET /api/v1/health (Lemonade's JSON health endpoint). No real Lemonade
//! needed: a tiny fake HTTP server answers with JSON captured from the live
//! Lemonade 2026.41.1 server on t440 (2026-10-25).

use nauti_nodes::adapter::lemonade::{version_from_health, LemonadeConfig, LemonadeRuntime};
use nauti_nodes::runtime::{ModelRuntime, RuntimeHealth};

const HEALTH_LIVE: &str = r#"{"all_models_loaded":[{"backend_alive":true,"backend_health":"ready","backend_url":"http://127.0.0.1:8001/v1","checkpoint":"Qwen/Qwen3-VL-4B-Instruct-GGUF:Q4_K_M","device":"gpu","launch_command":["/var/cache/lemonade/bin/llamacpp/vulkan/llama-server","-m","/m.gguf","--ctx-size","35410","--port","8001"],"loaded":true,"model_name":"Qwen3-VL-4B-Instruct-GGUF","recipe":"llamacpp","status":"ready","type":"llm"}],"model_loaded":"Qwen3-VL-4B-Instruct-GGUF","status":"ok","version":"2026.41.1","websocket_port":9000}"#;
const HEALTH_NO_VERSION: &str = r#"{"status":"ok","all_models_loaded":[]}"#;
const MODELS: &str = r#"{"data":[{"id":"Qwen3-0.6B-GGUF","object":"model","owned_by":"lemonade","recipe":"llamacpp","downloaded":true},{"id":"Qwen3-VL-4B-Instruct-GGUF","object":"model","owned_by":"lemonade","recipe":"llamacpp","downloaded":true}],"object":"list"}"#;

/// Minimal HTTP/1.1 server: /api/v1/health -> `health`, /v1/models and
/// /api/v1/models -> MODELS, /health -> HTML (like real Lemonade), else 404.
async fn fake_lemonade(health: &'static str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                let path = req.split_whitespace().nth(1).unwrap_or("").to_string();
                let (status, ctype, body) = match path.as_str() {
                    "/api/v1/health" => ("200 OK", "application/json", health),
                    "/v1/models" | "/api/v1/models" => ("200 OK", "application/json", MODELS),
                    "/health" => ("200 OK", "text/html", "<html>web ui</html>"),
                    _ => ("404 Not Found", "text/plain", "not found"),
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

fn runtime(base_url: String) -> LemonadeRuntime {
    LemonadeRuntime::new(LemonadeConfig { base_url, ..LemonadeConfig::default() })
}

#[test]
fn version_from_health_reads_top_level_version() {
    let live: serde_json::Value = serde_json::from_str(HEALTH_LIVE).unwrap();
    assert_eq!(version_from_health(&live), Some("2026.41.1".to_string()));
    let none: serde_json::Value = serde_json::from_str(HEALTH_NO_VERSION).unwrap();
    assert_eq!(version_from_health(&none), None);
    assert_eq!(version_from_health(&serde_json::json!({"version": 11})), None);
    assert_eq!(version_from_health(&serde_json::json!({"version": ""})), None);
}

#[tokio::test]
async fn probe_reports_version_from_api_v1_health() {
    let rt = runtime(fake_lemonade(HEALTH_LIVE).await);
    let probe = rt.probe().await;
    assert_eq!(probe.health, RuntimeHealth::Healthy);
    assert_eq!(probe.software_version.as_deref(), Some("2026.41.1"));
    assert!(probe.available_models.iter().any(|m| m == "Qwen3-0.6B-GGUF"));
}

#[tokio::test]
async fn probe_version_is_none_when_health_has_no_version() {
    let rt = runtime(fake_lemonade(HEALTH_NO_VERSION).await);
    let probe = rt.probe().await;
    assert_eq!(probe.health, RuntimeHealth::Healthy);
    assert_eq!(probe.software_version, None);
}

#[tokio::test]
async fn probe_unreachable_server_is_unavailable_without_version() {
    let rt = runtime("http://127.0.0.1:1".to_string());
    let probe = rt.probe().await;
    assert_eq!(probe.health, RuntimeHealth::Unavailable);
    assert_eq!(probe.software_version, None);
}
