//! Live integration test for `LemonadeRuntime` against a real Lemonade server.
//!
//! Ignored by default. Run with:
//!   LEMONADE_TEST_URL=http://127.0.0.1:13410 \
//!   LEMONADE_TEST_MODEL=Phi-3-mini-4k-instruct-Q4_K_M \
//!   cargo test --test lemonade_live -- --ignored --nocapture
//!
//! Every step goes through the adapter's public `ModelRuntime` API, except the
//! final unload: the trait has no unload method, so the test unloads with a raw
//! POST to `/api/v1/unload` to leave the server as it found it.

use nauti_nodes::adapter::lemonade::{LemonadeConfig, LemonadeRuntime};
use nauti_nodes::runtime::{ChatMessage, ModelRuntime, RuntimeHealth};
use std::time::{Duration, Instant};

fn msg(text: &str) -> Vec<ChatMessage> {
    vec![ChatMessage {
        role: "user".into(),
        content: text.into(),
    }]
}

#[tokio::test]
#[ignore = "needs a live Lemonade server; set LEMONADE_TEST_URL"]
async fn lemonade_adapter_live() {
    let base_url = std::env::var("LEMONADE_TEST_URL").expect("set LEMONADE_TEST_URL");
    let model = std::env::var("LEMONADE_TEST_MODEL")
        .unwrap_or_else(|_| "Phi-3-mini-4k-instruct-Q4_K_M".into());
    let rt = LemonadeRuntime::new(LemonadeConfig {
        base_url: base_url.clone(),
        api_key: std::env::var("LEMONADE_TEST_API_KEY").ok(),
        default_model: "default".into(),
        supports_streaming: true,
        supports_cancellation: true,
    });
    let mut failures: Vec<String> = Vec::new();

    // 1. probe (health + list_models)
    let probe = rt.probe().await;
    println!("[probe] health={:?} models={:?} version={:?}", probe.health, probe.available_models, probe.software_version);
    if probe.health != RuntimeHealth::Healthy {
        failures.push(format!("probe.health = {:?}", probe.health));
    }
    if !probe.available_models.iter().any(|m| m == &model) {
        failures.push(format!("probe.available_models lacks {}", model));
    }

    // 2. load
    let t = Instant::now();
    let load = rt.ensure_model_available(&model).await;
    println!("[load] {:?} in {:.1}s", load, t.elapsed().as_secs_f64());
    if load != Ok(true) {
        failures.push(format!("ensure_model_available = {:?}", load));
    }

    // 3. infer
    let t = Instant::now();
    match rt.infer(&model, msg("Reply with the single word: pong"), Some(16)).await {
        Ok(r) => {
            println!("[infer] {:.1}s content={:?} tokens_used={} finish={:?}", t.elapsed().as_secs_f64(), r.content, r.tokens_used, r.finish_reason);
            if r.content.trim().is_empty() {
                failures.push("infer returned empty content".into());
            }
        }
        Err(e) => failures.push(format!("infer error: {}", e)),
    }

    // 4. stream
    let t = Instant::now();
    match rt.stream(&model, msg("Count from 1 to 5."), Some(32)).await {
        Ok(mut rx) => {
            let mut chunks = 0usize;
            let mut text = String::new();
            while let Ok(Some(c)) = tokio::time::timeout(Duration::from_secs(60), rx.recv()).await {
                chunks += 1;
                text.push_str(&c);
            }
            println!("[stream] {:.1}s chunks={} text={:?}", t.elapsed().as_secs_f64(), chunks, text);
            if chunks == 0 {
                failures.push("stream yielded 0 chunks".into());
            }
        }
        Err(e) => failures.push(format!("stream error: {}", e)),
    }

    // 5. unload (raw; not part of the adapter API)
    let r = reqwest::Client::new()
        .post(format!("{}/api/v1/unload", base_url.trim_end_matches('/')))
        .json(&serde_json::json!({ "model_name": model }))
        .send()
        .await;
    println!("[unload raw /api/v1/unload] {:?}", r.as_ref().map(|r| r.status()));

    println!("[summary] failures={:#?}", failures);
    assert!(failures.is_empty(), "adapter live failures: {:?}", failures);
}
