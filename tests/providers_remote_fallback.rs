//! NN1c: remote API model fallback with a stub runtime (no network).

use nauti_nodes::providers::remote_api::{infer_with_fallback, should_try_next_model, status_in_error};
use nauti_nodes::runtime::{
    ChatMessage, InferenceResponse, InferenceStream, ModelRuntime, RuntimeHealth, RuntimeProbe,
};
use std::collections::HashMap;
use std::sync::Mutex;

/// Answers `infer` from a fixed table: model id -> Ok(content) or Err(text).
struct StubRuntime {
    answers: HashMap<String, Result<String, String>>,
    calls: Mutex<Vec<String>>,
}

impl StubRuntime {
    fn new(answers: &[(&str, Result<&str, &str>)]) -> Self {
        Self {
            answers: answers
                .iter()
                .map(|(m, r)| (m.to_string(), r.map(str::to_string).map_err(str::to_string)))
                .collect(),
            calls: Mutex::new(Vec::new()),
        }
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ModelRuntime for StubRuntime {
    async fn probe(&self) -> RuntimeProbe {
        unimplemented!("not used by these tests")
    }
    async fn health(&self) -> RuntimeHealth {
        RuntimeHealth::Healthy
    }
    async fn list_models(&self) -> Vec<String> {
        self.answers.keys().cloned().collect()
    }
    async fn ensure_model_available(&self, _model_id: &str) -> Result<bool, String> {
        Ok(true)
    }
    async fn infer(
        &self,
        model: &str,
        _messages: Vec<ChatMessage>,
        _max_tokens: Option<u32>,
    ) -> Result<InferenceResponse, String> {
        self.calls.lock().unwrap().push(model.to_string());
        match self.answers.get(model) {
            Some(Ok(content)) => Ok(InferenceResponse {
                content: content.clone(),
                model: "stub-reported-name".into(),
                tokens_used: 1,
                finish_reason: Some("stop".into()),
            }),
            Some(Err(e)) => Err(e.clone()),
            None => Err("nim inference returned status 404 Not Found".into()),
        }
    }
    async fn stream(
        &self,
        _model: &str,
        _messages: Vec<ChatMessage>,
        _max_tokens: Option<u32>,
    ) -> Result<InferenceStream, String> {
        Err("not used".into())
    }
}

fn msgs() -> Vec<ChatMessage> {
    vec![ChatMessage { role: "user".into(), content: "ping".into() }]
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn status_is_read_from_nim_error_text() {
    assert_eq!(status_in_error("nim inference returned status 404 Not Found"), Some(404));
    assert_eq!(status_in_error("nim stream returned status 503 Service Unavailable"), Some(503));
    assert_eq!(status_in_error("nim inference request failed: connection refused"), None);
    assert_eq!(status_in_error(""), None);
}

#[test]
fn which_errors_move_to_the_next_model() {
    for code in ["404 Not Found", "410 Gone", "429 Too Many Requests", "500 Internal Server Error", "503 Service Unavailable", "599 x"] {
        assert!(should_try_next_model(&format!("nim inference returned status {code}")), "{code}");
    }
    for code in ["400 Bad Request", "401 Unauthorized", "403 Forbidden", "200 OK"] {
        assert!(!should_try_next_model(&format!("nim inference returned status {code}")), "{code}");
    }
    assert!(!should_try_next_model("nim inference request failed: dns error"));
}

#[tokio::test]
async fn falls_back_on_404_and_records_the_model_that_answered() {
    let rt = StubRuntime::new(&[
        ("minimaxai/minimax-m3.1-flash", Err("nim inference returned status 404 Not Found")),
        ("google/diffusiongemma-26b-a4b-it", Ok("pong")),
    ]);
    let models = ids(&["minimaxai/minimax-m3.1-flash", "google/diffusiongemma-26b-a4b-it"]);
    let r = infer_with_fallback(&rt, &models, msgs(), Some(8)).await.unwrap();
    assert_eq!(r.content, "pong");
    assert_eq!(r.model, "google/diffusiongemma-26b-a4b-it");
    assert_eq!(rt.calls(), models);
}

#[tokio::test]
async fn first_success_stops_the_loop() {
    let rt = StubRuntime::new(&[("a", Ok("one")), ("b", Ok("two"))]);
    let r = infer_with_fallback(&rt, &ids(&["a", "b"]), msgs(), None).await.unwrap();
    assert_eq!(r.model, "a");
    assert_eq!(rt.calls(), ids(&["a"]));
}

#[tokio::test]
async fn non_retryable_error_is_returned_at_once() {
    let rt = StubRuntime::new(&[
        ("a", Err("nim inference returned status 401 Unauthorized")),
        ("b", Ok("never")),
    ]);
    let e = infer_with_fallback(&rt, &ids(&["a", "b"]), msgs(), None).await.unwrap_err();
    assert!(e.contains("401"), "{e}");
    assert_eq!(rt.calls(), ids(&["a"]));
}

#[tokio::test]
async fn all_failing_returns_the_last_error() {
    let rt = StubRuntime::new(&[
        ("a", Err("nim inference returned status 429 Too Many Requests")),
        ("b", Err("nim inference returned status 503 Service Unavailable")),
    ]);
    let e = infer_with_fallback(&rt, &ids(&["a", "b"]), msgs(), None).await.unwrap_err();
    assert!(e.contains("503"), "{e}");
    assert_eq!(rt.calls(), ids(&["a", "b"]));
}

#[tokio::test]
async fn no_models_is_an_error() {
    let rt = StubRuntime::new(&[]);
    let e = infer_with_fallback(&rt, &[], msgs(), None).await.unwrap_err();
    assert_eq!(e, "no remote models configured");
    assert!(rt.calls().is_empty());
}
