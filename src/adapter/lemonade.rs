//! Lemonade Server Runtime Adapter
//!
//! AMD's Lemonade server provides a richer API than generic OpenAI endpoints.
//! This adapter implements the ModelRuntime trait for Lemonade.
//!
//! Lemonade lifecycle semantics differ from NIM/llama.cpp:
//! - Models are loaded via a dedicated load endpoint
//! - Health is probed via a status endpoint
//! - Supports streaming and cancellation

use crate::capabilities::RuntimeClass;
use crate::runtime::{
    ChatMessage, InferenceResponse, InferenceStream, ModelRuntime, RuntimeHealth, RuntimeProbe,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Configuration for a Lemonade runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LemonadeConfig {
    /// Base URL of the Lemonade server (e.g., http://127.0.0.1:8000)
    pub base_url: String,

    /// API key (if required)
    pub api_key: Option<String>,

    /// Default model to use
    pub default_model: String,

    /// Whether this runtime supports streaming
    pub supports_streaming: bool,

    /// Whether this runtime supports cancellation
    pub supports_cancellation: bool,
}

impl Default for LemonadeConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8000".into(),
            api_key: None,
            default_model: "default".into(),
            supports_streaming: true,
            supports_cancellation: true,
        }
    }
}

/// Lemonade 11.9 JSON health endpoint (`/health` is the HTML web UI).
const HEALTH_PATH: &str = "/api/v1/health";
/// Lemonade 11.9 load endpoint. `/v1/models/load` does not exist (404).
const LOAD_PATH: &str = "/api/v1/load";

/// Request body for `POST /api/v1/load`. Lemonade requires `model_name`;
/// a `model` key is rejected with HTTP 400 ("type must be string, but is null").
fn load_body(model_id: &str) -> serde_json::Value {
    serde_json::json!({ "model_name": model_id })
}

/// Map a `/api/v1/health` JSON body to a health state.
fn health_from_json(json: &serde_json::Value) -> RuntimeHealth {
    match json.get("status").and_then(|s| s.as_str()) {
        Some("ok") => RuntimeHealth::Healthy,
        _ => RuntimeHealth::Degraded,
    }
}

/// Lemonade's version from a `/api/v1/health` JSON body (top-level `"version"`).
pub fn version_from_health(json: &serde_json::Value) -> Option<String> {
    match json.get("version").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => Some(v.to_string()),
        _ => None,
    }
}

/// Lemonade runtime adapter.
pub struct LemonadeRuntime {
    config: LemonadeConfig,
    client: reqwest::Client,
}

impl LemonadeRuntime {
    pub fn new(config: LemonadeConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Build the full URL for a Lemonade endpoint.
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.config.base_url, path)
    }

    /// Add auth headers if API key is configured.
    fn auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = &self.config.api_key {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", key)).unwrap(),
            );
        }
        headers
    }

    /// One GET of `/api/v1/health`: health state plus the reported version.
    async fn health_and_version(&self) -> (RuntimeHealth, Option<String>) {
        // Lemonade 11.x serves its web UI (HTML, 200) at `/health`, so a 2xx there
        // proves nothing. Use the JSON API and require `status == "ok"`.
        match self.client.get(self.url(HEALTH_PATH)).headers(self.auth_headers()).send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<serde_json::Value>().await {
                Ok(json) => (health_from_json(&json), version_from_health(&json)),
                Err(_) => (RuntimeHealth::Degraded, None),
            },
            Ok(_) => (RuntimeHealth::Degraded, None),
            Err(_) => (RuntimeHealth::Unavailable, None),
        }
    }
}

#[async_trait]
impl ModelRuntime for LemonadeRuntime {
    async fn probe(&self) -> RuntimeProbe {
        let (health, software_version) = self.health_and_version().await;
        let models = self.list_models().await;

        RuntimeProbe {
            runtime_class: RuntimeClass::Lemonade,
            health,
            available_models: models,
            supports_streaming: self.config.supports_streaming,
            supports_cancellation: self.config.supports_cancellation,
            supports_embeddings: false,
            endpoint_url: self.config.base_url.clone(),
            runtime_confidence: 0.7, // richer API = higher initial confidence
            software_version,
        }
    }

    async fn health(&self) -> RuntimeHealth {
        self.health_and_version().await.0
    }

    async fn list_models(&self) -> Vec<String> {
        match self.client.get(self.url("/v1/models")).send().await {
            Ok(resp) if resp.status().is_success() => {
                // Parse the models list
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(models) = json.get("data").and_then(|d| d.as_array()) {
                        return models
                            .iter()
                            .filter_map(|m| m.get("id").and_then(|id| id.as_str()))
                            .map(|s| s.to_string())
                            .collect();
                    }
                }
                vec![self.config.default_model.clone()]
            }
            _ => vec![self.config.default_model.clone()],
        }
    }

    async fn ensure_model_available(&self, model_id: &str) -> Result<bool, String> {
        // Lemonade loads models via a dedicated endpoint
        let body = load_body(model_id);

        let resp = self
            .client
            .post(self.url(LOAD_PATH))
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("lemonade load request failed: {}", e))?;

        Ok(resp.status().is_success())
    }

    async fn infer(
        &self,
        model: &str,
        messages: Vec<ChatMessage>,
        max_tokens: Option<u32>,
    ) -> Result<InferenceResponse, String> {
        let body = serde_json::json!({
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
        });

        let resp = self
            .client
            .post(self.url("/v1/chat/completions"))
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("lemonade inference request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!(
                "lemonade inference returned status {}",
                resp.status()
            ));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("lemonade response parse failed: {}", e))?;

        let content = json
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|c| c.first())
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        Ok(InferenceResponse {
            content,
            model: model.to_string(),
            tokens_used: 0,
            finish_reason: None,
        })
    }

    async fn stream(
        &self,
        model: &str,
        messages: Vec<ChatMessage>,
        max_tokens: Option<u32>,
    ) -> Result<InferenceStream, String> {
        let body = serde_json::json!({
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "stream": true,
        });

        let resp = self
            .client
            .post(self.url("/v1/chat/completions"))
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("lemonade stream request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("lemonade stream returned status {}", resp.status()));
        }

        // Parse SSE stream and forward chunks through an mpsc channel
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut byte_stream = resp.bytes_stream();

        tokio::spawn(async move {
            use futures_util::StreamExt;
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&bytes).to_string();
                        // Extract content from SSE data lines
                        for line in text.lines() {
                            if let Some(data) = line.strip_prefix("data: ") {
                                if data == "[DONE]" {
                                    continue;
                                }
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(data) {
                                    if let Some(content) = json
                                        .get("choices")
                                        .and_then(|c| c.as_array())
                                        .and_then(|c| c.first())
                                        .and_then(|c| c.get("delta"))
                                        .and_then(|d| d.get("content"))
                                        .and_then(|c| c.as_str())
                                    {
                                        let _ = tx.send(content.to_string()).await;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lemonade_config_default() {
        let config = LemonadeConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:8000");
        assert!(config.supports_streaming);
        assert!(config.supports_cancellation);
    }

    #[test]
    fn lemonade_runtime_creation() {
        let runtime = LemonadeRuntime::new(LemonadeConfig::default());
        assert_eq!(runtime.config.base_url, "http://127.0.0.1:8000");
    }

    #[test]
    fn lemonade_url_building() {
        let runtime = LemonadeRuntime::new(LemonadeConfig::default());
        assert_eq!(runtime.url("/health"), "http://127.0.0.1:8000/health");
        assert_eq!(runtime.url("/v1/models"), "http://127.0.0.1:8000/v1/models");
    }

    #[test]
    fn lemonade_11_9_endpoint_paths_and_load_body() {
        let runtime = LemonadeRuntime::new(LemonadeConfig::default());
        assert_eq!(runtime.url(HEALTH_PATH), "http://127.0.0.1:8000/api/v1/health");
        assert_eq!(runtime.url(LOAD_PATH), "http://127.0.0.1:8000/api/v1/load");
        assert_eq!(load_body("m"), serde_json::json!({ "model_name": "m" }));
    }

    #[test]
    fn health_requires_status_ok() {
        assert_eq!(health_from_json(&serde_json::json!({"status": "ok", "version": "11.9.0"})), RuntimeHealth::Healthy);
        assert_eq!(health_from_json(&serde_json::json!({"status": "starting"})), RuntimeHealth::Degraded);
        assert_eq!(health_from_json(&serde_json::json!({})), RuntimeHealth::Degraded);
    }
}
