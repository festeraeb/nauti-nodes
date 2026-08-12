//! NVIDIA NIM Runtime Adapter
//!
//! NVIDIA NIM provides a rich API for model management and inference.
//! This adapter implements the ModelRuntime trait for NIM.
//!
//! NIM lifecycle semantics differ from Lemonade/llama.cpp:
//! - Models are managed via NIM's model management API
//! - Health is probed via a status endpoint
//! - Supports streaming and cancellation

use crate::capabilities::RuntimeClass;
use crate::runtime::{
    ChatMessage, InferenceResponse, InferenceStream, ModelRuntime, RuntimeHealth, RuntimeProbe,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Configuration for a NIM runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NimConfig {
    /// Base URL of the NIM server (e.g., http://127.0.0.1:8000)
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

impl Default for NimConfig {
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

/// NVIDIA NIM runtime adapter.
pub struct NimRuntime {
    config: NimConfig,
    client: reqwest::Client,
}

impl NimRuntime {
    pub fn new(config: NimConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Build the full URL for a NIM endpoint.
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
}

#[async_trait]
impl ModelRuntime for NimRuntime {
    async fn probe(&self) -> RuntimeProbe {
        let health = self.health().await;
        let models = self.list_models().await;

        RuntimeProbe {
            runtime_class: RuntimeClass::NvidiaNim,
            health,
            available_models: models,
            supports_streaming: self.config.supports_streaming,
            supports_cancellation: self.config.supports_cancellation,
            supports_embeddings: true, // NIM supports embeddings
            endpoint_url: self.config.base_url.clone(),
            runtime_confidence: 0.7, // richer API = higher initial confidence
            software_version: None,
        }
    }

    async fn health(&self) -> RuntimeHealth {
        match self.client.get(self.url("/v1/health/ready")).send().await {
            Ok(resp) if resp.status().is_success() => RuntimeHealth::Healthy,
            Ok(_) => RuntimeHealth::Degraded,
            Err(_) => RuntimeHealth::Unavailable,
        }
    }

    async fn list_models(&self) -> Vec<String> {
        match self.client.get(self.url("/v1/models")).send().await {
            Ok(resp) if resp.status().is_success() => {
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
        // NIM manages models via its model management API
        // Check if the model is already available first
        let models = self.list_models().await;
        if models.iter().any(|m| m == model_id) {
            return Ok(true);
        }

        // Try to load the model via NIM's model management endpoint
        let body = serde_json::json!({
            "model": model_id,
        });

        let resp = self
            .client
            .post(self.url("/v1/models/load"))
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("nim load request failed: {}", e))?;

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
            .map_err(|e| format!("nim inference request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("nim inference returned status {}", resp.status()));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("nim response parse failed: {}", e))?;

        let content = json
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|c| c.first())
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        let tokens_used = json
            .get("usage")
            .and_then(|u| u.get("total_tokens"))
            .and_then(|t| t.as_u64())
            .unwrap_or(0) as u32;

        Ok(InferenceResponse {
            content,
            model: model.to_string(),
            tokens_used,
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
            .map_err(|e| format!("nim stream request failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("nim stream returned status {}", resp.status()));
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
    fn nim_config_default() {
        let config = NimConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:8000");
        assert!(config.supports_streaming);
        assert!(config.supports_cancellation);
    }

    #[test]
    fn nim_runtime_creation() {
        let runtime = NimRuntime::new(NimConfig::default());
        assert_eq!(runtime.config.base_url, "http://127.0.0.1:8000");
    }

    #[test]
    fn nim_url_building() {
        let runtime = NimRuntime::new(NimConfig::default());
        assert_eq!(runtime.url("/health"), "http://127.0.0.1:8000/health");
        assert_eq!(runtime.url("/v1/models"), "http://127.0.0.1:8000/v1/models");
    }
}
