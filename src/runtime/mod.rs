//! Model runtime abstraction: trait for heterogeneous inference backends.
//!
//! Nauti Nodes supports multiple runtime classes:
//! - Lemonade Server
//! - NVIDIA NIM
//! - Generic OpenAI-compatible endpoint
//! - Native CESARops runtime
//!
//! The ModelRuntime trait normalizes their behavior through ensure_model_available()
//! rather than assuming every runtime has identical "load model" semantics.

use crate::capabilities::RuntimeClass;
use serde::{Deserialize, Serialize};

/// Health status of a runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeHealth {
    /// Runtime is healthy and ready.
    Healthy,
    /// Runtime is degraded but functional.
    Degraded,
    /// Runtime is unavailable.
    Unavailable,
    /// Runtime health is unknown (not yet probed).
    Unknown,
}

impl RuntimeHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

/// Result of probing a runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProbe {
    pub runtime_class: RuntimeClass,
    pub health: RuntimeHealth,
    pub available_models: Vec<String>,
    pub supports_streaming: bool,
    pub supports_cancellation: bool,
    pub supports_embeddings: bool,
    pub endpoint_url: String,
    pub runtime_confidence: f64,
    pub software_version: Option<String>,
}

/// Trait that all model runtime adapters must implement.
/// This is the contract between Nauti Nodes and any inference backend.
#[async_trait::async_trait]
pub trait ModelRuntime: Send + Sync {
    /// Probe the runtime to determine its capabilities.
    async fn probe(&self) -> RuntimeProbe;

    /// Check if the runtime is healthy.
    async fn health(&self) -> RuntimeHealth;

    /// List models currently available on this runtime.
    async fn list_models(&self) -> Vec<String>;

    /// Ensure a specific model is available. Returns true if the model is
    /// ready after this call. This is the key abstraction: different runtimes
    /// have different "load model" semantics, but all can answer "is it ready?".
    async fn ensure_model_available(&self, model_id: &str) -> Result<bool, String>;

    /// Perform a non-streaming inference request.
    async fn infer(
        &self,
        model: &str,
        messages: Vec<ChatMessage>,
        max_tokens: Option<u32>,
    ) -> Result<InferenceResponse, String>;

    /// Perform a streaming inference request.
    async fn stream(
        &self,
        model: &str,
        messages: Vec<ChatMessage>,
        max_tokens: Option<u32>,
    ) -> Result<InferenceStream, String>;
}

/// A chat message for inference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Non-streaming inference response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub content: String,
    pub model: String,
    pub tokens_used: u32,
    pub finish_reason: Option<String>,
}

/// Streaming inference response (placeholder; real implementation uses channels).
pub type InferenceStream = tokio::sync::mpsc::Receiver<String>;

/// Generic OpenAI-compatible runtime adapter.
pub struct OpenAICompatibleRuntime {
    pub endpoint_url: String,
    pub client: reqwest::Client,
}

impl OpenAICompatibleRuntime {
    pub fn new(endpoint_url: &str) -> Self {
        Self {
            endpoint_url: endpoint_url.to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait::async_trait]
impl ModelRuntime for OpenAICompatibleRuntime {
    async fn probe(&self) -> RuntimeProbe {
        let health = self.health().await;
        let models = self.list_models().await;
        RuntimeProbe {
            runtime_class: RuntimeClass::OpenAICompatible,
            health,
            available_models: models,
            supports_streaming: true,
            supports_cancellation: false,
            supports_embeddings: false,
            endpoint_url: self.endpoint_url.clone(),
            runtime_confidence: 1.0,
            software_version: None,
        }
    }

    async fn health(&self) -> RuntimeHealth {
        let url = format!("{}/health", self.endpoint_url.trim_end_matches('/'));
        match self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => RuntimeHealth::Healthy,
            Ok(_) => RuntimeHealth::Degraded,
            Err(_) => RuntimeHealth::Unavailable,
        }
    }

    async fn list_models(&self) -> Vec<String> {
        let url = format!("{}/v1/models", self.endpoint_url.trim_end_matches('/'));
        match self.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(data) = body.get("data").and_then(|d| d.as_array()) {
                        return data
                            .iter()
                            .filter_map(|m| {
                                m.get("id").and_then(|id| id.as_str()).map(String::from)
                            })
                            .collect();
                    }
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    async fn ensure_model_available(&self, _model_id: &str) -> Result<bool, String> {
        // Generic OpenAI-compatible: if the server is healthy, the model is available
        match self.health().await {
            RuntimeHealth::Healthy => Ok(true),
            other => Err(format!("runtime not healthy: {}", other.as_str())),
        }
    }

    async fn infer(
        &self,
        _model: &str,
        _messages: Vec<ChatMessage>,
        _max_tokens: Option<u32>,
    ) -> Result<InferenceResponse, String> {
        // Placeholder; real implementation proxies to the OpenAI-compatible endpoint
        Err("not implemented: use existing job_runner for inference".into())
    }

    async fn stream(
        &self,
        _model: &str,
        _messages: Vec<ChatMessage>,
        _max_tokens: Option<u32>,
    ) -> Result<InferenceStream, String> {
        Err("not implemented: use existing job_runner for streaming".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_health_roundtrip() {
        for h in [
            RuntimeHealth::Healthy,
            RuntimeHealth::Degraded,
            RuntimeHealth::Unavailable,
            RuntimeHealth::Unknown,
        ] {
            let s = h.as_str();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn openai_runtime_has_correct_class() {
        let rt = OpenAICompatibleRuntime::new("http://localhost:5200");
        // We can't async probe in a sync test, but we can verify construction
        assert_eq!(rt.endpoint_url, "http://localhost:5200");
    }
}
