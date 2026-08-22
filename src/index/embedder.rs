//! Embedder abstraction and real HTTP client for local vector embeddings.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Trait for real text and code embedding backends.
#[async_trait::async_trait]
pub trait Embedder: Send + Sync {
    /// Returns the embedding model ID.
    fn model_id(&self) -> &str;

    /// Returns the vector dimension.
    fn dimensions(&self) -> usize;

    /// Generates embedding vector for a single text chunk.
    async fn embed(&self, text: &str) -> anyhow::Result<Vec<f32>>;

    /// Generates embedding vectors for a batch of text chunks.
    async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>>;
}

/// HTTP-backed embedding client communicating with a real local embedding server
/// (e.g. EmbeddingGemma / llama.cpp / Candle on localhost:5210).
pub struct HttpEmbedder {
    endpoint_url: String,
    model_id: String,
    dimensions: usize,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    input: &'a [String],
    model: &'a str,
}

#[derive(Deserialize)]
struct EmbeddingResponseData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingResponseData>,
}

impl HttpEmbedder {
    pub fn new(endpoint_url: &str, model_id: &str, dimensions: usize) -> Self {
        Self {
            endpoint_url: endpoint_url.trim_end_matches('/').to_string(),
            model_id: model_id.to_string(),
            dimensions,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn default_local() -> Self {
        Self::new(
            "http://127.0.0.1:5210/v1/embeddings",
            "embedding-gemma",
            768,
        )
    }

    /// Probes whether the embedding service is currently reachable.
    pub async fn is_available(&self) -> bool {
        let health_url = if self.endpoint_url.ends_with("/v1/embeddings") {
            self.endpoint_url.replace("/v1/embeddings", "/health")
        } else {
            format!("{}/health", self.endpoint_url)
        };
        match self
            .client
            .get(&health_url)
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl Embedder for HttpEmbedder {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn embed(&self, text: &str) -> anyhow::Result<Vec<f32>> {
        let batch = vec![text.to_string()];
        let mut results = self.embed_batch(&batch).await?;
        results
            .pop()
            .ok_or_else(|| anyhow::anyhow!("EMBEDDER NOT AVAILABLE: empty response from backend"))
    }

    async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let body = EmbeddingRequest {
            input: texts,
            model: &self.model_id,
        };

        let resp = self
            .client
            .post(&self.endpoint_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("EMBEDDER NOT AVAILABLE: connection failed ({e})"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().await.unwrap_or_default();
            anyhow::bail!("EMBEDDER NOT AVAILABLE: backend HTTP {status} ({err_body})");
        }

        let mut parsed: EmbeddingResponse = resp.json().await.map_err(|e| {
            anyhow::anyhow!("EMBEDDER NOT AVAILABLE: failed to parse embeddings JSON ({e})")
        })?;

        parsed.data.sort_by_key(|d| d.index);
        let vectors: Vec<Vec<f32>> = parsed.data.into_iter().map(|d| d.embedding).collect();
        Ok(vectors)
    }
}
