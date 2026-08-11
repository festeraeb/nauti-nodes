//! OpenAI-compatible chat/completions streaming (llama-server).

use crate::types::errors::{Error, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: Option<StreamDelta>,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamChunk {
    choices: Option<Vec<StreamChoice>>,
}

/// Stream tokens from llama-server; invokes `on_delta` for each piece.
pub async fn stream_chat(
    client: &reqwest::Client,
    base_url: &str,
    model: &str,
    messages: &[ChatMessage],
    max_tokens: u32,
    temperature: f32,
    cancelled: Arc<AtomicBool>,
    mut on_delta: impl FnMut(String) -> Result<()> + Send,
) -> Result<()> {
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    let req = ChatRequest {
        model: model.to_string(),
        messages: messages.to_vec(),
        stream: true,
        max_tokens,
        temperature,
    };
    let resp = client
        .post(&url)
        .json(&req)
        .timeout(std::time::Duration::from_secs(600))
        .send()
        .await
        .map_err(|e| Error::Http(format!("{url}: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::Inference(format!("{url} HTTP {status}: {body}")));
    }

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    while let Some(chunk) = stream.next().await {
        if cancelled.load(Ordering::Relaxed) {
            debug!("stream cancelled");
            break;
        }
        let bytes = chunk.map_err(|e| Error::Http(e.to_string()))?;
        buf.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf = buf[pos + 2..].to_string();
            for line in frame.lines() {
                let line = line.trim();
                if !line.starts_with("data: ") {
                    continue;
                }
                let data = line.trim_start_matches("data: ").trim();
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }
                if let Ok(chunk) = serde_json::from_str::<StreamChunk>(data) {
                    if let Some(choices) = chunk.choices {
                        for c in choices {
                            if let Some(delta) = c.delta {
                                if let Some(text) = delta.content {
                                    if !text.is_empty() {
                                        on_delta(text)?;
                                    }
                                }
                            }
                            if c.finish_reason.is_some() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn health_ok(client: &reqwest::Client, base_url: &str) -> bool {
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}
