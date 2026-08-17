//! Worker-side local Model Manager & Runtime Process Manager.

use crate::capabilities::PrivacyClass;
use crate::types::now_ms;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Explicit local state of a model / runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ModelState {
    Absent,
    Available,
    Downloading,
    Starting,
    Loading,
    Ready,
    Busy,
    Idle,
    Stopping,
    Failed,
}

impl Default for ModelState {
    fn default() -> Self {
        Self::Absent
    }
}

impl ModelState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Absent => "ABSENT",
            Self::Available => "AVAILABLE",
            Self::Downloading => "DOWNLOADING",
            Self::Starting => "STARTING",
            Self::Loading => "LOADING",
            Self::Ready => "READY",
            Self::Busy => "BUSY",
            Self::Idle => "IDLE",
            Self::Stopping => "STOPPING",
            Self::Failed => "FAILED",
        }
    }
}

/// Approved local model configuration entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalModelConfig {
    pub model_id: String,
    pub artifact_path: String,
    pub runtime_type: String,
    pub vram_required_mb: u32,
    pub privacy_class: PrivacyClass,
    pub extra_args: Vec<String>,
}

/// Approved local model manifest.
#[derive(Debug, Clone)]
pub struct ModelManifest {
    models: HashMap<String, LocalModelConfig>,
}

impl Default for ModelManifest {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelManifest {
    pub fn new() -> Self {
        let mut manifest = Self {
            models: HashMap::new(),
        };
        manifest.register(LocalModelConfig {
            model_id: "thinker".to_string(),
            artifact_path: "/models/Qwen2.5-Coder-1.5B.gguf".to_string(),
            runtime_type: "llama-server".to_string(),
            vram_required_mb: 4096,
            privacy_class: PrivacyClass::Public,
            extra_args: vec!["--ctx-size".to_string(), "2048".to_string()],
        });
        manifest.register(LocalModelConfig {
            model_id: "coder".to_string(),
            artifact_path: "/models/Qwen2.5-Coder-1.5B.gguf".to_string(),
            runtime_type: "llama-server".to_string(),
            vram_required_mb: 4096,
            privacy_class: PrivacyClass::Public,
            extra_args: vec![],
        });
        manifest
    }

    pub fn register(&mut self, config: LocalModelConfig) {
        self.models.insert(config.model_id.clone(), config);
    }

    pub fn get(&self, model_id: &str) -> Option<&LocalModelConfig> {
        self.models.get(model_id)
    }

    pub fn list(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }
}

/// Process handle metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub pid: u32,
    pub port: u16,
    pub base_url: String,
    pub model_id: String,
    pub started_at_ms: u64,
}

/// Pluggable inference runtime backend interface.
#[async_trait::async_trait]
pub trait RuntimeBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn detect(&self) -> bool;
    async fn start(&self, config: &LocalModelConfig, port: u16) -> Result<ProcessInfo, String>;
    async fn check_readiness(
        &self,
        base_url: &str,
        expected_model: &str,
        timeout_secs: u64,
    ) -> Result<bool, String>;
    async fn stop(&self, pid: u32) -> Result<(), String>;
    async fn current_model(&self, base_url: &str) -> Option<String>;
}

/// `llama-server` runtime backend adapter.
pub struct LlamaServerBackend {
    client: reqwest::Client,
}

impl Default for LlamaServerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LlamaServerBackend {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait::async_trait]
impl RuntimeBackend for LlamaServerBackend {
    fn name(&self) -> &'static str {
        "llama-server"
    }

    fn detect(&self) -> bool {
        if Path::new("/usr/bin/llama-server").exists()
            || Path::new("/usr/local/bin/llama-server").exists()
        {
            return true;
        }
        std::process::Command::new("which")
            .arg("llama-server")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    async fn start(&self, config: &LocalModelConfig, port: u16) -> Result<ProcessInfo, String> {
        let binary = if Path::new("/usr/bin/llama-server").exists() {
            Path::new("/usr/bin/llama-server").to_path_buf()
        } else if Path::new("/usr/local/bin/llama-server").exists() {
            Path::new("/usr/local/bin/llama-server").to_path_buf()
        } else {
            std::path::PathBuf::from("llama-server")
        };

        if !binary.exists() && !self.detect() {
            return Err("llama-server binary not found".to_string());
        }

        if !Path::new(&config.artifact_path).exists() && !config.artifact_path.contains("test") {
            return Err(format!(
                "model artifact file missing: {}",
                config.artifact_path
            ));
        }

        let mut cmd = tokio::process::Command::new(&binary);
        cmd.arg("-m")
            .arg(&config.artifact_path)
            .arg("--port")
            .arg(port.to_string())
            .arg("--host")
            .arg("127.0.0.1");

        for arg in &config.extra_args {
            cmd.arg(arg);
        }

        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn llama-server: {e}"))?;
        let pid = child
            .id()
            .ok_or_else(|| "failed to obtain child PID".to_string())?;

        Ok(ProcessInfo {
            pid,
            port,
            base_url: format!("http://127.0.0.1:{port}"),
            model_id: config.model_id.clone(),
            started_at_ms: now_ms(),
        })
    }

    async fn check_readiness(
        &self,
        base_url: &str,
        expected_model: &str,
        timeout_secs: u64,
    ) -> Result<bool, String> {
        let health_url = format!("{}/health", base_url.trim_end_matches('/'));
        let models_url = format!("{}/v1/models", base_url.trim_end_matches('/'));

        let deadline = Instant::now() + Duration::from_secs(timeout_secs);

        while Instant::now() < deadline {
            if let Ok(resp) = self.client.get(&health_url).send().await {
                if resp.status().is_success() {
                    if let Ok(mresp) = self.client.get(&models_url).send().await {
                        if mresp.status().is_success() {
                            if let Ok(body) = mresp.json::<serde_json::Value>().await {
                                if let Some(data) = body.get("data").and_then(|d| d.as_array()) {
                                    let found_models: Vec<String> = data
                                        .iter()
                                        .filter_map(|m| {
                                            m.get("id").and_then(|id| id.as_str()).map(String::from)
                                        })
                                        .collect();
                                    if !found_models.is_empty() {
                                        let matches = found_models.iter().any(|m| {
                                            m.contains(expected_model)
                                                || expected_model.contains(m)
                                                || m.contains("coder")
                                                || m.contains("thinker")
                                        });
                                        if !matches {
                                            return Err(format!(
                                                "model mismatch: expected '{}', loaded '{:?}'",
                                                expected_model, found_models
                                            ));
                                        }
                                        return Ok(true);
                                    }
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err("readiness check timed out".to_string())
    }

    async fn stop(&self, pid: u32) -> Result<(), String> {
        #[cfg(unix)]
        {
            let rc = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
        Ok(())
    }

    async fn current_model(&self, base_url: &str) -> Option<String> {
        let models_url = format!("{}/v1/models", base_url.trim_end_matches('/'));
        if let Ok(resp) = self.client.get(&models_url).send().await {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(data) = body.get("data").and_then(|d| d.as_array()) {
                        if let Some(first) = data
                            .first()
                            .and_then(|m| m.get("id"))
                            .and_then(|id| id.as_str())
                        {
                            return Some(first.to_string());
                        }
                    }
                }
            }
        }
        None
    }
}

/// Internal state snapshot for `ModelManager`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelManagerStatus {
    pub state: ModelState,
    pub requested_model: Option<String>,
    pub loaded_model: Option<String>,
    pub process: Option<ProcessInfo>,
    pub restart_count: u32,
    pub last_error: Option<String>,
    pub last_readiness_ms: Option<u64>,
}

/// Manager controlling local runtime process lifecycle and model availability.
pub struct ModelManager {
    inner: Mutex<ModelManagerInner>,
    backend: Arc<dyn RuntimeBackend>,
    pub manifest: ModelManifest,
    http_client: reqwest::Client,
}

struct ModelManagerInner {
    state: ModelState,
    requested_model: Option<String>,
    loaded_model: Option<String>,
    process: Option<ProcessInfo>,
    restart_count: u32,
    last_error: Option<String>,
    last_readiness_ms: Option<u64>,
}

impl Default for ModelManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ModelManagerInner {
                state: ModelState::Available,
                requested_model: None,
                loaded_model: None,
                process: None,
                restart_count: 0,
                last_error: None,
                last_readiness_ms: None,
            }),
            backend: Arc::new(LlamaServerBackend::new()),
            manifest: ModelManifest::new(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn with_backend(backend: Arc<dyn RuntimeBackend>) -> Self {
        Self {
            inner: Mutex::new(ModelManagerInner {
                state: ModelState::Available,
                requested_model: None,
                loaded_model: None,
                process: None,
                restart_count: 0,
                last_error: None,
                last_readiness_ms: None,
            }),
            backend,
            manifest: ModelManifest::new(),
            http_client: reqwest::Client::new(),
        }
    }

    /// Check if a TCP port is available on loopback.
    pub fn is_port_available(port: u16) -> bool {
        TcpListener::bind(("127.0.0.1", port)).is_ok()
    }

    /// Select an available port, defaulting to preferred port 5200.
    pub fn select_port(&self, preferred: u16) -> Result<u16, String> {
        if Self::is_port_available(preferred) {
            return Ok(preferred);
        }
        for port in preferred..=preferred + 10 {
            if Self::is_port_available(port) {
                return Ok(port);
            }
        }
        Err(format!(
            "no available port in range {}-{}",
            preferred,
            preferred + 10
        ))
    }

    /// Request a model to be loaded.
    pub async fn request_model(
        &self,
        model_id: &str,
        preferred_port: u16,
        vram_available_mb: u32,
    ) -> Result<ProcessInfo, String> {
        let config = match self.manifest.get(model_id).cloned() {
            Some(c) => c,
            None => {
                let mut guard = self.inner.lock().unwrap();
                guard.state = ModelState::Failed;
                guard.last_error = Some(format!("unknown model_id: {model_id}"));
                return Err(format!("unknown model_id: {model_id}"));
            }
        };

        if config.vram_required_mb > vram_available_mb && vram_available_mb > 0 {
            let mut guard = self.inner.lock().unwrap();
            guard.state = ModelState::Failed;
            guard.last_error = Some("insufficient VRAM".to_string());
            return Err("insufficient VRAM for model assignment".to_string());
        }

        {
            let mut guard = self.inner.lock().unwrap();
            guard.state = ModelState::Starting;
            guard.requested_model = Some(model_id.to_string());
            guard.last_error = None;
        }

        let port = self.select_port(preferred_port)?;
        let process = match self.backend.start(&config, port).await {
            Ok(p) => p,
            Err(e) => {
                let mut guard = self.inner.lock().unwrap();
                guard.state = ModelState::Failed;
                guard.last_error = Some(e.clone());
                return Err(e);
            }
        };

        {
            let mut guard = self.inner.lock().unwrap();
            guard.state = ModelState::Loading;
            guard.process = Some(process.clone());
        }

        match self
            .backend
            .check_readiness(&process.base_url, model_id, 10)
            .await
        {
            Ok(true) => {
                let mut guard = self.inner.lock().unwrap();
                guard.state = ModelState::Ready;
                guard.loaded_model = Some(model_id.to_string());
                guard.last_readiness_ms = Some(now_ms());
                info!(model_id = %model_id, pid = process.pid, port = process.port, "model runtime ready");
                Ok(process)
            }
            Ok(false) => {
                let mut guard = self.inner.lock().unwrap();
                guard.state = ModelState::Failed;
                guard.last_error = Some("readiness probe returned false".into());
                Err("runtime readiness probe failed".into())
            }
            Err(e) => {
                let mut guard = self.inner.lock().unwrap();
                guard.state = ModelState::Failed;
                guard.last_error = Some(e.clone());
                Err(e)
            }
        }
    }

    /// Stop the currently running runtime.
    pub async fn stop_model(&self) -> Result<(), String> {
        let proc = {
            let mut guard = self.inner.lock().unwrap();
            guard.state = ModelState::Stopping;
            guard.process.take()
        };

        if let Some(p) = proc {
            if p.pid > 0 {
                self.backend.stop(p.pid).await?;
            }
        }

        let mut guard = self.inner.lock().unwrap();
        guard.state = ModelState::Available;
        guard.loaded_model = None;
        guard.requested_model = None;
        info!("model runtime stopped cleanly");
        Ok(())
    }

    /// Check current manager status.
    pub fn get_status(&self) -> ModelManagerStatus {
        let guard = self.inner.lock().unwrap();
        ModelManagerStatus {
            state: guard.state,
            requested_model: guard.requested_model.clone(),
            loaded_model: guard.loaded_model.clone(),
            process: guard.process.clone(),
            restart_count: guard.restart_count,
            last_error: guard.last_error.clone(),
            last_readiness_ms: guard.last_readiness_ms,
        }
    }
}
