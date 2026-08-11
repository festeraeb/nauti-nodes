pub mod errors;

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub use errors::{Error, Result};

pub type NodeId = String;
pub type JobId = String;

pub const MAILBOX_CAP: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub max_batch: u32,
    pub models: Vec<String>,
    pub vram_mb: u32,
    #[serde(default)]
    pub role: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeStatistics {
    pub active_jobs: u32,
    pub avg_latency_ms: f64,
    pub free_vram_mb: u32,
    pub tokens_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub id: NodeId,
    pub addr: String,
    pub capabilities: NodeCapabilities,
    pub public_key: Vec<u8>,
    /// OpenAI-compatible llama-server base URL
    #[serde(default)]
    pub inference_url: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub gpu_name: String,
    #[serde(default = "default_true")]
    pub online: bool,
}

fn default_true() -> bool {
    true
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct NodeRuntime {
    pub metadata: std::sync::RwLock<NodeMetadata>,
    pub mailbox: tokio::sync::mpsc::Sender<WorkerMessage>,
    pub stats: std::sync::RwLock<NodeStatistics>,
    pub last_heartbeat: AtomicU64,
    pub active_jobs: AtomicU32,
}

impl NodeRuntime {
    pub fn new(meta: NodeMetadata, mailbox: tokio::sync::mpsc::Sender<WorkerMessage>) -> Self {
        Self {
            metadata: std::sync::RwLock::new(meta),
            mailbox,
            stats: std::sync::RwLock::new(NodeStatistics::default()),
            last_heartbeat: AtomicU64::new(now_ms()),
            active_jobs: AtomicU32::new(0),
        }
    }

    pub fn touch_heartbeat(&self, stats: NodeStatistics) {
        self.last_heartbeat.store(now_ms(), Ordering::Relaxed);
        let jobs = stats.active_jobs;
        if let Ok(mut s) = self.stats.write() {
            *s = stats;
        }
        self.active_jobs.store(jobs, Ordering::Relaxed);
    }

    pub fn last_heartbeat_ms(&self) -> u64 {
        self.last_heartbeat.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub enum WorkerMessage {
    JobAssigned { job_id: JobId },
    Cancel { job_id: JobId },
}
