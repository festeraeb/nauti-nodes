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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegisteredHardwareClaim {
    pub gpu_name: String,
    pub vram_mb: u32,
    pub models: Vec<String>,
}

impl Default for RegisteredHardwareClaim {
    fn default() -> Self {
        Self {
            gpu_name: "UNKNOWN".to_string(),
            vram_mb: 0,
            models: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedHardware {
    pub gpu_name: String,
    pub vram_mb: u32,
    pub vendor: String,
    pub device_count: u32,
    pub observed_at: u64,
    pub driver_version: Option<String>,
    pub temperature_c: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconciliationStatus {
    MATCH,
    MISMATCH,
    UNVERIFIED,
    STALE,
    OFFLINE,
    PARTIAL_MATCH,
}

impl Default for ReconciliationStatus {
    fn default() -> Self {
        Self::UNVERIFIED
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReconciledHardwareState {
    pub status: ReconciliationStatus,
    pub fields: Vec<String>,
}

impl Default for ReconciledHardwareState {
    fn default() -> Self {
        Self {
            status: ReconciliationStatus::UNVERIFIED,
            fields: Vec::new(),
        }
    }
}

pub fn reconcile_claim(claim: &RegisteredHardwareClaim, observed: &Option<ObservedHardware>) -> ReconciledHardwareState {
    if let Some(obs) = observed {
        let mut fields = Vec::new();
        let c_gpu = claim.gpu_name.to_lowercase();
        let o_gpu = obs.gpu_name.to_lowercase();
        
        if c_gpu != o_gpu {
            fields.push("model mismatch".to_string());
            let c_vendor = if c_gpu.contains("nvidia") || c_gpu.contains("rtx") || c_gpu.contains("gtx") || c_gpu.contains("tesla") || c_gpu.contains("quadro") {
                "nvidia"
            } else if c_gpu.contains("amd") || c_gpu.contains("radeon") {
                "amd"
            } else if c_gpu.contains("intel") {
                "intel"
            } else {
                "other"
            };
            let o_vendor = obs.vendor.to_lowercase();
            if c_vendor != o_vendor && o_vendor != "unknown" {
                fields.push("vendor mismatch".to_string());
            }
        }
        
        if claim.vram_mb != obs.vram_mb {
            fields.push("VRAM mismatch".to_string());
        }
        
        if obs.device_count == 0 {
            fields.push("device-count mismatch".to_string());
        }

        let status = if fields.is_empty() {
            ReconciliationStatus::MATCH
        } else {
            ReconciliationStatus::MISMATCH
        };
        
        ReconciledHardwareState { status, fields }
    } else {
        ReconciledHardwareState {
            status: ReconciliationStatus::UNVERIFIED,
            fields: Vec::new(),
        }
    }
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
    #[serde(default)]
    pub claim: RegisteredHardwareClaim,
    #[serde(default)]
    pub observed: Option<ObservedHardware>,
    #[serde(default)]
    pub reconciliation: ReconciledHardwareState,
    #[serde(default)]
    pub last_hardware_probe: Option<u64>,
    #[serde(default = "default_health")]
    pub health_state: String,
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            id: String::new(),
            addr: String::new(),
            capabilities: NodeCapabilities {
                max_batch: 1,
                models: Vec::new(),
                vram_mb: 0,
                role: String::new(),
            },
            public_key: Vec::new(),
            inference_url: String::new(),
            role: String::new(),
            gpu_name: String::new(),
            online: true,
            claim: RegisteredHardwareClaim::default(),
            observed: None,
            reconciliation: ReconciledHardwareState::default(),
            last_hardware_probe: None,
            health_state: "ONLINE".to_string(),
        }
    }
}

fn default_health() -> String {
    "ONLINE".to_string()
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

#[cfg(test)]
mod reconciliation_tests {
    use super::*;

    #[test]
    fn test_reconcile_match() {
        let claim = RegisteredHardwareClaim {
            gpu_name: "NVIDIA RTX 2060 SUPER".to_string(),
            vram_mb: 16384,
            models: vec!["coder-lane-a".to_string()],
        };
        let observed = Some(ObservedHardware {
            gpu_name: "NVIDIA RTX 2060 SUPER".to_string(),
            vram_mb: 16384,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            observed_at: now_ms(),
            driver_version: None,
            temperature_c: None,
        });
        
        let state = reconcile_claim(&claim, &observed);
        assert_eq!(state.status, ReconciliationStatus::MATCH);
        assert!(state.fields.is_empty());
    }

    #[test]
    fn test_reconcile_mismatch_vram() {
        let claim = RegisteredHardwareClaim {
            gpu_name: "NVIDIA RTX 2060 SUPER".to_string(),
            vram_mb: 16384,
            models: vec!["coder-lane-a".to_string()],
        };
        let observed = Some(ObservedHardware {
            gpu_name: "NVIDIA RTX 2060 SUPER".to_string(),
            vram_mb: 8192,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            observed_at: now_ms(),
            driver_version: None,
            temperature_c: None,
        });
        
        let state = reconcile_claim(&claim, &observed);
        assert_eq!(state.status, ReconciliationStatus::MISMATCH);
        assert!(state.fields.contains(&"VRAM mismatch".to_string()));
    }

    #[test]
    fn test_reconcile_mismatch_gpu() {
        let claim = RegisteredHardwareClaim {
            gpu_name: "GTX 1070".to_string(),
            vram_mb: 8192,
            models: vec![],
        };
        let observed = Some(ObservedHardware {
            gpu_name: "Tesla P100".to_string(),
            vram_mb: 8192,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            observed_at: now_ms(),
            driver_version: None,
            temperature_c: None,
        });
        
        let state = reconcile_claim(&claim, &observed);
        assert_eq!(state.status, ReconciliationStatus::MISMATCH);
        assert!(state.fields.contains(&"model mismatch".to_string()));
    }

    #[test]
    fn test_reconcile_mismatch_vendor() {
        let claim = RegisteredHardwareClaim {
            gpu_name: "Radeon RX 580".to_string(),
            vram_mb: 8192,
            models: vec![],
        };
        let observed = Some(ObservedHardware {
            gpu_name: "RTX 2060".to_string(),
            vram_mb: 8192,
            vendor: "NVIDIA".to_string(),
            device_count: 1,
            observed_at: now_ms(),
            driver_version: None,
            temperature_c: None,
        });
        
        let state = reconcile_claim(&claim, &observed);
        assert_eq!(state.status, ReconciliationStatus::MISMATCH);
        assert!(state.fields.contains(&"model mismatch".to_string()));
        assert!(state.fields.contains(&"vendor mismatch".to_string()));
    }

    #[test]
    fn test_reconcile_unverified() {
        let claim = RegisteredHardwareClaim {
            gpu_name: "RTX 2060".to_string(),
            vram_mb: 8192,
            models: vec![],
        };
        let state = reconcile_claim(&claim, &None);
        assert_eq!(state.status, ReconciliationStatus::UNVERIFIED);
        assert!(state.fields.is_empty());
    }
}
