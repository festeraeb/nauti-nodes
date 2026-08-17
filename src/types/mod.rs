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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TelemetrySourceType {
    ConfiguredStatic,
    RegisteredClaim,
    PersistedCachedObservation,
    RemoteLiveObservation,
    LocalLiveObservation,
    DerivedFromLiveObservation,
    UnknownProvenance,
}

impl Default for TelemetrySourceType {
    fn default() -> Self {
        Self::UnknownProvenance
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedHardware {
    pub gpu_name: String,
    pub vram_mb: u32,
    pub vendor: String,
    pub device_count: u32,
    pub driver_version: Option<String>,
    pub temperature_c: Option<u32>,

    // Provenance Metadata
    pub node_id: String,
    pub observer_node_id: String,
    pub source_type: TelemetrySourceType,
    pub provider: String,
    pub observed_at: Option<u64>,
    pub received_at: Option<u64>,

    // Optional attributes
    pub measurement_id: Option<String>,
    pub device_id: Option<String>,
    pub sequence: Option<u64>,
    pub age_ms: Option<u64>,
    pub source_endpoint: Option<String>,
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

pub fn reconcile_claim(
    claim: &RegisteredHardwareClaim,
    observed: &Option<ObservedHardware>,
) -> ReconciledHardwareState {
    if let Some(obs) = observed {
        let mut fields = Vec::new();
        let c_gpu = claim.gpu_name.to_lowercase();
        let o_gpu = obs.gpu_name.to_lowercase();

        if c_gpu != o_gpu {
            fields.push("model mismatch".to_string());
            let c_vendor = if c_gpu.contains("nvidia")
                || c_gpu.contains("rtx")
                || c_gpu.contains("gtx")
                || c_gpu.contains("tesla")
                || c_gpu.contains("quadro")
            {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeLifecycleState {
    Joining,
    Verifying,
    Online,
    Degraded,
    Draining,
    Offline,
    Withdrawn,
    Revoked,
}

impl Default for NodeLifecycleState {
    fn default() -> Self {
        Self::Online
    }
}

impl NodeLifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Joining => "JOINING",
            Self::Verifying => "VERIFYING",
            Self::Online => "ONLINE",
            Self::Degraded => "DEGRADED",
            Self::Draining => "DRAINING",
            Self::Offline => "OFFLINE",
            Self::Withdrawn => "WITHDRAWN",
            Self::Revoked => "REVOKED",
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
    #[serde(default)]
    pub lifecycle_state: NodeLifecycleState,
    /// Chunk 7: truth state of the inference runtime on this node.
    /// Defaults to Ready for RC1 wire compatibility (signed volunteer
    /// workers assert their own runtime truth via heartbeats); coordinator
    /// restart recovery resets recovered records to Unknown (fail closed).
    #[serde(default)]
    pub runtime_state: crate::eligibility::RuntimeState,
    /// Chunk 7: authoritative active Forge lease reference (None = no lease).
    /// Maintained by the LeaseManager under the metadata write lock so the
    /// eligibility decision has a single composition point.
    #[serde(default)]
    pub active_lease: Option<crate::eligibility::ActiveLeaseRef>,
    /// Chunk 7: when the operational state last changed (ms since epoch).
    #[serde(default)]
    pub last_state_change_ms: u64,
    /// Chunk 7: human-readable failure reason when the node is failing closed.
    #[serde(default)]
    pub failure_reason: String,
    /// Chunk 7: coordinator-confirmed in-flight shared assignments.
    /// Lives INSIDE the metadata lock so `try_assign` can atomically check
    /// eligibility and claim the slot (closes the work-vs-lease race).
    /// Not serialized on the wire.
    #[serde(skip)]
    pub assigned_jobs: u32,
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
            lifecycle_state: NodeLifecycleState::Online,
            runtime_state: crate::eligibility::RuntimeState::default(),
            active_lease: None,
            last_state_change_ms: 0,
            failure_reason: String::new(),
            assigned_jobs: 0,
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
    /// Worker-reported active jobs (from heartbeats; observational).
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

    /// Coordinator-side confirmed assignment count.
    pub fn assigned_jobs_count(&self) -> u32 {
        self.metadata.read().map(|m| m.assigned_jobs).unwrap_or(0)
    }

    /// Atomically confirm a shared-work assignment on this node.
    ///
    /// Fail closed: if ANY authoritative state says the GPU is not eligible
    /// (lease active, draining, revoked, runtime down, ...), the assignment
    /// is refused. The eligibility check and the slot increment happen under
    /// the SAME metadata write lock that lease transitions use, so a lease
    /// granted concurrently between node selection and this call causes the
    /// assignment to be rejected (no TOCTOU window).
    pub fn try_assign(&self) -> bool {
        let mut md = match self.metadata.write() {
            Ok(m) => m,
            Err(_) => return false,
        };
        if crate::eligibility::evaluate_metadata(&md).is_err() {
            return false;
        }
        md.assigned_jobs = md.assigned_jobs.saturating_add(1);
        true
    }

    /// Release a previously confirmed assignment slot.
    pub fn release_assignment(&self) {
        if let Ok(mut md) = self.metadata.write() {
            md.assigned_jobs = md.assigned_jobs.saturating_sub(1);
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
            driver_version: None,
            temperature_c: None,
            node_id: "test-node".to_string(),
            observer_node_id: "coordinator".to_string(),
            source_type: TelemetrySourceType::LocalLiveObservation,
            provider: "all-smi".to_string(),
            observed_at: Some(now_ms()),
            received_at: Some(now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
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
            driver_version: None,
            temperature_c: None,
            node_id: "test-node".to_string(),
            observer_node_id: "coordinator".to_string(),
            source_type: TelemetrySourceType::LocalLiveObservation,
            provider: "all-smi".to_string(),
            observed_at: Some(now_ms()),
            received_at: Some(now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
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
            driver_version: None,
            temperature_c: None,
            node_id: "test-node".to_string(),
            observer_node_id: "coordinator".to_string(),
            source_type: TelemetrySourceType::LocalLiveObservation,
            provider: "all-smi".to_string(),
            observed_at: Some(now_ms()),
            received_at: Some(now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
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
            driver_version: None,
            temperature_c: None,
            node_id: "test-node".to_string(),
            observer_node_id: "coordinator".to_string(),
            source_type: TelemetrySourceType::LocalLiveObservation,
            provider: "all-smi".to_string(),
            observed_at: Some(now_ms()),
            received_at: Some(now_ms()),
            measurement_id: None,
            device_id: None,
            sequence: None,
            age_ms: None,
            source_endpoint: None,
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
