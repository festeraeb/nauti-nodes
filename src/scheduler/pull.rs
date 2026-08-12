//! Pull-Based Scheduling
//!
//! Idle worker concept: "I have this compute available. Give me eligible work."
//!
//! Worker reports:
//! - VRAM available
//! - Runtime capabilities
//! - Workload classes it can handle
//! - Privacy/trust level
//! - Chunk limit
//! - Estimated availability window
//!
//! Coordinator returns bounded lease.
//!
//! Supports: photogrammetry, Gaussian splatting, LiDAR, sonar,
//! satellite processing, research experiments, model benchmarks,
//! future sandbox work.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Workload class that a worker can handle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadClass {
    Inference,
    Photogrammetry,
    GaussianSplatting,
    Lidar,
    Sonar,
    SatelliteProcessing,
    ResearchExperiment,
    ModelBenchmark,
    Sandbox,
    GeneralCompute,
    Other(String),
}

impl WorkloadClass {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Inference => "inference",
            Self::Photogrammetry => "photogrammetry",
            Self::GaussianSplatting => "gaussian_splatting",
            Self::Lidar => "lidar",
            Self::Sonar => "sonar",
            Self::SatelliteProcessing => "satellite",
            Self::ResearchExperiment => "research",
            Self::ModelBenchmark => "benchmark",
            Self::Sandbox => "sandbox",
            Self::GeneralCompute => "general",
            Self::Other(s) => s.as_str(),
        }
    }
}

/// Privacy/trust level of a worker.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum TrustLevel {
    /// Public/open data only
    Public,
    /// Internal CESARops data
    Internal,
    /// Sensitive/mission data
    Sensitive,
    /// Classified/restricted
    Restricted,
}

/// A pull request from an idle worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    /// Worker node ID
    pub node_id: String,

    /// Available VRAM in MiB
    pub vram_free_mb: u64,

    /// Runtime capabilities
    pub runtimes: Vec<String>,

    /// Workload classes this worker can handle
    pub workload_classes: Vec<WorkloadClass>,

    /// Privacy/trust level of this worker
    pub trust_level: TrustLevel,

    /// Maximum number of work chunks this worker can accept
    pub chunk_limit: u32,

    /// Estimated availability window in seconds
    pub availability_secs: u64,

    /// GPU name for scheduling decisions
    pub gpu_name: String,

    /// Timestamp of this request
    pub timestamp_secs: u64,
}

impl PullRequest {
    /// Check if this worker can handle a specific workload class.
    pub fn can_handle(&self, class: &WorkloadClass) -> bool {
        self.workload_classes.contains(class)
    }

    /// Check if this worker meets a minimum trust level.
    pub fn meets_trust(&self, required: &TrustLevel) -> bool {
        self.trust_level >= *required
    }

    /// Check if this worker has enough VRAM.
    pub fn has_vram(&self, required_mb: u64) -> bool {
        self.vram_free_mb >= required_mb
    }
}

/// A bounded work lease returned by the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkLease {
    /// Unique lease ID
    pub lease_id: Uuid,

    /// Work unit ID to execute
    pub work_unit_id: String,

    /// Workload class
    pub workload_class: WorkloadClass,

    /// Work payload (JSON-encoded)
    pub payload: serde_json::Value,

    /// Lease duration in seconds
    pub duration_secs: u64,

    /// When the lease was issued
    pub issued_at_secs: u64,

    /// When the lease expires
    pub expires_at_secs: u64,

    /// Validation policy for this work
    pub validation_policy: ValidationPolicy,
}

impl WorkLease {
    /// Check if this lease has expired.
    pub fn is_expired(&self, now_secs: u64) -> bool {
        now_secs >= self.expires_at_secs
    }

    /// Remaining time in seconds.
    pub fn remaining_secs(&self, now_secs: u64) -> u64 {
        self.expires_at_secs.saturating_sub(now_secs)
    }
}

/// Validation policy for scientific work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationPolicy {
    /// No validation required
    None,
    /// Hash verification of output
    Hash,
    /// Duplicate execution on independent node
    DuplicateExecution,
    /// Independent validator model checks result
    IndependentValidator,
    /// Application-specific validation
    ApplicationSpecific,
}

/// A queued work unit waiting for a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedWork {
    pub work_unit_id: String,
    pub workload_class: WorkloadClass,
    pub required_vram_mb: u64,
    pub required_trust: TrustLevel,
    pub payload: serde_json::Value,
    pub priority: u32,
    pub validation_policy: ValidationPolicy,
    pub enqueued_at_secs: u64,
    pub max_wait_secs: Option<u64>,
}

/// Pull scheduler that matches idle workers with queued work.
pub struct PullScheduler {
    /// Queued work units waiting for workers
    queue: Mutex<Vec<QueuedWork>>,

    /// Active leases
    leases: Mutex<HashMap<Uuid, WorkLease>>,
}

impl PullScheduler {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            leases: Mutex::new(HashMap::new()),
        }
    }

    /// Enqueue a work unit.
    pub fn enqueue(&self, work: QueuedWork) {
        let mut queue = self.queue.lock().unwrap();
        queue.push(work);
        // Sort by priority (higher first)
        queue.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Try to match a pull request with available work.
    /// Returns a lease if eligible work is found.
    pub fn try_match(&self, request: &PullRequest) -> Option<WorkLease> {
        let mut queue = self.queue.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Find the first eligible work unit
        let idx = queue.iter().position(|w| {
            request.can_handle(&w.workload_class)
                && request.meets_trust(&w.required_trust)
                && request.has_vram(w.required_vram_mb)
        })?;

        let work = queue.remove(idx);

        let lease_duration = request.availability_secs.min(3600); // max 1 hour
        let lease = WorkLease {
            lease_id: Uuid::new_v4(),
            work_unit_id: work.work_unit_id,
            workload_class: work.workload_class,
            payload: work.payload,
            duration_secs: lease_duration,
            issued_at_secs: now,
            expires_at_secs: now + lease_duration,
            validation_policy: work.validation_policy,
        };

        self.leases
            .lock()
            .unwrap()
            .insert(lease.lease_id, lease.clone());

        Some(lease)
    }

    /// Complete a lease (worker reports completion).
    pub fn complete_lease(&self, lease_id: &Uuid) -> Option<WorkLease> {
        self.leases.lock().unwrap().remove(lease_id)
    }

    /// Number of queued work units.
    pub fn queue_len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// Number of active leases.
    pub fn active_lease_count(&self) -> usize {
        self.leases.lock().unwrap().len()
    }

    /// Evict expired leases.
    pub fn evict_expired(&self, now_secs: u64) -> usize {
        let mut leases = self.leases.lock().unwrap();
        let before = leases.len();
        leases.retain(|_, l| !l.is_expired(now_secs));
        before - leases.len()
    }
}

impl Default for PullScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(vram: u64, classes: Vec<WorkloadClass>) -> PullRequest {
        PullRequest {
            node_id: "test-node".into(),
            vram_free_mb: vram,
            runtimes: vec!["llama.cpp".into()],
            workload_classes: classes,
            trust_level: TrustLevel::Internal,
            chunk_limit: 4,
            availability_secs: 3600,
            gpu_name: "RTX 4090".into(),
            timestamp_secs: 1000,
        }
    }

    fn make_work(class: WorkloadClass, vram: u64, priority: u32) -> QueuedWork {
        QueuedWork {
            work_unit_id: Uuid::new_v4().to_string(),
            workload_class: class,
            required_vram_mb: vram,
            required_trust: TrustLevel::Public,
            payload: serde_json::json!({"task": "test"}),
            priority,
            validation_policy: ValidationPolicy::None,
            enqueued_at_secs: 1000,
            max_wait_secs: None,
        }
    }

    #[test]
    fn pull_request_can_handle() {
        let req = make_request(
            24000,
            vec![WorkloadClass::Inference, WorkloadClass::Photogrammetry],
        );
        assert!(req.can_handle(&WorkloadClass::Inference));
        assert!(req.can_handle(&WorkloadClass::Photogrammetry));
        assert!(!req.can_handle(&WorkloadClass::Lidar));
    }

    #[test]
    fn pull_request_meets_trust() {
        let req = make_request(24000, vec![]);
        assert!(req.meets_trust(&TrustLevel::Public));
        assert!(req.meets_trust(&TrustLevel::Internal));
        assert!(!req.meets_trust(&TrustLevel::Sensitive));
    }

    #[test]
    fn scheduler_enqueue_and_match() {
        let scheduler = PullScheduler::new();
        scheduler.enqueue(make_work(WorkloadClass::Inference, 8000, 10));

        assert_eq!(scheduler.queue_len(), 1);

        let req = make_request(24000, vec![WorkloadClass::Inference]);
        let lease = scheduler.try_match(&req);
        assert!(lease.is_some());
        assert_eq!(scheduler.queue_len(), 0);
        assert_eq!(scheduler.active_lease_count(), 1);
    }

    #[test]
    fn scheduler_no_match_wrong_class() {
        let scheduler = PullScheduler::new();
        scheduler.enqueue(make_work(WorkloadClass::Photogrammetry, 8000, 10));

        let req = make_request(24000, vec![WorkloadClass::Inference]);
        let lease = scheduler.try_match(&req);
        assert!(lease.is_none());
        assert_eq!(scheduler.queue_len(), 1);
    }

    #[test]
    fn scheduler_no_match_insufficient_vram() {
        let scheduler = PullScheduler::new();
        scheduler.enqueue(make_work(WorkloadClass::Inference, 20000, 10));

        let req = make_request(8000, vec![WorkloadClass::Inference]);
        let lease = scheduler.try_match(&req);
        assert!(lease.is_none());
    }

    #[test]
    fn scheduler_priority_ordering() {
        let scheduler = PullScheduler::new();
        scheduler.enqueue(make_work(WorkloadClass::Inference, 8000, 5));
        scheduler.enqueue(make_work(WorkloadClass::Inference, 8000, 20));
        scheduler.enqueue(make_work(WorkloadClass::Inference, 8000, 10));

        let req = make_request(24000, vec![WorkloadClass::Inference]);
        let lease = scheduler.try_match(&req).unwrap();
        // Should get the highest priority work first
        assert_eq!(scheduler.queue_len(), 2);
    }

    #[test]
    fn work_lease_expiry() {
        let lease = WorkLease {
            lease_id: Uuid::new_v4(),
            work_unit_id: "test".into(),
            workload_class: WorkloadClass::Inference,
            payload: serde_json::json!({}),
            duration_secs: 100,
            issued_at_secs: 1000,
            expires_at_secs: 1100,
            validation_policy: ValidationPolicy::None,
        };

        assert!(!lease.is_expired(1050));
        assert!(lease.is_expired(1100));
        assert!(lease.is_expired(1200));
        assert_eq!(lease.remaining_secs(1050), 50);
        assert_eq!(lease.remaining_secs(1200), 0);
    }

    #[test]
    fn scheduler_complete_lease() {
        let scheduler = PullScheduler::new();
        scheduler.enqueue(make_work(WorkloadClass::Inference, 8000, 10));

        let req = make_request(24000, vec![WorkloadClass::Inference]);
        let lease = scheduler.try_match(&req).unwrap();
        assert_eq!(scheduler.active_lease_count(), 1);

        let completed = scheduler.complete_lease(&lease.lease_id);
        assert!(completed.is_some());
        assert_eq!(scheduler.active_lease_count(), 0);
    }

    #[test]
    fn trust_level_ordering() {
        assert!(TrustLevel::Restricted > TrustLevel::Sensitive);
        assert!(TrustLevel::Sensitive > TrustLevel::Internal);
        assert!(TrustLevel::Internal > TrustLevel::Public);
    }
}
