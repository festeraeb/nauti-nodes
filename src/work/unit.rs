//! Generalized work unit for the SETI@home-style federation.
//!
//! Work units are the fundamental abstraction for distributed compute.
//! They support inference, photogrammetry, LiDAR, sonar, research experiments,
//! sandbox coding, and any future workload type.
//!
//! Lifecycle: AVAILABLE -> LEASED -> RUNNING -> COMPLETED/FAILED/EXPIRED

use crate::capabilities::{PrivacyClass, WorkloadType};
use serde::{Deserialize, Serialize};

/// Lifecycle state of a work unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkUnitState {
    /// Available for lease by an eligible worker.
    Available,
    /// Leased to a worker; waiting for execution to begin.
    Leased,
    /// Currently being executed by a worker.
    Running,
    /// Execution paused; checkpoint saved.
    Checkpointed,
    /// Execution completed successfully.
    Completed,
    /// Execution failed.
    Failed,
    /// Lease expired before completion.
    Expired,
    /// Result validated by the validation policy.
    Validated,
    /// Result rejected by validation.
    Rejected,
}

impl WorkUnitState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::Checkpointed => "checkpointed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Expired => "expired",
            Self::Validated => "validated",
            Self::Rejected => "rejected",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "available" => Self::Available,
            "leased" => Self::Leased,
            "running" => Self::Running,
            "checkpointed" => Self::Checkpointed,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "expired" => Self::Expired,
            "validated" => Self::Validated,
            "rejected" => Self::Rejected,
            _ => Self::Available,
        }
    }
}

/// Validation policy for work results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationPolicy {
    /// No validation required; accept first result.
    None,
    /// Result must match a deterministic hash.
    DeterministicHash { expected_hash: String },
    /// Run on multiple independent nodes and compare outputs.
    DuplicateExecution { required_matches: u32 },
    /// Result must be approved by an independent validator node.
    IndependentValidator { validator_node_id: String },
    /// Application-specific validation (deferred to workload provider).
    ApplicationSpecific,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self::None
    }
}

/// Checkpoint policy for long-running work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointPolicy {
    /// No checkpointing; restart from beginning on failure.
    None,
    /// Periodic checkpoints at the specified interval (seconds).
    Periodic { interval_secs: u32 },
    /// Checkpoint on preemption signal.
    OnPreemption,
    /// Both periodic and on-preemption.
    PeriodicAndPreemption { interval_secs: u32 },
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self::None
    }
}

/// A generalized work unit that can be distributed across the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkUnit {
    /// Unique work unit ID.
    pub work_unit_id: String,
    /// Mission this work belongs to.
    pub mission_id: String,
    /// Type of workload.
    pub workload_type: WorkloadType,
    /// Priority (higher = more important). 0 = default.
    pub priority: u32,
    /// Current lifecycle state.
    pub state: WorkUnitState,
    /// Privacy/trust class.
    pub privacy_class: PrivacyClass,
    /// Minimum VRAM required (MB).
    pub min_vram_mb: u32,
    /// Preferred VRAM (MB). Used for chunk sizing.
    pub preferred_vram_mb: u32,
    /// Estimated compute duration (seconds).
    pub estimated_duration_secs: u32,
    /// Whether this work unit can be split into smaller chunks.
    pub splittable: bool,
    /// Whether this work unit supports checkpointing.
    pub checkpointable: bool,
    /// Checkpoint policy.
    pub checkpoint_policy: CheckpointPolicy,
    /// Validation policy.
    pub validation_policy: ValidationPolicy,
    /// Maximum number of retry attempts on failure.
    pub max_retries: u32,
    /// Current retry count.
    pub retry_count: u32,
    /// Deadline (millis since epoch). None = no deadline.
    pub deadline_ms: Option<u64>,
    /// Artifact references (URLs, hashes, etc.) needed by the worker.
    pub artifact_refs: Vec<String>,
    /// Input data (serialized workload-specific payload).
    pub input_data: serde_json::Value,
    /// Output data (set on completion).
    pub output_data: Option<serde_json::Value>,
    /// Node currently leasing this work unit.
    pub leased_to: Option<String>,
    /// When the lease started (millis since epoch).
    pub lease_started_ms: Option<u64>,
    /// When the lease expires (millis since epoch).
    pub lease_expires_ms: Option<u64>,
    /// When the work unit was created (millis since epoch).
    pub created_at_ms: u64,
    /// When the work unit was last updated (millis since epoch).
    pub updated_at_ms: u64,
}

impl WorkUnit {
    /// Create a new work unit in Available state.
    pub fn new(
        work_unit_id: &str,
        mission_id: &str,
        workload_type: WorkloadType,
        priority: u32,
        privacy_class: PrivacyClass,
        min_vram_mb: u32,
        estimated_duration_secs: u32,
    ) -> Self {
        let now = crate::types::now_ms();
        Self {
            work_unit_id: work_unit_id.to_string(),
            mission_id: mission_id.to_string(),
            workload_type,
            priority,
            state: WorkUnitState::Available,
            privacy_class,
            min_vram_mb,
            preferred_vram_mb: min_vram_mb * 2,
            estimated_duration_secs,
            splittable: false,
            checkpointable: false,
            checkpoint_policy: CheckpointPolicy::None,
            validation_policy: ValidationPolicy::None,
            max_retries: 3,
            retry_count: 0,
            deadline_ms: None,
            artifact_refs: Vec::new(),
            input_data: serde_json::Value::Null,
            output_data: None,
            leased_to: None,
            lease_started_ms: None,
            lease_expires_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    /// Create a simple inference work unit.
    pub fn inference(
        work_unit_id: &str,
        mission_id: &str,
        model: &str,
        messages: Vec<serde_json::Value>,
        priority: u32,
    ) -> Self {
        let mut wu = Self::new(
            work_unit_id,
            mission_id,
            WorkloadType::Inference,
            priority,
            PrivacyClass::Public,
            4096,
            30,
        );
        wu.input_data = serde_json::json!({
            "model": model,
            "messages": messages,
        });
        wu
    }

    /// Check if this work unit is eligible for external alpha volunteer nodes (PUBLIC only).
    pub fn is_eligible_for_external_alpha(&self) -> bool {
        matches!(self.privacy_class, PrivacyClass::Public)
    }

    /// Check if the lease is still valid.
    pub fn lease_valid(&self) -> bool {
        if self.state != WorkUnitState::Leased && self.state != WorkUnitState::Running {
            return false;
        }
        match self.lease_expires_ms {
            Some(expires) => crate::types::now_ms() < expires,
            None => true,
        }
    }

    /// Check if the work unit is eligible for a given node's capabilities.
    pub fn eligible_for(
        &self,
        node_vram_mb: u32,
        node_privacy: &PrivacyClass,
        node_workload_types: &[WorkloadType],
    ) -> bool {
        // Must be available
        if self.state != WorkUnitState::Available {
            return false;
        }
        // VRAM check
        if node_vram_mb < self.min_vram_mb {
            return false;
        }
        // Privacy check
        if !self.privacy_compatible(node_privacy) {
            return false;
        }
        // Workload type check
        if !node_workload_types.contains(&self.workload_type) {
            return false;
        }
        true
    }

    fn privacy_compatible(&self, node_privacy: &PrivacyClass) -> bool {
        use PrivacyClass::*;
        match (&self.privacy_class, node_privacy) {
            (Public, _) => true,
            (FederationSafe, Public) => false,
            (FederationSafe, FederationSafe) => true,
            (FederationSafe, TrustedLineage) => true,
            (FederationSafe, LocalOnly) => true,
            (TrustedLineage, Public) => false,
            (TrustedLineage, FederationSafe) => false,
            (TrustedLineage, TrustedLineage) => true,
            (TrustedLineage, LocalOnly) => true,
            (LocalOnly, LocalOnly) => true,
            (LocalOnly, _) => false,
        }
    }

    /// Transition to Leased state.
    pub fn lease(&mut self, node_id: &str, lease_duration_ms: u64) {
        let now = crate::types::now_ms();
        self.state = WorkUnitState::Leased;
        self.leased_to = Some(node_id.to_string());
        self.lease_started_ms = Some(now);
        self.lease_expires_ms = Some(now + lease_duration_ms);
        self.updated_at_ms = now;
    }

    /// Transition to Running state.
    pub fn start(&mut self) {
        self.state = WorkUnitState::Running;
        self.updated_at_ms = crate::types::now_ms();
    }

    /// Complete with output data.
    pub fn complete(&mut self, output: serde_json::Value) {
        self.state = WorkUnitState::Completed;
        self.output_data = Some(output);
        self.updated_at_ms = crate::types::now_ms();
    }

    /// Mark as failed.
    pub fn fail(&mut self) {
        self.state = WorkUnitState::Failed;
        self.retry_count += 1;
        self.updated_at_ms = crate::types::now_ms();
    }

    /// Mark as expired (lease timeout).
    pub fn expire(&mut self) {
        self.state = WorkUnitState::Expired;
        self.updated_at_ms = crate::types::now_ms();
    }

    /// Reset to Available for retry.
    pub fn retry(&mut self) -> bool {
        if self.retry_count >= self.max_retries {
            return false;
        }
        self.state = WorkUnitState::Available;
        self.leased_to = None;
        self.lease_started_ms = None;
        self.lease_expires_ms = None;
        self.updated_at_ms = crate::types::now_ms();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_work_unit_is_available() {
        let wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::Public,
            4096,
            30,
        );
        assert_eq!(wu.state, WorkUnitState::Available);
        assert!(wu.leased_to.is_none());
    }

    #[test]
    fn inference_work_unit() {
        let wu = WorkUnit::inference("wu-inf-1", "m-1", "thinker", vec![], 0);
        assert_eq!(wu.workload_type, WorkloadType::Inference);
        assert!(wu.input_data.get("model").is_some());
    }

    #[test]
    fn lease_and_complete() {
        let mut wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::Public,
            4096,
            30,
        );
        wu.lease("node-A", 60_000);
        assert_eq!(wu.state, WorkUnitState::Leased);
        assert_eq!(wu.leased_to, Some("node-A".into()));
        assert!(wu.lease_valid());
        wu.start();
        assert_eq!(wu.state, WorkUnitState::Running);
        wu.complete(serde_json::json!({"tokens": 42}));
        assert_eq!(wu.state, WorkUnitState::Completed);
    }

    #[test]
    fn fail_and_retry() {
        let mut wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::Public,
            4096,
            30,
        );
        wu.fail();
        assert_eq!(wu.state, WorkUnitState::Failed);
        assert_eq!(wu.retry_count, 1);
        assert!(wu.retry());
        assert_eq!(wu.state, WorkUnitState::Available);
    }

    #[test]
    fn max_retries() {
        let mut wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::Public,
            4096,
            30,
        );
        wu.max_retries = 2;
        wu.fail();
        wu.retry();
        wu.fail();
        wu.retry();
        wu.fail();
        assert!(!wu.retry()); // exhausted
    }

    #[test]
    fn eligibility() {
        let wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::Public,
            4096,
            30,
        );
        assert!(wu.eligible_for(8192, &PrivacyClass::Public, &[WorkloadType::Inference]));
        assert!(!wu.eligible_for(2048, &PrivacyClass::Public, &[WorkloadType::Inference])); // not enough VRAM
        assert!(!wu.eligible_for(8192, &PrivacyClass::Public, &[WorkloadType::Photogrammetry]));
        // wrong workload
    }

    #[test]
    fn privacy_eligibility() {
        let wu = WorkUnit::new(
            "wu-1",
            "m-1",
            WorkloadType::Inference,
            0,
            PrivacyClass::LocalOnly,
            4096,
            30,
        );
        assert!(wu.eligible_for(8192, &PrivacyClass::LocalOnly, &[WorkloadType::Inference]));
        assert!(!wu.eligible_for(8192, &PrivacyClass::Public, &[WorkloadType::Inference]));
    }

    #[test]
    fn workload_type_roundtrip() {
        for wt in [
            WorkloadType::Inference,
            WorkloadType::Photogrammetry,
            WorkloadType::Other,
        ] {
            assert_eq!(WorkloadType::from_str(wt.as_str()), wt);
        }
    }
}
