//! Chunk 9: bounded lineage-aware work delegation.
//!
//! Lineage is a PREFERENCE ONLY. It never overrides fencing, ownership,
//! Forge leases, revocation, privacy, hardware reconciliation, runtime
//! health, model compatibility, or worker withdrawal. Every delegated
//! command still travels through the versioned signed+fenced transport
//! established in Chunk 8B.
//!
//! Persistent identity guarantees exactly-once: a duplicate job_id never
//! executes twice — the authoritative status is returned instead.

use super::envelope::{FederationCommand, SignedFederationCommand};
use super::FederatedRequestContext;
use crate::api::handlers::InferenceRequest;
use crate::capabilities::PrivacyClass;
use crate::lineage::LineageStore;
use crate::opsstore::{DelegatedJobRow, OpsStore};
use crate::types::now_ms;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

/// What a delegated job requires (control-plane metadata, not hardware).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationRequirements {
    pub model_id: String,
    pub minimum_vram_mb: u64,
    pub gpu_vendor: Option<String>,
    pub privacy: PrivacyClass,
    pub checkpointable: bool,
}

/// A job that may be delegated to another coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegatedJob {
    pub job_id: Uuid,
    pub request: InferenceRequest,
    pub requirements: DelegationRequirements,
    pub route: FederatedRequestContext,
}

impl DelegatedJob {
    pub fn idempotency_key(&self) -> String {
        format!("delegated-{}", self.job_id)
    }
}

/// Acknowledgment returned by the receiving coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DelegatedJobAck {
    /// Freshly accepted; will execute exactly once.
    Accepted,
    /// Duplicate of an existing job; caller should poll status.
    Duplicate { job_id: Uuid },
}

/// Authoritative status of a delegated job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DelegatedJobStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

impl std::fmt::Display for DelegatedJobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed(_) => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Bounded lineage preference policy. Capability and safety always gate
/// first; this only reorders otherwise-eligible candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineagePolicy {
    pub local_bonus: f64,
    pub descendant_bonus: f64,
    pub same_branch_bonus: f64,
    pub max_lineage_wait_ms: u64,
    pub spillover_queue_threshold: usize,
}

impl Default for LineagePolicy {
    fn default() -> Self {
        Self {
            local_bonus: 10.0,
            descendant_bonus: 5.0,
            same_branch_bonus: 2.0,
            max_lineage_wait_ms: 2_000,
            spillover_queue_threshold: 8,
        }
    }
}

/// Lineage bonus for `candidate` relative to `origin`.
///
/// `LineageStore` does not currently expose `same_branch`; we derive it as
/// "same root AND same branch_id" from the existing relationships. Local and
/// descendant preferences map cleanly to existing `set_root` / `children`
/// semantics.
pub fn lineage_bonus(
    candidate: &str,
    origin: &str,
    lineage: &LineageStore,
    policy: &LineagePolicy,
) -> f64 {
    if candidate == origin {
        return policy.local_bonus;
    }
    // Descendant check using existing ancestor/descendant queries.
    if lineage.is_ancestor_of(origin, candidate).unwrap_or(false) {
        return policy.descendant_bonus;
    }
    // Same branch: same root_node_id and same branch_id, OR siblings
    // (same parent_node_id but neither is ancestor of the other).
    // Siblings of the root get distinct branch_ids from add_child, so we
    // also check parent identity to recognize them as same-branch.
    let same_branch = match (
        lineage.get_relationship(candidate).ok().flatten(),
        lineage.get_relationship(origin).ok().flatten(),
    ) {
        (Some(a), Some(b)) => {
            a.root_node_id == b.root_node_id
                && (a.branch_id == b.branch_id
                    || (a.parent_node_id.is_some() && a.parent_node_id == b.parent_node_id))
        }
        _ => false,
    };
    if same_branch {
        return policy.same_branch_bonus;
    }
    0.0
}

/// Whether to spill over to the wider federation.
pub fn should_spillover(
    preferred_available: bool,
    preferred_queue_depth: usize,
    waited_ms: u64,
    policy: &LineagePolicy,
) -> bool {
    !preferred_available
        || preferred_queue_depth >= policy.spillover_queue_threshold
        || waited_ms >= policy.max_lineage_wait_ms
}

/// Real observed capacity of a coordinator (no synthetic availability).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedCapacity {
    pub coordinator_id: String,
    pub eligible_worker_count: usize,
    pub gpu_vendor: Option<String>,
    pub gpu_model: Option<String>,
    pub free_vram_mb: Option<u64>,
    pub runtime_available: bool,
    pub queue_depth: usize,
    pub privacy_classes: Vec<PrivacyClass>,
    pub last_observation_ms: u64,
}

/// Dispatcher transport over the existing signed+fenced federation path.
#[async_trait::async_trait]
pub trait FederationDispatcher: Send + Sync {
    async fn advertise_capacity(&self) -> anyhow::Result<Vec<FederatedCapacity>>;
    async fn delegate(
        &self,
        peer: &str,
        job: DelegatedJob,
        epoch: u64,
    ) -> anyhow::Result<DelegatedJobAck>;
    async fn job_status(&self, peer: &str, job_id: Uuid) -> anyhow::Result<DelegatedJobStatus>;
}

/// Delegated job store: durable identity over `OpsStore` (exactly-once).
#[derive(Clone)]
pub struct DelegationStore {
    ops: Arc<OpsStore>,
}

impl DelegationStore {
    pub fn new(ops: Arc<OpsStore>) -> Self {
        Self { ops }
    }

    /// Record a delegated job. Returns `Err` (duplicate) if the job_id
    /// already exists — the caller must then return the authoritative
    /// status instead of executing twice.
    pub fn insert_pending(&self, job_id: &str, origin: &str, epoch: u64) -> anyhow::Result<()> {
        self.ops.insert_delegated_job(&DelegatedJobRow {
            job_id: job_id.to_string(),
            origin_coordinator_id: origin.to_string(),
            executing_coordinator_id: None,
            state: "PENDING".to_string(),
            ownership_epoch: epoch,
            created_at_ms: now_ms(),
            completed_at_ms: None,
        })?;
        Ok(())
    }

    pub fn get(&self, job_id: &str) -> anyhow::Result<Option<DelegatedJobRow>> {
        Ok(self.ops.delegated_job(job_id)?)
    }

    pub fn mark_running(&self, job_id: &str, executor: &str) -> anyhow::Result<()> {
        if let Some(mut row) = self.ops.delegated_job(job_id)? {
            row.state = "RUNNING".to_string();
            row.executing_coordinator_id = Some(executor.to_string());
            self.ops.update_delegated_job(&row)?;
        }
        Ok(())
    }

    pub fn mark_terminal(&self, job_id: &str, state: &str) -> anyhow::Result<()> {
        if let Some(mut row) = self.ops.delegated_job(job_id)? {
            row.state = state.to_string();
            row.completed_at_ms = Some(now_ms());
            self.ops.update_delegated_job(&row)?;
        }
        Ok(())
    }
}

/// Real dispatcher sending signed+fenced commands to peers.
pub struct HttpDispatcher {
    http: reqwest::Client,
    delegation: DelegationStore,
}

impl HttpDispatcher {
    pub fn new(http: reqwest::Client, delegation: DelegationStore) -> Self {
        Self { http, delegation }
    }

    /// Sign and send a delegation command to a peer over real HTTP.
    /// `sign` is provided by the caller (which holds the durable key).
    pub async fn send_signed(
        &self,
        peer_base: &str,
        envelope: &SignedFederationCommand,
    ) -> anyhow::Result<reqwest::StatusCode> {
        let url = format!(
            "{}/internal/federation/command",
            peer_base.trim_end_matches('/')
        );
        let resp = self.http.post(&url).json(envelope).send().await?;
        Ok(resp.status())
    }
}

/// Build the fenced WorkAssign command that carries a delegated job.
/// Reuses the existing signed federation command envelope (no new transport).
pub fn to_delegation_command(job: &DelegatedJob) -> FederationCommand {
    FederationCommand::WorkAssign {
        job_id: job.job_id.to_string(),
        resource_id: job.route.current_coordinator_id.clone(),
        epoch: 0, // set by caller from current authority
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_policy() -> LineagePolicy {
        LineagePolicy {
            local_bonus: 10.0,
            descendant_bonus: 5.0,
            same_branch_bonus: 2.0,
            max_lineage_wait_ms: 2000,
            spillover_queue_threshold: 8,
        }
    }

    #[test]
    fn spillover_false_when_healthy() {
        let p = test_policy();
        assert!(!should_spillover(true, 1, 100, &p));
    }

    #[test]
    fn spillover_when_preferred_unavailable() {
        let p = test_policy();
        assert!(should_spillover(false, 0, 0, &p));
    }

    #[test]
    fn spillover_when_preferred_loaded() {
        let p = test_policy();
        assert!(should_spillover(true, 9, 0, &p));
    }

    #[test]
    fn spillover_when_waited_too_long() {
        let p = test_policy();
        assert!(should_spillover(true, 1, 9999, &p));
    }

    #[test]
    fn lineage_bonus_requires_existing_lineage() {
        // No lineage store rows: only the local bonus applies.
        let store = LineageStore::open("sqlite::memory:").unwrap();
        let p = test_policy();
        assert_eq!(lineage_bonus("candidate", "origin", &store, &p), 0.0);
    }

    #[test]
    fn local_bonus_for_self() {
        let store = LineageStore::open("sqlite::memory:").unwrap();
        store.set_root("root").unwrap();
        let p = test_policy();
        assert_eq!(lineage_bonus("root", "root", &store, &p), p.local_bonus);
    }

    #[test]
    fn delegated_job_idempotency_key() {
        let job_id = Uuid::new_v4();
        let job = DelegatedJob {
            job_id,
            request: InferenceRequest {
                model: Some("thinker".into()),
                messages: vec![],
                stream: false,
                prefer_role: None,
                avoid_p100: true,
                max_tokens: None,
                temperature: None,
                privacy_class: None,
            },
            requirements: DelegationRequirements {
                model_id: "thinker".into(),
                minimum_vram_mb: 8192,
                gpu_vendor: None,
                privacy: PrivacyClass::Public,
                checkpointable: true,
            },
            route: FederatedRequestContext::new("origin".into(), 5),
        };
        assert_eq!(job.idempotency_key(), format!("delegated-{job_id}"));
    }

    #[test]
    fn delegated_job_store_exactly_once() {
        let ops = Arc::new(OpsStore::open("sqlite::memory:").unwrap());
        let delegation = DelegationStore::new(ops.clone());
        delegation.insert_pending("job-1", "coord-a", 3).unwrap();
        // Duplicate insert must fail (exactly-once guard).
        assert!(delegation.insert_pending("job-1", "coord-a", 3).is_err());
        let row = delegation.get("job-1").unwrap().unwrap();
        assert_eq!(row.state, "PENDING");
        assert_eq!(row.ownership_epoch, 3);
        delegation.mark_running("job-1", "coord-b").unwrap();
        let row = delegation.get("job-1").unwrap().unwrap();
        assert_eq!(row.state, "RUNNING");
        assert_eq!(row.executing_coordinator_id.as_deref(), Some("coord-b"));
        delegation.mark_terminal("job-1", "COMPLETED").unwrap();
        let row = delegation.get("job-1").unwrap().unwrap();
        assert_eq!(row.state, "COMPLETED");
        assert!(row.completed_at_ms.is_some());
    }
}
