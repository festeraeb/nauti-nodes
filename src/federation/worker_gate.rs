//! Worker-side fenced command execution (Chunk 8B.2).
//!
//! This is the versioned command path: ownership-changing commands reach a
//! worker ONLY as [`SignedFederationCommand`] envelopes. Every command is
//! (1) authenticated against the durable coordinator identity and
//! (2) fenced against the durable per-resource epoch BEFORE any state is
//! mutated. There is no un-fenced back door: the worker exposes no other
//! command surface.
//!
//! Execution truth: commands mutate REAL state (SQLite lease store,
//! checkpoint registry, ops journal, job registry). Nothing here simulates
//! execution; a command either really applied or really rejected.

use super::envelope::{FederationAuthError, FederationAuthenticator, SignedFederationCommand};
use super::fence::{FenceDecision, FenceGate};
use crate::eligibility::LeaseState;
use crate::jobs::{JobRecord, JobStore};
use crate::lease::{LeaseError, LeaseManager, ReserveOutcome};
use crate::opsstore::{OpsEventKind, OpsStore};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Observable counters for fenced command handling. All values come from
/// real accept/reject decisions — never synthesized.
#[derive(Debug, Default)]
pub struct FenceCounters {
    pub commands_accepted_total: AtomicU64,
    pub commands_rejected_auth_total: AtomicU64,
    pub commands_rejected_stale_epoch_total: AtomicU64,
    pub commands_rejected_authority_conflict_total: AtomicU64,
    /// Re-deliveries of an already-applied command (blocked, not re-run).
    pub duplicate_commands_blocked_total: AtomicU64,
    pub checkpoint_resumes_total: AtomicU64,
    /// A checkpoint resume that found the checkpoint already consumed.
    pub checkpoint_double_resume_blocked_total: AtomicU64,
}

/// Outcome of a successfully fenced + authenticated command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    /// The command mutated real state.
    Applied,
    /// The same command (same authority + idempotency key) was already
    /// applied; nothing ran again.
    IdempotentDuplicate,
    /// Lease release arrived for an already-terminal lease: no-op success.
    LeaseAlreadyTerminal,
    /// Checkpoint was already consumed: resume blocked (exactly-once).
    CheckpointAlreadyConsumed,
}

/// Why a command did not execute.
#[derive(Debug)]
pub enum CommandReject {
    Auth(FederationAuthError),
    Fence(FenceDecision),
    Execution(String),
}

impl std::fmt::Display for CommandReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auth(e) => write!(f, "federation auth rejected: {e}"),
            Self::Fence(d) => write!(f, "fence rejected: {d:?}"),
            Self::Execution(e) => write!(f, "command execution failed: {e}"),
        }
    }
}

/// Worker-side gate: authenticator + durable fence + real executors.
pub struct WorkerFenceState {
    pub authenticator: FederationAuthenticator,
    pub gate: FenceGate,
    pub leases: Arc<LeaseManager>,
    pub ops: Arc<OpsStore>,
    pub jobs: Arc<JobStore>,
    /// idempotency_key -> authority that applied it.
    applied: DashMap<String, String>,
    /// resource_id -> runtime ownership marker ("STOPPED" / model id).
    runtime_state: DashMap<String, String>,
    pub counters: FenceCounters,
}

impl WorkerFenceState {
    pub fn new(
        authenticator: FederationAuthenticator,
        gate: FenceGate,
        leases: Arc<LeaseManager>,
        ops: Arc<OpsStore>,
        jobs: Arc<JobStore>,
    ) -> Self {
        Self {
            authenticator,
            gate,
            leases,
            ops,
            jobs,
            applied: DashMap::new(),
            runtime_state: DashMap::new(),
            counters: FenceCounters::default(),
        }
    }

    /// Number of applied (not duplicated) commands for one idempotency key.
    pub fn applied_by(&self, idempotency_key: &str) -> Option<String> {
        self.applied.get(idempotency_key).map(|v| v.clone())
    }

    pub fn runtime_marker(&self, resource_id: &str) -> Option<String> {
        self.runtime_state.get(resource_id).map(|v| v.clone())
    }

    /// Full pipeline: authenticate -> fence -> execute. Fail closed at
    /// every stage.
    pub fn handle(&self, env: &SignedFederationCommand) -> Result<CommandOutcome, CommandReject> {
        // 1. Authentication (version, trust, signature, revocation,
        //    freshness, replay).
        if let Err(e) = self.authenticator.authenticate(env) {
            self.counters
                .commands_rejected_auth_total
                .fetch_add(1, Ordering::SeqCst);
            let _ = self.ops.record(
                OpsEventKind::OwnershipConflictBlocked,
                Some(env.command.resource_id()),
                None,
                None,
                None,
                &format!("federation command auth rejected: {e}"),
                "AUTH_REJECTED",
            );
            return Err(CommandReject::Auth(e));
        }

        let resource = env.command.resource_id().to_string();
        let epoch = env.command.epoch();
        let authority = env.sender_coordinator_id.clone();

        // 2. Durable fence.
        let decision = self
            .gate
            .check(&resource, epoch, &authority)
            .map_err(|e| CommandReject::Execution(e.to_string()))?;
        match &decision {
            FenceDecision::RejectedStaleEpoch { .. } => {
                self.counters
                    .commands_rejected_stale_epoch_total
                    .fetch_add(1, Ordering::SeqCst);
                let _ = self.ops.record(
                    OpsEventKind::OwnershipConflictBlocked,
                    Some(&resource),
                    None,
                    None,
                    None,
                    &format!(
                        "stale-epoch command rejected ({} {} epoch={})",
                        env.command.kind_str(),
                        authority,
                        epoch
                    ),
                    "STALE_EPOCH_REJECTED",
                );
                return Err(CommandReject::Fence(decision));
            }
            FenceDecision::RejectedAuthorityConflict { .. } => {
                self.counters
                    .commands_rejected_authority_conflict_total
                    .fetch_add(1, Ordering::SeqCst);
                let _ = self.ops.record(
                    OpsEventKind::OwnershipConflictBlocked,
                    Some(&resource),
                    None,
                    None,
                    None,
                    &format!(
                        "authority-conflict command rejected ({} {} epoch={})",
                        env.command.kind_str(),
                        authority,
                        epoch
                    ),
                    "AUTHORITY_CONFLICT_REJECTED",
                );
                return Err(CommandReject::Fence(decision));
            }
            _ => {}
        }

        // 3. Idempotency: an already-applied command from the same
        //    authority must not execute twice.
        let key = env.command.idempotency_key();
        if let Some(prev) = self.applied.get(&key) {
            if *prev == authority {
                self.counters
                    .duplicate_commands_blocked_total
                    .fetch_add(1, Ordering::SeqCst);
                let _ = self.ops.record(
                    OpsEventKind::DuplicateRequest,
                    Some(&resource),
                    None,
                    None,
                    None,
                    &format!(
                        "duplicate {} blocked (already applied)",
                        env.command.kind_str()
                    ),
                    "DUPLICATE_BLOCKED",
                );
                return Ok(CommandOutcome::IdempotentDuplicate);
            }
            // A different authority already applied this key: only possible
            // after an epoch takeover; fall through and let the new
            // authority's command apply (fence already accepted it).
        }

        // 4. Execute against real state.
        let outcome = self.execute(env)?;
        if outcome == CommandOutcome::Applied {
            self.applied.insert(key, authority.clone());
        }
        self.counters
            .commands_accepted_total
            .fetch_add(1, Ordering::SeqCst);
        Ok(outcome)
    }

    fn execute(&self, env: &SignedFederationCommand) -> Result<CommandOutcome, CommandReject> {
        match &env.command {
            super::envelope::FederationCommand::LeaseGrant {
                lease_id,
                mission_id,
                resource_id,
                epoch,
            } => {
                // Fenced reserve through the REAL lease state machine.
                match self.leases.begin_reserve_fenced(
                    *epoch,
                    mission_id,
                    resource_id,
                    Some(lease_id),
                    None,
                    0,
                    0,
                    true,
                ) {
                    Ok(ReserveOutcome::Idempotent(_)) => {
                        return Ok(CommandOutcome::IdempotentDuplicate);
                    }
                    Ok(ReserveOutcome::Created(_)) => {}
                    Err(LeaseError::StaleEpoch { .. }) => {
                        return Err(CommandReject::Fence(FenceDecision::RejectedStaleEpoch {
                            received: *epoch,
                            current: self.leases.current_authority_epoch(),
                        }));
                    }
                    Err(e) => return Err(CommandReject::Execution(e.to_string())),
                }
                // Drive the state machine to LEASED (ownership yielded).
                let steps: [fn(&LeaseManager, &str) -> Result<(), String>; 3] = [
                    |m, id| {
                        m.mark_scheduler_excluded(id)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    },
                    |m, id| {
                        m.mark_runtime_exited(id)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    },
                    |m, id| {
                        m.mark_gpu_released(id)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    },
                ];
                for step in steps {
                    step(&self.leases, lease_id).map_err(CommandReject::Execution)?;
                }
                self.leases
                    .grant(lease_id)
                    .map_err(|e| CommandReject::Execution(e.to_string()))?;
                let _ = self.ops.record(
                    OpsEventKind::LeaseGranted,
                    Some(resource_id),
                    Some(lease_id),
                    None,
                    Some(mission_id),
                    &format!("fenced lease grant accepted at epoch {epoch}"),
                    LeaseState::Leased.as_str(),
                );
                Ok(CommandOutcome::Applied)
            }
            super::envelope::FederationCommand::LeaseRelease {
                lease_id,
                mission_id,
                resource_id,
                ..
            } => {
                match self
                    .leases
                    .begin_release(mission_id, resource_id)
                    .map_err(|e| CommandReject::Execution(e.to_string()))?
                {
                    None => Ok(CommandOutcome::LeaseAlreadyTerminal),
                    Some(rec) => {
                        self.leases
                            .mark_recovery_started(&rec.lease_id)
                            .map_err(|e| CommandReject::Execution(e.to_string()))?;
                        self.leases
                            .mark_runtime_ready(&rec.lease_id)
                            .map_err(|e| CommandReject::Execution(e.to_string()))?;
                        self.leases
                            .complete_release(&rec.lease_id)
                            .map_err(|e| CommandReject::Execution(e.to_string()))?;
                        let _ = self.ops.record(
                            OpsEventKind::LeaseReleased,
                            Some(resource_id),
                            Some(lease_id),
                            None,
                            Some(mission_id),
                            "fenced lease release accepted",
                            LeaseState::Released.as_str(),
                        );
                        Ok(CommandOutcome::Applied)
                    }
                }
            }
            super::envelope::FederationCommand::WorkAssign {
                job_id,
                resource_id,
                ..
            } => {
                // Real job registry entry (assignment truth). Execution of
                // inference itself belongs to the runtime path; here we
                // record the authoritative assignment exactly once.
                if self.jobs.get(job_id).is_some() {
                    return Ok(CommandOutcome::IdempotentDuplicate);
                }
                let (tx, _rx) = tokio::sync::broadcast::channel(16);
                self.jobs.insert(
                    job_id.clone(),
                    Arc::new(JobRecord {
                        node_id: resource_id.clone(),
                        inference_url: String::new(),
                        model: String::new(),
                        messages: Vec::new(),
                        api_key: None,
                        reserved_tokens: 0,
                        cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        tx,
                        done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    }),
                );
                let _ = self.ops.record(
                    OpsEventKind::JobResumed,
                    Some(resource_id),
                    None,
                    Some(job_id),
                    None,
                    "fenced work assignment accepted",
                    "ASSIGNED",
                );
                Ok(CommandOutcome::Applied)
            }
            super::envelope::FederationCommand::RuntimeStart {
                resource_id,
                model_id,
                ..
            } => {
                self.runtime_state
                    .insert(resource_id.clone(), model_id.clone());
                let _ = self.ops.record(
                    OpsEventKind::RuntimeReady,
                    Some(resource_id),
                    None,
                    None,
                    None,
                    &format!("fenced runtime start accepted (model {model_id})"),
                    "RUNTIME_STARTED",
                );
                Ok(CommandOutcome::Applied)
            }
            super::envelope::FederationCommand::RuntimeStop { resource_id, .. } => {
                self.runtime_state
                    .insert(resource_id.clone(), "STOPPED".to_string());
                let _ = self.ops.record(
                    OpsEventKind::RuntimeExited,
                    Some(resource_id),
                    None,
                    None,
                    None,
                    "fenced runtime stop accepted",
                    "RUNTIME_STOPPED",
                );
                Ok(CommandOutcome::Applied)
            }
            super::envelope::FederationCommand::CheckpointResume {
                job_id,
                resource_id,
                ..
            } => {
                // Exactly-once: consume_checkpoint is an atomic transition.
                let consumed = self
                    .ops
                    .consume_checkpoint(job_id)
                    .map_err(|e| CommandReject::Execution(e.to_string()))?;
                if !consumed {
                    self.counters
                        .checkpoint_double_resume_blocked_total
                        .fetch_add(1, Ordering::SeqCst);
                    let _ = self.ops.record(
                        OpsEventKind::DuplicateRequest,
                        Some(resource_id),
                        None,
                        Some(job_id),
                        None,
                        "checkpoint resume blocked: already consumed",
                        "DUPLICATE_BLOCKED",
                    );
                    return Ok(CommandOutcome::CheckpointAlreadyConsumed);
                }
                self.counters
                    .checkpoint_resumes_total
                    .fetch_add(1, Ordering::SeqCst);
                let _ = self.ops.record(
                    OpsEventKind::JobResumed,
                    Some(resource_id),
                    None,
                    Some(job_id),
                    None,
                    "fenced checkpoint resume accepted",
                    "RESUMED",
                );
                Ok(CommandOutcome::Applied)
            }
        }
    }
}
