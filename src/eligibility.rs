//! Centralized GPU ownership + scheduler eligibility (Chunk 7).
//!
//! NON-NEGOTIABLE INVARIANT:
//! A physical GPU must never simultaneously belong to Nauti shared compute
//! AND an exclusive Forge lease. If ownership is uncertain: FAIL CLOSED.
//!
//! Every code path capable of selecting a worker MUST go through
//! [`evaluate_node`] / [`evaluate_metadata`] instead of re-implementing
//! subtly different eligibility rules.

use crate::types::{NodeLifecycleState, NodeMetadata, NodeRuntime, ReconciliationStatus};
use serde::{Deserialize, Serialize};

/// Authoritative answer to "who currently owns this GPU?".
///
/// Derived from the single authoritative composition of node lifecycle,
/// health state, runtime state and the active Forge lease reference.
/// Node lifecycle and GPU ownership are related but not conflated:
/// a node may be ONLINE while its GPU is ForgeLeased, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GpuOwnership {
    /// GPU is in the shared pool and currently has no assigned work.
    SharedAvailable,
    /// GPU is in the shared pool and currently executing shared work.
    SharedAssigned,
    /// GPU is being drained ahead of a Forge lease.
    Draining,
    /// GPU is exclusively owned by a Forge lease.
    ForgeLeased,
    /// GPU is returning from a Forge lease to the shared pool; ownership
    /// transfer is not yet verified complete.
    Recovering,
    /// GPU cannot be assigned: offline, revoked, withdrawn, or uncertain.
    Unavailable,
}

impl GpuOwnership {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SharedAvailable => "SHARED_AVAILABLE",
            Self::SharedAssigned => "SHARED_ASSIGNED",
            Self::Draining => "DRAINING",
            Self::ForgeLeased => "FORGE_LEASED",
            Self::Recovering => "RECOVERING",
            Self::Unavailable => "UNAVAILABLE",
        }
    }
}

/// Truth state of the inference runtime on a node.
///
/// For volunteer workers registered via signed join, the worker itself is the
/// runtime authority and reports runtime truth through heartbeats (a worker
/// whose local runtime is down heartbeats `free_vram_mb == 0` and
/// `tokens_per_sec == 0`). For coordinator-managed local runtimes the
/// coordinator's `ModelManager` is the authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    /// Runtime liveness has not been established since the last coordinator
    /// start / state reset. FAIL CLOSED: not eligible.
    Unknown,
    /// Runtime is live and (where coordinator-managed) model readiness and
    /// identity have been verified.
    Ready,
    /// Runtime is known to be down.
    Unavailable,
}

impl Default for RuntimeState {
    /// RC1 compatibility default: signed volunteer workers assert their own
    /// runtime truth via heartbeats, so freshly registered worker metadata
    /// starts Ready. Coordinator restart recovery explicitly resets this to
    /// Unknown until fresh truth arrives.
    fn default() -> Self {
        Self::Ready
    }
}

impl RuntimeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Ready => "READY",
            Self::Unavailable => "UNAVAILABLE",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "READY" => Self::Ready,
            "UNAVAILABLE" => Self::Unavailable,
            _ => Self::Unknown,
        }
    }
}

/// Forge lease lifecycle states (Chunk 7 formal state machine).
///
/// ```text
/// (no lease = AVAILABLE)
///   -> LEASE_REQUESTED -> DRAINING -> LEASE_GRANTED -> LEASED
///   -> RELEASING -> RECOVERING -> RELEASED (= AVAILABLE again)
/// Failure states: LEASE_FAILED, RECOVERY_FAILED, STALE.
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeaseState {
    LeaseRequested,
    Draining,
    LeaseGranted,
    Leased,
    Releasing,
    Recovering,
    /// Terminal: lease fully released, GPU returned to the shared pool.
    Released,
    /// Terminal: lease could not be granted.
    LeaseFailed,
    /// Terminal: post-lease recovery failed; operator attention required.
    RecoveryFailed,
    /// Terminal: lease was interrupted (e.g. coordinator restart before
    /// grant); kept excluded until Forge re-requests.
    Stale,
}

impl LeaseState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LeaseRequested => "LEASE_REQUESTED",
            Self::Draining => "DRAINING",
            Self::LeaseGranted => "LEASE_GRANTED",
            Self::Leased => "LEASED",
            Self::Releasing => "RELEASING",
            Self::Recovering => "RECOVERING",
            Self::Released => "RELEASED",
            Self::LeaseFailed => "LEASE_FAILED",
            Self::RecoveryFailed => "RECOVERY_FAILED",
            Self::Stale => "STALE",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "LEASE_REQUESTED" => Self::LeaseRequested,
            "DRAINING" => Self::Draining,
            "LEASE_GRANTED" => Self::LeaseGranted,
            "LEASED" => Self::Leased,
            "RELEASING" => Self::Releasing,
            "RECOVERING" => Self::Recovering,
            "RELEASED" => Self::Released,
            "LEASE_FAILED" => Self::LeaseFailed,
            "RECOVERY_FAILED" => Self::RecoveryFailed,
            "STALE" => Self::Stale,
            _ => return None,
        })
    }

    /// States in which the GPU must NOT be eligible for shared work.
    /// Fail closed for every non-terminal state.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::LeaseRequested
                | Self::Draining
                | Self::LeaseGranted
                | Self::Leased
                | Self::Releasing
                | Self::Recovering
        )
    }

    pub fn is_terminal(&self) -> bool {
        !self.is_active()
    }
}

/// A compact lease reference embedded in node metadata so that the
/// eligibility decision has ONE authoritative composition point
/// (the metadata write lock) without requiring registry callers to
/// hold a second lock.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActiveLeaseRef {
    pub lease_id: String,
    pub mission_id: String,
    pub state: LeaseState,
    /// Chunk 8: authority epoch under which this lease was granted.
    /// Used for fencing: a stale coordinator's leases are rejected.
    pub epoch: u64,
}

/// Why a node/GPU is not eligible for shared work. Fail-closed reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ineligibility {
    Offline,
    Revoked,
    Withdrawn,
    Draining,
    DegradedBeyondEligibility,
    StaleHeartbeat,
    StaleState,
    ForgeLeaseRequested,
    ForgeLeased,
    RecoveryIncomplete,
    RuntimeUnavailable,
    RuntimeUnknown,
    HardwareMismatch,
    NotLiveObserved,
    OwnershipUncertain,
    ModelUnavailable,
}

impl Ineligibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Offline => "OFFLINE",
            Self::Revoked => "REVOKED",
            Self::Withdrawn => "WITHDRAWN",
            Self::Draining => "DRAINING",
            Self::DegradedBeyondEligibility => "DEGRADED_BEYOND_ELIGIBILITY",
            Self::StaleHeartbeat => "STALE_HEARTBEAT",
            Self::StaleState => "STALE_STATE",
            Self::ForgeLeaseRequested => "FORGE_LEASE_REQUESTED",
            Self::ForgeLeased => "FORGE_LEASED",
            Self::RecoveryIncomplete => "RECOVERY_INCOMPLETE",
            Self::RuntimeUnavailable => "RUNTIME_UNAVAILABLE",
            Self::RuntimeUnknown => "RUNTIME_UNKNOWN",
            Self::HardwareMismatch => "HARDWARE_MISMATCH",
            Self::NotLiveObserved => "NOT_LIVE_OBSERVED",
            Self::OwnershipUncertain => "OWNERSHIP_UNCERTAIN",
            Self::ModelUnavailable => "MODEL_UNAVAILABLE",
        }
    }
}

impl std::fmt::Display for Ineligibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Derive the authoritative GPU ownership for a node record.
/// `assigned_jobs` is the coordinator-side count of in-flight shared jobs.
pub fn derive_ownership(md: &NodeMetadata, assigned_jobs: u32) -> GpuOwnership {
    // Terminal node states own nothing.
    if matches!(
        md.lifecycle_state,
        NodeLifecycleState::Revoked | NodeLifecycleState::Withdrawn | NodeLifecycleState::Offline
    ) || !md.online
    {
        return GpuOwnership::Unavailable;
    }
    if md.health_state == "REVOKED"
        || md.health_state == "WITHDRAWN"
        || md.health_state == "OFFLINE"
    {
        return GpuOwnership::Unavailable;
    }

    if let Some(lease) = &md.active_lease {
        match lease.state {
            LeaseState::LeaseRequested | LeaseState::Draining => return GpuOwnership::Draining,
            LeaseState::LeaseGranted | LeaseState::Leased => return GpuOwnership::ForgeLeased,
            LeaseState::Releasing | LeaseState::Recovering => return GpuOwnership::Recovering,
            // Terminal lease states fall through to shared ownership.
            LeaseState::Released
            | LeaseState::LeaseFailed
            | LeaseState::RecoveryFailed
            | LeaseState::Stale => {}
        }
    }

    if md.health_state == "RESERVED_FORGE" || md.health_state == "PREEMPTING" {
        return GpuOwnership::ForgeLeased;
    }
    if md.health_state == "DRAINING" || matches!(md.lifecycle_state, NodeLifecycleState::Draining) {
        return GpuOwnership::Draining;
    }
    if md.health_state == "RECOVERING" {
        return GpuOwnership::Recovering;
    }

    if assigned_jobs > 0 {
        GpuOwnership::SharedAssigned
    } else {
        GpuOwnership::SharedAvailable
    }
}

/// State-based eligibility evaluation on node metadata only.
///
/// Returns the authoritative ownership when the GPU may carry shared work
/// (`SharedAvailable` / `SharedAssigned`), or the fail-closed reason.
pub fn evaluate_metadata(md: &NodeMetadata) -> Result<GpuOwnership, Ineligibility> {
    // 1. Basic liveness.
    if !md.online {
        return Err(Ineligibility::Offline);
    }

    // 2. Lifecycle state.
    match md.lifecycle_state {
        NodeLifecycleState::Revoked => return Err(Ineligibility::Revoked),
        NodeLifecycleState::Withdrawn => return Err(Ineligibility::Withdrawn),
        NodeLifecycleState::Draining => return Err(Ineligibility::Draining),
        NodeLifecycleState::Offline => return Err(Ineligibility::Offline),
        NodeLifecycleState::Degraded => return Err(Ineligibility::DegradedBeyondEligibility),
        // Joining/Verifying are not trusted for work until fully Online.
        NodeLifecycleState::Joining | NodeLifecycleState::Verifying => {
            return Err(Ineligibility::OwnershipUncertain)
        }
        NodeLifecycleState::Online => {}
    }

    // 3. Health state strings (RC1 operational vocabulary).
    match md.health_state.as_str() {
        "REVOKED" => return Err(Ineligibility::Revoked),
        "WITHDRAWN" => return Err(Ineligibility::Withdrawn),
        "DRAINING" => return Err(Ineligibility::Draining),
        "OFFLINE" => return Err(Ineligibility::Offline),
        "DEGRADED" | "STALE" => return Err(Ineligibility::DegradedBeyondEligibility),
        "PREEMPTING" | "RESERVED_FORGE" => return Err(Ineligibility::ForgeLeased),
        "RECOVERING" => return Err(Ineligibility::RecoveryIncomplete),
        _ => {}
    }

    // 4. Authoritative lease reference (fail closed on any non-terminal lease).
    if let Some(lease) = &md.active_lease {
        if lease.state.is_active() {
            return Err(match lease.state {
                LeaseState::LeaseRequested | LeaseState::Draining => {
                    Ineligibility::ForgeLeaseRequested
                }
                LeaseState::LeaseGranted | LeaseState::Leased => Ineligibility::ForgeLeased,
                LeaseState::Releasing | LeaseState::Recovering => Ineligibility::RecoveryIncomplete,
                _ => Ineligibility::OwnershipUncertain,
            });
        }
    }

    // 5. Runtime truth. An open worker heartbeat is NOT proof the inference
    //    runtime is healthy; runtime_state carries that truth.
    match md.runtime_state {
        RuntimeState::Unavailable => return Err(Ineligibility::RuntimeUnavailable),
        RuntimeState::Unknown => return Err(Ineligibility::RuntimeUnknown),
        RuntimeState::Ready => {}
    }

    // 6. Hardware reconciliation truth.
    match md.reconciliation.status {
        ReconciliationStatus::MISMATCH => return Err(Ineligibility::HardwareMismatch),
        ReconciliationStatus::STALE => return Err(Ineligibility::StaleState),
        ReconciliationStatus::OFFLINE => return Err(Ineligibility::Offline),
        ReconciliationStatus::MATCH
        | ReconciliationStatus::PARTIAL_MATCH
        | ReconciliationStatus::UNVERIFIED => {}
    }

    // 7. Live-observation provenance policy (RC1 rule).
    if let Some(obs) = &md.observed {
        let live = matches!(
            obs.source_type,
            crate::types::TelemetrySourceType::LocalLiveObservation
                | crate::types::TelemetrySourceType::RemoteLiveObservation
        );
        if !live {
            return Err(Ineligibility::NotLiveObserved);
        }
    }
    // observed == None: RC1 allows self-claimed volunteer nodes when the
    // Forge observer is absent (UNVERIFIED reconciliation).

    // 8. Ownership summary must be shared. The live assigned-jobs counter is
    // applied by `evaluate_node`; metadata-only evaluation assumes no
    // coordinator-side assignments.
    let ownership = derive_ownership(md, 0);
    match ownership {
        GpuOwnership::SharedAvailable | GpuOwnership::SharedAssigned => Ok(ownership),
        GpuOwnership::Draining => Err(Ineligibility::Draining),
        GpuOwnership::ForgeLeased => Err(Ineligibility::ForgeLeased),
        GpuOwnership::Recovering => Err(Ineligibility::RecoveryIncomplete),
        GpuOwnership::Unavailable => Err(Ineligibility::OwnershipUncertain),
    }
}

/// Full eligibility evaluation including heartbeat freshness.
///
/// `stale_after_ms` bounds how old the last heartbeat may be.
pub fn evaluate_node(
    rt: &NodeRuntime,
    now_ms: u64,
    stale_after_ms: u64,
) -> Result<GpuOwnership, Ineligibility> {
    let md = match rt.metadata.read() {
        Ok(m) => m,
        // A poisoned metadata lock is ownership-uncertain. Fail closed.
        Err(_) => return Err(Ineligibility::OwnershipUncertain),
    };
    let age = now_ms.saturating_sub(rt.last_heartbeat_ms());
    if age > stale_after_ms {
        return Err(Ineligibility::StaleHeartbeat);
    }
    let assigned = rt.assigned_jobs_count();
    // Re-derive ownership with the live assigned-jobs counter.
    let base = evaluate_metadata(&md)?;
    Ok(match base {
        GpuOwnership::SharedAvailable | GpuOwnership::SharedAssigned => {
            if assigned > 0 {
                GpuOwnership::SharedAssigned
            } else {
                GpuOwnership::SharedAvailable
            }
        }
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        NodeCapabilities, NodeMetadata, ReconciledHardwareState, RegisteredHardwareClaim,
    };

    fn base_meta() -> NodeMetadata {
        NodeMetadata {
            id: "n1".into(),
            addr: "127.0.0.1:1".into(),
            inference_url: "http://127.0.0.1:5200".into(),
            role: "thinker".into(),
            gpu_name: "RTX 2060 SUPER".into(),
            online: true,
            public_key: vec![1; 32],
            capabilities: NodeCapabilities {
                max_batch: 1,
                models: vec!["thinker".into()],
                vram_mb: 16384,
                role: "thinker".into(),
            },
            claim: RegisteredHardwareClaim::default(),
            observed: None,
            reconciliation: ReconciledHardwareState {
                status: ReconciliationStatus::MATCH,
                fields: vec![],
            },
            last_hardware_probe: None,
            health_state: "ONLINE".into(),
            lifecycle_state: NodeLifecycleState::Online,
            ..Default::default()
        }
    }

    #[test]
    fn online_shared_node_is_eligible() {
        let md = base_meta();
        assert_eq!(
            evaluate_metadata(&md).unwrap(),
            GpuOwnership::SharedAvailable
        );
    }

    #[test]
    fn reserved_forge_health_fails_closed() {
        let mut md = base_meta();
        md.health_state = "RESERVED_FORGE".into();
        assert_eq!(evaluate_metadata(&md), Err(Ineligibility::ForgeLeased));
    }

    #[test]
    fn active_lease_ref_fails_closed_even_if_health_online() {
        let mut md = base_meta();
        md.active_lease = Some(ActiveLeaseRef {
            lease_id: "l1".into(),
            mission_id: "m1".into(),
            state: LeaseState::Leased,
            epoch: 0,
        });
        assert_eq!(evaluate_metadata(&md), Err(Ineligibility::ForgeLeased));

        md.active_lease.as_mut().unwrap().state = LeaseState::Recovering;
        assert_eq!(
            evaluate_metadata(&md),
            Err(Ineligibility::RecoveryIncomplete)
        );

        md.active_lease.as_mut().unwrap().state = LeaseState::Released;
        assert!(evaluate_metadata(&md).is_ok());
    }

    #[test]
    fn runtime_unavailable_fails_closed() {
        let mut md = base_meta();
        md.runtime_state = RuntimeState::Unavailable;
        assert_eq!(
            evaluate_metadata(&md),
            Err(Ineligibility::RuntimeUnavailable)
        );
        md.runtime_state = RuntimeState::Unknown;
        assert_eq!(evaluate_metadata(&md), Err(Ineligibility::RuntimeUnknown));
    }

    #[test]
    fn stale_reconciliation_fails_closed() {
        let mut md = base_meta();
        md.reconciliation.status = ReconciliationStatus::STALE;
        assert_eq!(evaluate_metadata(&md), Err(Ineligibility::StaleState));
        md.reconciliation.status = ReconciliationStatus::MISMATCH;
        assert_eq!(evaluate_metadata(&md), Err(Ineligibility::HardwareMismatch));
    }

    #[test]
    fn lifecycle_states_fail_closed() {
        for (state, expected) in [
            (NodeLifecycleState::Revoked, Ineligibility::Revoked),
            (NodeLifecycleState::Withdrawn, Ineligibility::Withdrawn),
            (NodeLifecycleState::Draining, Ineligibility::Draining),
            (NodeLifecycleState::Offline, Ineligibility::Offline),
            (
                NodeLifecycleState::Degraded,
                Ineligibility::DegradedBeyondEligibility,
            ),
            (
                NodeLifecycleState::Joining,
                Ineligibility::OwnershipUncertain,
            ),
        ] {
            let mut md = base_meta();
            md.lifecycle_state = state;
            assert_eq!(evaluate_metadata(&md), Err(expected), "state {state:?}");
        }
    }

    #[test]
    fn ownership_derivation() {
        let md = base_meta();
        assert_eq!(derive_ownership(&md, 0), GpuOwnership::SharedAvailable);
        assert_eq!(derive_ownership(&md, 2), GpuOwnership::SharedAssigned);

        let mut leased = base_meta();
        leased.active_lease = Some(ActiveLeaseRef {
            lease_id: "l".into(),
            mission_id: "m".into(),
            state: LeaseState::LeaseGranted,
            epoch: 0,
        });
        assert_eq!(derive_ownership(&leased, 0), GpuOwnership::ForgeLeased);

        let mut offline = base_meta();
        offline.online = false;
        assert_eq!(derive_ownership(&offline, 0), GpuOwnership::Unavailable);
    }
}
