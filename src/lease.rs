//! Formal Forge lease lifecycle manager (Chunk 7).
//!
//! State machine:
//!
//! ```text
//! (AVAILABLE — no lease row)
//!   -> LEASE_REQUESTED -> DRAINING -> LEASE_GRANTED -> LEASED
//!   -> RELEASING -> RECOVERING -> RELEASED (= AVAILABLE)
//! Failure states: LEASE_FAILED, RECOVERY_FAILED, STALE.
//! ```
//!
//! Truth lives in SQLite (`forge_leases`). The in-memory
//! `PreemptionManager.reservations` map is a derived RC1-compatibility mirror
//! maintained by the handlers; it is NOT authoritative.
//!
//! "Lease Granted" is only recorded after Nauti has actually yielded
//! ownership (scheduler exclusion applied, active shared work stopped,
//! local runtime exited) — never merely because an HTTP request was accepted.

use crate::eligibility::{ActiveLeaseRef, LeaseState};
use crate::node::registry::NodeRegistry;
use crate::opsstore::{LeaseRecord, OpsEventKind, OpsStore};
use crate::types::now_ms;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// Lease decision errors.
#[derive(Debug)]
pub enum LeaseError {
    /// A different lease already holds this node. Fail explicitly; never
    /// silently replace an existing lease.
    Conflict(LeaseRecord),
    /// No matching lease found for the requested operation.
    NotFound,
    /// Release request mission/node does not match the active lease.
    Mismatch {
        expected_mission: String,
    },
    Store(String),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(l) => write!(
                f,
                "GPU already leased (lease {} mission {} state {})",
                l.lease_id,
                l.mission_id,
                l.state.as_str()
            ),
            Self::NotFound => write!(f, "no matching lease"),
            Self::Mismatch { expected_mission } => write!(
                f,
                "release does not match active lease (expected mission {expected_mission})"
            ),
            Self::Store(e) => write!(f, "lease store error: {e}"),
        }
    }
}

/// Outcome of a reserve request.
#[derive(Debug)]
pub enum ReserveOutcome {
    /// A new lease was created (LEASE_REQUESTED).
    Created(LeaseRecord),
    /// Duplicate request for the same lease: idempotent return of the
    /// existing record (no re-drain, no re-preemption).
    Idempotent(LeaseRecord),
}

pub struct LeaseManager {
    store: Arc<OpsStore>,
    /// Serializes lease decision critical sections (reserve/release).
    decision: Mutex<()>,
}

impl LeaseManager {
    pub fn new(store: Arc<OpsStore>) -> Self {
        Self {
            store,
            decision: Mutex::new(()),
        }
    }

    fn err(e: crate::types::Error) -> LeaseError {
        LeaseError::Store(e.to_string())
    }

    // ── Queries ──────────────────────────────────────────────────────────

    pub fn active_lease_for_node(&self, node_id: &str) -> Option<LeaseRecord> {
        self.store.active_lease_for_node(node_id).ok().flatten()
    }

    pub fn has_active_lease(&self, node_id: &str) -> bool {
        self.active_lease_for_node(node_id).is_some()
    }

    pub fn lease_by_mission_node(&self, mission_id: &str, node_id: &str) -> Option<LeaseRecord> {
        self.store
            .lease_by_mission_node(mission_id, node_id)
            .ok()
            .flatten()
    }

    pub fn lease_by_id(&self, lease_id: &str) -> Option<LeaseRecord> {
        self.store.lease_by_id(lease_id).ok().flatten()
    }

    pub fn all_active_leases(&self) -> Vec<LeaseRecord> {
        self.store.all_active_leases().unwrap_or_default()
    }

    pub fn all_leases(&self, limit: u64) -> Vec<LeaseRecord> {
        self.store.all_leases(limit).unwrap_or_default()
    }

    // ── Reserve path ─────────────────────────────────────────────────────

    /// Begin a lease request. Idempotent for the same (mission_id, node_id)
    /// while a non-terminal lease exists. Conflicting missions fail with
    /// [`LeaseError::Conflict`].
    pub fn begin_reserve(
        &self,
        mission_id: &str,
        node_id: &str,
        lease_id_hint: Option<&str>,
        device_id: Option<&str>,
        vram_mb: u64,
        priority: u32,
        allow_preemption: bool,
    ) -> Result<ReserveOutcome, LeaseError> {
        let _guard = self
            .decision
            .lock()
            .map_err(|e| LeaseError::Store(e.to_string()))?;

        // Idempotency: same mission+node with an existing lease.
        if let Some(existing) = self
            .store
            .lease_by_mission_node(mission_id, node_id)
            .map_err(Self::err)?
        {
            if existing.state.is_active() {
                let _ = self.store.record(
                    OpsEventKind::DuplicateRequest,
                    Some(node_id),
                    Some(&existing.lease_id),
                    None,
                    Some(mission_id),
                    "duplicate reserve for active lease; idempotent return",
                    existing.state.as_str(),
                );
                return Ok(ReserveOutcome::Idempotent(existing));
            }
            // Terminal previous lease for this mission: fall through and
            // create a fresh lease (mission re-request after completion).
        }

        // Conflict: a DIFFERENT lease holds the node.
        if let Some(holder) = self
            .store
            .active_lease_for_node(node_id)
            .map_err(Self::err)?
        {
            let _ = self.store.record(
                OpsEventKind::LeaseConflict,
                Some(node_id),
                Some(&holder.lease_id),
                None,
                Some(mission_id),
                "conflicting lease attempt blocked",
                holder.state.as_str(),
            );
            return Err(LeaseError::Conflict(holder));
        }

        let lease_id = lease_id_hint
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let record = LeaseRecord {
            lease_id: lease_id.clone(),
            mission_id: mission_id.to_string(),
            node_id: node_id.to_string(),
            device_id: device_id.map(|s| s.to_string()),
            state: LeaseState::LeaseRequested,
            vram_mb,
            priority,
            allow_preemption,
            requested_at_ms: now_ms(),
            scheduler_excluded_at_ms: None,
            runtime_exited_at_ms: None,
            gpu_released_at_ms: None,
            granted_at_ms: None,
            releasing_at_ms: None,
            released_at_ms: None,
            recovery_started_at_ms: None,
            runtime_ready_at_ms: None,
            scheduler_eligible_at_ms: None,
            first_inference_at_ms: None,
            failure_reason: None,
            updated_at_ms: now_ms(),
        };
        self.store.insert_lease(&record).map_err(Self::err)?;
        let _ = self.store.record(
            OpsEventKind::LeaseRequested,
            Some(node_id),
            Some(&lease_id),
            None,
            Some(mission_id),
            &format!("vram_mb={vram_mb} priority={priority}"),
            LeaseState::LeaseRequested.as_str(),
        );
        info!(%lease_id, %mission_id, %node_id, "lease requested");
        Ok(ReserveOutcome::Created(record))
    }

    fn transition(
        &self,
        lease_id: &str,
        f: impl FnOnce(&mut LeaseRecord),
    ) -> Result<LeaseRecord, LeaseError> {
        let _guard = self
            .decision
            .lock()
            .map_err(|e| LeaseError::Store(e.to_string()))?;
        let mut rec = self
            .store
            .lease_by_id(lease_id)
            .map_err(Self::err)?
            .ok_or(LeaseError::NotFound)?;
        f(&mut rec);
        rec.updated_at_ms = now_ms();
        self.store.update_lease(&rec).map_err(Self::err)?;
        Ok(rec)
    }

    /// Scheduler exclusion applied (node removed from shared scheduling).
    pub fn mark_scheduler_excluded(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::Draining;
            l.scheduler_excluded_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::SchedulerExcluded,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "node excluded from shared scheduling",
            LeaseState::Draining.as_str(),
        );
        Ok(rec)
    }

    /// Inference runtime exited (GPU resource released by Nauti side).
    pub fn mark_runtime_exited(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.runtime_exited_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::RuntimeExited,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "inference runtime exited ahead of lease grant",
            rec.state.as_str(),
        );
        Ok(rec)
    }

    /// GPU resource fully released (jobs checkpointed/preempted + runtime out).
    pub fn mark_gpu_released(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.gpu_released_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::GpuReleased,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "GPU resource released to Forge",
            rec.state.as_str(),
        );
        Ok(rec)
    }

    /// Grant the lease. MUST only be called after Nauti has actually yielded
    /// the GPU (scheduler excluded, shared work stopped, runtime exited).
    pub fn grant(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::LeaseGranted;
            l.granted_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::LeaseGranted,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "ownership yielded to Forge",
            LeaseState::LeaseGranted.as_str(),
        );
        // Immediately onward to LEASED: Forge holds the GPU.
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::Leased;
        })?;
        info!(%lease_id, node_id = %rec.node_id, "lease granted");
        Ok(rec)
    }

    // ── Release path ─────────────────────────────────────────────────────

    /// Begin releasing a lease. Idempotent for already-released leases of the
    /// same mission (returns the terminal record with state Released-ish via
    /// `Ok(None)`), explicit mismatch for wrong missions.
    ///
    /// Returns `Some(record)` when a release transition should proceed,
    /// `None` when this is a duplicate release of an already terminal lease.
    pub fn begin_release(
        &self,
        mission_id: &str,
        node_id: &str,
    ) -> Result<Option<LeaseRecord>, LeaseError> {
        let _guard = self
            .decision
            .lock()
            .map_err(|e| LeaseError::Store(e.to_string()))?;

        // Exact mission+node lease first.
        if let Some(rec) = self
            .store
            .lease_by_mission_node(mission_id, node_id)
            .map_err(Self::err)?
        {
            if rec.state.is_terminal() {
                let _ = self.store.record(
                    OpsEventKind::DuplicateRequest,
                    Some(node_id),
                    Some(&rec.lease_id),
                    None,
                    Some(mission_id),
                    "duplicate release for terminal lease; idempotent",
                    rec.state.as_str(),
                );
                return Ok(None);
            }
            let mut rec = rec;
            rec.state = LeaseState::Releasing;
            rec.releasing_at_ms = Some(now_ms());
            rec.updated_at_ms = now_ms();
            self.store.update_lease(&rec).map_err(Self::err)?;
            let _ = self.store.record(
                OpsEventKind::LeaseReleased,
                Some(node_id),
                Some(&rec.lease_id),
                None,
                Some(mission_id),
                "release started",
                LeaseState::Releasing.as_str(),
            );
            return Ok(Some(rec));
        }

        // A different mission's lease holds the node: explicit failure.
        if let Some(holder) = self
            .store
            .active_lease_for_node(node_id)
            .map_err(Self::err)?
        {
            return Err(LeaseError::Mismatch {
                expected_mission: holder.mission_id,
            });
        }

        Ok(None)
    }

    pub fn mark_recovery_started(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::Recovering;
            l.recovery_started_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::RecoveryStarted,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "shared compute recovery started",
            LeaseState::Recovering.as_str(),
        );
        Ok(rec)
    }

    pub fn mark_runtime_ready(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.runtime_ready_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::RuntimeReady,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "shared runtime ready after lease release",
            rec.state.as_str(),
        );
        Ok(rec)
    }

    /// Complete the release: GPU returns to the shared pool.
    pub fn complete_release(&self, lease_id: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::Released;
            l.released_at_ms = Some(now_ms());
            l.scheduler_eligible_at_ms = Some(now_ms());
        })?;
        let _ = self.store.record(
            OpsEventKind::NodeEligible,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            "lease released; node scheduler-eligible",
            LeaseState::Released.as_str(),
        );
        info!(%lease_id, node_id = %rec.node_id, "lease released");
        Ok(rec)
    }

    pub fn fail_recovery(&self, lease_id: &str, reason: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::RecoveryFailed;
            l.failure_reason = Some(reason.to_string());
        })?;
        let _ = self.store.record(
            OpsEventKind::RuntimeFailed,
            Some(&rec.node_id),
            Some(lease_id),
            None,
            Some(&rec.mission_id),
            reason,
            LeaseState::RecoveryFailed.as_str(),
        );
        Ok(rec)
    }

    pub fn fail_lease(&self, lease_id: &str, reason: &str) -> Result<LeaseRecord, LeaseError> {
        let rec = self.transition(lease_id, |l| {
            l.state = LeaseState::LeaseFailed;
            l.failure_reason = Some(reason.to_string());
        })?;
        warn!(%lease_id, %reason, "lease failed");
        Ok(rec)
    }

    /// Record the first successful inference on a node after lease release
    /// (observational telemetry for the lease that previously held it).
    pub fn record_first_inference_for_node(&self, node_id: &str) {
        if let Some(rec) = self
            .store
            .lease_by_id(
                &self
                    .store
                    .all_leases(50)
                    .unwrap_or_default()
                    .into_iter()
                    .find(|l| l.node_id == node_id && l.state == LeaseState::Released)
                    .map(|l| l.lease_id)
                    .unwrap_or_default(),
            )
            .ok()
            .flatten()
        {
            if rec.first_inference_at_ms.is_none() {
                let mut rec = rec;
                rec.first_inference_at_ms = Some(now_ms());
                let _ = self.store.update_lease(&rec);
            }
        }
    }

    // ── Registry overlays ────────────────────────────────────────────────

    /// Apply the authoritative lease truth onto a registry node record.
    /// Returns true if an active lease overlay was applied.
    pub fn apply_overlay_to_node(&self, registry: &NodeRegistry, node_id: &str) -> bool {
        let Some(lease) = self.active_lease_for_node(node_id) else {
            return false;
        };
        if let Some(rt) = registry.get_node(node_id) {
            if let Ok(mut md) = rt.metadata.write() {
                md.health_state = "RESERVED_FORGE".to_string();
                md.online = false;
                md.active_lease = Some(ActiveLeaseRef {
                    lease_id: lease.lease_id.clone(),
                    mission_id: lease.mission_id.clone(),
                    state: lease.state,
                    epoch: 0,
                });
            }
        }
        true
    }

    /// Update the embedded lease reference state (kept under the metadata
    /// write lock so eligibility reads are atomic).
    pub fn sync_lease_ref_on_node(&self, registry: &NodeRegistry, rec: &LeaseRecord) {
        if let Some(rt) = registry.get_node(&rec.node_id) {
            if let Ok(mut md) = rt.metadata.write() {
                md.active_lease = Some(ActiveLeaseRef {
                    lease_id: rec.lease_id.clone(),
                    mission_id: rec.mission_id.clone(),
                    state: rec.state,
                    epoch: 0,
                });
            }
        }
    }

    /// Coordinator startup recovery.
    ///
    /// - Leases already GRANTED/LEASED survive restart: Forge ownership truth
    ///   is preserved and the node stays excluded.
    /// - RELEASING/RECOVERING survive: recovery was not verified; fail closed
    ///   until release is re-driven.
    /// - LEASE_REQUESTED/DRAINING (grant never confirmed) are marked STALE:
    ///   Forge never received a grant, so it must re-request.
    ///
    /// Returns the leases that remain active (exclusion preserved).
    pub fn restore_overlays(&self, registry: &NodeRegistry) -> Vec<LeaseRecord> {
        let mut kept = Vec::new();
        for lease in self.all_active_leases() {
            match lease.state {
                LeaseState::LeaseRequested | LeaseState::Draining => {
                    let mut stale = lease.clone();
                    stale.state = LeaseState::Stale;
                    stale.failure_reason =
                        Some("coordinator restart before lease grant".to_string());
                    stale.updated_at_ms = now_ms();
                    let _ = self.store.update_lease(&stale);
                    let _ = self.store.record(
                        OpsEventKind::LeaseStale,
                        Some(&lease.node_id),
                        Some(&lease.lease_id),
                        None,
                        Some(&lease.mission_id),
                        "lease interrupted by coordinator restart before grant",
                        LeaseState::Stale.as_str(),
                    );
                    warn!(
                        lease_id = %lease.lease_id,
                        node_id = %lease.node_id,
                        "lease marked STALE after coordinator restart (grant never confirmed)"
                    );
                }
                _ => {
                    self.apply_overlay_to_node(registry, &lease.node_id);
                    let _ = self.store.record(
                        OpsEventKind::StateRecovered,
                        Some(&lease.node_id),
                        Some(&lease.lease_id),
                        None,
                        Some(&lease.mission_id),
                        &format!("active lease recovered in state {}", lease.state.as_str()),
                        lease.state.as_str(),
                    );
                    kept.push(lease);
                }
            }
        }
        kept
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mgr() -> LeaseManager {
        LeaseManager::new(Arc::new(OpsStore::open("sqlite::memory:").unwrap()))
    }

    #[test]
    fn reserve_idempotent_same_mission() {
        let m = mgr();
        let r1 = m
            .begin_reserve("mission-a", "node-1", None, None, 8192, 10, true)
            .unwrap();
        let r2 = m
            .begin_reserve("mission-a", "node-1", None, None, 8192, 10, true)
            .unwrap();
        match (r1, r2) {
            (ReserveOutcome::Created(a), ReserveOutcome::Idempotent(b)) => {
                assert_eq!(a.lease_id, b.lease_id);
            }
            _ => panic!("expected created then idempotent"),
        }
    }

    #[test]
    fn conflicting_mission_fails_explicitly() {
        let m = mgr();
        m.begin_reserve("mission-a", "node-1", None, None, 8192, 10, true)
            .unwrap();
        let err = m
            .begin_reserve("mission-b", "node-1", None, None, 8192, 10, true)
            .unwrap_err();
        assert!(matches!(err, LeaseError::Conflict(_)));
    }

    #[test]
    fn full_lease_lifecycle_transitions() {
        let m = mgr();
        let rec = match m
            .begin_reserve(
                "mission-a",
                "node-1",
                Some("lease-fixed"),
                None,
                8192,
                10,
                true,
            )
            .unwrap()
        {
            ReserveOutcome::Created(r) => r,
            _ => panic!("expected created"),
        };
        assert_eq!(rec.lease_id, "lease-fixed");
        assert_eq!(rec.state, LeaseState::LeaseRequested);

        let rec = m.mark_scheduler_excluded(&rec.lease_id).unwrap();
        assert_eq!(rec.state, LeaseState::Draining);
        assert!(rec.scheduler_excluded_at_ms.is_some());

        let rec = m.mark_runtime_exited(&rec.lease_id).unwrap();
        assert!(rec.runtime_exited_at_ms.is_some());
        let rec = m.mark_gpu_released(&rec.lease_id).unwrap();
        assert!(rec.gpu_released_at_ms.is_some());

        let rec = m.grant(&rec.lease_id).unwrap();
        assert_eq!(rec.state, LeaseState::Leased);
        assert!(rec.granted_at_ms.is_some());
        assert!(m.has_active_lease("node-1"));

        // Release path.
        let rel = m.begin_release("mission-a", "node-1").unwrap().unwrap();
        assert_eq!(rel.state, LeaseState::Releasing);
        let rec = m.mark_recovery_started(&rel.lease_id).unwrap();
        assert_eq!(rec.state, LeaseState::Recovering);
        let rec = m.mark_runtime_ready(&rec.lease_id).unwrap();
        assert!(rec.runtime_ready_at_ms.is_some());
        let rec = m.complete_release(&rec.lease_id).unwrap();
        assert_eq!(rec.state, LeaseState::Released);
        assert!(rec.scheduler_eligible_at_ms.is_some());
        assert!(!m.has_active_lease("node-1"));

        // Duplicate release is idempotent (None => nothing to do).
        assert!(m.begin_release("mission-a", "node-1").unwrap().is_none());

        // Wrong-mission release on a free node is a no-op; on a leased node
        // it must mismatch.
        m.begin_reserve("mission-c", "node-1", None, None, 8192, 10, true)
            .unwrap();
        let err = m.begin_release("mission-WRONG", "node-1").unwrap_err();
        assert!(matches!(err, LeaseError::Mismatch { .. }));
    }

    #[test]
    fn restore_marks_ungranted_stale_and_keeps_granted() {
        let m = mgr();
        // Un-granted lease.
        m.begin_reserve("mission-x", "node-x", None, None, 1, 1, true)
            .unwrap();
        // Granted lease.
        let rec = match m
            .begin_reserve("mission-y", "node-y", None, None, 1, 1, true)
            .unwrap()
        {
            ReserveOutcome::Created(r) => r,
            _ => panic!(),
        };
        m.mark_scheduler_excluded(&rec.lease_id).unwrap();
        m.mark_runtime_exited(&rec.lease_id).unwrap();
        m.mark_gpu_released(&rec.lease_id).unwrap();
        m.grant(&rec.lease_id).unwrap();

        let registry = NodeRegistry::new();
        let kept = m.restore_overlays(&registry);
        assert_eq!(kept.len(), 1, "only the granted lease survives restart");
        assert_eq!(kept[0].node_id, "node-y");

        let stale = m.lease_by_mission_node("mission-x", "node-x").unwrap();
        assert_eq!(stale.state, LeaseState::Stale);
        assert!(!m.has_active_lease("node-x"));
        assert!(m.has_active_lease("node-y"));
    }
}
