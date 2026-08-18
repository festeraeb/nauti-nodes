//! Federation snapshot and catch-up (Chunk 8B.13).
//!
//! A new coordinator with empty local state must be able to obtain enough
//! current federation state to operate SAFELY AS A FOLLOWER. The snapshot
//! carries exactly the scheduler-critical truth — no unbounded journal
//! replay:
//!
//! - current authority id + epoch
//! - active leases (ownership exclusions)
//! - revocations
//! - lineage relationships
//! - durable resource fences
//!
//! Catch-up never grants authority: the catching coordinator adopts the
//! snapshot epoch as a FOLLOWER (mutation rights stay off) and only the
//! failover election path (Chunk 8B.4) can promote it.

use super::{CoordinatorId, Epoch};
use crate::lineage::store::NodeRelationship;
use crate::node::revocation::RevocationEntry;
use crate::opsstore::{FenceRow, LeaseRecord};
use crate::state::AppState;
use serde::{Deserialize, Serialize};

/// Bounded current-state snapshot (no historical journal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationSnapshot {
    pub authority_id: CoordinatorId,
    pub epoch: Epoch,
    pub active_leases: Vec<LeaseRecord>,
    pub revoked: Vec<RevocationEntry>,
    pub lineage: Vec<NodeRelationship>,
    pub fences: Vec<FenceRow>,
    pub generated_at_ms: u64,
}

/// What catch-up actually applied (observational counts).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatchupReport {
    pub leases_imported: u64,
    pub leases_skipped_existing: u64,
    pub revocations_imported: u64,
    pub lineage_imported: u64,
    pub lineage_skipped: u64,
    pub fences_imported: u64,
    pub fences_kept_newer_local: u64,
    pub adopted_follower_epoch: bool,
}

/// Capture a snapshot from REAL current state.
pub fn capture(state: &AppState) -> crate::types::Result<FederationSnapshot> {
    let (authority_id, epoch) = match crate::state::identity_snapshot(state) {
        Some(id) => (id.node_identity.node_id.clone(), id.authority_epoch.value()),
        None => (String::new(), 0),
    };
    Ok(FederationSnapshot {
        authority_id,
        epoch,
        active_leases: state.leases.all_active_leases(),
        revoked: state.revocations.list_revoked()?,
        lineage: state
            .lineage
            .as_ref()
            .map(|l| l.all_active())
            .transpose()?
            .unwrap_or_default(),
        fences: state.ops.all_fences()?,
        generated_at_ms: crate::types::now_ms(),
    })
}

/// Apply a peer snapshot to local state so this coordinator can operate
/// safely as a follower. Idempotent and fail-closed:
///
/// - leases already known are skipped (no duplicate rows)
/// - lineage rows that would conflict or cycle are skipped, not forced
/// - fences NEVER roll back a newer local fence (a stale snapshot cannot
///   overwrite a higher epoch — mandatory case 35)
/// - the local identity adopts the snapshot epoch as FOLLOWER only
pub fn apply(state: &AppState, snap: &FederationSnapshot) -> crate::types::Result<CatchupReport> {
    let mut report = CatchupReport::default();

    // 1. Active leases (ownership exclusions survive catch-up).
    for lease in &snap.active_leases {
        if state.leases.lease_by_id(&lease.lease_id).is_some() {
            report.leases_skipped_existing += 1;
        } else {
            state.ops.insert_lease(lease)?;
            report.leases_imported += 1;
        }
    }

    // 2. Revocations (terminal operator truth).
    for entry in &snap.revoked {
        state
            .revocations
            .revoke(&entry.identifier, &format!("catch-up: {}", entry.reason))?;
        report.revocations_imported += 1;
    }

    // 3. Lineage relationships (skip conflicts/cycles; never force).
    //    Roots are imported first so children can attach to them.
    if let Some(lineage) = state.lineage.as_ref() {
        for rel in snap.lineage.iter().filter(|r| {
            r.status == crate::lineage::store::RelationshipStatus::Active
                && r.parent_node_id.is_none()
        }) {
            lineage.set_root(&rel.node_id)?; // idempotent
            report.lineage_imported += 1;
        }
        for rel in snap.lineage.iter().filter(|r| {
            r.status == crate::lineage::store::RelationshipStatus::Active
                && r.parent_node_id.is_some()
        }) {
            let exists = lineage
                .get_relationship(&rel.node_id)?
                .map(|existing| {
                    existing.status == crate::lineage::store::RelationshipStatus::Active
                })
                .unwrap_or(false);
            if exists {
                report.lineage_skipped += 1;
                continue;
            }
            match lineage.add_child(&rel.node_id, rel.parent_node_id.as_deref().unwrap()) {
                Ok(_) => report.lineage_imported += 1,
                Err(_) => report.lineage_skipped += 1,
            }
        }
    }

    // 4. Durable fences — never downgrade a newer local fence.
    for fence in &snap.fences {
        match state.ops.get_fence(&fence.resource_id)? {
            Some(local) if local.epoch > fence.epoch => {
                report.fences_kept_newer_local += 1;
            }
            _ => {
                state.ops.upsert_fence(fence)?;
                report.fences_imported += 1;
            }
        }
    }

    // 5. Adopt the epoch as FOLLOWER (no mutation rights).
    if let Some(id) = crate::state::identity_snapshot(state) {
        let mut updated = (*id).clone();
        let before = updated.authority_epoch.value();
        updated.adopt_follower_epoch(snap.epoch);
        updated.last_synced_at_ms = crate::types::now_ms();
        report.adopted_follower_epoch = updated.authority_epoch.value() > before;
        // Raise the lease authority floor and swap the shared identity
        // handle so all readers see the caught-up epoch.
        state
            .leases
            .set_authority_epoch(updated.authority_epoch.value());
        crate::state::install_identity(state, updated);
    }

    Ok(report)
}
