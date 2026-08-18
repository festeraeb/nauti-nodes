//! Worker-side fencing gate (Chunk 8B.2).
//!
//! Every ownership-changing command that arrives at a worker (or at any
//! coordinator acting on a resource) must pass this gate. The gate is
//! backed by the durable `federation_fence` table in SQLite, so the
//! highest accepted epoch per resource SURVIVES RESTARTS — a fence can
//! never roll back because a process died.
//!
//! Reference behavior (fail closed):
//!
//! ```text
//! received_epoch <  current_epoch            -> REJECT (stale epoch)
//! received_epoch == current_epoch, same auth -> ACCEPT / IDEMPOTENT
//! received_epoch == current_epoch, diff auth -> REJECT (authority conflict)
//! received_epoch >  current_epoch            -> ACCEPT AND PERSIST NEW AUTHORITY
//! ```

use crate::opsstore::{FenceRow, OpsStore};
use crate::types::now_ms;
use std::sync::Arc;

/// The fence gate's decision for one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceDecision {
    /// First time this resource sees any authority, or a strictly newer
    /// epoch arrived. The new authority was persisted.
    AcceptedNewAuthority { previous_epoch: Option<u64> },
    /// Same epoch AND same authority as the durable fence: idempotent
    /// re-delivery. No state changed.
    AcceptedIdempotent,
    /// The command carries an epoch older than the durable fence.
    RejectedStaleEpoch { received: u64, current: u64 },
    /// Same epoch but a DIFFERENT authority: dual-authority attempt.
    RejectedAuthorityConflict {
        received_authority: String,
        current_authority: String,
    },
}

impl FenceDecision {
    pub fn accepted(&self) -> bool {
        matches!(
            self,
            Self::AcceptedNewAuthority { .. } | Self::AcceptedIdempotent
        )
    }
}

/// Durable per-resource fence gate.
pub struct FenceGate {
    store: Arc<OpsStore>,
}

impl FenceGate {
    pub fn new(store: Arc<OpsStore>) -> Self {
        Self { store }
    }

    /// The durable fence for a resource, if one exists.
    pub fn current_fence(&self, resource_id: &str) -> crate::types::Result<Option<FenceRow>> {
        self.store.get_fence(resource_id)
    }

    /// The highest accepted epoch for a resource (0 if never fenced).
    pub fn current_epoch(&self, resource_id: &str) -> crate::types::Result<u64> {
        Ok(self
            .store
            .get_fence(resource_id)?
            .map(|f| f.epoch)
            .unwrap_or(0))
    }

    /// Decide whether a command from `authority_id` at `received_epoch`
    /// may touch `resource_id`. Acceptance of a newer authority persists
    /// the new fence durably BEFORE returning, so a crash immediately
    /// after acceptance still remembers the new authority.
    pub fn check(
        &self,
        resource_id: &str,
        received_epoch: u64,
        authority_id: &str,
    ) -> crate::types::Result<FenceDecision> {
        let current = self.store.get_fence(resource_id)?;
        let decision = match &current {
            None => FenceDecision::AcceptedNewAuthority {
                previous_epoch: None,
            },
            Some(fence) => match received_epoch.cmp(&fence.epoch) {
                std::cmp::Ordering::Less => FenceDecision::RejectedStaleEpoch {
                    received: received_epoch,
                    current: fence.epoch,
                },
                std::cmp::Ordering::Equal => {
                    if fence.authority_coordinator_id == authority_id {
                        FenceDecision::AcceptedIdempotent
                    } else {
                        FenceDecision::RejectedAuthorityConflict {
                            received_authority: authority_id.to_string(),
                            current_authority: fence.authority_coordinator_id.clone(),
                        }
                    }
                }
                std::cmp::Ordering::Greater => FenceDecision::AcceptedNewAuthority {
                    previous_epoch: Some(fence.epoch),
                },
            },
        };

        // Persist BEFORE reporting acceptance (durable, no rollback).
        if let FenceDecision::AcceptedNewAuthority { .. } = &decision {
            self.store.upsert_fence(&FenceRow {
                resource_id: resource_id.to_string(),
                epoch: received_epoch,
                authority_coordinator_id: authority_id.to_string(),
                updated_at_ms: now_ms(),
            })?;
        }
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> FenceGate {
        FenceGate::new(Arc::new(OpsStore::open("sqlite::memory:").unwrap()))
    }

    #[test]
    fn first_authority_accepted_and_persisted() {
        let g = gate();
        let d = g.check("res-1", 5, "coord-a").unwrap();
        assert_eq!(
            d,
            FenceDecision::AcceptedNewAuthority {
                previous_epoch: None
            }
        );
        assert_eq!(g.current_epoch("res-1").unwrap(), 5);
    }

    #[test]
    fn older_epoch_rejected() {
        let g = gate();
        g.check("res-1", 5, "coord-a").unwrap();
        let d = g.check("res-1", 4, "coord-a").unwrap();
        assert_eq!(
            d,
            FenceDecision::RejectedStaleEpoch {
                received: 4,
                current: 5
            }
        );
        // Even a different authority cannot use the old epoch.
        let d = g.check("res-1", 4, "coord-b").unwrap();
        assert_eq!(
            d,
            FenceDecision::RejectedStaleEpoch {
                received: 4,
                current: 5
            }
        );
    }

    #[test]
    fn same_epoch_same_authority_idempotent() {
        let g = gate();
        g.check("res-1", 5, "coord-a").unwrap();
        let d = g.check("res-1", 5, "coord-a").unwrap();
        assert_eq!(d, FenceDecision::AcceptedIdempotent);
    }

    #[test]
    fn same_epoch_different_authority_conflict() {
        let g = gate();
        g.check("res-1", 5, "coord-a").unwrap();
        let d = g.check("res-1", 5, "coord-b").unwrap();
        assert_eq!(
            d,
            FenceDecision::RejectedAuthorityConflict {
                received_authority: "coord-b".into(),
                current_authority: "coord-a".into()
            }
        );
        // Fence unchanged after the conflict.
        let fence = g.current_fence("res-1").unwrap().unwrap();
        assert_eq!(fence.epoch, 5);
        assert_eq!(fence.authority_coordinator_id, "coord-a");
    }

    #[test]
    fn newer_epoch_accepted_and_persisted() {
        let g = gate();
        g.check("res-1", 5, "coord-a").unwrap();
        let d = g.check("res-1", 6, "coord-b").unwrap();
        assert_eq!(
            d,
            FenceDecision::AcceptedNewAuthority {
                previous_epoch: Some(5)
            }
        );
        let fence = g.current_fence("res-1").unwrap().unwrap();
        assert_eq!(fence.epoch, 6);
        assert_eq!(fence.authority_coordinator_id, "coord-b");
        // The deposed authority is now stale.
        let d = g.check("res-1", 5, "coord-a").unwrap();
        assert_eq!(
            d,
            FenceDecision::RejectedStaleEpoch {
                received: 5,
                current: 6
            }
        );
    }

    #[test]
    fn fence_survives_restart_on_real_file() {
        let tmp = tempfile::tempdir().unwrap();
        let db_url = format!("sqlite:{}/fence.db", tmp.path().display());

        {
            let store = Arc::new(OpsStore::open(&db_url).unwrap());
            let g = FenceGate::new(store);
            g.check("res-x", 9, "coord-a").unwrap();
        }
        // "Restart": reopen the same durable database.
        {
            let store = Arc::new(OpsStore::open(&db_url).unwrap());
            let g = FenceGate::new(store);
            assert_eq!(g.current_epoch("res-x").unwrap(), 9);
            let d = g.check("res-x", 8, "coord-b").unwrap();
            assert_eq!(
                d,
                FenceDecision::RejectedStaleEpoch {
                    received: 8,
                    current: 9
                },
                "fence must not roll back across restart"
            );
        }
    }
}
