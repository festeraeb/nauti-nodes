//! Federation core types (Chunk 8B).
//!
//! Adapted to the existing Nauti architecture:
//! - Local federation membership lifecycle stays in
//!   [`crate::node::coordinator_identity::FederationState`] (already wired
//!   into `AppState`, `/v1/node/identity` and the persistence layer).
//! - [`CoordinatorRole`] is the federation-topology role of a coordinator
//!   (peer view / local view), mapped from `FederationState` plus the
//!   authority election result.
//! - [`LeaseId`] is `String` here, not `Uuid`: the Chunk 7 lease store
//!   accepts caller-provided lease id hints (tests and Forge both use
//!   non-UUID hints), so the behavioral contract is preserved.
//!
//! Production-truth note: every value in this module is either pure
//! deterministic logic or state supplied by configuration / real runtime
//! events. Nothing here generates synthetic federation telemetry.

use crate::node::coordinator_identity::{CoordinatorIdentity, FederationState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Stable identifier of a coordinator in the federation.
pub type CoordinatorId = String;

/// Stable identifier of a fenceable resource (node/GPU path).
pub type ResourceId = String;

/// Lease identifier. Adapted from the reference `Uuid` to `String` because
/// the existing lease store accepts operator/Forge-provided id hints.
pub type LeaseId = String;

/// Monotonic authority epoch (fencing token).
pub type Epoch = u64;

/// Role of a coordinator within the federation topology.
///
/// Distinct from [`FederationState`] (membership lifecycle): a coordinator
/// is `Active` as a member while its *role* may be `Follower` or
/// `Authority` depending on the epoch election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordinatorRole {
    /// Handshake not yet validated.
    Joining,
    /// Active member that accepts another coordinator's authority.
    Follower,
    /// Active member holding the highest valid authority epoch.
    Authority,
    /// Temporarily unable to participate; must not issue mutations.
    Degraded,
    /// Removed from the federation (operator action or unreachable).
    Removed,
    /// Identity revoked (operator action).
    Revoked,
}

impl CoordinatorRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Joining => "JOINING",
            Self::Follower => "FOLLOWER",
            Self::Authority => "AUTHORITY",
            Self::Degraded => "DEGRADED",
            Self::Removed => "REMOVED",
            Self::Revoked => "REVOKED",
        }
    }

    /// Local view of this coordinator's role.
    ///
    /// An `Active` member that may issue mutations and holds a nonzero
    /// epoch is the local authority in single-coordinator deployments; in a
    /// multi-coordinator deployment the definitive answer comes from
    /// [`select_authority`] over the peer `AuthorityRecord`s.
    pub fn from_local_identity(id: &CoordinatorIdentity) -> Self {
        match id.federation_state {
            FederationState::Joining => Self::Joining,
            FederationState::Degraded => Self::Degraded,
            FederationState::Unreachable | FederationState::Removed => Self::Removed,
            FederationState::Revoked => Self::Revoked,
            FederationState::Active => {
                if id.is_authority_candidate() {
                    Self::Authority
                } else {
                    Self::Follower
                }
            }
        }
    }

    /// Map a peer's membership state to a role view. `is_authority` marks
    /// the peer currently holding the highest valid epoch.
    pub fn from_peer_state(state: &FederationState, is_authority: bool) -> Self {
        match state {
            FederationState::Joining => Self::Joining,
            FederationState::Degraded => Self::Degraded,
            FederationState::Unreachable | FederationState::Removed => Self::Removed,
            FederationState::Revoked => Self::Revoked,
            FederationState::Active => {
                if is_authority {
                    Self::Authority
                } else {
                    Self::Follower
                }
            }
        }
    }
}

/// A coordinator's claim of authority at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityRecord {
    pub coordinator_id: CoordinatorId,
    pub epoch: Epoch,
    pub acquired_at_ms: i64,
}

/// Deterministic authority election over a set of authority records.
///
/// Highest epoch wins. Ties break deterministically: earliest
/// `acquired_at_ms` first (first acquirer keeps authority), then smallest
/// `coordinator_id`. Returns `None` for an empty set.
pub fn select_authority(records: &[AuthorityRecord]) -> Option<&AuthorityRecord> {
    records.iter().max_by(|a, b| {
        a.epoch
            .cmp(&b.epoch)
            .then_with(|| b.acquired_at_ms.cmp(&a.acquired_at_ms))
            .then_with(|| b.coordinator_id.cmp(&a.coordinator_id))
    })
}

/// A fencing token binding a resource to the authority epoch under which it
/// was last mutated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceFence {
    pub resource_id: ResourceId,
    pub epoch: Epoch,
    pub authority_coordinator_id: CoordinatorId,
}

impl ResourceFence {
    /// A fence is current when it was issued under an epoch at least as new
    /// as the observer's local epoch. A fence issued by a stale coordinator
    /// (lower epoch) is not current and must not be honored.
    pub fn is_current(&self, local_epoch: Epoch) -> bool {
        crate::state::is_lease_epoch_current(self.epoch, local_epoch)
    }
}

/// A known federation peer coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationPeer {
    pub coordinator_id: CoordinatorId,
    /// Hex-encoded ed25519 public key. Empty until learned via handshake.
    pub public_key_hex: String,
    /// Base URL of the peer coordinator API.
    pub endpoint: String,
    pub role: CoordinatorRole,
    /// Parent coordinator in a multi-level tree, if any.
    pub parent_id: Option<CoordinatorId>,
    /// Last time this peer was heard from (millis since unix epoch).
    pub last_seen_ms: Option<i64>,
}

/// In-memory registry of federation peers.
///
/// Seeded from `Config.federation_peers` endpoints; identity fields are
/// filled in as real handshakes occur. Never invents peer state.
#[derive(Debug, Default)]
pub struct PeerRegistry {
    peers: dashmap::DashMap<CoordinatorId, FederationPeer>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed peers from configured coordinator endpoints. The endpoint is
    /// used as the provisional coordinator id until the real identity is
    /// learned via handshake.
    pub fn seed_from_endpoints(endpoints: &[String]) -> Self {
        let registry = Self::new();
        for endpoint in endpoints {
            let endpoint = endpoint.trim();
            if endpoint.is_empty() {
                continue;
            }
            registry.upsert(FederationPeer {
                coordinator_id: endpoint.to_string(),
                public_key_hex: String::new(),
                endpoint: endpoint.to_string(),
                role: CoordinatorRole::Joining,
                parent_id: None,
                last_seen_ms: None,
            });
        }
        registry
    }

    pub fn upsert(&self, peer: FederationPeer) {
        self.peers.insert(peer.coordinator_id.clone(), peer);
    }

    pub fn get(&self, coordinator_id: &str) -> Option<FederationPeer> {
        self.peers.get(coordinator_id).map(|p| p.clone())
    }

    pub fn set_role(&self, coordinator_id: &str, role: CoordinatorRole) {
        if let Some(mut p) = self.peers.get_mut(coordinator_id) {
            p.role = role;
        }
    }

    pub fn mark_seen(&self, coordinator_id: &str, ts_ms: i64) {
        if let Some(mut p) = self.peers.get_mut(coordinator_id) {
            p.last_seen_ms = Some(ts_ms);
        }
    }

    /// Deterministic snapshot sorted by coordinator id.
    pub fn list(&self) -> Vec<FederationPeer> {
        let mut v: Vec<FederationPeer> = self.peers.iter().map(|p| p.clone()).collect();
        v.sort_by(|a, b| a.coordinator_id.cmp(&b.coordinator_id));
        v
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// Routing errors for federated request forwarding. Fail closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FederationRouteError {
    /// The request already visited the target coordinator (loop).
    LoopDetected(CoordinatorId),
    /// Forwarding would exceed the configured hop budget.
    MaxHopsExceeded { hop_count: u16, max_hops: u16 },
}

impl std::fmt::Display for FederationRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoopDetected(id) => write!(f, "federated request loop detected at {id}"),
            Self::MaxHopsExceeded {
                hop_count,
                max_hops,
            } => {
                write!(
                    f,
                    "federated request exceeded max hops ({hop_count}/{max_hops})"
                )
            }
        }
    }
}

impl std::error::Error for FederationRouteError {}

/// Context carried by a request that may traverse multiple coordinators.
///
/// Provides loop prevention (visited set) and a hard hop budget. Both
/// checks fail closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedRequestContext {
    pub request_id: Uuid,
    pub origin_coordinator_id: CoordinatorId,
    pub current_coordinator_id: CoordinatorId,
    pub visited: BTreeSet<CoordinatorId>,
    pub hop_count: u16,
    pub max_hops: u16,
}

impl FederatedRequestContext {
    /// Start a new federated request at the origin coordinator.
    pub fn new(origin_coordinator_id: CoordinatorId, max_hops: u16) -> Self {
        let mut visited = BTreeSet::new();
        visited.insert(origin_coordinator_id.clone());
        Self {
            request_id: Uuid::new_v4(),
            origin_coordinator_id: origin_coordinator_id.clone(),
            current_coordinator_id: origin_coordinator_id,
            visited,
            hop_count: 0,
            max_hops,
        }
    }

    /// Whether another forward is permitted by the hop budget.
    pub fn can_forward(&self) -> bool {
        self.hop_count < self.max_hops
    }

    pub fn has_visited(&self, coordinator_id: &str) -> bool {
        self.visited.contains(coordinator_id)
    }

    /// Forward the request to the next coordinator.
    ///
    /// Fails closed on loops (target already visited) and when the hop
    /// budget is exhausted.
    pub fn forward(&mut self, next: &CoordinatorId) -> Result<(), FederationRouteError> {
        if self.hop_count >= self.max_hops {
            return Err(FederationRouteError::MaxHopsExceeded {
                hop_count: self.hop_count,
                max_hops: self.max_hops,
            });
        }
        if self.visited.contains(next) {
            return Err(FederationRouteError::LoopDetected(next.clone()));
        }
        self.visited.insert(next.clone());
        self.current_coordinator_id = next.clone();
        self.hop_count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, epoch: Epoch, acquired: i64) -> AuthorityRecord {
        AuthorityRecord {
            coordinator_id: id.to_string(),
            epoch,
            acquired_at_ms: acquired,
        }
    }

    #[test]
    fn authority_election_highest_epoch_wins() {
        let records = vec![rec("a", 3, 100), rec("b", 7, 200), rec("c", 5, 50)];
        let winner = select_authority(&records).unwrap();
        assert_eq!(winner.coordinator_id, "b");
    }

    #[test]
    fn authority_election_tie_breaks_on_earliest_acquisition() {
        let records = vec![rec("a", 7, 200), rec("b", 7, 100)];
        let winner = select_authority(&records).unwrap();
        assert_eq!(
            winner.coordinator_id, "b",
            "earliest acquisition holds authority"
        );
    }

    #[test]
    fn authority_election_final_tie_breaks_on_id_for_determinism() {
        let records = vec![rec("a", 7, 100), rec("b", 7, 100)];
        let winner = select_authority(&records).unwrap();
        assert_eq!(winner.coordinator_id, "a");
    }

    #[test]
    fn authority_election_empty_set() {
        assert!(select_authority(&[]).is_none());
    }

    #[test]
    fn resource_fence_currency() {
        let fence = ResourceFence {
            resource_id: "node-1".into(),
            epoch: 5,
            authority_coordinator_id: "a".into(),
        };
        assert!(fence.is_current(5));
        assert!(
            fence.is_current(3),
            "newer fence is current for older observer"
        );
        assert!(!fence.is_current(6), "stale fence is not current");
    }

    #[test]
    fn federated_context_forwards_within_budget() {
        let mut ctx = FederatedRequestContext::new("a".into(), 3);
        assert!(ctx.can_forward());
        ctx.forward(&"b".into()).unwrap();
        ctx.forward(&"c".into()).unwrap();
        ctx.forward(&"d".into()).unwrap();
        assert_eq!(ctx.hop_count, 3);
        assert_eq!(ctx.current_coordinator_id, "d");
        assert_eq!(ctx.origin_coordinator_id, "a");
        assert_eq!(ctx.visited.len(), 4);
    }

    #[test]
    fn federated_context_rejects_loops() {
        let mut ctx = FederatedRequestContext::new("a".into(), 8);
        ctx.forward(&"b".into()).unwrap();
        // Back to origin: loop.
        assert_eq!(
            ctx.forward(&"a".into()),
            Err(FederationRouteError::LoopDetected("a".into()))
        );
        // Any revisited node: loop.
        assert_eq!(
            ctx.forward(&"b".into()),
            Err(FederationRouteError::LoopDetected("b".into()))
        );
        // Failed forwards must not mutate routing state.
        assert_eq!(ctx.hop_count, 1);
        assert_eq!(ctx.current_coordinator_id, "b");
    }

    #[test]
    fn federated_context_enforces_max_hops() {
        let mut ctx = FederatedRequestContext::new("a".into(), 1);
        ctx.forward(&"b".into()).unwrap();
        assert_eq!(
            ctx.forward(&"c".into()),
            Err(FederationRouteError::MaxHopsExceeded {
                hop_count: 1,
                max_hops: 1
            })
        );
        assert!(!ctx.can_forward());
    }

    #[test]
    fn peer_registry_seed_and_update() {
        let reg = PeerRegistry::seed_from_endpoints(&[
            "http://10.0.0.2:8081".into(),
            "  ".into(),
            "http://10.0.0.3:8081".into(),
        ]);
        assert_eq!(reg.len(), 2, "blank endpoints are not seeded");
        let p = reg.get("http://10.0.0.2:8081").unwrap();
        assert_eq!(p.role, CoordinatorRole::Joining);
        assert!(p.public_key_hex.is_empty());
        assert!(p.last_seen_ms.is_none());

        reg.set_role("http://10.0.0.2:8081", CoordinatorRole::Follower);
        reg.mark_seen("http://10.0.0.2:8081", 1234);
        let p = reg.get("http://10.0.0.2:8081").unwrap();
        assert_eq!(p.role, CoordinatorRole::Follower);
        assert_eq!(p.last_seen_ms, Some(1234));

        let list = reg.list();
        assert_eq!(list.len(), 2);
        assert!(list[0].coordinator_id <= list[1].coordinator_id);
    }

    #[test]
    fn role_mapping_from_federation_state() {
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Joining, false),
            CoordinatorRole::Joining
        );
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Active, true),
            CoordinatorRole::Authority
        );
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Active, false),
            CoordinatorRole::Follower
        );
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Degraded, true),
            CoordinatorRole::Degraded,
            "degraded member is never authority"
        );
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Unreachable, false),
            CoordinatorRole::Removed
        );
        assert_eq!(
            CoordinatorRole::from_peer_state(&FederationState::Revoked, false),
            CoordinatorRole::Revoked
        );
    }

    #[test]
    fn local_role_from_identity() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        assert_eq!(
            CoordinatorRole::from_local_identity(&id),
            CoordinatorRole::Joining
        );
        id.join_federation(vec!["p".into()]);
        assert_eq!(
            CoordinatorRole::from_local_identity(&id),
            CoordinatorRole::Authority,
            "single-coordinator active member with epoch>0 is local authority"
        );
        id.set_degraded();
        assert_eq!(
            CoordinatorRole::from_local_identity(&id),
            CoordinatorRole::Degraded
        );
    }

    #[test]
    fn core_types_roundtrip_serde() {
        let peer = FederationPeer {
            coordinator_id: "c1".into(),
            public_key_hex: "ab".into(),
            endpoint: "http://x".into(),
            role: CoordinatorRole::Follower,
            parent_id: Some("c0".into()),
            last_seen_ms: Some(7),
        };
        let s = serde_json::to_string(&peer).unwrap();
        let back: FederationPeer = serde_json::from_str(&s).unwrap();
        assert_eq!(back.coordinator_id, "c1");

        let record = rec("c1", 9, 42);
        let s = serde_json::to_string(&record).unwrap();
        let back: AuthorityRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(back.epoch, 9);

        let ctx = FederatedRequestContext::new("a".into(), 4);
        let s = serde_json::to_string(&ctx).unwrap();
        let back: FederatedRequestContext = serde_json::from_str(&s).unwrap();
        assert_eq!(back.request_id, ctx.request_id);
        assert_eq!(back.max_hops, 4);
    }
}
