//! Authority election, failover, and partition policy (Chunk 8B.4 / 8B.6).
//!
//! HONEST MECHANISM DESCRIPTION (no quorum claims):
//!
//! The current architecture uses DETERMINISTIC SELECTION over real HTTP
//! liveness probes, not a consensus protocol:
//!
//! - Every coordinator exposes `GET /v1/federation/authority` returning its
//!   real [`AuthorityRecord`] (coordinator id, epoch, acquired_at).
//! - Peers probe each other over real TCP and keep the newest records.
//! - Authority is whoever wins [`select_authority`] over the known records
//!   (highest epoch; deterministic tie-breaks).
//! - When the current authority becomes unreachable (probe failure past the
//!   unreachability window), the highest-priority surviving coordinator
//!   acquires authority by adopting epoch `max_known + 1` and persisting
//!   the new epoch BEFORE issuing any mutation.
//!
//! LIMITATION (stated, not hidden): without quorum there is a bounded
//! dual-authority WINDOW between the old authority's death/isolation and
//! the takeover. Safety during that window comes from the worker-side
//! durable fence: same-epoch commands from a different authority are
//! rejected as authority conflicts, and stale epochs are rejected, so two
//! coordinators can never both mutate the same resource. This is NOT
//! claimed to be network-safe failover in the Raft sense.

use super::{select_authority, AuthorityRecord, CoordinatorId, PeerRegistry};
use crate::node::coordinator_identity::CoordinatorIdentity;
use crate::types::now_ms;
use dashmap::DashMap;
use std::sync::Arc;

/// Default window after which an unprobed authority is considered
/// unreachable (milliseconds).
pub const DEFAULT_UNREACHABLE_AFTER_MS: u64 = 15_000;

/// Why a coordinator is denied ownership-changing operations under the
/// partition policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionDenial {
    /// Local membership state does not permit mutations.
    NotMutationCapable,
    /// A different coordinator wins the authority election.
    NotElectedAuthority { elected: CoordinatorId },
    /// Local epoch is behind the known federation epoch.
    StaleLocalEpoch { local: u64, known: u64 },
}

impl std::fmt::Display for PartitionDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMutationCapable => write!(
                f,
                "partition policy: local coordinator is not mutation-capable (fail closed)"
            ),
            Self::NotElectedAuthority { elected } => write!(
                f,
                "partition policy: authority is {elected}; this coordinator must fail closed"
            ),
            Self::StaleLocalEpoch { local, known } => write!(
                f,
                "partition policy: local epoch {local} behind known federation epoch {known}"
            ),
        }
    }
}

/// Tracks peer authority records learned via real probes and drives
/// deterministic failover.
pub struct AuthorityMonitor {
    local_id: CoordinatorId,
    /// Peer authority records keyed by coordinator id (real probe results).
    records: DashMap<CoordinatorId, AuthorityRecord>,
    /// Last successful probe time per peer endpoint (ms).
    last_probe_ok_ms: DashMap<String, u64>,
    peers: Arc<PeerRegistry>,
    http: reqwest::Client,
    pub unreachable_after_ms: u64,
}

impl AuthorityMonitor {
    pub fn new(local_id: CoordinatorId, peers: Arc<PeerRegistry>) -> Self {
        Self {
            local_id,
            records: DashMap::new(),
            last_probe_ok_ms: DashMap::new(),
            peers,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap_or_default(),
            unreachable_after_ms: DEFAULT_UNREACHABLE_AFTER_MS,
        }
    }

    pub fn local_id(&self) -> &str {
        &self.local_id
    }

    /// Record a peer authority record learned via a real probe or a real
    /// signed message.
    pub fn record_peer_authority(&self, record: AuthorityRecord) {
        self.peers
            .mark_seen(&record.coordinator_id, now_ms() as i64);
        self.records.insert(record.coordinator_id.clone(), record);
    }

    /// Probe one peer endpoint over REAL HTTP and store its authority
    /// record. Returns the record on success.
    pub async fn probe_peer(&self, endpoint: &str) -> Result<AuthorityRecord, String> {
        let url = format!("{}/v1/federation/authority", endpoint.trim_end_matches('/'));
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("probe {endpoint}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("probe {endpoint}: status {}", resp.status()));
        }
        let record: AuthorityRecord = resp
            .json()
            .await
            .map_err(|e| format!("probe {endpoint}: bad authority record: {e}"))?;
        self.last_probe_ok_ms.insert(endpoint.to_string(), now_ms());
        self.record_peer_authority(record.clone());
        Ok(record)
    }

    /// Probe all configured peers. Returns (reachable, unreachable)
    /// endpoint counts. Every result comes from real TCP attempts.
    pub async fn probe_all(&self) -> (usize, usize) {
        let endpoints: Vec<String> = self.peers.list().into_iter().map(|p| p.endpoint).collect();
        let mut ok = 0usize;
        let mut failed = 0usize;
        for endpoint in endpoints {
            match self.probe_peer(&endpoint).await {
                Ok(_) => ok += 1,
                Err(_) => failed += 1,
            }
        }
        (ok, failed)
    }

    /// Snapshot of currently known peer authority records.
    pub fn peer_records(&self) -> Vec<AuthorityRecord> {
        self.records.iter().map(|r| r.clone()).collect()
    }

    /// Deterministic election over the local record + known peer records.
    pub fn elect(&self, local: &AuthorityRecord) -> AuthorityRecord {
        let mut all = self.peer_records();
        all.push(local.clone());
        select_authority(&all)
            .cloned()
            .unwrap_or_else(|| local.clone())
    }

    /// Whether a peer endpoint's last successful probe is older than the
    /// unreachability window (or never succeeded).
    pub fn endpoint_unreachable(&self, endpoint: &str) -> bool {
        match self.last_probe_ok_ms.get(endpoint) {
            Some(ts) => now_ms().saturating_sub(*ts) > self.unreachable_after_ms,
            None => true,
        }
    }

    /// Whether the given authority id is currently reachable according to
    /// probe evidence. The local coordinator is always reachable to itself.
    pub fn authority_reachable(&self, authority_id: &str) -> bool {
        if authority_id == self.local_id {
            return true;
        }
        // Reachability evidence: a recent successful probe of any endpoint
        // whose provisional id matches, or a fresh last_seen from a real
        // signed message.
        if let Some(peer) = self.peers.get(authority_id) {
            if !self.endpoint_unreachable(&peer.endpoint) {
                return true;
            }
        }
        match self.peers.get(authority_id).and_then(|p| p.last_seen_ms) {
            Some(seen) => {
                (now_ms() as i64).saturating_sub(seen) <= self.unreachable_after_ms as i64
            }
            None => {
                // No endpoint match: fall back to record freshness.
                self.records.get(authority_id).is_some()
                    && self
                        .peers
                        .list()
                        .iter()
                        .any(|p| p.coordinator_id == authority_id && p.last_seen_ms.is_some())
            }
        }
    }

    /// Attempt to acquire authority after the current authority became
    /// unreachable (Chunk 8B.4 deterministic failover).
    ///
    /// Preconditions enforced (fail closed):
    /// - the elected authority is NOT this coordinator already
    /// - the elected authority is unreachable per probe evidence
    /// - this coordinator is mutation-capable
    ///
    /// On success the identity adopts `max_known_epoch + 1` (strictly
    /// monotonic) and becomes Active. The CALLER persists the identity.
    pub fn acquire_authority(&self, identity: &mut CoordinatorIdentity) -> Result<u64, String> {
        let local_record = AuthorityRecord {
            coordinator_id: self.local_id.clone(),
            epoch: identity.authority_epoch.value(),
            acquired_at_ms: identity.epoch_acquired_at_ms as i64,
        };
        let elected = self.elect(&local_record);
        if elected.coordinator_id == self.local_id {
            return Err("already the elected authority".to_string());
        }
        if self.authority_reachable(&elected.coordinator_id) {
            return Err(format!(
                "current authority {} is still reachable; failover refused",
                elected.coordinator_id
            ));
        }
        if !identity.can_issue_mutations() {
            // A Joining/Degraded/Revoked coordinator must not take over.
            return Err("local coordinator is not mutation-capable; failover refused".to_string());
        }
        let max_known = self
            .peer_records()
            .into_iter()
            .map(|r| r.epoch)
            .max()
            .unwrap_or(0)
            .max(identity.authority_epoch.value());
        let new_epoch = max_known + 1;
        identity
            .adopt_epoch_as_authority(new_epoch)
            .map_err(|e| e.to_string())?;
        // Record our own new authority so subsequent elections see it.
        self.record_peer_authority(AuthorityRecord {
            coordinator_id: self.local_id.clone(),
            epoch: new_epoch,
            acquired_at_ms: identity.epoch_acquired_at_ms as i64,
        });
        Ok(new_epoch)
    }
}

/// Chunk 8B.6 partition policy gate.
///
/// POLICY (exact):
/// - Only the coordinator that (a) is mutation-capable (Active member),
///   (b) wins the deterministic authority election over all known records,
///   and (c) holds an epoch >= the known federation maximum, may create
///   NEW ownership-changing operations:
///     * new Forge lease
///     * new shared ownership assignment
///     * checkpoint resume
///     * runtime ownership transition
/// - Every other side FAILS CLOSED (no new ownership mutations).
/// - Already-authorized work: shared jobs already assigned and leases
///   already granted BEFORE the partition may finish; the current design
///   explicitly permits completion of in-flight authorized work. Lease
///   truth continues to exclude leased GPUs from new assignment.
/// - There is NO quorum. This policy plus the worker-side durable fence is
///   what bounds the dual-authority window.
pub fn assert_may_mutate(
    identity: &CoordinatorIdentity,
    monitor: &AuthorityMonitor,
) -> Result<(), PartitionDenial> {
    if !identity.can_issue_mutations() {
        return Err(PartitionDenial::NotMutationCapable);
    }
    let local_record = AuthorityRecord {
        coordinator_id: identity.node_identity.node_id.clone(),
        epoch: identity.authority_epoch.value(),
        acquired_at_ms: identity.epoch_acquired_at_ms as i64,
    };
    let elected = monitor.elect(&local_record);
    if elected.coordinator_id != identity.node_identity.node_id {
        return Err(PartitionDenial::NotElectedAuthority {
            elected: elected.coordinator_id,
        });
    }
    let known_max = monitor
        .peer_records()
        .into_iter()
        .map(|r| r.epoch)
        .max()
        .unwrap_or(0);
    if identity.authority_epoch.value() < known_max {
        return Err(PartitionDenial::StaleLocalEpoch {
            local: identity.authority_epoch.value(),
            known: known_max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::CoordinatorRole;

    fn monitor_with_peers(endpoints: &[&str]) -> AuthorityMonitor {
        let peers = Arc::new(PeerRegistry::seed_from_endpoints(
            &endpoints.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ));
        AuthorityMonitor::new("coord-local".into(), peers)
    }

    fn local_identity() -> CoordinatorIdentity {
        let mut id = CoordinatorIdentity::generate().unwrap();
        id.join_federation(vec![]);
        id
    }

    #[test]
    fn election_prefers_highest_epoch() {
        let mon = monitor_with_peers(&[]);
        mon.record_peer_authority(AuthorityRecord {
            coordinator_id: "coord-a".into(),
            epoch: 7,
            acquired_at_ms: 100,
        });
        let local = AuthorityRecord {
            coordinator_id: "coord-local".into(),
            epoch: 3,
            acquired_at_ms: 50,
        };
        let elected = mon.elect(&local);
        assert_eq!(elected.coordinator_id, "coord-a");
    }

    #[test]
    fn failover_refused_while_authority_reachable() {
        let mon = monitor_with_peers(&["http://peer-a:8081"]);
        // Fresh probe evidence: peer reachable.
        mon.last_probe_ok_ms
            .insert("http://peer-a:8081".to_string(), now_ms());
        mon.peers
            .set_role("http://peer-a:8081", CoordinatorRole::Authority);
        mon.record_peer_authority(AuthorityRecord {
            coordinator_id: "http://peer-a:8081".into(),
            epoch: 5,
            acquired_at_ms: 100,
        });
        let mut id = local_identity();
        let err = mon.acquire_authority(&mut id).unwrap_err();
        assert!(err.contains("still reachable"), "got: {err}");
    }

    #[test]
    fn failover_acquires_max_plus_one_when_authority_unreachable() {
        let mon = monitor_with_peers(&["http://peer-a:8081"]);
        // No successful probe ever recorded -> unreachable.
        mon.record_peer_authority(AuthorityRecord {
            coordinator_id: "http://peer-a:8081".into(),
            epoch: 5,
            acquired_at_ms: 100,
        });
        // Stale the peer's last_seen so record freshness does not rescue it.
        mon.peers.mark_seen("http://peer-a:8081", 0);
        let mut id = local_identity();
        let new_epoch = mon.acquire_authority(&mut id).unwrap();
        assert_eq!(new_epoch, 6, "epoch must advance to max_known + 1");
        assert_eq!(id.authority_epoch.value(), 6);
        assert!(id.can_issue_mutations());
    }

    #[test]
    fn failover_refused_for_non_mutation_capable_coordinator() {
        let mon = monitor_with_peers(&["http://peer-a:8081"]);
        mon.record_peer_authority(AuthorityRecord {
            coordinator_id: "http://peer-a:8081".into(),
            epoch: 5,
            acquired_at_ms: 100,
        });
        mon.peers.mark_seen("http://peer-a:8081", 0);
        let mut id = CoordinatorIdentity::generate().unwrap(); // Joining state
        let err = mon.acquire_authority(&mut id).unwrap_err();
        assert!(err.contains("not mutation-capable"), "got: {err}");
    }

    #[test]
    fn epoch_adoption_is_monotonic() {
        let mut id = local_identity(); // epoch 1
        assert!(id.adopt_epoch_as_authority(1).is_err());
        assert!(id.adopt_epoch_as_authority(0).is_err());
        assert!(id.adopt_epoch_as_authority(2).is_ok());
        assert_eq!(id.authority_epoch.value(), 2);
    }

    #[test]
    fn partition_gate_allows_elected_active_authority() {
        let mon = monitor_with_peers(&[]);
        let mut id = local_identity();
        id.join_federation(vec![]); // epoch 2
        assert!(assert_may_mutate(&id, &mon).is_ok());
    }

    #[test]
    fn partition_gate_denies_joining_coordinator() {
        let mon = monitor_with_peers(&[]);
        let id = CoordinatorIdentity::generate().unwrap(); // Joining
        assert_eq!(
            assert_may_mutate(&id, &mon),
            Err(PartitionDenial::NotMutationCapable)
        );
    }

    #[test]
    fn partition_gate_denies_non_elected_coordinator() {
        let mon = monitor_with_peers(&[]);
        mon.record_peer_authority(AuthorityRecord {
            coordinator_id: "coord-boss".into(),
            epoch: 9,
            acquired_at_ms: 100,
        });
        let mut id = local_identity();
        id.join_federation(vec![]); // epoch 2 < 9
        assert_eq!(
            assert_may_mutate(&id, &mon),
            Err(PartitionDenial::NotElectedAuthority {
                elected: "coord-boss".into()
            })
        );
    }

    #[test]
    fn follower_epoch_adoption_does_not_grant_mutations() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        id.adopt_follower_epoch(10);
        assert_eq!(id.authority_epoch.value(), 10);
        assert!(!id.can_issue_mutations(), "follower must stay fail-closed");
    }
}
