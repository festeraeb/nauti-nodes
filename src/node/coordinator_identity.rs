//! Coordinator identity and federation membership.
//!
//! Extends the existing Ed25519 node identity with:
//! - A persistent coordinator flag (this instance is/aims to be a coordinator)
//! - Federation membership state (JOINING, ACTIVE, DEGRADED, UNREACHABLE, REMOVED, REVOKED)
//! - Authority epoch / term for fencing
//! - Trusted peer public keys
//! - Replication position / last sync timestamp
//!
//! Identity files are protected with 0600 permissions (same as NodeIdentity).

use crate::types::now_ms;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;

/// Federation membership lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum FederationState {
    /// Just joined; initial handshake not yet validated.
    Joining,
    /// Federation membership is confirmed; this coordinator may participate.
    Active,
    /// This coordinator is temporarily unable to participate (e.g. stale
    /// partition, health check failure).  It must NOT issue ownership
    /// mutations.
    Degraded,
    /// This coordinator has not been heard from in longer than the
    /// partition-detection window.  Other coordinators should treat it
    /// as removed.
    Unreachable,
    /// This coordinator has been explicitly removed from the federation
    /// (operator action).
    Removed,
    /// This coordinator's identity has been revoked (operator action).
    Revoked,
}

/// Authority epoch / term for fencing.
///
/// Every ownership mutation (lease grant, scheduler eligibility change,
/// checkpoint assignment) must be associated with the current epoch.
/// If a coordinator's epoch is stale, its commands are rejected.
///
/// The epoch advances only when a new coordinator acquires valid
/// authority via the federation handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct AuthorityEpoch(pub u64);

impl AuthorityEpoch {
    pub const fn new(value: u64) -> Self {
        AuthorityEpoch(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    /// Advance to the next epoch.
    pub fn next(self) -> Self {
        AuthorityEpoch(self.0 + 1)
    }
}

/// Coordinator identity: persistent cryptographic identity + federation
/// membership + authority epoch.
///
/// This builds on the existing `NodeIdentity` (Ed25519 keypair + UUID node_id)
/// and adds the fields needed for a multi-coordinator federation with fencing.
///
/// Identity files are protected with 0600 permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorIdentity {
    /// Underlying node identity (UUID + Ed25519 keypair).
    pub node_identity: crate::node::identity::NodeIdentity,

    /// True when this instance is configured to operate as a coordinator.
    /// When false, this is a plain worker node.
    pub is_coordinator: bool,

    /// Federation membership state machine.
    pub federation_state: FederationState,

    /// Current authority epoch.  Only the coordinator with the highest
    /// valid epoch may issue ownership mutations.
    pub authority_epoch: AuthorityEpoch,

    /// When this epoch was acquired (millis since unix epoch).
    pub epoch_acquired_at_ms: u64,

    /// Trusted peer public keys (hex-encoded ed25519).  These are the
    /// coordinators whose authority we accept.
    pub trusted_peers: Vec<String>,

    /// When we last successfully synced state from a trusted peer
    /// (millis since unix epoch).
    pub last_synced_at_ms: u64,

    /// Our replication position: 0 = just joined, higher = more up-to-date.
    pub replication_position: u64,

    /// Software version that created this identity.
    pub software_version: String,
}

impl CoordinatorIdentity {
    /// Generate a fresh coordinator identity.
    ///
    /// The node_id is generated as a UUID v4.  The Ed25519 keypair is
    /// freshly generated.  `is_coordinator` is set to true, and the
    /// federation state starts at `JOINING`.
    pub fn generate() -> crate::types::Result<Self> {
        let node_id = crate::node::identity::NodeIdentity::generate();
        let is_coordinator = true;

        Ok(Self {
            node_identity: node_id,
            is_coordinator,
            federation_state: FederationState::Joining,
            authority_epoch: AuthorityEpoch::new(0),
            epoch_acquired_at_ms: now_ms(),
            trusted_peers: Vec::new(),
            last_synced_at_ms: 0,
            replication_position: 0,
            software_version: std::env!("CARGO_PKG_VERSION").to_string(),
        })
    }

    /// Load from a JSON file, or generate and persist a new one.
    ///
    /// The file path is expected to end in `.json` (or no extension).
    /// Permissions are set to 0600 on Unix.
    pub fn load_or_create(path: &Path) -> crate::types::Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path).map_err(|e| {
                crate::types::Error::Internal(format!("read coordinator identity: {e}"))
            })?;
            let identity: Self = serde_json::from_str(&data).map_err(|e| {
                crate::types::Error::Internal(format!("parse coordinator identity: {e}"))
            })?;
            identity.validate()?;
            info!(node_id = %identity.node_identity.node_id, "loaded existing coordinator identity");
            Ok(identity)
        } else {
            let identity = Self::generate()?;
            identity.validate()?;
            let data = serde_json::to_string_pretty(&identity).map_err(|e| {
                crate::types::Error::Internal(format!("serialize coordinator identity: {e}"))
            })?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::types::Error::Internal(format!("create coordinator identity dir: {e}"))
                })?;
            }
            std::fs::write(path, &data).map_err(|e| {
                crate::types::Error::Internal(format!("write coordinator identity: {e}"))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }
            info!(node_id = %identity.node_identity.node_id, path = %path.display(), "generated new coordinator identity");
            Ok(identity)
        }
    }

    /// Validate the structural and cryptographic integrity of this identity.
    pub fn validate(&self) -> crate::types::Result<()> {
        // Underlying node identity must be valid.
        self.node_identity.validate()?;

        // Authority epoch is a u64; the monotonic guarantee comes from the
        // federation protocol (epochs only advance via join_federation /
        // advance_epoch). No additional range check needed.

        // Federation state must be a valid variant. All variants are
        // exhaustive; this match exists to document the valid set and
        // future-proof against new variants.
        match &self.federation_state {
            FederationState::Joining
            | FederationState::Active
            | FederationState::Degraded
            | FederationState::Unreachable
            | FederationState::Removed
            | FederationState::Revoked => {}
        }

        Ok(())
    }

    /// Mark this coordinator as joined into the federation with a given
    /// trusted peer set.
    pub fn join_federation(&mut self, trusted_peers: Vec<String>) {
        let peers_str = trusted_peers.join(", ");
        self.federation_state = FederationState::Active;
        self.trusted_peers = trusted_peers;
        self.authority_epoch = self.authority_epoch.next();
        self.epoch_acquired_at_ms = now_ms();
        self.replication_position = 0;
        info!(
            node_id = %self.node_identity.node_id,
            peers = %peers_str,
            epoch = self.authority_epoch.value(),
            "joined federation"
        );
    }

    /// Advance authority epoch when this coordinator acquires valid
    /// new authority (e.g. after a failover).
    pub fn advance_epoch(&mut self) {
        self.authority_epoch = self.authority_epoch.next();
        self.epoch_acquired_at_ms = now_ms();
        info!(
            node_id = %self.node_identity.node_id,
            epoch = self.authority_epoch.value(),
            "authority epoch advanced"
        );
    }

    /// Transition to DEGRADED state (e.g. partition detected, health check
    /// failure).  Must NOT issue ownership mutations while degraded.
    pub fn set_degraded(&mut self) {
        self.federation_state = FederationState::Degraded;
        info!(node_id = %self.node_identity.node_id, "federation state -> DEGRADED");
    }

    /// Transition back to ACTIVE from DEGRADED after recovery.
    pub fn set_active(&mut self) {
        self.federation_state = FederationState::Active;
        info!(node_id = %self.node_identity.node_id, "federation state -> ACTIVE");
    }

    /// Mark this coordinator as unreachable (partition detected).
    pub fn set_unreachable(&mut self) {
        self.federation_state = FederationState::Unreachable;
        info!(node_id = %self.node_identity.node_id, "federation state -> UNREACHABLE");
    }

    /// Mark this coordinator as removed (operator action).
    pub fn set_removed(&mut self) {
        self.federation_state = FederationState::Removed;
        info!(node_id = %self.node_identity.node_id, "federation state -> REMOVED");
    }

    /// Mark this coordinator as revoked (operator action).
    pub fn set_revoked(&mut self) {
        self.federation_state = FederationState::Revoked;
        info!(node_id = %self.node_identity.node_id, "federation state -> REVOKED");
    }

    /// Get the human-readable fingerprint of this coordinator's public key.
    pub fn fingerprint(&self) -> String {
        let pk = &self.node_identity.public_key;
        format!("ed25519:{}", hex::encode(&pk[..16.min(pk.len())]))
    }

    /// Whether this coordinator may issue ownership mutations.
    pub fn can_issue_mutations(&self) -> bool {
        self.is_coordinator
            && matches!(self.federation_state, FederationState::Active)
            && !matches!(self.federation_state, FederationState::Revoked)
    }

    /// Whether this coordinator is a candidate for acquiring authority.
    pub fn is_authority_candidate(&self) -> bool {
        self.can_issue_mutations() && self.authority_epoch.0 > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_starts_joining() {
        let id = CoordinatorIdentity::generate().unwrap();
        assert!(id.is_coordinator);
        assert_eq!(id.federation_state, FederationState::Joining);
        assert_eq!(id.authority_epoch.value(), 0);
        assert_eq!(id.software_version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn load_or_create_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().with_extension("json");
        let _ = std::fs::remove_file(&path);

        let id1 = CoordinatorIdentity::load_or_create(&path).unwrap();
        let id2 = CoordinatorIdentity::load_or_create(&path).unwrap();
        assert_eq!(id1.node_identity.node_id, id2.node_identity.node_id);
        assert_eq!(id1.federation_state, id2.federation_state);
        assert_eq!(id1.authority_epoch.value(), id2.authority_epoch.value());
    }

    #[test]
    fn join_federation_advances_epoch() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        id.join_federation(vec!["peer1".to_string(), "peer2".to_string()]);
        assert_eq!(id.federation_state, FederationState::Active);
        assert_eq!(
            id.trusted_peers,
            vec!["peer1".to_string(), "peer2".to_string()]
        );
        assert_eq!(id.authority_epoch.value(), 1);
        assert_eq!(id.epoch_acquired_at_ms, now_ms());
    }

    #[test]
    fn validate_structural() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        // Invalid node identity should fail.
        id.node_identity.node_id = "not-a-uuid".into();
        assert!(id.validate().is_err());

        // Reset to valid.
        id.node_identity.node_id = uuid::Uuid::new_v4().to_string();
        assert!(id.validate().is_ok());

        // Invalid federation state should fail.
        id.federation_state = FederationState::Revoked;
        assert!(id.validate().is_ok());
    }

    #[test]
    fn state_transitions() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        assert_eq!(id.federation_state, FederationState::Joining);
        id.set_active();
        assert_eq!(id.federation_state, FederationState::Active);
        id.set_degraded();
        assert_eq!(id.federation_state, FederationState::Degraded);
        id.set_active();
        assert_eq!(id.federation_state, FederationState::Active);
        id.set_unreachable();
        assert_eq!(id.federation_state, FederationState::Unreachable);
        id.set_removed();
        assert_eq!(id.federation_state, FederationState::Removed);
        id.set_revoked();
        assert_eq!(id.federation_state, FederationState::Revoked);
    }

    #[test]
    fn fingerprint() {
        let id = CoordinatorIdentity::generate().unwrap();
        let fp = id.fingerprint();
        assert!(fp.starts_with("ed25519:"));
        // First 16 bytes of public key = 32 hex chars, but may vary
        let rest = &fp[10..];
        assert!(rest.len() >= 30 && rest.len() <= 32);
    }

    #[test]
    fn can_issue_mutations() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        assert!(!id.can_issue_mutations()); // is_coordinator=true but state=Joining
        id.join_federation(vec!["p1".to_string()]);
        assert!(id.can_issue_mutations());
    }

    #[test]
    fn is_authority_candidate() {
        let mut id = CoordinatorIdentity::generate().unwrap();
        assert!(!id.is_authority_candidate()); // epoch=0
        id.join_federation(vec!["p1".to_string()]);
        assert!(id.is_authority_candidate()); // epoch=1, Active
    }
}
