//! Federation message authentication envelope (Chunk 8B.3).
//!
//! Generalizes the EXISTING node auth primitives (`node::auth::sign_message`,
//! `verify_signature`, `NonceTracker`) to coordinator-to-worker /
//! coordinator-to-coordinator commands. No parallel auth system.
//!
//! Every ownership-changing command travels in a [`SignedFederationCommand`]
//! carrying:
//! - sender coordinator id + sender public key (durable Ed25519 identity)
//! - timestamp + nonce (anti-replay, same 300s window as node auth)
//! - the fenced command payload (epoch + authority are inside the command)
//! - signature over the canonical message
//!
//! The command path is VERSIONED: receivers reject any envelope whose
//! `protocol_version` they do not implement instead of silently bypassing
//! fencing.

use crate::node::auth::{sign_message, verify_signature, NonceTracker};
use crate::node::revocation::RevocationStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Federation command protocol version. Bump when the command grammar
/// changes; receivers fail closed on unknown versions.
pub const FEDERATION_PROTOCOL_VERSION: u16 = 1;

/// Timestamp validity window for federation envelopes (seconds).
pub const FEDERATION_TIMESTAMP_WINDOW_SECS: u64 = 300;

/// Ownership-changing commands that must pass the worker-side fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FederationCommand {
    /// Grant an exclusive lease over a resource to a mission.
    LeaseGrant {
        lease_id: String,
        mission_id: String,
        resource_id: String,
        epoch: u64,
    },
    /// Release a lease; the resource returns to the shared pool.
    LeaseRelease {
        lease_id: String,
        mission_id: String,
        resource_id: String,
        epoch: u64,
    },
    /// Assign shared work to a resource.
    WorkAssign {
        job_id: String,
        resource_id: String,
        epoch: u64,
    },
    /// Start a runtime on a resource (ownership-sensitive).
    RuntimeStart {
        resource_id: String,
        model_id: String,
        epoch: u64,
    },
    /// Stop a runtime on a resource (ownership-sensitive).
    RuntimeStop { resource_id: String, epoch: u64 },
    /// Resume a checkpointed job on a resource.
    CheckpointResume {
        job_id: String,
        resource_id: String,
        epoch: u64,
    },
}

impl FederationCommand {
    /// The authority epoch carried by this command.
    pub fn epoch(&self) -> u64 {
        match self {
            Self::LeaseGrant { epoch, .. }
            | Self::LeaseRelease { epoch, .. }
            | Self::WorkAssign { epoch, .. }
            | Self::RuntimeStart { epoch, .. }
            | Self::RuntimeStop { epoch, .. }
            | Self::CheckpointResume { epoch, .. } => *epoch,
        }
    }

    /// The resource this command mutates (fence key).
    pub fn resource_id(&self) -> &str {
        match self {
            Self::LeaseGrant { resource_id, .. }
            | Self::LeaseRelease { resource_id, .. }
            | Self::WorkAssign { resource_id, .. }
            | Self::RuntimeStart { resource_id, .. }
            | Self::RuntimeStop { resource_id, .. }
            | Self::CheckpointResume { resource_id, .. } => resource_id,
        }
    }

    /// Idempotency key: identical commands re-delivered by the same
    /// authority must not execute twice. The key includes the command
    /// KIND so a grant and a later release of the same lease do not
    /// collide.
    pub fn idempotency_key(&self) -> String {
        match self {
            Self::LeaseGrant { lease_id, .. } => format!("lease-grant:{lease_id}"),
            Self::LeaseRelease { lease_id, .. } => format!("lease-release:{lease_id}"),
            Self::WorkAssign { job_id, .. } => format!("work-assign:{job_id}"),
            Self::CheckpointResume { job_id, .. } => format!("checkpoint-resume:{job_id}"),
            Self::RuntimeStart {
                resource_id,
                model_id,
                ..
            } => format!("runtime-start:{resource_id}:{model_id}"),
            Self::RuntimeStop { resource_id, .. } => {
                format!("runtime-stop:{resource_id}")
            }
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::LeaseGrant { .. } => "LEASE_GRANT",
            Self::LeaseRelease { .. } => "LEASE_RELEASE",
            Self::WorkAssign { .. } => "WORK_ASSIGN",
            Self::RuntimeStart { .. } => "RUNTIME_START",
            Self::RuntimeStop { .. } => "RUNTIME_STOP",
            Self::CheckpointResume { .. } => "CHECKPOINT_RESUME",
        }
    }
}

/// Signed envelope for one federation command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedFederationCommand {
    pub protocol_version: u16,
    pub sender_coordinator_id: String,
    pub sender_public_key_hex: String,
    pub command: FederationCommand,
    pub timestamp_secs: u64,
    pub nonce: String,
    pub signature_hex: String,
}

impl SignedFederationCommand {
    /// Deterministic canonical message covering every field that must be
    /// authenticated (sender, payload, freshness). Same construction as the
    /// node auth canonical messages.
    pub fn canonical_message(&self) -> String {
        let payload = serde_json::to_string(&self.command).unwrap_or_default();
        format!(
            "federation-cmd-v{}|sender={}|key={}|ts={}|nonce={}|payload={}",
            self.protocol_version,
            self.sender_coordinator_id,
            self.sender_public_key_hex,
            self.timestamp_secs,
            self.nonce,
            payload
        )
    }

    /// Sign this envelope with the sender's durable Ed25519 key.
    pub fn sign(&mut self, signing_key: &ed25519_dalek::SigningKey) {
        self.signature_hex = sign_message(signing_key, &self.canonical_message());
    }

    /// Verify the envelope signature only (not freshness/replay/trust).
    pub fn verify_signature(&self) -> Result<(), String> {
        verify_signature(
            &self.sender_public_key_hex,
            &self.canonical_message(),
            &self.signature_hex,
        )
        .map_err(|e| e.to_string())
    }

    /// Build and sign a fresh envelope (sender-side helper).
    pub fn build_signed(
        sender_coordinator_id: &str,
        sender_public_key_hex: &str,
        signing_key: &ed25519_dalek::SigningKey,
        command: FederationCommand,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut env = Self {
            protocol_version: FEDERATION_PROTOCOL_VERSION,
            sender_coordinator_id: sender_coordinator_id.to_string(),
            sender_public_key_hex: sender_public_key_hex.to_string(),
            command,
            timestamp_secs: now,
            nonce: uuid::Uuid::new_v4().to_string(),
            signature_hex: String::new(),
        };
        env.sign(signing_key);
        env
    }
}

/// Why a federation envelope was rejected. Fail closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FederationAuthError {
    UnsupportedProtocol(u16),
    UnknownCoordinator(String),
    InvalidSignature,
    StaleTimestamp { age_secs: u64 },
    ReplayedNonce(String),
    RevokedCoordinator(String),
}

impl std::fmt::Display for FederationAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedProtocol(v) => {
                write!(f, "unsupported federation protocol version {v}")
            }
            Self::UnknownCoordinator(id) => write!(f, "unknown coordinator {id}"),
            Self::InvalidSignature => write!(f, "invalid federation signature"),
            Self::StaleTimestamp { age_secs } => {
                write!(f, "stale federation timestamp (age {age_secs}s)")
            }
            Self::ReplayedNonce(n) => write!(f, "replayed federation nonce {n}"),
            Self::RevokedCoordinator(id) => write!(f, "coordinator {id} is revoked"),
        }
    }
}

impl std::error::Error for FederationAuthError {}

/// Envelope authenticator. Reuses the existing anti-replay primitives.
///
/// Trust model: a sender is known if its public key is in the configured
/// trusted coordinator keys OR its coordinator id is registered as a peer.
/// Revocation is checked against the durable revocation store.
pub struct FederationAuthenticator {
    trusted_keys: Vec<String>,
    nonces: Arc<NonceTracker>,
    revocations: Arc<RevocationStore>,
}

impl FederationAuthenticator {
    pub fn new(
        trusted_keys: Vec<String>,
        nonces: Arc<NonceTracker>,
        revocations: Arc<RevocationStore>,
    ) -> Self {
        Self {
            trusted_keys,
            nonces,
            revocations,
        }
    }

    /// Full authentication pipeline. On success the nonce is consumed so
    /// the exact same envelope can never be accepted twice.
    pub fn authenticate(&self, env: &SignedFederationCommand) -> Result<(), FederationAuthError> {
        // 1. Version gate: never silently bypass fencing on unknown grammar.
        if env.protocol_version != FEDERATION_PROTOCOL_VERSION {
            return Err(FederationAuthError::UnsupportedProtocol(
                env.protocol_version,
            ));
        }
        // 2. Known coordinator (trusted key or known peer id).
        let key_trusted = self
            .trusted_keys
            .iter()
            .any(|k| k.eq_ignore_ascii_case(&env.sender_public_key_hex));
        if !key_trusted && !self.is_known_peer(&env.sender_coordinator_id) {
            return Err(FederationAuthError::UnknownCoordinator(
                env.sender_coordinator_id.clone(),
            ));
        }
        // 3. Signature.
        env.verify_signature()
            .map_err(|_| FederationAuthError::InvalidSignature)?;
        // 4. Revocation (id or key).
        if self.revocations.is_revoked(&env.sender_coordinator_id)
            || self.revocations.is_revoked(&env.sender_public_key_hex)
        {
            return Err(FederationAuthError::RevokedCoordinator(
                env.sender_coordinator_id.clone(),
            ));
        }
        // 5. Freshness.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.abs_diff(env.timestamp_secs);
        if age > FEDERATION_TIMESTAMP_WINDOW_SECS {
            return Err(FederationAuthError::StaleTimestamp { age_secs: age });
        }
        // 6. Replay.
        if self.nonces.is_used(&env.nonce) {
            return Err(FederationAuthError::ReplayedNonce(env.nonce.clone()));
        }
        self.nonces.mark_used(env.nonce.clone());
        Ok(())
    }

    /// Peer-id knowledge hook. Overridden by registries that know peers;
    /// the base authenticator only trusts configured keys.
    fn is_known_peer(&self, _coordinator_id: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::identity::NodeIdentity;

    fn auth_with_key(pub_hex: &str) -> (FederationAuthenticator, Arc<NonceTracker>) {
        let nonces = Arc::new(NonceTracker::new());
        let revocations = Arc::new(RevocationStore::open("sqlite::memory:").unwrap());
        let auth =
            FederationAuthenticator::new(vec![pub_hex.to_string()], nonces.clone(), revocations);
        (auth, nonces)
    }

    fn sender() -> (NodeIdentity, ed25519_dalek::SigningKey, String) {
        let id = NodeIdentity::generate();
        let key = id.signing_key().unwrap();
        let pub_hex = hex::encode(&id.public_key);
        (id, key, pub_hex)
    }

    fn sample_command() -> FederationCommand {
        FederationCommand::LeaseGrant {
            lease_id: "lease-1".into(),
            mission_id: "mission-1".into(),
            resource_id: "node-1".into(),
            epoch: 3,
        }
    }

    #[test]
    fn valid_envelope_authenticates() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key(&pub_hex);
        let env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        assert!(auth.authenticate(&env).is_ok());
    }

    #[test]
    fn unknown_coordinator_rejected() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key("deadbeef");
        let env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        assert_eq!(
            auth.authenticate(&env),
            Err(FederationAuthError::UnknownCoordinator(id.node_id))
        );
    }

    #[test]
    fn tampered_payload_rejected() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key(&pub_hex);
        let mut env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        // Tamper after signing: raise the epoch.
        env.command = FederationCommand::LeaseGrant {
            lease_id: "lease-1".into(),
            mission_id: "mission-1".into(),
            resource_id: "node-1".into(),
            epoch: 99,
        };
        assert_eq!(
            auth.authenticate(&env),
            Err(FederationAuthError::InvalidSignature)
        );
    }

    #[test]
    fn replayed_envelope_rejected() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key(&pub_hex);
        let env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        assert!(auth.authenticate(&env).is_ok());
        assert_eq!(
            auth.authenticate(&env),
            Err(FederationAuthError::ReplayedNonce(env.nonce.clone()))
        );
    }

    #[test]
    fn stale_timestamp_rejected() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key(&pub_hex);
        let mut env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        env.timestamp_secs = env.timestamp_secs.saturating_sub(1000);
        env.sign(&key); // re-sign so only freshness fails
        match auth.authenticate(&env) {
            Err(FederationAuthError::StaleTimestamp { .. }) => {}
            other => panic!("expected StaleTimestamp, got {other:?}"),
        }
    }

    #[test]
    fn revoked_coordinator_rejected() {
        let (id, key, pub_hex) = sender();
        let nonces = Arc::new(NonceTracker::new());
        let revocations = Arc::new(RevocationStore::open("sqlite::memory:").unwrap());
        revocations.revoke(&id.node_id, "test revocation").unwrap();
        let auth = FederationAuthenticator::new(vec![pub_hex.clone()], nonces, revocations);
        let env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        assert_eq!(
            auth.authenticate(&env),
            Err(FederationAuthError::RevokedCoordinator(id.node_id))
        );
    }

    #[test]
    fn unsupported_protocol_rejected() {
        let (id, key, pub_hex) = sender();
        let (auth, _) = auth_with_key(&pub_hex);
        let mut env =
            SignedFederationCommand::build_signed(&id.node_id, &pub_hex, &key, sample_command());
        env.protocol_version = 999;
        env.sign(&key);
        assert_eq!(
            auth.authenticate(&env),
            Err(FederationAuthError::UnsupportedProtocol(999))
        );
    }

    #[test]
    fn command_accessors_and_idempotency_keys() {
        let cmd = sample_command();
        assert_eq!(cmd.epoch(), 3);
        assert_eq!(cmd.resource_id(), "node-1");
        assert_eq!(cmd.idempotency_key(), "lease-grant:lease-1");
        assert_eq!(cmd.kind_str(), "LEASE_GRANT");

        let resume = FederationCommand::CheckpointResume {
            job_id: "j1".into(),
            resource_id: "n1".into(),
            epoch: 2,
        };
        assert_eq!(resume.idempotency_key(), "checkpoint-resume:j1");
        let release = FederationCommand::LeaseRelease {
            lease_id: "lease-1".into(),
            mission_id: "m".into(),
            resource_id: "n1".into(),
            epoch: 3,
        };
        assert_ne!(
            release.idempotency_key(),
            cmd.idempotency_key(),
            "grant and release of the same lease must not collide"
        );
    }
}
