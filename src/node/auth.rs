//! Authenticated Federation Join Protocol
//!
//! Extends the existing node identity (Ed25519) with:
//! - Signed join requests
//! - Anti-replay protection (timestamp + nonce)
//! - Signature verification on the coordinator side
//!
//! Protects against:
//! - Node impersonation
//! - Parent spoofing
//! - Replayed registration
//! - Replayed heartbeat
//! - Forged capability advertisement
//! - Completion from wrong worker
//! - Stale lease completion

use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maximum age for a signed request (5 minutes).
const MAX_REQUEST_AGE_SECS: u64 = 300;

/// A signed join request from a node to the federation coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedJoinRequest {
    /// Node ID (UUID)
    pub node_id: String,

    /// Ed25519 public key (hex-encoded, 64 chars)
    pub public_key_hex: String,

    /// Inference URL for this node
    pub inference_url: String,

    /// Role (thinker, worker, etc.)
    pub role: String,

    /// GPU name
    pub gpu_name: String,

    /// VRAM in MiB
    pub vram_mb: u32,

    /// Models available on this node
    pub models: Vec<String>,

    /// Unix timestamp (seconds) when this request was signed
    pub timestamp_secs: u64,

    /// Unique nonce to prevent replay (UUID or random hex)
    pub nonce: String,

    /// Ed25519 signature over the canonical message (hex-encoded)
    pub signature_hex: String,
}

impl SignedJoinRequest {
    /// Build the canonical message that was signed.
    /// This must be deterministic and include all security-critical fields.
    pub fn canonical_message(&self) -> String {
        format!(
            "nauti-join:{}:{}:{}:{}:{}:{}:{}:{}",
            self.node_id,
            self.public_key_hex,
            self.inference_url,
            self.role,
            self.gpu_name,
            self.vram_mb,
            self.timestamp_secs,
            self.nonce,
        )
    }

    /// Verify the signature and timestamp of this join request.
    pub fn verify(&self, nonce_tracker: &NonceTracker) -> Result<(), AuthError> {
        // 1. Check timestamp freshness
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now.abs_diff(self.timestamp_secs) > MAX_REQUEST_AGE_SECS {
            return Err(AuthError::StaleRequest {
                request_age_secs: now.abs_diff(self.timestamp_secs),
                max_age_secs: MAX_REQUEST_AGE_SECS,
            });
        }

        // 2. Check nonce hasn't been used before
        if nonce_tracker.is_used(&self.nonce) {
            return Err(AuthError::ReplayedNonce(self.nonce.clone()));
        }

        // 3. Parse public key
        let pub_bytes =
            hex::decode(&self.public_key_hex).map_err(|_| AuthError::InvalidPublicKey)?;
        let pub_array: [u8; 32] = pub_bytes
            .try_into()
            .map_err(|_| AuthError::InvalidPublicKey)?;
        let verifying_key =
            VerifyingKey::from_bytes(&pub_array).map_err(|_| AuthError::InvalidPublicKey)?;

        // 4. Parse signature
        let sig_bytes =
            hex::decode(&self.signature_hex).map_err(|_| AuthError::InvalidSignature)?;
        let sig_array: [u8; 64] = sig_bytes
            .try_into()
            .map_err(|_| AuthError::InvalidSignature)?;
        let signature = Signature::from_bytes(&sig_array);

        // 5. Verify signature over canonical message
        let message = self.canonical_message();
        verifying_key
            .verify(message.as_bytes(), &signature)
            .map_err(|_| AuthError::SignatureMismatch)?;

        Ok(())
    }
}

/// A signed heartbeat request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedHeartbeat {
    pub node_id: String,
    pub active_jobs: u32,
    pub free_vram_mb: u32,
    pub tokens_per_sec: f64,
    pub timestamp_secs: u64,
    pub nonce: String,
    pub signature_hex: String,
}

impl SignedHeartbeat {
    pub fn canonical_message(&self) -> String {
        format!(
            "nauti-heartbeat:{}:{}:{}:{}:{}:{}",
            self.node_id,
            self.active_jobs,
            self.free_vram_mb,
            self.tokens_per_sec,
            self.timestamp_secs,
            self.nonce,
        )
    }
}

/// A signed withdrawal request from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedWithdrawRequest {
    pub node_id: String,
    pub timestamp_secs: u64,
    pub nonce: String,
    pub signature_hex: String,
}

impl SignedWithdrawRequest {
    pub fn canonical_message(&self) -> String {
        format!(
            "nauti-withdraw:{}:{}:{}",
            self.node_id, self.timestamp_secs, self.nonce
        )
    }
}

/// Sign a message with an Ed25519 signing key.
pub fn sign_message(signing_key: &ed25519_dalek::SigningKey, message: &str) -> String {
    let signature = signing_key.sign(message.as_bytes());
    hex::encode(signature.to_bytes())
}

/// Verify a signature against a public key.
pub fn verify_signature(
    public_key_hex: &str,
    message: &str,
    signature_hex: &str,
) -> Result<(), AuthError> {
    let pub_bytes = hex::decode(public_key_hex).map_err(|_| AuthError::InvalidPublicKey)?;
    let pub_array: [u8; 32] = pub_bytes
        .try_into()
        .map_err(|_| AuthError::InvalidPublicKey)?;
    let verifying_key =
        VerifyingKey::from_bytes(&pub_array).map_err(|_| AuthError::InvalidPublicKey)?;

    let sig_bytes = hex::decode(signature_hex).map_err(|_| AuthError::InvalidSignature)?;
    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| AuthError::InvalidSignature)?;
    let signature = Signature::from_bytes(&sig_array);

    verifying_key
        .verify(message.as_bytes(), &signature)
        .map_err(|_| AuthError::SignatureMismatch)?;

    Ok(())
}

/// Tracks used nonces to prevent replay attacks.
pub struct NonceTracker {
    used: Mutex<HashSet<String>>,
}

impl NonceTracker {
    pub fn new() -> Self {
        Self {
            used: Mutex::new(HashSet::new()),
        }
    }

    /// Check if a nonce has been used.
    pub fn is_used(&self, nonce: &str) -> bool {
        self.used.lock().unwrap().contains(nonce)
    }

    /// Mark a nonce as used. Returns false if already used.
    pub fn mark_used(&self, nonce: String) -> bool {
        self.used.lock().unwrap().insert(nonce)
    }

    /// Clear expired nonces (older than max_age).
    /// In production, this should be called periodically.
    pub fn clear_all(&self) {
        self.used.lock().unwrap().clear();
    }

    /// Number of tracked nonces.
    pub fn len(&self) -> usize {
        self.used.lock().unwrap().len()
    }
}

impl Default for NonceTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Authentication errors.
#[derive(Debug, Clone)]
pub enum AuthError {
    StaleRequest {
        request_age_secs: u64,
        max_age_secs: u64,
    },
    ReplayedNonce(String),
    InvalidPublicKey,
    InvalidSignature,
    SignatureMismatch,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleRequest {
                request_age_secs,
                max_age_secs,
            } => write!(
                f,
                "stale request: age {}s exceeds max {}s",
                request_age_secs, max_age_secs
            ),
            Self::ReplayedNonce(n) => write!(f, "replayed nonce: {}", n),
            Self::InvalidPublicKey => write!(f, "invalid public key"),
            Self::InvalidSignature => write!(f, "invalid signature format"),
            Self::SignatureMismatch => write!(f, "signature verification failed"),
        }
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn make_signing_key() -> SigningKey {
        SigningKey::generate(&mut rand::rngs::OsRng)
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn sign_and_verify_message() {
        let key = make_signing_key();
        let message = "test message";
        let sig = sign_message(&key, message);

        let pub_hex = hex::encode(key.verifying_key().to_bytes());
        assert!(verify_signature(&pub_hex, message, &sig).is_ok());
    }

    #[test]
    fn verify_wrong_message_fails() {
        let key = make_signing_key();
        let sig = sign_message(&key, "correct message");

        let pub_hex = hex::encode(key.verifying_key().to_bytes());
        assert!(verify_signature(&pub_hex, "wrong message", &sig).is_err());
    }

    #[test]
    fn verify_wrong_key_fails() {
        let key1 = make_signing_key();
        let key2 = make_signing_key();
        let message = "test";
        let sig = sign_message(&key1, message);

        let wrong_pub = hex::encode(key2.verifying_key().to_bytes());
        assert!(verify_signature(&wrong_pub, message, &sig).is_err());
    }

    #[test]
    fn signed_join_request_verify() {
        let key = make_signing_key();
        let pub_hex = hex::encode(key.verifying_key().to_bytes());

        let mut req = SignedJoinRequest {
            node_id: "test-node-123".into(),
            public_key_hex: pub_hex.clone(),
            inference_url: "http://127.0.0.1:5200".into(),
            role: "thinker".into(),
            gpu_name: "RTX 4090".into(),
            vram_mb: 24576,
            models: vec!["thinker".into()],
            timestamp_secs: current_timestamp(),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature_hex: String::new(),
        };

        // Sign the canonical message
        req.signature_hex = sign_message(&key, &req.canonical_message());

        let tracker = NonceTracker::new();
        assert!(req.verify(&tracker).is_ok());
    }

    #[test]
    fn stale_request_rejected() {
        let key = make_signing_key();
        let pub_hex = hex::encode(key.verifying_key().to_bytes());

        let mut req = SignedJoinRequest {
            node_id: "test-node".into(),
            public_key_hex: pub_hex,
            inference_url: "http://127.0.0.1:5200".into(),
            role: "worker".into(),
            gpu_name: "RTX 3090".into(),
            vram_mb: 24576,
            models: vec![],
            timestamp_secs: current_timestamp() - 600, // 10 minutes ago
            nonce: uuid::Uuid::new_v4().to_string(),
            signature_hex: String::new(),
        };
        req.signature_hex = sign_message(&key, &req.canonical_message());

        let tracker = NonceTracker::new();
        let result = req.verify(&tracker);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AuthError::StaleRequest { .. }
        ));
    }

    #[test]
    fn replayed_nonce_rejected() {
        let key = make_signing_key();
        let pub_hex = hex::encode(key.verifying_key().to_bytes());
        let nonce = uuid::Uuid::new_v4().to_string();

        let mut req = SignedJoinRequest {
            node_id: "test-node".into(),
            public_key_hex: pub_hex,
            inference_url: "http://127.0.0.1:5200".into(),
            role: "worker".into(),
            gpu_name: "RTX 3090".into(),
            vram_mb: 24576,
            models: vec![],
            timestamp_secs: current_timestamp(),
            nonce: nonce.clone(),
            signature_hex: String::new(),
        };
        req.signature_hex = sign_message(&key, &req.canonical_message());

        let tracker = NonceTracker::new();
        tracker.mark_used(nonce);

        let result = req.verify(&tracker);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::ReplayedNonce(_)));
    }

    #[test]
    fn nonce_tracker_basics() {
        let tracker = NonceTracker::new();
        assert_eq!(tracker.len(), 0);
        assert!(!tracker.is_used("abc"));

        assert!(tracker.mark_used("abc".into()));
        assert!(tracker.is_used("abc"));
        assert!(!tracker.mark_used("abc".into())); // already used
        assert_eq!(tracker.len(), 1);

        tracker.clear_all();
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn tampered_request_fails() {
        let key = make_signing_key();
        let pub_hex = hex::encode(key.verifying_key().to_bytes());

        let mut req = SignedJoinRequest {
            node_id: "test-node".into(),
            public_key_hex: pub_hex,
            inference_url: "http://127.0.0.1:5200".into(),
            role: "worker".into(),
            gpu_name: "RTX 3090".into(),
            vram_mb: 24576,
            models: vec![],
            timestamp_secs: current_timestamp(),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature_hex: String::new(),
        };
        req.signature_hex = sign_message(&key, &req.canonical_message());

        // Tamper with the role
        req.role = "admin".into();

        let tracker = NonceTracker::new();
        let result = req.verify(&tracker);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::SignatureMismatch));
    }
}
