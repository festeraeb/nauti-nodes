//! Durable node identity: UUID node ID + ed25519 keypair.
//!
//! Each Nauti Node installation gets a unique, persistent identity that
//! survives restarts. The node_id is a UUID (not a hostname). The ed25519
//! keypair enables future cryptographic authentication without replacing
//! the node ID scheme.

use crate::types::now_ms;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;

/// Durable identity for a Nauti Node installation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    /// UUID-based node ID. Stable across restarts.
    pub node_id: String,
    /// Ed25519 public key (32 bytes).
    pub public_key: Vec<u8>,
    /// Ed25519 secret key (32 bytes). Persisted locally; never transmitted.
    pub secret_key: Vec<u8>,
    /// When this identity was first created (millis since epoch).
    pub created_at_ms: u64,
    /// Software version that created this identity.
    pub software_version: String,
}

impl NodeIdentity {
    /// Generate a fresh identity.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let public_key = signing_key.verifying_key().to_bytes().to_vec();
        let secret_key = signing_key.to_bytes().to_vec();
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            public_key,
            secret_key,
            created_at_ms: now_ms(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Load from a JSON file, or generate and persist a new one.
    pub fn load_or_create(path: &Path) -> crate::types::Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path)
                .map_err(|e| crate::types::Error::Internal(format!("read identity: {e}")))?;
            let identity: Self = serde_json::from_str(&data)
                .map_err(|e| crate::types::Error::Internal(format!("parse identity: {e}")))?;
            info!(node_id = %identity.node_id, "loaded existing node identity");
            Ok(identity)
        } else {
            let identity = Self::generate();
            let data = serde_json::to_string_pretty(&identity)
                .map_err(|e| crate::types::Error::Internal(format!("serialize identity: {e}")))?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::types::Error::Internal(format!("create identity dir: {e}"))
                })?;
            }
            std::fs::write(path, &data)
                .map_err(|e| crate::types::Error::Internal(format!("write identity: {e}")))?;
            info!(node_id = %identity.node_id, path = %path.display(), "generated new node identity");
            Ok(identity)
        }
    }

    /// Get the ed25519 SigningKey from the stored secret bytes.
    pub fn signing_key(&self) -> crate::types::Result<SigningKey> {
        let bytes: [u8; 32] = self
            .secret_key
            .clone()
            .try_into()
            .map_err(|_| crate::types::Error::Internal("invalid secret key length".into()))?;
        Ok(SigningKey::from_bytes(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn generate_has_valid_uuid() {
        let id = NodeIdentity::generate();
        uuid::Uuid::parse_str(&id.node_id).expect("node_id must be a valid UUID");
        assert_eq!(id.public_key.len(), 32);
        assert_eq!(id.secret_key.len(), 32);
    }

    #[test]
    fn load_or_create_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().with_extension("json");
        let _ = std::fs::remove_file(&path);

        let id1 = NodeIdentity::load_or_create(&path).unwrap();
        let id2 = NodeIdentity::load_or_create(&path).unwrap();
        assert_eq!(id1.node_id, id2.node_id);
        assert_eq!(id1.public_key, id2.public_key);
        assert_eq!(id1.created_at_ms, id2.created_at_ms);
    }

    #[test]
    fn signing_key_roundtrip() {
        let id = NodeIdentity::generate();
        let sk = id.signing_key().unwrap();
        let vk = sk.verifying_key();
        assert_eq!(vk.to_bytes().to_vec(), id.public_key);
    }
}
