use crate::types::errors::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMode {
    Coordinator,
    Worker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub mode: RuntimeMode,
    pub listen_port: u16,
    pub db_url: String,
    pub auth_keypair_path: String,
    pub identity_path: String,
    pub forge_url: String,
    pub coordinator_url: String,
    pub local_inference_url: String,
    pub worker_id: String,
    pub worker_role: String,
    pub worker_gpu: String,
    pub strict_alpha: bool,
    pub protocol_version: u32,
    pub external_alpha_enabled: bool,
    pub is_external_alpha: bool,
    pub admin_key: Option<String>,
    /// Chunk 8: path to the coordinator identity file (Ed25519 + UUID).
    /// If None, the coordinator identity is generated in-memory and not
    /// persisted (single-coordinator mode).
    pub coordinator_identity_path: Option<String>,
    /// Chunk 8: trusted coordinator public keys (hex-encoded ed25519).
    /// Used for federation authentication.
    pub trusted_coordinator_keys: Vec<String>,
    /// Chunk 8: federation peer URLs (coordinator base URLs).
    pub federation_peers: Vec<String>,
    /// Trust `X-Nexus-Peer-*` headers as the worker identity on join and refuse
    /// unattributed joins. Only safe on a listener reachable solely through the
    /// Lemonade Nexus service ingress. Env `NAUTI_NEXUS_INGRESS_ONLY=1`.
    pub nexus_ingress_only: bool,
    /// The Nexus permission a worker needs to join (`admin` always passes).
    /// Env `NAUTI_NEXUS_REQUIRED_PERMISSION`.
    pub nexus_required_permission: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::Coordinator,
            listen_port: 8099,
            db_url: "sqlite:nauti.db".to_string(),
            auth_keypair_path: "/var/lib/nauti-inferer/auth.key".to_string(),
            identity_path: "/var/lib/nauti-nodes/identity.json".to_string(),
            forge_url: "http://127.0.0.1:9100".to_string(),
            coordinator_url: "http://127.0.0.1:8099".to_string(),
            local_inference_url: "http://127.0.0.1:5200".to_string(),
            worker_id: "RTX2060".to_string(),
            worker_role: "thinker".to_string(),
            worker_gpu: "RTX 2060 SUPER".to_string(),
            strict_alpha: true,
            protocol_version: crate::node::auth::CURRENT_PROTOCOL_VERSION,
            external_alpha_enabled: true,
            is_external_alpha: true,
            admin_key: None,
            coordinator_identity_path: None,
            trusted_coordinator_keys: Vec::new(),
            federation_peers: Vec::new(),
            nexus_ingress_only: false,
            nexus_required_permission: crate::node::nexus_peer::DEFAULT_REQUIRED_PERMISSION.to_string(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mut c = Self::default();
        if let Ok(p) = std::env::var("NAUTI_LISTEN_PORT") {
            c.listen_port = p
                .parse()
                .map_err(|_| Error::Config("invalid NAUTI_LISTEN_PORT".into()))?;
        }
        if let Ok(u) = std::env::var("NAUTI_DB_URL") {
            c.db_url = u;
        }
        if let Ok(m) = std::env::var("NAUTI_MODE") {
            c.mode = match m.to_lowercase().as_str() {
                "worker" => RuntimeMode::Worker,
                _ => RuntimeMode::Coordinator,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_IDENTITY_PATH") {
            c.identity_path = v;
        }
        if let Ok(v) = std::env::var("NAUTI_AUTH_KEYPAIR_PATH") {
            c.auth_keypair_path = v;
        }
        if let Ok(v) = std::env::var("FORGE_URL") {
            c.forge_url = v;
        }
        if let Ok(v) = std::env::var("NAUTI_COORDINATOR_URL") {
            c.coordinator_url = v;
        }
        if let Ok(v) = std::env::var("LOCAL_INFERENCE_URL") {
            c.local_inference_url = v;
        }
        if let Ok(v) = std::env::var("NAUTI_NEXUS_INGRESS_ONLY") {
            c.nexus_ingress_only = matches!(v.trim(), "1" | "true" | "yes" | "on");
        }
        if let Ok(v) = std::env::var("NAUTI_NEXUS_REQUIRED_PERMISSION") {
            if !v.trim().is_empty() {
                c.nexus_required_permission = v.trim().to_string();
            }
        }
        if let Ok(v) = std::env::var("NAUTI_WORKER_ID") {
            c.worker_id = v;
        }
        if let Ok(v) = std::env::var("NAUTI_WORKER_ROLE") {
            c.worker_role = v;
        }
        if let Ok(v) = std::env::var("NAUTI_WORKER_GPU") {
            c.worker_gpu = v;
        }
        if let Ok(v) = std::env::var("NAUTI_STRICT_ALPHA") {
            c.strict_alpha = match v.to_lowercase().as_str() {
                "0" | "false" | "no" | "off" => false,
                _ => true,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_ALPHA_MODE") {
            c.strict_alpha = match v.to_lowercase().as_str() {
                "0" | "false" | "no" | "off" => false,
                _ => true,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_EXTERNAL_ALPHA_ENABLED") {
            c.external_alpha_enabled = match v.to_lowercase().as_str() {
                "0" | "false" | "no" | "off" => false,
                _ => true,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_KILL_SWITCH") {
            // NAUTI_KILL_SWITCH=1 disables external alpha
            c.external_alpha_enabled = match v.to_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => false,
                _ => true,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_IS_EXTERNAL_ALPHA") {
            c.is_external_alpha = match v.to_lowercase().as_str() {
                "0" | "false" | "no" | "off" => false,
                _ => true,
            };
        }
        if let Ok(v) = std::env::var("NAUTI_ADMIN_KEY") {
            c.admin_key = Some(v);
        }
        // Chunk 8: coordinator identity path.
        if let Ok(v) = std::env::var("NAUTI_COORDINATOR_IDENTITY_PATH") {
            c.coordinator_identity_path = Some(v);
        }
        // Chunk 8: trusted coordinator public keys (comma-separated hex).
        if let Ok(v) = std::env::var("NAUTI_TRUSTED_COORDINATOR_KEYS") {
            c.trusted_coordinator_keys = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        // Chunk 8: federation peer URLs (comma-separated).
        if let Ok(v) = std::env::var("NAUTI_FEDERATION_PEERS") {
            c.federation_peers = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        Ok(c)
    }

    /// Return a sanitized version of this config with private keys and credentials scrubbed.
    pub fn sanitize(&self) -> Self {
        let mut s = self.clone();
        s.auth_keypair_path = "[REDACTED]".to_string();
        if !s.db_url.starts_with("sqlite::memory:") {
            s.db_url = "[REDACTED_DB_URL]".to_string();
        }
        if s.admin_key.is_some() {
            s.admin_key = Some("[REDACTED_ADMIN_KEY]".to_string());
        }
        s
    }
}
