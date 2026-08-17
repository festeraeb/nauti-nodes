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
        if let Ok(v) = std::env::var("FORGE_URL") {
            c.forge_url = v;
        }
        if let Ok(v) = std::env::var("NAUTI_COORDINATOR_URL") {
            c.coordinator_url = v;
        }
        if let Ok(v) = std::env::var("LOCAL_INFERENCE_URL") {
            c.local_inference_url = v;
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
        Ok(c)
    }

    /// Return a sanitized version of this config with private keys and credentials scrubbed.
    pub fn sanitize(&self) -> Self {
        let mut s = self.clone();
        s.auth_keypair_path = "[REDACTED]".to_string();
        if !s.db_url.starts_with("sqlite::memory:") {
            s.db_url = "[REDACTED_DB_URL]".to_string();
        }
        s
    }
}
