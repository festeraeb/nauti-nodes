//! Peer Discovery
//!
//! Enables nodes to discover and communicate with peers in the federation.
//! Supports:
//! - Peer advertisement (broadcasting capabilities)
//! - Peer discovery (finding eligible peers)
//! - Direct-peer-capable execution (bypassing coordinator for known peers)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A discovered peer in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Node ID (UUID)
    pub node_id: String,

    /// Ed25519 public key (hex-encoded)
    pub public_key_hex: String,

    /// Inference URL for this peer
    pub inference_url: String,

    /// Role (thinker, worker, etc.)
    pub role: String,

    /// GPU name
    pub gpu_name: String,

    /// VRAM in MiB
    pub vram_mb: u32,

    /// Models available on this peer
    pub models: Vec<String>,

    /// Network address (if known)
    pub address: Option<String>,

    /// Last time this peer was seen (millis since epoch)
    pub last_seen_ms: u64,

    /// Whether this peer supports direct execution
    pub direct_capable: bool,
}

impl PeerInfo {
    /// Check if this peer is still considered alive (seen within timeout).
    pub fn is_alive(&self, timeout_ms: u64, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_seen_ms) < timeout_ms
    }

    /// Check if this peer can handle a specific model.
    pub fn has_model(&self, model: &str) -> bool {
        self.models.iter().any(|m| m == model)
    }

    /// Check if this peer has enough VRAM for a model of given size.
    pub fn can_fit_model(&self, model_size_mb: u32) -> bool {
        self.vram_mb >= model_size_mb
    }
}

/// Peer discovery service.
///
/// Maintains a registry of known peers and their capabilities.
pub struct PeerRegistry {
    /// Known peers indexed by node_id
    peers: Mutex<HashMap<String, PeerInfo>>,

    /// Default timeout for peer liveness (milliseconds)
    peer_timeout_ms: u64,
}

impl PeerRegistry {
    pub fn new(peer_timeout_ms: u64) -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
            peer_timeout_ms,
        }
    }

    /// Register or update a peer.
    pub fn upsert_peer(&self, peer: PeerInfo) {
        self.peers
            .lock()
            .unwrap()
            .insert(peer.node_id.clone(), peer);
    }

    /// Remove a peer by node_id.
    pub fn remove_peer(&self, node_id: &str) -> Option<PeerInfo> {
        self.peers.lock().unwrap().remove(node_id)
    }

    /// Get a peer by node_id.
    pub fn get_peer(&self, node_id: &str) -> Option<PeerInfo> {
        self.peers.lock().unwrap().get(node_id).cloned()
    }

    /// List all known peers.
    pub fn list_peers(&self) -> Vec<PeerInfo> {
        self.peers.lock().unwrap().values().cloned().collect()
    }

    /// List alive peers.
    pub fn list_alive_peers(&self, now_ms: u64) -> Vec<PeerInfo> {
        self.peers
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_alive(self.peer_timeout_ms, now_ms))
            .cloned()
            .collect()
    }

    /// Find peers that can serve a specific model.
    pub fn find_peers_for_model(&self, model: &str, now_ms: u64) -> Vec<PeerInfo> {
        self.list_alive_peers(now_ms)
            .into_iter()
            .filter(|p| p.has_model(model))
            .collect()
    }

    /// Find peers with a specific role.
    pub fn find_peers_by_role(&self, role: &str, now_ms: u64) -> Vec<PeerInfo> {
        self.list_alive_peers(now_ms)
            .into_iter()
            .filter(|p| p.role == role)
            .collect()
    }

    /// Find direct-capable peers (can execute without coordinator).
    pub fn find_direct_peers(&self, now_ms: u64) -> Vec<PeerInfo> {
        self.list_alive_peers(now_ms)
            .into_iter()
            .filter(|p| p.direct_capable)
            .collect()
    }

    /// Number of known peers.
    pub fn peer_count(&self) -> usize {
        self.peers.lock().unwrap().len()
    }

    /// Number of alive peers.
    pub fn alive_peer_count(&self, now_ms: u64) -> usize {
        self.list_alive_peers(now_ms).len()
    }

    /// Evict stale peers (not seen within timeout).
    pub fn evict_stale(&self, now_ms: u64) -> usize {
        let mut peers = self.peers.lock().unwrap();
        let before = peers.len();
        peers.retain(|_, p| p.is_alive(self.peer_timeout_ms, now_ms));
        before - peers.len()
    }
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new(300_000) // 5 minutes default timeout
    }
}

/// A peer advertisement message (broadcast to the network).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAdvertisement {
    pub node_id: String,
    pub public_key_hex: String,
    pub inference_url: String,
    pub role: String,
    pub gpu_name: String,
    pub vram_mb: u32,
    pub models: Vec<String>,
    pub direct_capable: bool,
    pub timestamp_secs: u64,
}

/// A peer discovery request (query for peers matching criteria).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDiscoveryRequest {
    /// Required role (if any)
    pub role: Option<String>,
    /// Required model (if any)
    pub model: Option<String>,
    /// Minimum VRAM in MiB (if any)
    pub min_vram_mb: Option<u32>,
    /// Whether direct-capable peers are required
    pub direct_capable_only: bool,
}

impl PeerDiscoveryRequest {
    /// Check if a peer matches this discovery request.
    pub fn matches(&self, peer: &PeerInfo) -> bool {
        if let Some(ref role) = self.role {
            if peer.role != *role {
                return false;
            }
        }
        if let Some(ref model) = self.model {
            if !peer.has_model(model) {
                return false;
            }
        }
        if let Some(min_vram) = self.min_vram_mb {
            if peer.vram_mb < min_vram {
                return false;
            }
        }
        if self.direct_capable_only && !peer.direct_capable {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer(id: &str, role: &str, models: Vec<&str>) -> PeerInfo {
        PeerInfo {
            node_id: id.into(),
            public_key_hex: "aa".repeat(32),
            inference_url: format!("http://127.0.0.1:{}", 5200 + id.len()),
            role: role.into(),
            gpu_name: "RTX 4090".into(),
            vram_mb: 24576,
            models: models.into_iter().map(|s| s.to_string()).collect(),
            address: None,
            last_seen_ms: 1000,
            direct_capable: true,
        }
    }

    #[test]
    fn peer_is_alive() {
        let peer = make_peer("node-1", "thinker", vec!["thinker"]);
        assert!(peer.is_alive(300_000, 1000)); // just seen
        assert!(peer.is_alive(300_000, 100_000)); // within timeout
        assert!(!peer.is_alive(300_000, 400_000)); // too old
    }

    #[test]
    fn peer_has_model() {
        let peer = make_peer("node-1", "thinker", vec!["thinker", "coder"]);
        assert!(peer.has_model("thinker"));
        assert!(peer.has_model("coder"));
        assert!(!peer.has_model("vision"));
    }

    #[test]
    fn registry_upsert_and_get() {
        let registry = PeerRegistry::new(300_000);
        let peer = make_peer("node-1", "thinker", vec!["thinker"]);
        registry.upsert_peer(peer.clone());

        assert_eq!(registry.peer_count(), 1);
        let found = registry.get_peer("node-1").unwrap();
        assert_eq!(found.role, "thinker");
    }

    #[test]
    fn registry_remove_peer() {
        let registry = PeerRegistry::new(300_000);
        registry.upsert_peer(make_peer("node-1", "thinker", vec![]));
        assert_eq!(registry.peer_count(), 1);

        let removed = registry.remove_peer("node-1");
        assert!(removed.is_some());
        assert_eq!(registry.peer_count(), 0);
    }

    #[test]
    fn registry_find_by_model() {
        let registry = PeerRegistry::new(300_000);
        registry.upsert_peer(make_peer("node-1", "thinker", vec!["thinker"]));
        registry.upsert_peer(make_peer("node-2", "coder", vec!["coder"]));
        registry.upsert_peer(make_peer("node-3", "thinker", vec!["thinker", "coder"]));

        let thinkers = registry.find_peers_for_model("thinker", 1000);
        assert_eq!(thinkers.len(), 2);

        let coders = registry.find_peers_for_model("coder", 1000);
        assert_eq!(coders.len(), 2);
    }

    #[test]
    fn registry_find_by_role() {
        let registry = PeerRegistry::new(300_000);
        registry.upsert_peer(make_peer("node-1", "thinker", vec![]));
        registry.upsert_peer(make_peer("node-2", "worker", vec![]));
        registry.upsert_peer(make_peer("node-3", "thinker", vec![]));

        let thinkers = registry.find_peers_by_role("thinker", 1000);
        assert_eq!(thinkers.len(), 2);
    }

    #[test]
    fn registry_evict_stale() {
        let registry = PeerRegistry::new(1000); // 1 second timeout
        registry.upsert_peer(make_peer("node-1", "thinker", vec![]));
        registry.upsert_peer(make_peer("node-2", "worker", vec![]));

        // Both alive at t=1000
        assert_eq!(registry.alive_peer_count(1000), 2);

        // Both stale at t=3000
        let evicted = registry.evict_stale(3000);
        assert_eq!(evicted, 2);
        assert_eq!(registry.peer_count(), 0);
    }

    #[test]
    fn discovery_request_matches() {
        let peer = make_peer("node-1", "thinker", vec!["thinker"]);

        let req1 = PeerDiscoveryRequest {
            role: Some("thinker".into()),
            model: None,
            min_vram_mb: None,
            direct_capable_only: false,
        };
        assert!(req1.matches(&peer));

        let req2 = PeerDiscoveryRequest {
            role: Some("worker".into()),
            model: None,
            min_vram_mb: None,
            direct_capable_only: false,
        };
        assert!(!req2.matches(&peer));

        let req3 = PeerDiscoveryRequest {
            role: None,
            model: Some("thinker".into()),
            min_vram_mb: None,
            direct_capable_only: false,
        };
        assert!(req3.matches(&peer));

        let req4 = PeerDiscoveryRequest {
            role: None,
            model: None,
            min_vram_mb: Some(30000),
            direct_capable_only: false,
        };
        assert!(!req4.matches(&peer)); // 24576 < 30000
    }

    #[test]
    fn discovery_request_direct_only() {
        let mut peer = make_peer("node-1", "thinker", vec![]);
        peer.direct_capable = false;

        let req = PeerDiscoveryRequest {
            role: None,
            model: None,
            min_vram_mb: None,
            direct_capable_only: true,
        };
        assert!(!req.matches(&peer));
    }
}