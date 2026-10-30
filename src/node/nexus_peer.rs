//! Nexus-attributed peer identity.
//!
//! When a nauti-nodes coordinator is reached through a Lemonade Nexus service
//! ingress, the Nexus SDK strips any client-supplied `X-Nexus-*` header and
//! sets these from the authenticated mesh peer list:
//!
//! | header | value |
//! |---|---|
//! | `X-Nexus-Peer-Node` | the peer's node id |
//! | `X-Nexus-Peer-Key` | the peer's management key, else its mesh public key |
//! | `X-Nexus-Peer-Permissions` | the peer's permissions, comma-separated |
//! | `X-Nexus-Family` | the matched catalog family |
//!
//! (lemonade-nexus `docs/SDK-Guide.md`, "Service ingress".)
//!
//! Those headers are trustworthy ONLY on a listener that nothing but the Nexus
//! ingress can reach (a loopback bind behind the SDK, or the mesh tunnel
//! address). On any other listener a client can forge them. That is why the
//! gate is off by default and the operator turns it on per deployment with
//! `NAUTI_NEXUS_INGRESS_ONLY=1`, after binding the coordinator accordingly.
//!
//! With the gate on, a worker's identity is the Nexus node id the mesh
//! attributed to the connection, not a body field. This replaces the local
//! keypair handshake of Federation V1 issues 02 (identity) and 04 (join) for
//! federated workers; LAN-only nodes keep the local path.

use axum::http::HeaderMap;
use std::fmt;

pub const HEADER_PEER_NODE: &str = "x-nexus-peer-node";
pub const HEADER_PEER_KEY: &str = "x-nexus-peer-key";
pub const HEADER_PEER_PERMISSIONS: &str = "x-nexus-peer-permissions";
pub const HEADER_FAMILY: &str = "x-nexus-family";

/// The permission a worker needs on the coordinator's device (or an ancestor
/// group) to join. `admin` always passes, as the Nexus ingress itself allows.
pub const DEFAULT_REQUIRED_PERMISSION: &str = "svc:nauti-worker";

const MAX_HEADER_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NexusPeer {
    pub node_id: String,
    pub key: String,
    pub permissions: Vec<String>,
    pub family: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NexusReject {
    /// The gate is on and the request carries no Nexus attribution.
    NotAttributed,
    MalformedHeader(&'static str),
    PermissionDenied { required: String },
    /// The body names a node other than the one the mesh attributed.
    NodeMismatch { attributed: String, claimed: String },
}

impl fmt::Display for NexusReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NexusReject::NotAttributed => {
                write!(f, "request is not attributed to a Nexus peer (no X-Nexus-Peer-* headers)")
            }
            NexusReject::MalformedHeader(h) => write!(f, "malformed {h} header"),
            NexusReject::PermissionDenied { required } => {
                write!(f, "Nexus peer lacks the {required} permission")
            }
            NexusReject::NodeMismatch { attributed, claimed } => write!(
                f,
                "body node_id {claimed} is not the Nexus-attributed peer {attributed}"
            ),
        }
    }
}

impl std::error::Error for NexusReject {}

fn header_str<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<Option<&'a str>, NexusReject> {
    match headers.get(name) {
        None => Ok(None),
        Some(v) => {
            let s = v.to_str().map_err(|_| NexusReject::MalformedHeader(name))?.trim();
            if s.is_empty() || s.len() > MAX_HEADER_LEN {
                return Err(NexusReject::MalformedHeader(name));
            }
            Ok(Some(s))
        }
    }
}

fn valid_token(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '='))
}

impl NexusPeer {
    /// Reads the attribution headers. `Ok(None)` when none are present; an
    /// error when they are present but unusable.
    pub fn from_headers(headers: &HeaderMap) -> Result<Option<NexusPeer>, NexusReject> {
        let node = header_str(headers, HEADER_PEER_NODE)?;
        let key = header_str(headers, HEADER_PEER_KEY)?;
        let perms = header_str(headers, HEADER_PEER_PERMISSIONS)?;
        if node.is_none() && key.is_none() && perms.is_none() {
            return Ok(None);
        }
        let node_id = node.ok_or(NexusReject::MalformedHeader(HEADER_PEER_NODE))?;
        if !valid_token(node_id) {
            return Err(NexusReject::MalformedHeader(HEADER_PEER_NODE));
        }
        let key = key.ok_or(NexusReject::MalformedHeader(HEADER_PEER_KEY))?;
        if !valid_token(key) {
            return Err(NexusReject::MalformedHeader(HEADER_PEER_KEY));
        }
        let permissions: Vec<String> = perms
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        if permissions.iter().any(|p| !valid_token(p)) {
            return Err(NexusReject::MalformedHeader(HEADER_PEER_PERMISSIONS));
        }
        let family = header_str(headers, HEADER_FAMILY)?.map(str::to_string);
        Ok(Some(NexusPeer {
            node_id: node_id.to_string(),
            key: key.to_string(),
            permissions,
            family,
        }))
    }

    pub fn has_permission(&self, required: &str) -> bool {
        self.permissions.iter().any(|p| p == "admin" || p == required)
    }
}

/// The join gate. Disabled, it admits everything and attributes nothing (the
/// pre-Nexus behaviour). Enabled, every join must be Nexus-attributed, hold
/// the required permission, and name itself as the attributed peer.
#[derive(Debug, Clone)]
pub struct NexusGate {
    pub enabled: bool,
    pub required_permission: String,
}

impl NexusGate {
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            enabled: config.nexus_ingress_only,
            required_permission: config.nexus_required_permission.clone(),
        }
    }

    pub fn admit(
        &self,
        headers: &HeaderMap,
        claimed_node_id: &str,
    ) -> Result<Option<NexusPeer>, NexusReject> {
        let peer = NexusPeer::from_headers(headers)?;
        if !self.enabled {
            // Attribution is recorded when present, never required.
            return Ok(peer);
        }
        let peer = peer.ok_or(NexusReject::NotAttributed)?;
        if !peer.has_permission(&self.required_permission) {
            return Err(NexusReject::PermissionDenied {
                required: self.required_permission.clone(),
            });
        }
        if peer.node_id != claimed_node_id {
            return Err(NexusReject::NodeMismatch {
                attributed: peer.node_id,
                claimed: claimed_node_id.to_string(),
            });
        }
        Ok(Some(peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(node: &str, key: &str, perms: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(HEADER_PEER_NODE, HeaderValue::from_str(node).unwrap());
        h.insert(HEADER_PEER_KEY, HeaderValue::from_str(key).unwrap());
        h.insert(HEADER_PEER_PERMISSIONS, HeaderValue::from_str(perms).unwrap());
        h
    }

    fn gate(enabled: bool) -> NexusGate {
        NexusGate { enabled, required_permission: DEFAULT_REQUIRED_PERMISSION.into() }
    }

    #[test]
    fn no_headers_is_unattributed() {
        assert_eq!(NexusPeer::from_headers(&HeaderMap::new()).unwrap(), None);
    }

    #[test]
    fn full_headers_parse() {
        let p = NexusPeer::from_headers(&headers("n1", "k1", "svc:nauti-worker, connect_private"))
            .unwrap()
            .unwrap();
        assert_eq!(p.node_id, "n1");
        assert_eq!(p.permissions, vec!["svc:nauti-worker", "connect_private"]);
        assert!(p.has_permission("svc:nauti-worker"));
        assert!(!p.has_permission("svc:other"));
    }

    #[test]
    fn admin_covers_everything() {
        let p = NexusPeer::from_headers(&headers("n1", "k1", "admin")).unwrap().unwrap();
        assert!(p.has_permission("svc:anything"));
    }

    #[test]
    fn partial_headers_are_malformed() {
        let mut h = HeaderMap::new();
        h.insert(HEADER_PEER_KEY, HeaderValue::from_static("k"));
        assert_eq!(
            NexusPeer::from_headers(&h),
            Err(NexusReject::MalformedHeader(HEADER_PEER_NODE))
        );
    }

    #[test]
    fn bad_characters_are_malformed() {
        let h = headers("n 1", "k1", "admin");
        assert_eq!(
            NexusPeer::from_headers(&h),
            Err(NexusReject::MalformedHeader(HEADER_PEER_NODE))
        );
    }

    #[test]
    fn disabled_gate_admits_without_attribution() {
        assert_eq!(gate(false).admit(&HeaderMap::new(), "any").unwrap(), None);
    }

    #[test]
    fn enabled_gate_requires_attribution() {
        assert_eq!(gate(true).admit(&HeaderMap::new(), "n1"), Err(NexusReject::NotAttributed));
    }

    #[test]
    fn enabled_gate_requires_permission() {
        let h = headers("n1", "k1", "connect_private");
        assert_eq!(
            gate(true).admit(&h, "n1"),
            Err(NexusReject::PermissionDenied { required: DEFAULT_REQUIRED_PERMISSION.into() })
        );
    }

    #[test]
    fn enabled_gate_rejects_a_forged_body_node_id() {
        let h = headers("n1", "k1", "svc:nauti-worker");
        assert_eq!(
            gate(true).admit(&h, "n2"),
            Err(NexusReject::NodeMismatch { attributed: "n1".into(), claimed: "n2".into() })
        );
    }

    #[test]
    fn enabled_gate_admits_the_attributed_peer() {
        let h = headers("n1", "k1", "svc:nauti-worker");
        let p = gate(true).admit(&h, "n1").unwrap().unwrap();
        assert_eq!(p.key, "k1");
    }
}
