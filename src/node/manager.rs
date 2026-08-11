use crate::types::{Error, NodeCapabilities, NodeMetadata, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use tracing::info;

pub struct ConnectRequest {
    pub node_id: String,
    pub addr: String,
    pub capabilities: NodeCapabilities,
    pub public_key: Vec<u8>,
}

pub struct AuthChallenge {
    pub nonce: [u8; 32],
}

pub fn new_challenge() -> AuthChallenge {
    let mut nonce = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut nonce);
    AuthChallenge { nonce }
}

pub fn sign_challenge(nonce: &[u8], signing_key: &SigningKey) -> Vec<u8> {
    signing_key.sign(nonce).to_bytes().to_vec()
}

pub fn verify_challenge(nonce: &[u8], signature: &[u8], public_key: &[u8]) -> Result<()> {
    let vk = VerifyingKey::try_from(public_key).map_err(|e| Error::Auth(e.to_string()))?;
    let sig = Signature::try_from(signature).map_err(|e| Error::Auth(e.to_string()))?;
    vk.verify(nonce, &sig)
        .map_err(|e| Error::Auth(e.to_string()))
}

pub fn connect_worker(
    req: ConnectRequest,
    challenge: &AuthChallenge,
    signature: &[u8],
) -> Result<NodeMetadata> {
    verify_challenge(&challenge.nonce, signature, &req.public_key)?;
    info!(node_id = %req.node_id, addr = %req.addr, "worker authenticated");
    let role = req.capabilities.role.clone();
    let gpu_name = req.node_id.clone();
    Ok(NodeMetadata {
        id: req.node_id,
        addr: req.addr.clone(),
        inference_url: req.addr,
        role,
        gpu_name,
        online: true,
        capabilities: req.capabilities,
        public_key: req.public_key,
    })
}

pub fn disconnect_worker(node_id: &str) {
    info!(node_id = %node_id, "worker disconnect");
}
