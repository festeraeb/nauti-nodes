//! Chunk 10: pluggable execution provider abstraction.
//!
//! Wraps the existing local worker execution path and the Chunk 9 federated
//! delegation path behind a common `ExecutionProvider` trait. Does NOT
//! duplicate scheduling, eligibility, or accounting logic — those remain in
//! their canonical locations.
//!
//! Provider selection is a PRE-ELIGIBILITY filter: it decides which backend
//! to try. The provider's internal path (local scheduler or federation
//! delegation) remains authoritative for actual assignment.

pub mod federated;
pub use federated::FederatedWorkerProvider;
pub use local::LocalWorkerProvider;
pub mod local;

use crate::api::handlers::InferenceRequest;
pub use crate::capabilities::PrivacyClass;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Backend kind for an execution provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderKind {
    /// Local worker pool managed by this coordinator's scheduler.
    LocalWorker,
    /// Federated delegation via Chunk 9 transport.
    FederatedWorker,
    /// Datacenter pool (future; not implemented).
    DatacenterPool,
    /// Remote inference API (future; not implemented).
    RemoteInferenceApi,
}

/// Capabilities reported by a provider. Derived from real state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub provider_id: String,
    pub kind: ProviderKind,
    pub models: Vec<String>,
    pub gpu_vendors: Vec<String>,
    pub total_vram_mb: Option<u64>,
    pub privacy_classes: Vec<PrivacyClass>,
    pub checkpointable: bool,
    pub queue_depth: usize,
    pub expected_latency_ms: Option<u64>,
    pub cost_units_per_1k_tokens: Option<f64>,
}

/// Health status of a provider. Distinct from availability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider_id: String,
    /// Provider is responsive and functioning.
    pub healthy: bool,
    /// Provider has eligible resources available for new work.
    pub available: bool,
    pub last_checked_ms: i64,
    pub reason: Option<String>,
}

/// A request to execute work on a provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub job_id: Uuid,
    pub model_id: String,
    pub privacy: PrivacyClass,
    pub minimum_vram_mb: u64,
    pub gpu_vendor: Option<String>,
    pub checkpointable: bool,
    /// The actual inference payload (typed, not JSON blob).
    pub request: InferenceRequest,
}

/// Handle to a submitted job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderJob {
    pub job_id: Uuid,
    pub provider_id: String,
    pub state: ProviderJobState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderJobState {
    Accepted,
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for ProviderJobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => write!(f, "ACCEPTED"),
            Self::Queued => write!(f, "QUEUED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Authoritative status of a provider job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderJobStatus {
    pub job_id: Uuid,
    pub provider_id: String,
    pub state: ProviderJobState,
    pub detail: Option<String>,
}

/// Common execution provider trait.
#[async_trait::async_trait]
pub trait ExecutionProvider: Send + Sync {
    fn id(&self) -> &str;
    fn kind(&self) -> ProviderKind;
    async fn capabilities(&self) -> anyhow::Result<ProviderCapabilities>;
    async fn health(&self) -> anyhow::Result<ProviderHealth>;
    async fn submit(&self, request: ProviderRequest) -> anyhow::Result<ProviderJob>;
    async fn status(&self, job_id: Uuid) -> anyhow::Result<ProviderJobStatus>;
    async fn cancel(&self, job_id: Uuid) -> anyhow::Result<()>;
}

/// Registry of execution providers.
pub struct ProviderRegistry {
    providers: HashMap<String, Arc<dyn ExecutionProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    pub fn register(&mut self, provider: Arc<dyn ExecutionProvider>) {
        self.providers.insert(provider.id().to_string(), provider);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn ExecutionProvider>> {
        self.providers.get(id).cloned()
    }

    pub fn all(&self) -> impl Iterator<Item = &Arc<dyn ExecutionProvider>> {
        self.providers.values()
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Policy controlling which providers may be selected.
#[derive(Debug, Clone)]
pub struct ProviderPolicy {
    pub prefer_local: bool,
    pub allow_federated: bool,
    pub allow_datacenter: bool,
    pub allow_remote: bool,
    pub maximum_cost_units: Option<f64>,
}

impl Default for ProviderPolicy {
    fn default() -> Self {
        Self {
            prefer_local: true,
            allow_federated: true,
            allow_datacenter: false,
            allow_remote: false,
            maximum_cost_units: None,
        }
    }
}

/// Whether a provider is allowed to serve a request under a policy.
pub fn provider_is_allowed(
    capability: &ProviderCapabilities,
    request: &ProviderRequest,
    policy: &ProviderPolicy,
) -> bool {
    // Privacy class must be supported.
    if !capability.privacy_classes.contains(&request.privacy) {
        return false;
    }

    // Model must be supported.
    if !capability.models.contains(&request.model_id) {
        return false;
    }

    // GPU vendor constraint.
    if let Some(required_vendor) = &request.gpu_vendor {
        if !capability
            .gpu_vendors
            .iter()
            .any(|v| v.eq_ignore_ascii_case(required_vendor))
        {
            return false;
        }
    }

    // VRAM constraint.
    if let Some(vram) = capability.total_vram_mb {
        if vram < request.minimum_vram_mb {
            return false;
        }
    }

    // Kind-based policy gates.
    match capability.kind {
        ProviderKind::LocalWorker => true,
        ProviderKind::FederatedWorker => policy.allow_federated,
        ProviderKind::DatacenterPool => policy.allow_datacenter,
        ProviderKind::RemoteInferenceApi => policy.allow_remote,
    }
}

/// Select the best provider for a request under a policy.
///
/// Returns `None` if no provider is eligible. Selection is by:
/// 1. Kind preference (local first if prefer_local)
/// 2. Queue depth (lower is better)
/// 3. Expected latency (lower is better)
pub async fn select_provider(
    registry: &ProviderRegistry,
    request: &ProviderRequest,
    policy: &ProviderPolicy,
) -> anyhow::Result<Option<Arc<dyn ExecutionProvider>>> {
    let mut candidates = Vec::new();

    for provider in registry.all() {
        let health = provider.health().await?;
        if !health.healthy || !health.available {
            continue;
        }

        let caps = provider.capabilities().await?;
        if !provider_is_allowed(&caps, request, policy) {
            continue;
        }

        let local_rank = match caps.kind {
            ProviderKind::LocalWorker if policy.prefer_local => 0,
            ProviderKind::FederatedWorker => 1,
            ProviderKind::DatacenterPool => 2,
            ProviderKind::RemoteInferenceApi => 3,
            ProviderKind::LocalWorker => 1,
        };

        candidates.push((
            local_rank,
            caps.queue_depth,
            caps.expected_latency_ms.unwrap_or(u64::MAX),
            provider.clone(),
        ));
    }

    candidates.sort_by_key(|x| (x.0, x.1, x.2));

    Ok(candidates.into_iter().next().map(|x| x.3))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_caps(kind: ProviderKind, models: Vec<&str>, vram: Option<u64>) -> ProviderCapabilities {
        ProviderCapabilities {
            provider_id: "test".into(),
            kind,
            models: models.into_iter().map(|s| s.to_string()).collect(),
            gpu_vendors: vec!["NVIDIA".into()],
            total_vram_mb: vram,
            privacy_classes: vec![PrivacyClass::Public],
            checkpointable: true,
            queue_depth: 0,
            expected_latency_ms: None,
            cost_units_per_1k_tokens: None,
        }
    }

    fn test_request(model: &str, privacy: PrivacyClass) -> ProviderRequest {
        ProviderRequest {
            job_id: Uuid::new_v4(),
            model_id: model.to_string(),
            privacy,
            minimum_vram_mb: 8192,
            gpu_vendor: None,
            checkpointable: true,
            request: InferenceRequest {
                model: Some(model.to_string()),
                messages: vec![],
                stream: false,
                prefer_role: None,
                avoid_p100: true,
                max_tokens: None,
                temperature: None,
                privacy_class: None,
            },
        }
    }

    #[test]
    fn provider_kind_serde() {
        let kind = ProviderKind::LocalWorker;
        let s = serde_json::to_string(&kind).unwrap();
        let back: ProviderKind = serde_json::from_str(&s).unwrap();
        assert_eq!(back, ProviderKind::LocalWorker);
    }

    #[test]
    fn policy_privacy_mismatch_rejected() {
        let caps = test_caps(ProviderKind::LocalWorker, vec!["thinker"], Some(16384));
        let req = test_request("thinker", PrivacyClass::TrustedLineage);
        let policy = ProviderPolicy::default();
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn policy_model_mismatch_rejected() {
        let caps = test_caps(ProviderKind::LocalWorker, vec!["thinker"], Some(16384));
        let req = test_request("coder", PrivacyClass::Public);
        let policy = ProviderPolicy::default();
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn policy_vram_mismatch_rejected() {
        let caps = test_caps(ProviderKind::LocalWorker, vec!["thinker"], Some(4096));
        let req = test_request("thinker", PrivacyClass::Public);
        let policy = ProviderPolicy::default();
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn policy_gpu_vendor_mismatch_rejected() {
        let mut caps = test_caps(ProviderKind::LocalWorker, vec!["thinker"], Some(16384));
        caps.gpu_vendors = vec!["AMD".into()];
        let mut req = test_request("thinker", PrivacyClass::Public);
        req.gpu_vendor = Some("NVIDIA".into());
        let policy = ProviderPolicy::default();
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn policy_federated_disallowed() {
        let caps = test_caps(ProviderKind::FederatedWorker, vec!["thinker"], Some(16384));
        let req = test_request("thinker", PrivacyClass::Public);
        let mut policy = ProviderPolicy::default();
        policy.allow_federated = false;
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn policy_remote_disallowed() {
        let caps = test_caps(
            ProviderKind::RemoteInferenceApi,
            vec!["thinker"],
            Some(16384),
        );
        let req = test_request("thinker", PrivacyClass::Public);
        let policy = ProviderPolicy::default();
        assert!(!provider_is_allowed(&caps, &req, &policy));
    }

    #[test]
    fn registry_register_and_get() {
        let registry = ProviderRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }
}
