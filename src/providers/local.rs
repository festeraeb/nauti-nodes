//! Local worker execution provider.
//!
//! Wraps the canonical local submission path: `fleet::pick_node_for_role` →
//! `confirm_assignment` → `Scheduler::schedule_job`. Does NOT duplicate
//! eligibility, accounting, or lease logic.

use super::{
    ExecutionProvider, ProviderCapabilities, ProviderHealth, ProviderJob, ProviderJobState,
    ProviderJobStatus, ProviderKind, ProviderRequest,
};
use crate::eligibility;
use crate::state::AppState;
use crate::types::now_ms;
use anyhow::Context;
use std::sync::Arc;
use uuid::Uuid;

/// Local worker provider wrapping the coordinator's own scheduler.
pub struct LocalWorkerProvider {
    id: String,
    state: Arc<AppState>,
}

impl LocalWorkerProvider {
    pub fn new(id: impl Into<String>, state: Arc<AppState>) -> Self {
        Self {
            id: id.into(),
            state,
        }
    }
}

#[async_trait::async_trait]
impl ExecutionProvider for LocalWorkerProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::LocalWorker
    }

    async fn capabilities(&self) -> anyhow::Result<ProviderCapabilities> {
        // Derive capabilities from the actual node registry.
        let nodes = self.state.registry.list_nodes();
        let mut models = Vec::new();
        let mut gpu_vendors = Vec::new();
        let mut total_vram_mb: u64 = 0;
        let mut queue_depth: usize = 0;

        for rt in &nodes {
            if let Ok(md) = rt.metadata.read() {
                for m in &md.capabilities.models {
                    if !models.contains(m) {
                        models.push(m.clone());
                    }
                }
                if let Some(obs) = &md.observed {
                    if !gpu_vendors.contains(&obs.vendor) {
                        gpu_vendors.push(obs.vendor.clone());
                    }
                }
                total_vram_mb += md.capabilities.vram_mb as u64;
            }
            queue_depth += rt.assigned_jobs_count() as usize;
        }

        Ok(ProviderCapabilities {
            provider_id: self.id.clone(),
            kind: ProviderKind::LocalWorker,
            models,
            gpu_vendors,
            total_vram_mb: if total_vram_mb > 0 {
                Some(total_vram_mb)
            } else {
                None
            },
            privacy_classes: vec![
                crate::capabilities::PrivacyClass::Public,
                crate::capabilities::PrivacyClass::FederationSafe,
            ],
            checkpointable: true,
            queue_depth,
            expected_latency_ms: None,
            cost_units_per_1k_tokens: None,
        })
    }

    async fn health(&self) -> anyhow::Result<ProviderHealth> {
        let nodes = self.state.registry.list_nodes();
        let now = now_ms();

        // Check if any node is eligible for work.
        let mut eligible_count = 0;
        let mut total_count = 0;
        let mut last_reason = None;

        for rt in &nodes {
            total_count += 1;
            match eligibility::evaluate_node(rt, now, crate::fleet::SCHEDULER_STALE_HEARTBEAT_MS) {
                Ok(_) => eligible_count += 1,
                Err(e) => last_reason = Some(e.as_str().to_string()),
            }
        }

        let healthy = total_count > 0;
        let available = eligible_count > 0;

        Ok(ProviderHealth {
            provider_id: self.id.clone(),
            healthy,
            available,
            last_checked_ms: now as i64,
            reason: if !available && last_reason.is_some() {
                last_reason
            } else if !healthy {
                Some("no nodes registered".into())
            } else {
                None
            },
        })
    }

    async fn submit(&self, request: ProviderRequest) -> anyhow::Result<ProviderJob> {
        // Use the canonical local submission path.
        let prefer = request
            .request
            .prefer_role
            .as_deref()
            .or(request.request.model.as_deref());

        let rt = crate::fleet::pick_node_for_role(
            &self.state.registry,
            prefer,
            request.request.avoid_p100,
        )
        .context("no eligible local nodes")?;

        // Race guard: confirm assignment.
        if !rt.try_assign() {
            anyhow::bail!("node became ineligible during assignment");
        }

        // Extract node info.
        let (node_id, inference_url) = {
            let md = rt.metadata.read().map_err(|e| anyhow::anyhow!("{e}"))?;
            (md.id.clone(), md.inference_url.clone())
        };

        // Create job record.
        let job_id = request.job_id.to_string();
        let (tx, _rx) = tokio::sync::broadcast::channel(256);
        let record = Arc::new(crate::jobs::JobRecord {
            node_id: node_id.clone(),
            inference_url,
            model: request.model_id.clone(),
            messages: request.request.messages.clone(),
            api_key: None, // Provider path does not handle API key accounting
            reserved_tokens: 0,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tx: tx.clone(),
            done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        self.state.jobs.insert(job_id.clone(), record.clone());

        // Register with preemption.
        self.state
            .preemption
            .register_job(crate::preemption::PreemptibleJob::new(
                job_id.clone(),
                node_id.clone(),
                true,
            ));

        // Spawn the job.
        crate::job_runner::spawn_job(
            self.state.http.clone(),
            self.state.jobs.clone(),
            job_id.clone(),
            record,
            self.state.scheduler.clone(),
            Some(rt),
            self.state.ops.clone(),
            self.state.preemption.clone(),
            self.state.metrics.clone(),
        );

        Ok(ProviderJob {
            job_id: request.job_id,
            provider_id: self.id.clone(),
            state: ProviderJobState::Accepted,
        })
    }

    async fn status(&self, job_id: Uuid) -> anyhow::Result<ProviderJobStatus> {
        let job_id_str = job_id.to_string();
        if let Some(job) = self.state.jobs.get(&job_id_str) {
            let state = if job.done.load(std::sync::atomic::Ordering::Relaxed) {
                ProviderJobState::Completed
            } else if job.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                ProviderJobState::Cancelled
            } else {
                ProviderJobState::Running
            };
            Ok(ProviderJobStatus {
                job_id,
                provider_id: self.id.clone(),
                state,
                detail: None,
            })
        } else {
            Ok(ProviderJobStatus {
                job_id,
                provider_id: self.id.clone(),
                state: ProviderJobState::Failed,
                detail: Some("job not found".into()),
            })
        }
    }

    async fn cancel(&self, job_id: Uuid) -> anyhow::Result<()> {
        let job_id_str = job_id.to_string();
        self.state.jobs.cancel(&job_id_str)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::node::auth::{sign_message, SignedJoinRequest};
    use crate::types::now_ms;

    fn test_config() -> Config {
        Config {
            db_url: "sqlite::memory:".to_string(),
            forge_url: "http://127.0.0.1:1".to_string(),
            identity_path: "/tmp/test-node.json".to_string(),
            coordinator_identity_path: None,
            federation_peers: Vec::new(),
            trusted_coordinator_keys: Vec::new(),
            ..Default::default()
        }
    }

    fn make_key() -> (ed25519_dalek::SigningKey, String) {
        let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let pub_hex = hex::encode(key.verifying_key().to_bytes());
        (key, pub_hex)
    }

    #[tokio::test]
    async fn local_provider_capabilities_from_registry() {
        let state = Arc::new(AppState::new(test_config()).unwrap());
        let provider = LocalWorkerProvider::new("local-test", state.clone());

        // Register a node.
        let (key, pub_hex) = make_key();
        let mut req = SignedJoinRequest {
            node_id: "test-node".into(),
            public_key_hex: pub_hex,
            inference_url: "http://127.0.0.1:5200".into(),
            role: "thinker".into(),
            gpu_name: "RTX 2060".into(),
            vram_mb: 16384,
            models: vec!["thinker".into()],
            timestamp_secs: now_ms() / 1000,
            nonce: uuid::Uuid::new_v4().to_string(),
            protocol_version: 1,
            signature_hex: String::new(),
        };
        req.signature_hex = sign_message(&key, &req.canonical_message());

        // The provider should report no capabilities until nodes are registered.
        let caps = provider.capabilities().await.unwrap();
        assert!(caps.models.is_empty());
        assert_eq!(caps.kind, ProviderKind::LocalWorker);
    }

    #[tokio::test]
    async fn local_provider_health_no_nodes() {
        let state = Arc::new(AppState::new(test_config()).unwrap());
        let provider = LocalWorkerProvider::new("local-test", state);
        let health = provider.health().await.unwrap();
        assert!(!health.healthy);
        assert!(!health.available);
        assert_eq!(health.reason, Some("no nodes registered".into()));
    }
}
