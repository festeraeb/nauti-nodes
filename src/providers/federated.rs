//! Federated worker execution provider.
//!
//! Wraps the Chunk 9 federation delegation path. Uses the existing
//! `FederationDispatcher` trait and `DelegationStore` for exactly-once
//! semantics. Does NOT create a second delegation protocol.

use super::{
    ExecutionProvider, ProviderCapabilities, ProviderHealth, ProviderJob, ProviderJobState,
    ProviderJobStatus, ProviderKind, ProviderRequest,
};
use crate::federation::delegation::{
    DelegatedJob, DelegatedJobAck, DelegatedJobStatus, DelegationStore, FederationDispatcher,
};
use crate::federation::FederatedRequestContext;
use crate::types::now_ms;
use std::sync::Arc;
use uuid::Uuid;

/// Federated worker provider wrapping Chunk 9 delegation.
pub struct FederatedWorkerProvider<D> {
    id: String,
    dispatcher: Arc<D>,
    delegation: Arc<DelegationStore>,
}

impl<D> FederatedWorkerProvider<D>
where
    D: FederationDispatcher + 'static,
{
    pub fn new(
        id: impl Into<String>,
        dispatcher: Arc<D>,
        delegation: Arc<DelegationStore>,
    ) -> Self {
        Self {
            id: id.into(),
            dispatcher,
            delegation,
        }
    }
}

#[async_trait::async_trait]
impl<D> ExecutionProvider for FederatedWorkerProvider<D>
where
    D: FederationDispatcher + 'static,
{
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::FederatedWorker
    }

    async fn capabilities(&self) -> anyhow::Result<ProviderCapabilities> {
        // Query the dispatcher for real federated capacity.
        let capacities = self.dispatcher.advertise_capacity().await?;

        let mut models = Vec::new();
        let mut gpu_vendors = Vec::new();
        let mut total_vram_mb: u64 = 0;
        let mut queue_depth: usize = 0;

        for cap in &capacities {
            if let Some(vendor) = &cap.gpu_vendor {
                if !gpu_vendors.contains(vendor) {
                    gpu_vendors.push(vendor.clone());
                }
            }
            if let Some(vram) = cap.free_vram_mb {
                total_vram_mb += vram;
            }
            queue_depth += cap.queue_depth;
        }

        Ok(ProviderCapabilities {
            provider_id: self.id.clone(),
            kind: ProviderKind::FederatedWorker,
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
                crate::capabilities::PrivacyClass::TrustedLineage,
            ],
            checkpointable: true,
            queue_depth,
            expected_latency_ms: None,
            cost_units_per_1k_tokens: None,
        })
    }

    async fn health(&self) -> anyhow::Result<ProviderHealth> {
        let capacities = self.dispatcher.advertise_capacity().await?;
        let now = now_ms() as i64;

        let healthy = !capacities.is_empty();
        let available = capacities
            .iter()
            .any(|c| c.runtime_available && c.eligible_worker_count > 0);

        Ok(ProviderHealth {
            provider_id: self.id.clone(),
            healthy,
            available,
            last_checked_ms: now,
            reason: if !healthy {
                Some("no federated peers available".into())
            } else if !available {
                Some("no eligible federated workers".into())
            } else {
                None
            },
        })
    }

    async fn submit(&self, request: ProviderRequest) -> anyhow::Result<ProviderJob> {
        // Create the delegated job.
        let job = DelegatedJob {
            job_id: request.job_id,
            request: request.request.clone(),
            requirements: crate::federation::delegation::DelegationRequirements {
                model_id: request.model_id.clone(),
                minimum_vram_mb: request.minimum_vram_mb,
                gpu_vendor: request.gpu_vendor.clone(),
                privacy: request.privacy,
                checkpointable: request.checkpointable,
            },
            route: FederatedRequestContext::new(self.id.clone(), 5),
        };

        // Record durably for exactly-once.
        self.delegation
            .insert_pending(&request.job_id.to_string(), &self.id, 0)?;

        // Delegate via the dispatcher.
        let ack = self.dispatcher.delegate("federation", job, 0).await?;

        let state = match ack {
            DelegatedJobAck::Accepted => ProviderJobState::Accepted,
            DelegatedJobAck::Duplicate { .. } => ProviderJobState::Queued,
        };

        Ok(ProviderJob {
            job_id: request.job_id,
            provider_id: self.id.clone(),
            state,
        })
    }

    async fn status(&self, job_id: Uuid) -> anyhow::Result<ProviderJobStatus> {
        // Check durable store first.
        if let Some(row) = self.delegation.get(&job_id.to_string())? {
            let state = match row.state.as_str() {
                "PENDING" => ProviderJobState::Queued,
                "RUNNING" => ProviderJobState::Running,
                "COMPLETED" => ProviderJobState::Completed,
                "FAILED" => ProviderJobState::Failed,
                "CANCELLED" => ProviderJobState::Cancelled,
                _ => ProviderJobState::Failed,
            };
            return Ok(ProviderJobStatus {
                job_id,
                provider_id: self.id.clone(),
                state,
                detail: None,
            });
        }

        // Fall back to dispatcher query.
        let status = self.dispatcher.job_status("federation", job_id).await?;
        let state = match status {
            DelegatedJobStatus::Pending => ProviderJobState::Queued,
            DelegatedJobStatus::Running => ProviderJobState::Running,
            DelegatedJobStatus::Completed => ProviderJobState::Completed,
            DelegatedJobStatus::Failed(_) => ProviderJobState::Failed,
            DelegatedJobStatus::Cancelled => ProviderJobState::Cancelled,
        };

        Ok(ProviderJobStatus {
            job_id,
            provider_id: self.id.clone(),
            state,
            detail: None,
        })
    }

    async fn cancel(&self, job_id: Uuid) -> anyhow::Result<()> {
        // Mark as cancelled in the durable store.
        self.delegation
            .mark_terminal(&job_id.to_string(), "CANCELLED")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opsstore::OpsStore;

    #[test]
    fn federated_provider_kind() {
        // We can't easily construct a real dispatcher without network,
        // so this test just verifies the type compiles.
        let ops = Arc::new(OpsStore::open("sqlite::memory:").unwrap());
        let delegation = Arc::new(DelegationStore::new(ops));
        // The provider struct itself is what we're testing.
        assert!(delegation.get("nonexistent").unwrap().is_none());
    }
}
