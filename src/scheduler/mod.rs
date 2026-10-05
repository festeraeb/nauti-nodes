pub mod affinity;
pub mod escalation;
pub mod fairness;
pub mod lineage_scheduler;
pub mod pull;
pub mod quotas;
pub mod scale_sim;
pub mod scoring;

use crate::node::registry::NodeRegistry;
use crate::scheduler::affinity::AffinityRouter;
use crate::scheduler::quotas::QuotaStore;
use crate::scheduler::scoring::compute_score;
use crate::types::{now_ms, Error, JobId, NodeId, Result, WorkerMessage};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

pub struct ActiveJob {
    pub node_id: NodeId,
    pub created_at: u64,
    pub cancelled: AtomicBool,
    pub stream_tx: Option<tokio::sync::mpsc::Sender<String>>,
}

pub struct Scheduler {
    jobs: DashMap<JobId, Arc<ActiveJob>>,
    registry: Arc<NodeRegistry>,
    affinity: AffinityRouter,
    quotas: Arc<QuotaStore>,
}

impl Scheduler {
    pub fn new(registry: Arc<NodeRegistry>, db_url: &str) -> Result<Self> {
        Ok(Self {
            jobs: DashMap::new(),
            registry,
            affinity: AffinityRouter::new(),
            quotas: Arc::new(QuotaStore::open(db_url)?),
        })
    }

    pub fn check_quota(&self, api_key: &str, need_tokens: u64) -> Result<()> {
        self.quotas.check_quota(api_key, need_tokens)
    }

    pub fn reserve_tokens(&self, api_key: &str, reserve_tokens: u64) -> Result<()> {
        self.quotas.reserve_tokens(api_key, reserve_tokens)
    }

    pub fn release_tokens(&self, api_key: &str, amount_tokens: u64) -> Result<()> {
        self.quotas.release_tokens(api_key, amount_tokens)
    }

    pub fn set_balance(&self, api_key: &str, balance_tokens: u64) -> Result<()> {
        self.quotas.set_balance(api_key, balance_tokens as i64)
    }

    pub fn schedule_job(
        &self,
        job_id: JobId,
        prompt_prefix: &str,
        api_key: Option<&str>,
        reserve_tokens: u64,
    ) -> Result<NodeId> {
        if let Some(key) = api_key {
            self.quotas.check_quota(key, reserve_tokens)?;
            self.quotas.reserve_tokens(key, reserve_tokens)?;
        }

        let node_id = if let Some(id) = self.affinity.lookup(prompt_prefix) {
            if self.registry.get_node(&id).is_some() {
                id
            } else {
                self.pick_best_node()?
            }
        } else {
            self.pick_best_node()?
        };

        self.affinity.remember(prompt_prefix, &node_id);

        let (stream_tx, _rx) = tokio::sync::mpsc::channel(256);
        let job = Arc::new(ActiveJob {
            node_id: node_id.clone(),
            created_at: now_ms(),
            cancelled: AtomicBool::new(false),
            stream_tx: Some(stream_tx),
        });
        self.jobs.insert(job_id.clone(), job);

        if let Some(node) = self.registry.get_node(&node_id) {
            let _ = node.mailbox.try_send(WorkerMessage::JobAssigned {
                job_id: job_id.clone(),
            });
        }

        info!(job_id = %job_id, node_id = %node_id, "job scheduled");
        Ok(node_id)
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<()> {
        let job = self
            .jobs
            .get(job_id)
            .ok_or_else(|| Error::JobNotFound(job_id.into()))?;
        job.cancelled.store(true, Ordering::Relaxed);
        if let Some(node) = self.registry.get_node(&job.node_id) {
            let _ = node.mailbox.try_send(WorkerMessage::Cancel {
                job_id: job_id.into(),
            });
        }
        self.jobs.remove(job_id);
        Ok(())
    }

    fn pick_best_node(&self) -> Result<NodeId> {
        // Chunk 7: this path also goes through the centralized fail-closed
        // eligibility gate. No node is ever selected on score alone.
        let now = now_ms();
        let nodes = self.registry.list_nodes();
        let best = nodes
            .iter()
            .filter(|rt| {
                crate::eligibility::evaluate_node(
                    rt,
                    now,
                    crate::fleet::SCHEDULER_STALE_HEARTBEAT_MS,
                )
                .is_ok()
            })
            .filter_map(|rt| {
                let stats = rt.stats.read().ok()?;
                let score = compute_score(&stats);
                Some((rt.metadata.read().ok()?.id.clone(), score))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        best.map(|(id, _)| id).ok_or(Error::NoNodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::registry::NodeRegistry;
    use crate::types::{NodeCapabilities, NodeMetadata, NodeStatistics};

    fn meta(id: &str) -> NodeMetadata {
        NodeMetadata {
            id: id.into(),
            addr: "127.0.0.1:1".into(),
            inference_url: "http://127.0.0.1:5001".into(),
            role: "coder".into(),
            gpu_name: "test".into(),
            online: true,
            public_key: vec![],
            capabilities: NodeCapabilities {
                max_batch: 1,
                models: vec![],
                vram_mb: 8192,
                role: "coder".into(),
            },
            claim: crate::types::RegisteredHardwareClaim {
                gpu_name: "test".into(),
                vram_mb: 8192,
                models: vec![],
            },
            observed: Some(crate::types::ObservedHardware {
                gpu_name: "test".into(),
                vram_mb: 8192,
                vendor: "NVIDIA".into(),
                device_count: 1,
                driver_version: None,
                temperature_c: None,
                node_id: id.into(),
                observer_node_id: "local".into(),
                source_type: crate::types::TelemetrySourceType::LocalLiveObservation,
                provider: "all-smi".into(),
                observed_at: Some(crate::types::now_ms()),
                received_at: Some(crate::types::now_ms()),
                measurement_id: None,
                device_id: None,
                sequence: None,
                age_ms: None,
                source_endpoint: None,
            }),
            reconciliation: crate::types::ReconciledHardwareState {
                status: crate::types::ReconciliationStatus::MATCH,
                fields: vec![],
            },
            last_hardware_probe: Some(crate::types::now_ms()),
            health_state: "ONLINE".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn schedule_picks_node() {
        let reg = Arc::new(NodeRegistry::new());
        let rt = reg.register_node(meta("gpu-a"));
        rt.touch_heartbeat(NodeStatistics {
            tokens_per_sec: 50.0,
            free_vram_mb: 8000,
            ..Default::default()
        });
        let sched = Scheduler::new(reg, "sqlite::memory:").unwrap();
        let node = sched.schedule_job("j1".into(), "hello", None, 0).unwrap();
        assert_eq!(node, "gpu-a");
    }
}
