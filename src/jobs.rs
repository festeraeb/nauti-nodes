//! Active inference jobs and SSE token broadcast.

use crate::adapter::llama::ChatMessage;
use crate::types::{Error, JobId, Result};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct JobRecord {
    pub node_id: String,
    pub inference_url: String,
    pub model: String,
    pub messages: Vec<ChatMessage>,
    /// API key associated with this job (used for Phase 4 credit ledger).
    pub api_key: Option<String>,
    /// Tokens reserved up-front; we release any unused remainder on completion/cancel.
    pub reserved_tokens: u64,
    pub cancelled: Arc<AtomicBool>,
    pub tx: broadcast::Sender<String>,
    pub done: Arc<AtomicBool>,
}

pub struct JobStore {
    jobs: DashMap<JobId, Arc<JobRecord>>,
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: DashMap::new(),
        }
    }

    pub fn insert(&self, job_id: JobId, record: Arc<JobRecord>) {
        self.jobs.insert(job_id, record);
    }

    pub fn get(&self, job_id: &str) -> Option<Arc<JobRecord>> {
        self.jobs.get(job_id).map(|e| e.clone())
    }

    pub fn remove(&self, job_id: &str) {
        self.jobs.remove(job_id);
    }

    pub fn cancel(&self, job_id: &str) -> Result<()> {
        let job = self
            .get(job_id)
            .ok_or_else(|| Error::JobNotFound(job_id.into()))?;
        job.cancelled.store(true, Ordering::Relaxed);
        self.remove(job_id);
        Ok(())
    }

    pub fn active_count(&self) -> usize {
        self.jobs.len()
    }
}
