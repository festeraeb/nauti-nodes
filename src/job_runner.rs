//! Execute inference jobs against fleet llama endpoints.

use crate::adapter::llama;
use crate::jobs::{JobRecord, JobStore};
use crate::scheduler::Scheduler;
use crate::types::Result;
use std::sync::Arc;
use tracing::{error, info};

pub fn spawn_job(
    client: reqwest::Client,
    jobs: Arc<JobStore>,
    job_id: String,
    record: Arc<JobRecord>,
    scheduler: Arc<Scheduler>,
) {
    tokio::spawn(async move {
        let cancelled = record.cancelled.clone();
        let tx = record.tx.clone();
        let url = record.inference_url.clone();
        let model = record.model.clone();
        let messages = record.messages.clone();
        let api_key = record.api_key.clone();
        let reserved_tokens = record.reserved_tokens;
        let mut used_estimate: u64 = 0;

        info!(job_id = %job_id, url = %url, model = %model, "job start");

        let send = |text: String| -> Result<()> {
            // Credit ledger token estimate: very rough chars->tokens heuristic.
            let chars = text.chars().count() as u64;
            let approx = std::cmp::max(1, chars / 4);
            used_estimate = used_estimate.saturating_add(approx);
            let _ = tx.send(text);
            Ok(())
        };

        let result = llama::stream_chat(
            &client,
            &url,
            &model,
            &messages,
            2048,
            0.7,
            cancelled.clone(),
            send,
        )
        .await;

        if let Err(e) = result {
            error!(job_id = %job_id, error = %e, "job failed");
            let _ = tx.send(format!("[error] {e}"));
        }

        record
            .done
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Release unused credits (Phase 4).
        if let Some(key) = api_key.as_deref() {
            if reserved_tokens > used_estimate {
                let unused = reserved_tokens - used_estimate;
                let _ = scheduler.release_tokens(key, unused);
            }
        }

        jobs.remove(&job_id);
        info!(job_id = %job_id, "job done");
    });
}
