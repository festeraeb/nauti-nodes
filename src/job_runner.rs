//! Execute inference jobs against fleet llama endpoints.
//!
//! Chunk 7 accounting guarantees:
//! - NO DOUBLE CHARGE: one ledger row per job_id, opened at reservation.
//! - NO DOUBLE REFUND: settlement is an atomic `ACTIVE -> terminal` ledger
//!   transition; duplicate completion signals are detected and ignored.
//! - NO SILENT LOSS: reservations orphaned by a coordinator crash are
//!   refunded exactly once at startup.

use crate::adapter::llama;
use crate::jobs::{JobRecord, JobStore};
use crate::metrics::FederationMetrics;
use crate::opsstore::{OpsEventKind, OpsStore};
use crate::preemption::PreemptionManager;
use crate::scheduler::Scheduler;
use crate::types::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info};

pub fn spawn_job(
    client: reqwest::Client,
    jobs: Arc<JobStore>,
    job_id: String,
    record: Arc<JobRecord>,
    scheduler: Arc<Scheduler>,
    node: Option<Arc<crate::types::NodeRuntime>>,
    ops: Arc<OpsStore>,
    preemption: Arc<PreemptionManager>,
    metrics: Arc<FederationMetrics>,
) {
    tokio::spawn(async move {
        let cancelled = record.cancelled.clone();
        let tx = record.tx.clone();
        let url = record.inference_url.clone();
        let model = record.model.clone();
        let messages = record.messages.clone();
        let api_key = record.api_key.clone();
        let reserved_tokens = record.reserved_tokens;
        let node_id = record.node_id.clone();
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

        let max_tokens = (reserved_tokens as u32).min(2048);
        let result = llama::stream_chat(
            &client,
            &url,
            &model,
            &messages,
            max_tokens,
            0.7,
            cancelled.clone(),
            send,
        )
        .await;

        let failed = result.is_err();
        if let Err(e) = &result {
            error!(job_id = %job_id, error = %e, "job failed");
            let _ = tx.send(format!("[error] {e}"));
        }

        record
            .done
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Chunk 7: settle accounting exactly once via the ledger guard.
        if let Some(key) = api_key.as_deref() {
            let refund = reserved_tokens.saturating_sub(used_estimate);
            let final_state = if failed { "FAILED" } else { "SETTLED" };
            match ops.settle_reservation(&job_id, used_estimate, refund, final_state) {
                Ok(true) => {
                    if refund > 0 {
                        let _ = scheduler.release_tokens(key, refund);
                    }
                    let _ = ops.record(
                        if failed {
                            OpsEventKind::JobFailed
                        } else {
                            OpsEventKind::JobCompleted
                        },
                        Some(&node_id),
                        None,
                        Some(&job_id),
                        None,
                        &format!("used={used_estimate} refund={refund}"),
                        final_state,
                    );
                    if failed {
                        metrics.failed_work_units.fetch_add(1, Ordering::Relaxed);
                    } else {
                        metrics.completed_work_units.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(false) => {
                    // Reservation already terminal. PREEMPTED is an expected
                    // race (lease drained this job); anything else is a real
                    // duplicate settlement and must be surfaced.
                    let row = ops.reservation(&job_id).ok().flatten();
                    let expected = row
                        .as_ref()
                        .map(|r| r.state == "PREEMPTED" || r.state == "REFUNDED")
                        .unwrap_or(false);
                    if !expected {
                        metrics
                            .accounting_inconsistencies_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = ops.record(
                        OpsEventKind::DuplicateRequest,
                        Some(&node_id),
                        None,
                        Some(&job_id),
                        None,
                        &format!(
                            "duplicate job settlement ignored (ledger state {:?})",
                            row.as_ref().map(|r| r.state.clone())
                        ),
                        "IGNORED",
                    );
                }
                Err(e) => {
                    metrics
                        .accounting_inconsistencies_total
                        .fetch_add(1, Ordering::Relaxed);
                    error!(job_id = %job_id, error = %e, "ledger settlement error");
                }
            }
        }

        if failed {
            preemption.fail_job(&job_id);
        } else {
            preemption.complete_job(&job_id);
        }
        if let Some(rt) = &node {
            rt.release_assignment();
        }

        jobs.remove(&job_id);
        info!(job_id = %job_id, "job done");
    });
}
