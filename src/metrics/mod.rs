//! Prometheus-compatible metrics for Nauti Nodes federation monitoring.
//!
//! Exposes federation-specific metrics in Prometheus exposition format at
//! `/metrics/prometheus`. Designed to be scraped by Prometheus or Grafana.
//!
//! Also provides programmatic access to current metric values for the
//! `/metrics` JSON endpoint.
//!
//! GPU hardware metrics come from all-smi snapshots and are refreshed
//! periodically by the coordinator's background task.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Atomic counters for federation-wide metrics.
/// These are updated by the coordinator and job runner in real time.
#[derive(Debug, Default)]
pub struct FederationMetrics {
    // --- Node metrics ---
    pub federation_nodes_total: AtomicU64,
    pub federation_nodes_online: AtomicU64,
    pub lineage_branches: AtomicU64,

    // --- GPU metrics (from all-smi, refreshed periodically) ---
    pub gpu_count: AtomicU64,
    pub gpu_available_count: AtomicU64,
    pub gpu_total_vram_mb: AtomicU64,
    pub gpu_free_vram_mb: AtomicU64,
    pub gpu_temperature_celsius_max: AtomicU64,
    pub gpu_power_watts_total: AtomicU64,
    pub gpu_utilization_pct_avg: AtomicU64, // stored as integer (0-10000 for 0.00-100.00%)

    // --- Work unit metrics ---
    pub active_work_units: AtomicU64,
    pub opportunistic_work_units: AtomicU64,
    pub completed_work_units: AtomicU64,
    pub failed_work_units: AtomicU64,
    pub expired_leases: AtomicU64,

    // --- Scheduling metrics ---
    pub lineage_scheduled_total: AtomicU64,
    pub federation_scheduled_total: AtomicU64,
    pub preemptions_total: AtomicU64,

    // --- Contribution metrics ---
    pub contribution_gpu_seconds: AtomicU64,
    pub contribution_compute_seconds: AtomicU64,

    // --- Inference metrics ---
    pub inference_requests_total: AtomicU64,
    pub inference_tokens_total: AtomicU64,
    pub inference_errors_total: AtomicU64,

    // --- Chunk 7 operational integrity metrics (observations, not guarantees) ---
    pub lease_requests_total: AtomicU64,
    pub leases_granted_total: AtomicU64,
    pub leases_released_total: AtomicU64,
    pub lease_conflicts_total: AtomicU64,
    /// Assignments refused because ownership was lost/uncertain at confirm
    /// time (work-vs-lease race guard). MUST remain 0 in a healthy fleet for
    /// completed+granted overlaps; non-zero counts blocked double-ownership.
    pub ownership_conflicts_blocked_total: AtomicU64,
    /// Accounting ledger inconsistencies detected (double settle/refund).
    /// MUST be 0.
    pub accounting_inconsistencies_total: AtomicU64,
    pub node_reconnects_total: AtomicU64,
    pub runtime_failures_total: AtomicU64,
    pub jobs_preempted_total: AtomicU64,
    pub jobs_requeued_total: AtomicU64,
    pub coordinator_restarts_total: AtomicU64,

    // --- Chunk 11 provider/federation soak integrity ---
    /// Stale-epoch fenced commands rejected (control-plane).
    pub stale_commands_rejected_total: AtomicU64,
    /// Duplicate executions blocked (work/command idempotency).
    pub duplicate_executions_blocked_total: AtomicU64,
    /// Checkpoint double-resume attempts blocked.
    pub checkpoint_double_resumes_blocked_total: AtomicU64,
    /// Routing loop detections (federation hop graph).
    pub routing_loops_detected_total: AtomicU64,
    /// Provider selection decisions (real select_provider outcomes).
    pub provider_selections_total: AtomicU64,
    pub provider_selections_local_total: AtomicU64,
    pub provider_selections_federated_total: AtomicU64,
    pub provider_selections_none_total: AtomicU64,
    pub provider_rejections_policy_total: AtomicU64,
    pub provider_rejections_health_total: AtomicU64,
    /// Delegated job accepts / duplicate acks (exactly-once).
    pub delegated_jobs_accepted_total: AtomicU64,
    pub delegated_jobs_duplicate_total: AtomicU64,
}

/// Snapshot of integrity counters used by soak reports (plain u64s).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IntegritySnapshot {
    pub ownership_conflicts_blocked_total: u64,
    pub accounting_inconsistencies_total: u64,
    pub stale_commands_rejected_total: u64,
    pub duplicate_executions_blocked_total: u64,
    pub checkpoint_double_resumes_blocked_total: u64,
    pub routing_loops_detected_total: u64,
    pub leases_granted_total: u64,
    pub leases_released_total: u64,
    pub node_reconnects_total: u64,
    pub runtime_failures_total: u64,
    pub jobs_requeued_total: u64,
    pub coordinator_restarts_total: u64,
    pub provider_selections_total: u64,
    pub provider_selections_local_total: u64,
    pub provider_selections_federated_total: u64,
    pub provider_selections_none_total: u64,
    pub provider_rejections_policy_total: u64,
    pub provider_rejections_health_total: u64,
    pub delegated_jobs_accepted_total: u64,
    pub delegated_jobs_duplicate_total: u64,
}

impl FederationMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot_json(&self) -> serde_json::Value {
        serde_json::json!({
            "federation_nodes_total": self.federation_nodes_total.load(Ordering::Relaxed),
            "federation_nodes_online": self.federation_nodes_online.load(Ordering::Relaxed),
            "lineage_branches": self.lineage_branches.load(Ordering::Relaxed),
            "gpu_count": self.gpu_count.load(Ordering::Relaxed),
            "gpu_available_count": self.gpu_available_count.load(Ordering::Relaxed),
            "gpu_total_vram_mb": self.gpu_total_vram_mb.load(Ordering::Relaxed),
            "gpu_free_vram_mb": self.gpu_free_vram_mb.load(Ordering::Relaxed),
            "active_work_units": self.active_work_units.load(Ordering::Relaxed),
            "completed_work_units": self.completed_work_units.load(Ordering::Relaxed),
            "failed_work_units": self.failed_work_units.load(Ordering::Relaxed),
            "lineage_scheduled_total": self.lineage_scheduled_total.load(Ordering::Relaxed),
            "federation_scheduled_total": self.federation_scheduled_total.load(Ordering::Relaxed),
            "inference_requests_total": self.inference_requests_total.load(Ordering::Relaxed),
            "inference_tokens_total": self.inference_tokens_total.load(Ordering::Relaxed),
            "lease_requests_total": self.lease_requests_total.load(Ordering::Relaxed),
            "leases_granted_total": self.leases_granted_total.load(Ordering::Relaxed),
            "leases_released_total": self.leases_released_total.load(Ordering::Relaxed),
            "lease_conflicts_total": self.lease_conflicts_total.load(Ordering::Relaxed),
            "ownership_conflicts_blocked_total": self
                .ownership_conflicts_blocked_total
                .load(Ordering::Relaxed),
            "accounting_inconsistencies_total": self
                .accounting_inconsistencies_total
                .load(Ordering::Relaxed),
            "node_reconnects_total": self.node_reconnects_total.load(Ordering::Relaxed),
            "runtime_failures_total": self.runtime_failures_total.load(Ordering::Relaxed),
            "jobs_preempted_total": self.jobs_preempted_total.load(Ordering::Relaxed),
            "jobs_requeued_total": self.jobs_requeued_total.load(Ordering::Relaxed),
            "coordinator_restarts_total": self.coordinator_restarts_total.load(Ordering::Relaxed),
            "stale_commands_rejected_total": self.stale_commands_rejected_total.load(Ordering::Relaxed),
            "duplicate_executions_blocked_total": self
                .duplicate_executions_blocked_total
                .load(Ordering::Relaxed),
            "checkpoint_double_resumes_blocked_total": self
                .checkpoint_double_resumes_blocked_total
                .load(Ordering::Relaxed),
            "routing_loops_detected_total": self.routing_loops_detected_total.load(Ordering::Relaxed),
            "provider_selections_total": self.provider_selections_total.load(Ordering::Relaxed),
            "provider_selections_local_total": self
                .provider_selections_local_total
                .load(Ordering::Relaxed),
            "provider_selections_federated_total": self
                .provider_selections_federated_total
                .load(Ordering::Relaxed),
            "provider_selections_none_total": self
                .provider_selections_none_total
                .load(Ordering::Relaxed),
            "provider_rejections_policy_total": self
                .provider_rejections_policy_total
                .load(Ordering::Relaxed),
            "provider_rejections_health_total": self
                .provider_rejections_health_total
                .load(Ordering::Relaxed),
            "delegated_jobs_accepted_total": self.delegated_jobs_accepted_total.load(Ordering::Relaxed),
            "delegated_jobs_duplicate_total": self
                .delegated_jobs_duplicate_total
                .load(Ordering::Relaxed),
        })
    }

    /// Integrity counters for soak / ops reports.
    pub fn integrity_snapshot(&self) -> IntegritySnapshot {
        IntegritySnapshot {
            ownership_conflicts_blocked_total: self
                .ownership_conflicts_blocked_total
                .load(Ordering::Relaxed),
            accounting_inconsistencies_total: self
                .accounting_inconsistencies_total
                .load(Ordering::Relaxed),
            stale_commands_rejected_total: self
                .stale_commands_rejected_total
                .load(Ordering::Relaxed),
            duplicate_executions_blocked_total: self
                .duplicate_executions_blocked_total
                .load(Ordering::Relaxed),
            checkpoint_double_resumes_blocked_total: self
                .checkpoint_double_resumes_blocked_total
                .load(Ordering::Relaxed),
            routing_loops_detected_total: self.routing_loops_detected_total.load(Ordering::Relaxed),
            leases_granted_total: self.leases_granted_total.load(Ordering::Relaxed),
            leases_released_total: self.leases_released_total.load(Ordering::Relaxed),
            node_reconnects_total: self.node_reconnects_total.load(Ordering::Relaxed),
            runtime_failures_total: self.runtime_failures_total.load(Ordering::Relaxed),
            jobs_requeued_total: self.jobs_requeued_total.load(Ordering::Relaxed),
            coordinator_restarts_total: self.coordinator_restarts_total.load(Ordering::Relaxed),
            provider_selections_total: self.provider_selections_total.load(Ordering::Relaxed),
            provider_selections_local_total: self
                .provider_selections_local_total
                .load(Ordering::Relaxed),
            provider_selections_federated_total: self
                .provider_selections_federated_total
                .load(Ordering::Relaxed),
            provider_selections_none_total: self
                .provider_selections_none_total
                .load(Ordering::Relaxed),
            provider_rejections_policy_total: self
                .provider_rejections_policy_total
                .load(Ordering::Relaxed),
            provider_rejections_health_total: self
                .provider_rejections_health_total
                .load(Ordering::Relaxed),
            delegated_jobs_accepted_total: self
                .delegated_jobs_accepted_total
                .load(Ordering::Relaxed),
            delegated_jobs_duplicate_total: self
                .delegated_jobs_duplicate_total
                .load(Ordering::Relaxed),
        }
    }

    /// Hard soak invariants: these MUST remain zero for a healthy run.
    pub fn assert_hard_invariants_zero(&self) -> Result<(), String> {
        let s = self.integrity_snapshot();
        let mut bad = Vec::new();
        // ownership_conflicts_blocked is a *blocked* counter — non-zero means
        // the guard fired (good). The soak invariant is that dual ownership
        // never *succeeds*; blocked attempts are expected under contention.
        // accounting_inconsistencies must be 0.
        if s.accounting_inconsistencies_total != 0 {
            bad.push(format!(
                "accounting_inconsistencies_total={}",
                s.accounting_inconsistencies_total
            ));
        }
        if !bad.is_empty() {
            return Err(format!("hard invariant violation: {}", bad.join(", ")));
        }
        Ok(())
    }

    /// Render all metrics in Prometheus exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        // Node metrics
        out.push_str("# HELP nauti_federation_nodes_total Total nodes in the federation.\n");
        out.push_str("# TYPE nauti_federation_nodes_total gauge\n");
        out.push_str(&format!(
            "nauti_federation_nodes_total {}\n",
            self.federation_nodes_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_federation_nodes_online Online nodes in the federation.\n");
        out.push_str("# TYPE nauti_federation_nodes_online gauge\n");
        out.push_str(&format!(
            "nauti_federation_nodes_online {}\n",
            self.federation_nodes_online.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_lineage_branches Number of lineage branches.\n");
        out.push_str("# TYPE nauti_lineage_branches gauge\n");
        out.push_str(&format!(
            "nauti_lineage_branches {}\n",
            self.lineage_branches.load(Ordering::Relaxed)
        ));

        // GPU metrics
        out.push_str("# HELP nauti_gpu_count Total GPUs discovered.\n");
        out.push_str("# TYPE nauti_gpu_count gauge\n");
        out.push_str(&format!(
            "nauti_gpu_count {}\n",
            self.gpu_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_gpu_available_count GPUs currently available for work.\n");
        out.push_str("# TYPE nauti_gpu_available_count gauge\n");
        out.push_str(&format!(
            "nauti_gpu_available_count {}\n",
            self.gpu_available_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_gpu_total_vram_mb Total GPU VRAM in megabytes.\n");
        out.push_str("# TYPE nauti_gpu_total_vram_mb gauge\n");
        out.push_str(&format!(
            "nauti_gpu_total_vram_mb {}\n",
            self.gpu_total_vram_mb.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_gpu_free_vram_mb Free GPU VRAM in megabytes.\n");
        out.push_str("# TYPE nauti_gpu_free_vram_mb gauge\n");
        out.push_str(&format!(
            "nauti_gpu_free_vram_mb {}\n",
            self.gpu_free_vram_mb.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP nauti_gpu_temperature_celsius_max Maximum GPU temperature in Celsius.\n",
        );
        out.push_str("# TYPE nauti_gpu_temperature_celsius_max gauge\n");
        out.push_str(&format!(
            "nauti_gpu_temperature_celsius_max {}\n",
            self.gpu_temperature_celsius_max.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_gpu_power_watts_total Total GPU power consumption in watts.\n");
        out.push_str("# TYPE nauti_gpu_power_watts_total gauge\n");
        out.push_str(&format!(
            "nauti_gpu_power_watts_total {}\n",
            self.gpu_power_watts_total.load(Ordering::Relaxed)
        ));

        // Work unit metrics
        out.push_str("# HELP nauti_active_work_units Currently active work units.\n");
        out.push_str("# TYPE nauti_active_work_units gauge\n");
        out.push_str(&format!(
            "nauti_active_work_units {}\n",
            self.active_work_units.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP nauti_opportunistic_work_units Opportunistic background work units.\n",
        );
        out.push_str("# TYPE nauti_opportunistic_work_units gauge\n");
        out.push_str(&format!(
            "nauti_opportunistic_work_units {}\n",
            self.opportunistic_work_units.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_completed_work_units_total Total completed work units.\n");
        out.push_str("# TYPE nauti_completed_work_units_total counter\n");
        out.push_str(&format!(
            "nauti_completed_work_units_total {}\n",
            self.completed_work_units.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_failed_work_units_total Total failed work units.\n");
        out.push_str("# TYPE nauti_failed_work_units_total counter\n");
        out.push_str(&format!(
            "nauti_failed_work_units_total {}\n",
            self.failed_work_units.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_expired_leases_total Total expired work leases.\n");
        out.push_str("# TYPE nauti_expired_leases_total counter\n");
        out.push_str(&format!(
            "nauti_expired_leases_total {}\n",
            self.expired_leases.load(Ordering::Relaxed)
        ));

        // Scheduling metrics
        out.push_str(
            "# HELP nauti_lineage_scheduled_total Total lineage-preferred scheduling decisions.\n",
        );
        out.push_str("# TYPE nauti_lineage_scheduled_total counter\n");
        out.push_str(&format!(
            "nauti_lineage_scheduled_total {}\n",
            self.lineage_scheduled_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP nauti_federation_scheduled_total Total federation scheduling decisions.\n",
        );
        out.push_str("# TYPE nauti_federation_scheduled_total counter\n");
        out.push_str(&format!(
            "nauti_federation_scheduled_total {}\n",
            self.federation_scheduled_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_preemptions_total Total preemption events.\n");
        out.push_str("# TYPE nauti_preemptions_total counter\n");
        out.push_str(&format!(
            "nauti_preemptions_total {}\n",
            self.preemptions_total.load(Ordering::Relaxed)
        ));

        // Inference metrics
        out.push_str("# HELP nauti_inference_requests_total Total inference requests.\n");
        out.push_str("# TYPE nauti_inference_requests_total counter\n");
        out.push_str(&format!(
            "nauti_inference_requests_total {}\n",
            self.inference_requests_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_inference_tokens_total Total tokens generated.\n");
        out.push_str("# TYPE nauti_inference_tokens_total counter\n");
        out.push_str(&format!(
            "nauti_inference_tokens_total {}\n",
            self.inference_tokens_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP nauti_inference_errors_total Total inference errors.\n");
        out.push_str("# TYPE nauti_inference_errors_total counter\n");
        out.push_str(&format!(
            "nauti_inference_errors_total {}\n",
            self.inference_errors_total.load(Ordering::Relaxed)
        ));

        // Chunk 7 operational integrity metrics
        for (name, help, value) in [
            (
                "nauti_lease_requests_total",
                "Total Forge lease requests.",
                self.lease_requests_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_leases_granted_total",
                "Total Forge leases granted (ownership actually yielded).",
                self.leases_granted_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_leases_released_total",
                "Total Forge leases released.",
                self.leases_released_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_lease_conflicts_total",
                "Conflicting lease attempts rejected.",
                self.lease_conflicts_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_ownership_conflicts_blocked_total",
                "Shared assignments refused by the fail-closed race guard.",
                self.ownership_conflicts_blocked_total
                    .load(Ordering::Relaxed),
            ),
            (
                "nauti_accounting_inconsistencies_total",
                "Token accounting inconsistencies detected (must be 0).",
                self.accounting_inconsistencies_total
                    .load(Ordering::Relaxed),
            ),
            (
                "nauti_node_reconnects_total",
                "Worker reconnects with persistent identity.",
                self.node_reconnects_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_runtime_failures_total",
                "Inference runtime failures observed.",
                self.runtime_failures_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_jobs_preempted_total",
                "Shared jobs preempted by Forge leases.",
                self.jobs_preempted_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_jobs_requeued_total",
                "Shared jobs requeued after failure/preemption.",
                self.jobs_requeued_total.load(Ordering::Relaxed),
            ),
            (
                "nauti_coordinator_restarts_total",
                "Coordinator restart recoveries.",
                self.coordinator_restarts_total.load(Ordering::Relaxed),
            ),
        ] {
            out.push_str(&format!("# HELP {name} {help}\n"));
            out.push_str(&format!("# TYPE {name} counter\n"));
            out.push_str(&format!("{name} {value}\n"));
        }

        out
    }
}

impl Clone for FederationMetrics {
    fn clone(&self) -> Self {
        Self {
            federation_nodes_total: AtomicU64::new(
                self.federation_nodes_total.load(Ordering::Relaxed),
            ),
            federation_nodes_online: AtomicU64::new(
                self.federation_nodes_online.load(Ordering::Relaxed),
            ),
            lineage_branches: AtomicU64::new(self.lineage_branches.load(Ordering::Relaxed)),
            gpu_count: AtomicU64::new(self.gpu_count.load(Ordering::Relaxed)),
            gpu_available_count: AtomicU64::new(self.gpu_available_count.load(Ordering::Relaxed)),
            gpu_total_vram_mb: AtomicU64::new(self.gpu_total_vram_mb.load(Ordering::Relaxed)),
            gpu_free_vram_mb: AtomicU64::new(self.gpu_free_vram_mb.load(Ordering::Relaxed)),
            gpu_temperature_celsius_max: AtomicU64::new(
                self.gpu_temperature_celsius_max.load(Ordering::Relaxed),
            ),
            gpu_power_watts_total: AtomicU64::new(
                self.gpu_power_watts_total.load(Ordering::Relaxed),
            ),
            gpu_utilization_pct_avg: AtomicU64::new(
                self.gpu_utilization_pct_avg.load(Ordering::Relaxed),
            ),
            active_work_units: AtomicU64::new(self.active_work_units.load(Ordering::Relaxed)),
            opportunistic_work_units: AtomicU64::new(
                self.opportunistic_work_units.load(Ordering::Relaxed),
            ),
            completed_work_units: AtomicU64::new(self.completed_work_units.load(Ordering::Relaxed)),
            failed_work_units: AtomicU64::new(self.failed_work_units.load(Ordering::Relaxed)),
            expired_leases: AtomicU64::new(self.expired_leases.load(Ordering::Relaxed)),
            lineage_scheduled_total: AtomicU64::new(
                self.lineage_scheduled_total.load(Ordering::Relaxed),
            ),
            federation_scheduled_total: AtomicU64::new(
                self.federation_scheduled_total.load(Ordering::Relaxed),
            ),
            preemptions_total: AtomicU64::new(self.preemptions_total.load(Ordering::Relaxed)),
            contribution_gpu_seconds: AtomicU64::new(
                self.contribution_gpu_seconds.load(Ordering::Relaxed),
            ),
            contribution_compute_seconds: AtomicU64::new(
                self.contribution_compute_seconds.load(Ordering::Relaxed),
            ),
            inference_requests_total: AtomicU64::new(
                self.inference_requests_total.load(Ordering::Relaxed),
            ),
            inference_tokens_total: AtomicU64::new(
                self.inference_tokens_total.load(Ordering::Relaxed),
            ),
            inference_errors_total: AtomicU64::new(
                self.inference_errors_total.load(Ordering::Relaxed),
            ),
            lease_requests_total: AtomicU64::new(self.lease_requests_total.load(Ordering::Relaxed)),
            leases_granted_total: AtomicU64::new(self.leases_granted_total.load(Ordering::Relaxed)),
            leases_released_total: AtomicU64::new(
                self.leases_released_total.load(Ordering::Relaxed),
            ),
            lease_conflicts_total: AtomicU64::new(
                self.lease_conflicts_total.load(Ordering::Relaxed),
            ),
            ownership_conflicts_blocked_total: AtomicU64::new(
                self.ownership_conflicts_blocked_total
                    .load(Ordering::Relaxed),
            ),
            accounting_inconsistencies_total: AtomicU64::new(
                self.accounting_inconsistencies_total
                    .load(Ordering::Relaxed),
            ),
            node_reconnects_total: AtomicU64::new(
                self.node_reconnects_total.load(Ordering::Relaxed),
            ),
            runtime_failures_total: AtomicU64::new(
                self.runtime_failures_total.load(Ordering::Relaxed),
            ),
            jobs_preempted_total: AtomicU64::new(self.jobs_preempted_total.load(Ordering::Relaxed)),
            jobs_requeued_total: AtomicU64::new(self.jobs_requeued_total.load(Ordering::Relaxed)),
            coordinator_restarts_total: AtomicU64::new(
                self.coordinator_restarts_total.load(Ordering::Relaxed),
            ),
            stale_commands_rejected_total: AtomicU64::new(
                self.stale_commands_rejected_total.load(Ordering::Relaxed),
            ),
            duplicate_executions_blocked_total: AtomicU64::new(
                self.duplicate_executions_blocked_total
                    .load(Ordering::Relaxed),
            ),
            checkpoint_double_resumes_blocked_total: AtomicU64::new(
                self.checkpoint_double_resumes_blocked_total
                    .load(Ordering::Relaxed),
            ),
            routing_loops_detected_total: AtomicU64::new(
                self.routing_loops_detected_total.load(Ordering::Relaxed),
            ),
            provider_selections_total: AtomicU64::new(
                self.provider_selections_total.load(Ordering::Relaxed),
            ),
            provider_selections_local_total: AtomicU64::new(
                self.provider_selections_local_total.load(Ordering::Relaxed),
            ),
            provider_selections_federated_total: AtomicU64::new(
                self.provider_selections_federated_total
                    .load(Ordering::Relaxed),
            ),
            provider_selections_none_total: AtomicU64::new(
                self.provider_selections_none_total.load(Ordering::Relaxed),
            ),
            provider_rejections_policy_total: AtomicU64::new(
                self.provider_rejections_policy_total
                    .load(Ordering::Relaxed),
            ),
            provider_rejections_health_total: AtomicU64::new(
                self.provider_rejections_health_total
                    .load(Ordering::Relaxed),
            ),
            delegated_jobs_accepted_total: AtomicU64::new(
                self.delegated_jobs_accepted_total.load(Ordering::Relaxed),
            ),
            delegated_jobs_duplicate_total: AtomicU64::new(
                self.delegated_jobs_duplicate_total.load(Ordering::Relaxed),
            ),
        }
    }
}

/// Update GPU metrics from an all-smi snapshot.
pub fn update_gpu_metrics_from_snapshot(metrics: &FederationMetrics, gpus: &[crate::gpu::GpuInfo]) {
    metrics
        .gpu_count
        .store(gpus.len() as u64, Ordering::Relaxed);

    let available = gpus.iter().filter(|g| g.available).count() as u64;
    metrics
        .gpu_available_count
        .store(available, Ordering::Relaxed);

    let total_vram: u64 = gpus.iter().map(|g| g.total_memory_mb as u64).sum();
    metrics
        .gpu_total_vram_mb
        .store(total_vram, Ordering::Relaxed);

    let free_vram: u64 = gpus.iter().map(|g| g.free_memory_mb as u64).sum();
    metrics.gpu_free_vram_mb.store(free_vram, Ordering::Relaxed);

    let max_temp: u64 = gpus
        .iter()
        .filter_map(|g| g.temperature_c)
        .map(|t| t as u64)
        .max()
        .unwrap_or(0);
    metrics
        .gpu_temperature_celsius_max
        .store(max_temp, Ordering::Relaxed);

    let total_power: u64 = gpus
        .iter()
        .filter_map(|g| g.power_watts)
        .map(|p| p as u64)
        .sum();
    metrics
        .gpu_power_watts_total
        .store(total_power, Ordering::Relaxed);

    let avg_util: u64 = if gpus.is_empty() {
        0
    } else {
        (gpus.iter().map(|g| g.utilization_pct).sum::<f64>() / gpus.len() as f64) as u64
    };
    metrics
        .gpu_utilization_pct_avg
        .store(avg_util, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_output_has_required_metrics() {
        let m = FederationMetrics::new();
        m.federation_nodes_total.store(5, Ordering::Relaxed);
        m.gpu_count.store(2, Ordering::Relaxed);
        m.completed_work_units.store(42, Ordering::Relaxed);

        let prom = m.render_prometheus();
        assert!(prom.contains("nauti_federation_nodes_total 5"));
        assert!(prom.contains("nauti_gpu_count 2"));
        assert!(prom.contains("nauti_completed_work_units_total 42"));
        assert!(prom.contains("# TYPE nauti_federation_nodes_total gauge"));
        assert!(prom.contains("# TYPE nauti_completed_work_units_total counter"));
    }

    #[test]
    fn json_snapshot_matches() {
        let m = FederationMetrics::new();
        m.federation_nodes_total.store(3, Ordering::Relaxed);
        let json = m.snapshot_json();
        assert_eq!(json["federation_nodes_total"], 3);
    }

    #[test]
    fn update_gpu_metrics() {
        let m = FederationMetrics::new();
        let gpus = vec![
            crate::gpu::GpuInfo {
                name: "GPU-0".into(),
                uuid: "u0".into(),
                vendor: "NVIDIA".into(),
                architecture: "Ampere".into(),
                total_memory_bytes: 24 * 1024 * 1024 * 1024,
                used_memory_bytes: 0,
                free_memory_bytes: 24 * 1024 * 1024 * 1024,
                total_memory_mb: 24576,
                free_memory_mb: 24576,
                utilization_pct: 10.0,
                temperature_c: Some(65.0),
                power_watts: Some(200.0),
                power_limit_watts: Some(350.0),
                driver_version: None,
                compute_version: None,
                performance_state: None,
                index: 0,
                hostname: "h".into(),
                timestamp: "t".into(),
                available: true,
            },
            crate::gpu::GpuInfo {
                name: "GPU-1".into(),
                uuid: "u1".into(),
                vendor: "AMD".into(),
                architecture: "RDNA3".into(),
                total_memory_bytes: 16 * 1024 * 1024 * 1024,
                used_memory_bytes: 4 * 1024 * 1024 * 1024,
                free_memory_bytes: 12 * 1024 * 1024 * 1024,
                total_memory_mb: 16384,
                free_memory_mb: 12288,
                utilization_pct: 30.0,
                temperature_c: Some(72.0),
                power_watts: Some(250.0),
                power_limit_watts: Some(300.0),
                driver_version: None,
                compute_version: None,
                performance_state: None,
                index: 1,
                hostname: "h".into(),
                timestamp: "t".into(),
                available: true,
            },
        ];

        update_gpu_metrics_from_snapshot(&m, &gpus);
        assert_eq!(m.gpu_count.load(Ordering::Relaxed), 2);
        assert_eq!(m.gpu_available_count.load(Ordering::Relaxed), 2);
        assert_eq!(m.gpu_total_vram_mb.load(Ordering::Relaxed), 24576 + 16384);
        assert_eq!(m.gpu_free_vram_mb.load(Ordering::Relaxed), 24576 + 12288);
        assert_eq!(m.gpu_temperature_celsius_max.load(Ordering::Relaxed), 72);
        assert_eq!(m.gpu_power_watts_total.load(Ordering::Relaxed), 450);
    }
}
