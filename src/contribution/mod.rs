//! Contribution accounting: tracking useful operational facts per node.
//!
//! Contribution is separate from lineage affinity, reliability, and runtime
//! confidence. It records actual useful work donated by a node.
//!
//! Inactive contribution should be capable of decaying in future policy.

use serde::{Deserialize, Serialize};

/// Contribution record for a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContributionRecord {
    pub node_id: String,
    pub accepted_work_units: u64,
    pub completed_work_units: u64,
    pub failed_work_units: u64,
    pub compute_seconds: f64,
    pub gpu_seconds: f64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub result_validation_success: u64,
    pub uptime_seconds: f64,
    pub first_seen_ms: u64,
    pub last_active_ms: u64,
}

impl ContributionRecord {
    pub fn new(node_id: &str) -> Self {
        let now = crate::types::now_ms();
        Self {
            node_id: node_id.to_string(),
            accepted_work_units: 0,
            completed_work_units: 0,
            failed_work_units: 0,
            compute_seconds: 0.0,
            gpu_seconds: 0.0,
            bytes_in: 0,
            bytes_out: 0,
            result_validation_success: 0,
            uptime_seconds: 0.0,
            first_seen_ms: now,
            last_active_ms: now,
        }
    }

    /// Record a completed work unit.
    pub fn record_completion(
        &mut self,
        compute_secs: f64,
        gpu_secs: f64,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        self.completed_work_units += 1;
        self.compute_seconds += compute_secs;
        self.gpu_seconds += gpu_secs;
        self.bytes_in += bytes_in;
        self.bytes_out += bytes_out;
        self.last_active_ms = crate::types::now_ms();
    }

    /// Record a failed work unit.
    pub fn record_failure(&mut self) {
        self.failed_work_units += 1;
        self.last_active_ms = crate::types::now_ms();
    }

    /// Record accepting a work unit.
    pub fn record_accept(&mut self) {
        self.accepted_work_units += 1;
        self.last_active_ms = crate::types::now_ms();
    }

    /// Record a successful validation.
    pub fn record_validation_success(&mut self) {
        self.result_validation_success += 1;
        self.last_active_ms = crate::types::now_ms();
    }

    /// Reliability score: completions / accepted (0.0 to 1.0).
    pub fn reliability(&self) -> f64 {
        if self.accepted_work_units == 0 {
            return 0.0;
        }
        self.completed_work_units as f64 / self.accepted_work_units as f64
    }

    /// Apply exponential decay with the given half-life in seconds.
    /// Returns a decayed copy (does not mutate).
    pub fn decayed(&self, half_life_secs: f64, now_ms: u64) -> Self {
        if half_life_secs <= 0.0 {
            return self.clone();
        }
        let elapsed_ms = now_ms.saturating_sub(self.last_active_ms);
        let elapsed_secs = elapsed_ms as f64 / 1000.0;
        let decay_factor = 0.5f64.powf(elapsed_secs / half_life_secs);
        let mut r = self.clone();
        r.completed_work_units = (self.completed_work_units as f64 * decay_factor) as u64;
        r.compute_seconds = self.compute_seconds * decay_factor;
        r.gpu_seconds = self.gpu_seconds * decay_factor;
        r.bytes_in = (self.bytes_in as f64 * decay_factor) as u64;
        r.bytes_out = (self.bytes_out as f64 * decay_factor) as u64;
        r.result_validation_success = (self.result_validation_success as f64 * decay_factor) as u64;
        r
    }
}

/// In-memory contribution ledger (backed by DashMap for concurrent access).
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct ContributionLedger {
    records: Arc<DashMap<String, ContributionRecord>>,
}

impl ContributionLedger {
    pub fn new() -> Self {
        Self {
            records: Arc::new(DashMap::new()),
        }
    }

    pub fn get_or_create(&self, node_id: &str) -> ContributionRecord {
        self.records
            .entry(node_id.to_string())
            .or_insert_with(|| ContributionRecord::new(node_id))
            .clone()
    }

    pub fn update<F: FnOnce(&mut ContributionRecord)>(&self, node_id: &str, f: F) {
        let mut entry = self
            .records
            .entry(node_id.to_string())
            .or_insert_with(|| ContributionRecord::new(node_id));
        f(entry.value_mut());
    }

    pub fn get(&self, node_id: &str) -> Option<ContributionRecord> {
        self.records.get(node_id).map(|r| r.clone())
    }

    pub fn all_records(&self) -> Vec<ContributionRecord> {
        self.records.iter().map(|r| r.value().clone()).collect()
    }

    /// Chunk 12: load the ledger from durable rows (coordinator restart).
    pub fn load_from_rows(&self, rows: Vec<crate::opsstore::ContributionRow>) {
        for row in rows {
            let rec = ContributionRecord {
                node_id: row.node_id,
                accepted_work_units: row.accepted_work_units,
                completed_work_units: row.completed_work_units,
                failed_work_units: row.failed_work_units,
                compute_seconds: row.compute_seconds,
                gpu_seconds: row.gpu_seconds,
                bytes_in: row.bytes_in,
                bytes_out: row.bytes_out,
                result_validation_success: row.result_validation_success,
                uptime_seconds: row.uptime_seconds,
                first_seen_ms: row.first_seen_ms,
                last_active_ms: row.last_active_ms,
            };
            self.records.insert(rec.node_id.clone(), rec);
        }
    }

    /// Chunk 12: snapshot the ledger as durable rows for persistence.
    pub fn to_rows(&self) -> Vec<crate::opsstore::ContributionRow> {
        self.all_records()
            .into_iter()
            .map(|r| crate::opsstore::ContributionRow {
                node_id: r.node_id,
                accepted_work_units: r.accepted_work_units,
                completed_work_units: r.completed_work_units,
                failed_work_units: r.failed_work_units,
                compute_seconds: r.compute_seconds,
                gpu_seconds: r.gpu_seconds,
                bytes_in: r.bytes_in,
                bytes_out: r.bytes_out,
                result_validation_success: r.result_validation_success,
                uptime_seconds: r.uptime_seconds,
                first_seen_ms: r.first_seen_ms,
                last_active_ms: r.last_active_ms,
                updated_at_ms: crate::types::now_ms(),
            })
            .collect()
    }

    /// Chunk 12: snapshot a single node's contribution as a durable row.
    pub fn get_row(&self, node_id: &str) -> Option<crate::opsstore::ContributionRow> {
        let r = self.get(node_id)?;
        Some(crate::opsstore::ContributionRow {
            node_id: r.node_id,
            accepted_work_units: r.accepted_work_units,
            completed_work_units: r.completed_work_units,
            failed_work_units: r.failed_work_units,
            compute_seconds: r.compute_seconds,
            gpu_seconds: r.gpu_seconds,
            bytes_in: r.bytes_in,
            bytes_out: r.bytes_out,
            result_validation_success: r.result_validation_success,
            uptime_seconds: r.uptime_seconds,
            first_seen_ms: r.first_seen_ms,
            last_active_ms: r.last_active_ms,
            updated_at_ms: crate::types::now_ms(),
        })
    }

    /// Chunk 12: flush a single node's contribution row to the durable store.
    pub fn flush_node(&self, ops: &crate::opsstore::OpsStore, node_id: &str) {
        if let Some(row) = self.get_row(node_id) {
            let _ = ops.upsert_contribution(&row);
        }
    }
}

impl Default for ContributionLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_record_has_zero_counts() {
        let r = ContributionRecord::new("node-1");
        assert_eq!(r.completed_work_units, 0);
        assert_eq!(r.failed_work_units, 0);
        assert_eq!(r.accepted_work_units, 0);
    }

    #[test]
    fn record_completion() {
        let mut r = ContributionRecord::new("node-1");
        r.record_accept();
        r.record_completion(10.0, 5.0, 1024, 2048);
        assert_eq!(r.completed_work_units, 1);
        assert_eq!(r.compute_seconds, 10.0);
        assert_eq!(r.gpu_seconds, 5.0);
        assert_eq!(r.bytes_in, 1024);
        assert_eq!(r.bytes_out, 2048);
    }

    #[test]
    fn reliability_score() {
        let mut r = ContributionRecord::new("node-1");
        r.record_accept();
        r.record_accept();
        r.record_completion(1.0, 1.0, 0, 0);
        r.record_failure();
        assert_eq!(r.reliability(), 0.5);
    }

    #[test]
    fn decay_reduces_counts() {
        let mut r = ContributionRecord::new("node-1");
        r.record_completion(100.0, 50.0, 10000, 20000);
        r.last_active_ms = crate::types::now_ms() - 10_000; // 10 seconds ago
        let decayed = r.decayed(10.0, crate::types::now_ms());
        // Half-life of 10 seconds, 10 seconds elapsed -> 0.5 factor
        assert_eq!(decayed.completed_work_units, 0); // 1 * 0.5 = 0 (integer)
        assert!((decayed.compute_seconds - 50.0).abs() < 1.0);
        assert!((decayed.gpu_seconds - 25.0).abs() < 1.0);
    }

    #[test]
    fn ledger_operations() {
        let ledger = ContributionLedger::new();
        ledger.update("node-1", |r| r.record_accept());
        ledger.update("node-1", |r| r.record_completion(5.0, 2.5, 100, 200));
        let r = ledger.get("node-1").unwrap();
        assert_eq!(r.accepted_work_units, 1);
        assert_eq!(r.completed_work_units, 1);
    }

    #[test]
    fn ledger_durable_roundtrip() {
        let ops = crate::opsstore::OpsStore::open("sqlite::memory:").unwrap();
        let ledger = ContributionLedger::new();
        ledger.update("node-1", |r| {
            r.record_accept();
            r.record_completion(10.0, 5.0, 1024, 2048);
        });
        ledger.flush_node(&ops, "node-1");
        assert_eq!(ops.load_contributions().unwrap().len(), 1);

        // Reload into a fresh ledger (simulates coordinator restart).
        let recovered = ContributionLedger::new();
        recovered.load_from_rows(ops.load_contributions().unwrap());
        let r = recovered.get("node-1").unwrap();
        assert_eq!(r.completed_work_units, 1);
        assert_eq!(r.bytes_in, 1024);
        assert_eq!(r.last_active_ms, r.last_active_ms);
    }
}
