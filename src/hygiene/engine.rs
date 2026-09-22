//! Evidence Hygiene Engine implementing Great Idea #87 hygiene, compaction, and lifecycle maintenance.

use super::policy::HygienePolicy;
use super::sanitizer::EvidenceSanitizer;
use crate::index::compiler_memory::{CompilerIncident, CompilerMemoryStore};
use crate::index::storage::StructuralStorage;
use crate::types::now_ms;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

/// Detailed report generated at the completion of an evidence hygiene cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HygieneCycleReport {
    pub sanitized_incidents_count: usize,
    pub orphaned_chunks_purged: usize,
    pub stale_incidents_expired: usize,
    pub duplicate_incidents_compacted: usize,
    pub integrity_records_checked: usize,
    pub integrity_violations_count: usize,
    pub total_symbols_after_cycle: usize,
    pub total_incidents_after_cycle: usize,
    pub total_chunks_after_cycle: usize,
    pub duration_ms: u64,
}

/// Core engine orchestrating hygiene, sanitization, and compaction rules.
pub struct EvidenceHygieneEngine {
    policy: HygienePolicy,
    sanitizer: EvidenceSanitizer,
}

impl EvidenceHygieneEngine {
    /// Creates a new EvidenceHygieneEngine with the specified policy.
    pub fn new(policy: HygienePolicy) -> Self {
        Self {
            policy,
            sanitizer: EvidenceSanitizer::new(),
        }
    }

    /// Runs a full hygiene cycle across structural storage and compiler memory.
    pub fn run_hygiene_cycle(
        &self,
        storage: &StructuralStorage,
        compiler_mem: &CompilerMemoryStore,
    ) -> Result<HygieneCycleReport, crate::types::Error> {
        let start_time = Instant::now();
        let current_time_ms = now_ms();

        // 1. Sanitize compiler incidents in place
        let mut sanitized_count = 0;
        let all_incidents = compiler_mem.list_all_incidents()?;
        for inc in &all_incidents {
            let sanitized = self.sanitizer.sanitize_incident(inc);
            if sanitized.diagnostic != inc.diagnostic {
                compiler_mem.update_incident(&sanitized)?;
                sanitized_count += 1;
            }
        }

        // 2. Purge orphaned symbol chunks
        let orphaned_chunks_purged = if self.policy.enable_orphan_purge {
            storage.purge_orphaned_chunks()?
        } else {
            0
        };

        // 3. Compact duplicate compiler incidents
        let duplicate_incidents_compacted = if self.policy.enable_deduplication {
            self.compact_duplicate_incidents(compiler_mem)?
        } else {
            0
        };

        // 4. Purge stale unverified incidents past TTL or quota limit
        let mut stale_incidents_expired = compiler_mem
            .purge_stale_unverified(self.policy.ttl_unverified_incidents_ms, current_time_ms)?;

        // Enforce max_unverified_per_error_code limit
        stale_incidents_expired += self.enforce_error_code_quotas(compiler_mem)?;

        // 5. Verify integrity of all stored symbols
        let (checked_count, violation_count) = self.verify_symbols_integrity(storage)?;

        // 6. Vacuum databases if enabled
        if self.policy.enable_auto_vacuum {
            let _ = storage.vacuum();
            let _ = compiler_mem.vacuum();
        }

        let total_symbols = storage.count_symbols()?;
        let total_incidents = compiler_mem.count_incidents()?;
        let total_chunks = storage.count_chunks()?;
        let duration_ms = start_time.elapsed().as_millis() as u64;

        let report = HygieneCycleReport {
            sanitized_incidents_count: sanitized_count,
            orphaned_chunks_purged,
            stale_incidents_expired,
            duplicate_incidents_compacted,
            integrity_records_checked: checked_count,
            integrity_violations_count: violation_count,
            total_symbols_after_cycle: total_symbols,
            total_incidents_after_cycle: total_incidents,
            total_chunks_after_cycle: total_chunks,
            duration_ms,
        };

        info!(
            "Completed evidence hygiene cycle in {}ms: {} orphaned purged, {} stale expired, {} compacted",
            duration_ms, orphaned_chunks_purged, stale_incidents_expired, duplicate_incidents_compacted
        );

        Ok(report)
    }

    /// Computes cryptographic SHA-256 digest of arbitrary payload.
    pub fn compute_sha256(payload: &[u8]) -> String {
        let mut context = Context::new(&SHA256);
        context.update(payload);
        let digest = context.finish();
        hex::encode(digest.as_ref())
    }

    fn compact_duplicate_incidents(
        &self,
        compiler_mem: &CompilerMemoryStore,
    ) -> Result<usize, crate::types::Error> {
        let incidents = compiler_mem.list_all_incidents()?;
        // Group by (repository_id, sorted error codes, normalized diagnostic)
        let mut groups: HashMap<String, Vec<CompilerIncident>> = HashMap::new();

        for inc in incidents {
            let mut sorted_codes = inc.error_codes.clone();
            sorted_codes.sort();
            let key = format!(
                "{}::{}::{}",
                inc.repository_id,
                sorted_codes.join(","),
                inc.diagnostic.trim()
            );
            groups.entry(key).or_default().push(inc);
        }

        let mut compacted_count = 0;
        for (_, group) in groups {
            if group.len() > 1 {
                // Keep the most recent verified incident or the newest recorded incident
                let mut sorted = group;
                sorted.sort_by(|a, b| {
                    b.verified_fixed
                        .cmp(&a.verified_fixed)
                        .then_with(|| b.recorded_at_ms.cmp(&a.recorded_at_ms))
                });

                let mut canonical = sorted[0].clone();
                let mut all_affected = canonical.affected_symbol_ids.clone();

                for duplicate in &sorted[1..] {
                    for sym in &duplicate.affected_symbol_ids {
                        if !all_affected.contains(sym) {
                            all_affected.push(sym.clone());
                        }
                    }
                    // Delete the duplicate record
                    compiler_mem.delete_incident(&duplicate.incident_id)?;
                    compacted_count += 1;
                }

                canonical.affected_symbol_ids = all_affected;
                compiler_mem.update_incident(&canonical)?;
            }
        }

        Ok(compacted_count)
    }

    fn enforce_error_code_quotas(
        &self,
        compiler_mem: &CompilerMemoryStore,
    ) -> Result<usize, crate::types::Error> {
        let incidents = compiler_mem.list_all_incidents()?;
        let mut by_code: HashMap<String, Vec<CompilerIncident>> = HashMap::new();

        for inc in incidents {
            if !inc.verified_fixed {
                for code in &inc.error_codes {
                    by_code.entry(code.clone()).or_default().push(inc.clone());
                }
            }
        }

        let mut purged = 0;
        for (_, mut group) in by_code {
            if group.len() > self.policy.max_unverified_per_error_code {
                group.sort_by(|a, b| b.recorded_at_ms.cmp(&a.recorded_at_ms));
                for excess in &group[self.policy.max_unverified_per_error_code..] {
                    if compiler_mem.delete_incident(&excess.incident_id)? {
                        purged += 1;
                    }
                }
            }
        }

        Ok(purged)
    }

    fn verify_symbols_integrity(
        &self,
        storage: &StructuralStorage,
    ) -> Result<(usize, usize), crate::types::Error> {
        let symbols = storage.list_all_symbols()?;
        let checked = symbols.len();
        let mut violations = 0;

        for sym in &symbols {
            if sym.content_hash.is_empty() {
                violations += 1;
            }
        }

        Ok((checked, violations))
    }
}
