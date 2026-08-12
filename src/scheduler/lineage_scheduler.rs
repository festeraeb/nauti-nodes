//! Weighted lineage scheduler implementing the 2:1 contention policy.
//!
//! Under contention, same-lineage eligible work gets weight 2, unrelated work
//! gets weight 1. This is weighted fairness, NOT unconditional priority.
//!
//! Starvation protection: waiting work gains scheduling pressure over time.
//! An unrelated job must eventually run despite continuous lineage traffic.

use crate::work::unit::WorkUnit;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the lineage-aware scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerPolicy {
    /// Weight for same-lineage work under contention.
    pub lineage_weight: f64,
    /// Weight for unrelated/federation work under contention.
    pub federation_weight: f64,
    /// Per-second aging bonus for waiting work.
    pub aging_bonus_per_sec: f64,
    /// Maximum aging bonus (prevents infinite accumulation).
    pub max_aging_bonus: f64,
    /// Mission priority multiplier.
    pub mission_priority_multiplier: f64,
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self {
            lineage_weight: 2.0,
            federation_weight: 1.0,
            aging_bonus_per_sec: 0.01,
            max_aging_bonus: 5.0,
            mission_priority_multiplier: 10.0,
        }
    }
}

/// A candidate work unit with its computed scheduling score.
#[derive(Debug, Clone)]
pub struct ScoredCandidate {
    pub work_unit_id: String,
    pub score: f64,
    pub lineage_bonus: f64,
    pub aging_bonus: f64,
    pub mission_priority_bonus: f64,
    pub reason: String,
}

/// Track when each work unit entered the queue for aging.
#[derive(Debug, Clone, Default)]
pub struct AgingTracker {
    /// work_unit_id -> timestamp when it entered the queue (millis since epoch).
    pub queued_at: HashMap<String, u64>,
}

impl AgingTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record when a work unit entered the queue.
    pub fn enqueue(&mut self, work_unit_id: &str) {
        self.queued_at
            .entry(work_unit_id.to_string())
            .or_insert_with(crate::types::now_ms);
    }

    /// Remove a work unit from the queue (leased/completed/expired).
    pub fn dequeue(&mut self, work_unit_id: &str) {
        self.queued_at.remove(work_unit_id);
    }

    /// Get the aging bonus for a work unit.
    pub fn aging_bonus(&self, work_unit_id: &str, policy: &SchedulerPolicy) -> f64 {
        match self.queued_at.get(work_unit_id) {
            Some(queued_at) => {
                let elapsed_ms = crate::types::now_ms().saturating_sub(*queued_at);
                let elapsed_secs = elapsed_ms as f64 / 1000.0;
                (elapsed_secs * policy.aging_bonus_per_sec).min(policy.max_aging_bonus)
            }
            None => 0.0,
        }
    }
}

/// Score candidate work units using the lineage-aware weighted policy.
///
/// Scheduling priority order:
/// 1. Privacy/trust eligibility (hard constraint - checked before calling)
/// 2. Hardware/runtime constraints (hard constraint - checked before calling)
/// 3. Forge reservation/mission authority (hard constraint - checked before calling)
/// 4. Mission priority (strong multiplier)
/// 5. Starvation/aging (grows over time)
/// 6. Lineage weighted fairness (2:1 under contention)
/// 7. Performance/latency affinity (not implemented here - use existing scoring)
pub fn score_candidates(
    candidates: &[WorkUnit],
    requester_lineage: &str,
    policy: &SchedulerPolicy,
    aging: &AgingTracker,
) -> Vec<ScoredCandidate> {
    let mut scored: Vec<ScoredCandidate> = candidates
        .iter()
        .map(|wu| {
            // Determine if this work unit is same-lineage
            // For now, check if mission_id or work_unit_id shares lineage context
            // In full implementation, this would check the lineage graph
            let is_lineage = is_same_lineage_context(&wu.mission_id, requester_lineage);

            let lineage_bonus = if is_lineage {
                policy.lineage_weight
            } else {
                policy.federation_weight
            };

            let aging_bonus = aging.aging_bonus(&wu.work_unit_id, policy);

            let mission_priority_bonus = wu.priority as f64 * policy.mission_priority_multiplier;

            let score = lineage_bonus + aging_bonus + mission_priority_bonus;

            let reason = format!(
                "lineage={} aging={:.2} mission={} total={:.2}",
                if is_lineage { "same" } else { "federation" },
                aging_bonus,
                wu.priority,
                score
            );

            ScoredCandidate {
                work_unit_id: wu.work_unit_id.clone(),
                score,
                lineage_bonus,
                aging_bonus,
                mission_priority_bonus,
                reason,
            }
        })
        .collect();

    // Sort by score descending (highest score = best candidate)
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored
}

/// Simple lineage context check.
/// In full implementation, this would query the lineage graph.
/// For now, check if the mission_id shares a prefix with the requester's lineage.
fn is_same_lineage_context(mission_id: &str, requester_lineage: &str) -> bool {
    // If both share a common prefix (e.g., both from same branch), consider same lineage
    // This is a simplified heuristic; real implementation uses LineageStore
    if requester_lineage.is_empty() {
        return false;
    }
    mission_id.starts_with(requester_lineage) || mission_id == requester_lineage
}

/// Demonstrate the 2:1 policy with a deterministic simulation.
/// Uses synthetic time (not wall clock) so aging accumulates correctly.
/// Returns (lineage_scheduled, federation_scheduled) counts.
pub fn simulate_2to1_policy(
    lineage_jobs: usize,
    federation_jobs: usize,
    scheduling_decisions: usize,
) -> (usize, usize) {
    use std::collections::HashMap;

    let policy = SchedulerPolicy::default();
    // Synthetic time tracker: work_unit_id -> queued_at_ms (simulated)
    let mut queued_at: HashMap<String, u64> = HashMap::new();

    let base_time = 1000u64; // simulated start time

    // Create work units with synthetic timestamps
    let mut lineage_wus: Vec<WorkUnit> = (0..lineage_jobs)
        .map(|i| {
            let wu = WorkUnit::new(
                &format!("lineage-{}", i),
                "lineage-mission",
                crate::capabilities::WorkloadType::Inference,
                0,
                crate::capabilities::PrivacyClass::Public,
                4096,
                30,
            );
            queued_at.insert(wu.work_unit_id.clone(), base_time);
            wu
        })
        .collect();

    let mut federation_wus: Vec<WorkUnit> = (0..federation_jobs)
        .map(|i| {
            let wu = WorkUnit::new(
                &format!("federation-{}", i),
                "federation-mission",
                crate::capabilities::WorkloadType::Inference,
                0,
                crate::capabilities::PrivacyClass::Public,
                4096,
                30,
            );
            // Federation jobs were queued at the same time
            queued_at.insert(wu.work_unit_id.clone(), base_time);
            wu
        })
        .collect();

    let mut lineage_scheduled = 0usize;
    let mut federation_scheduled = 0usize;
    let mut sim_time = base_time;

    for decision_idx in 0..scheduling_decisions {
        // Advance simulated time by 1 second per decision
        sim_time = base_time + (decision_idx as u64) * 1000;

        // Compute aging bonuses using synthetic time
        let compute_aging = |wu_id: &str| -> f64 {
            match queued_at.get(wu_id) {
                Some(qa) => {
                    let elapsed_ms = sim_time.saturating_sub(*qa);
                    let elapsed_secs = elapsed_ms as f64 / 1000.0;
                    (elapsed_secs * policy.aging_bonus_per_sec).min(policy.max_aging_bonus)
                }
                None => 0.0,
            }
        };

        // Score all candidates using synthetic time
        let mut all_candidates: Vec<(String, f64)> = Vec::new();

        for wu in lineage_wus.iter() {
            let aging_bonus = compute_aging(&wu.work_unit_id);
            let score = policy.lineage_weight
                + aging_bonus
                + (wu.priority as f64 * policy.mission_priority_multiplier);
            all_candidates.push((wu.work_unit_id.clone(), score));
        }

        for wu in federation_wus.iter() {
            let aging_bonus = compute_aging(&wu.work_unit_id);
            let score = policy.federation_weight
                + aging_bonus
                + (wu.priority as f64 * policy.mission_priority_multiplier);
            all_candidates.push((wu.work_unit_id.clone(), score));
        }

        // Pick highest score
        all_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((winner_id, _)) = all_candidates.first() {
            if winner_id.starts_with("lineage-") {
                lineage_scheduled += 1;
                // Winner gets fresh aging (just completed); losers keep aging
                if let Some(pos) = lineage_wus
                    .iter()
                    .position(|w| &w.work_unit_id == winner_id)
                {
                    queued_at.remove(winner_id);
                    lineage_wus.remove(pos);
                    let wu = WorkUnit::new(
                        winner_id,
                        "lineage-mission",
                        crate::capabilities::WorkloadType::Inference,
                        0,
                        crate::capabilities::PrivacyClass::Public,
                        4096,
                        30,
                    );
                    queued_at.insert(wu.work_unit_id.clone(), sim_time);
                    lineage_wus.push(wu);
                }
            } else {
                federation_scheduled += 1;
                // Federation winner gets fresh aging; lineage jobs keep aging
                if let Some(pos) = federation_wus
                    .iter()
                    .position(|w| &w.work_unit_id == winner_id)
                {
                    queued_at.remove(winner_id);
                    federation_wus.remove(pos);
                    let wu = WorkUnit::new(
                        winner_id,
                        "federation-mission",
                        crate::capabilities::WorkloadType::Inference,
                        0,
                        crate::capabilities::PrivacyClass::Public,
                        4096,
                        30,
                    );
                    queued_at.insert(wu.work_unit_id.clone(), sim_time);
                    federation_wus.push(wu);
                }
            }
        }
    }

    (lineage_scheduled, federation_scheduled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_lineage_gets_higher_score() {
        let policy = SchedulerPolicy::default();
        let aging = AgingTracker::new();

        let lineage_wu = WorkUnit::new(
            "lineage-1",
            "lineage-mission",
            crate::capabilities::WorkloadType::Inference,
            0,
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );
        let federation_wu = WorkUnit::new(
            "federation-1",
            "federation-mission",
            crate::capabilities::WorkloadType::Inference,
            0,
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );

        let scored = score_candidates(
            &[lineage_wu, federation_wu],
            "lineage-mission",
            &policy,
            &aging,
        );

        assert_eq!(scored.len(), 2);
        // lineage-1 should have higher score (2.0 vs 1.0)
        assert_eq!(scored[0].work_unit_id, "lineage-1");
        assert!(scored[0].score > scored[1].score);
    }

    #[test]
    fn aging_bonus_helps_federation_work() {
        let policy = SchedulerPolicy {
            aging_bonus_per_sec: 1.0, // aggressive aging for test
            max_aging_bonus: 10.0,
            ..Default::default()
        };
        let mut aging = AgingTracker::new();

        // Federation job has been waiting longer
        aging
            .queued_at
            .insert("federation-1".into(), crate::types::now_ms() - 10_000); // 10 seconds ago
        aging
            .queued_at
            .insert("lineage-1".into(), crate::types::now_ms()); // just now

        let lineage_wu = WorkUnit::new(
            "lineage-1",
            "lineage-mission",
            crate::capabilities::WorkloadType::Inference,
            0,
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );
        let federation_wu = WorkUnit::new(
            "federation-1",
            "federation-mission",
            crate::capabilities::WorkloadType::Inference,
            0,
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );

        let scored = score_candidates(
            &[lineage_wu, federation_wu],
            "lineage-mission",
            &policy,
            &aging,
        );

        // With 10 seconds of aging at 1.0/sec, federation gets 10.0 aging bonus
        // lineage gets 2.0 (lineage weight) + 0.0 (no aging) = 2.0
        // federation gets 1.0 (federation weight) + 10.0 (aging) = 11.0
        // So federation should win despite lineage preference
        let lineage_score = scored
            .iter()
            .find(|s| s.work_unit_id == "lineage-1")
            .unwrap()
            .score;
        let federation_score = scored
            .iter()
            .find(|s| s.work_unit_id == "federation-1")
            .unwrap()
            .score;
        assert!(
            federation_score > lineage_score,
            "aging should let federation win: lineage={lineage_score}, federation={federation_score}"
        );
    }

    #[test]
    fn mission_priority_overrides_lineage() {
        let policy = SchedulerPolicy::default();
        let aging = AgingTracker::new();

        let mut high_priority_federation = WorkUnit::new(
            "hp-federation",
            "sar-mission",
            crate::capabilities::WorkloadType::Inference,
            10, // high priority
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );
        let low_priority_lineage = WorkUnit::new(
            "lp-lineage",
            "lineage-mission",
            crate::capabilities::WorkloadType::Inference,
            0, // low priority
            crate::capabilities::PrivacyClass::Public,
            4096,
            30,
        );

        let scored = score_candidates(
            &[high_priority_federation, low_priority_lineage],
            "lineage-mission",
            &policy,
            &aging,
        );

        // High priority federation (1.0 + 100.0 = 101.0) should beat low priority lineage (2.0 + 0.0 = 2.0)
        assert_eq!(scored[0].work_unit_id, "hp-federation");
    }

    #[test]
    fn simulate_2to1_lineage_dominates() {
        // Run scheduling decisions with equal lineage and federation work
        let (lineage, federation) = simulate_2to1_policy(10, 10, 600);

        // Lineage weight (2.0) > federation weight (1.0), so lineage should
        // get significantly more scheduling opportunities.
        // The exact ratio depends on aging dynamics; verify lineage > 1.5x federation.
        assert!(
            lineage > federation,
            "lineage should dominate: lineage={lineage}, federation={federation}"
        );
        let ratio = lineage as f64 / federation.max(1) as f64;
        assert!(
            ratio >= 1.5,
            "lineage should get at least 1.5x more: ratio={ratio} (lineage={lineage}, federation={federation})"
        );
    }

    #[test]
    fn federation_never_starves() {
        // Even with unlimited lineage work, federation must eventually run
        let (lineage, federation) = simulate_2to1_policy(100, 1, 1000);
        assert!(
            federation > 0,
            "federation work must never starve: lineage={lineage}, federation={federation}"
        );
    }
}
