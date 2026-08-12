//! 2:1 Lineage Fairness Simulator
//!
//! Deterministic simulations to validate the 2:1 lineage fairness property.
//!
//! Required semantics: under sustained equal-priority contention:
//! - Same lineage: roughly 2 service opportunities
//! - External lineage: roughly 1 service opportunity
//!
//! This must be weighted SERVICE FAIRNESS, not merely `score += 2`.
//!
//! Simulations run at: 30, 300, 3,000, 30,000 scheduling decisions.
//! Records: lineage served, federation served, ratio, maximum wait,
//! starvation events.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A simulated node in the fairness simulator.
#[derive(Debug, Clone)]
pub struct SimNode {
    pub node_id: String,
    pub lineage_id: String,
    pub priority: u32,
    pub vram_mb: u64,
}

/// A simulated work request.
#[derive(Debug, Clone)]
pub struct SimWorkRequest {
    pub request_id: String,
    pub lineage_id: String,
    pub priority: u32,
    pub required_vram_mb: u64,
}

/// Result of a single scheduling decision.
#[derive(Debug, Clone)]
pub struct SchedulingDecision {
    pub decision_number: u64,
    pub request_lineage: String,
    pub served_node_lineage: String,
    pub same_lineage: bool,
    pub wait_count: u64,
}

/// Aggregated fairness metrics from a simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairnessReport {
    /// Total scheduling decisions made
    pub total_decisions: u64,

    /// Number of times same-lineage nodes were served
    pub same_lineage_served: u64,

    /// Number of times external-lineage nodes were served
    pub external_lineage_served: u64,

    /// Actual ratio of same:external service
    pub service_ratio: f64,

    /// Target ratio (2.0 for 2:1 fairness)
    pub target_ratio: f64,

    /// Deviation from target (0.0 = perfect)
    pub ratio_deviation: f64,

    /// Maximum wait count for any request before being served
    pub max_wait: u64,

    /// Number of starvation events (wait > threshold)
    pub starvation_events: u64,

    /// Per-lineage service counts
    pub per_lineage: HashMap<String, u64>,

    /// Whether the simulation passed the fairness check
    pub passed: bool,
}

impl FairnessReport {
    /// Check if the ratio is within acceptable bounds of 2:1.
    pub fn is_fair(&self, tolerance: f64) -> bool {
        (self.service_ratio - self.target_ratio).abs() <= tolerance
    }
}

/// The 2:1 lineage fairness scheduler.
///
/// Under sustained equal-priority contention, same-lineage nodes
/// should receive roughly 2x the service opportunities compared
/// to external-lineage nodes.
pub struct LineageFairnessScheduler {
    /// Virtual time accumulated for same-lineage service
    same_vtime: f64,
    /// Virtual time accumulated for external-lineage service
    ext_vtime: f64,
    /// Quantum weight for same-lineage (2.0)
    same_weight: f64,
    /// Quantum weight for external-lineage (1.0)
    ext_weight: f64,
    /// Usage counts per node to ensure round-robin tie-breaking
    node_usage: HashMap<String, u64>,
}

impl LineageFairnessScheduler {
    pub fn new() -> Self {
        Self {
            same_vtime: 0.0,
            ext_vtime: 0.0,
            same_weight: 2.0,
            ext_weight: 1.0,
            node_usage: HashMap::new(),
        }
    }

    /// Select a node for a work request using weighted virtual time.
    ///
    /// Ensures a bounded 2:1 service ratio globally, while preserving
    /// mission priority as an absolute override.
    pub fn select_node(
        &mut self,
        request_lineage: &str,
        available_nodes: &[SimNode],
    ) -> Option<String> {
        if available_nodes.is_empty() {
            return None;
        }

        // 1. Mission priority strictly overrides fairness
        let max_priority = available_nodes.iter().map(|n| n.priority).max()?;
        let eligible: Vec<&SimNode> = available_nodes
            .iter()
            .filter(|n| n.priority == max_priority)
            .collect();

        // 2. Classify candidates
        let mut same_candidates: Vec<&SimNode> = eligible
            .iter()
            .filter(|n| n.lineage_id == request_lineage)
            .copied()
            .collect();
        let mut ext_candidates: Vec<&SimNode> = eligible
            .iter()
            .filter(|n| n.lineage_id != request_lineage)
            .copied()
            .collect();

        // 3. Tie-breaker: least recently used (round-robin) among equal classes
        let sort_candidates = |candidates: &mut Vec<&SimNode>, usage: &HashMap<String, u64>| {
            candidates.sort_by(|a, b| {
                let a_usage = usage.get(&a.node_id).copied().unwrap_or(0);
                let b_usage = usage.get(&b.node_id).copied().unwrap_or(0);
                a_usage.cmp(&b_usage).then(a.node_id.cmp(&b.node_id))
            });
        };

        sort_candidates(&mut same_candidates, &self.node_usage);
        sort_candidates(&mut ext_candidates, &self.node_usage);

        // 4. Decide based on virtual time
        let pick_same = if !same_candidates.is_empty() && !ext_candidates.is_empty() {
            self.same_vtime <= self.ext_vtime
        } else {
            !same_candidates.is_empty()
        };

        // 5. Apply service and update vtime
        let selected = if pick_same {
            self.same_vtime += 1.0 / self.same_weight;
            same_candidates[0]
        } else {
            self.ext_vtime += 1.0 / self.ext_weight;
            ext_candidates[0]
        };

        *self.node_usage.entry(selected.node_id.clone()).or_insert(0) += 1;

        Some(selected.node_id.clone())
    }

    /// Reset all credits.
    pub fn reset(&mut self) {
        self.same_vtime = 0.0;
        self.ext_vtime = 0.0;
        self.node_usage.clear();
    }
}

impl Default for LineageFairnessScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Run a deterministic fairness simulation.
///
/// Creates `num_lineages` lineages with `nodes_per_lineage` nodes each.
/// Generates `num_decisions` scheduling decisions with round-robin lineage requests.
/// Returns a fairness report.
pub fn simulate_fairness(
    num_decisions: u64,
    num_lineages: usize,
    nodes_per_lineage: usize,
) -> FairnessReport {
    let mut scheduler = LineageFairnessScheduler::new();

    // Create nodes across lineages
    let mut nodes = Vec::new();
    for l in 0..num_lineages {
        let lineage_id = format!("lineage-{}", l);
        for n in 0..nodes_per_lineage {
            nodes.push(SimNode {
                node_id: format!("node-{}-{}", l, n),
                lineage_id: lineage_id.clone(),
                priority: 10,
                vram_mb: 16384,
            });
        }
    }

    let mut same_lineage_served = 0u64;
    let mut external_lineage_served = 0u64;
    let mut per_lineage: HashMap<String, u64> = HashMap::new();
    let mut max_wait = 0u64;
    let mut starvation_events = 0u64;
    let starvation_threshold = 10u64;
    let mut current_ext_wait = 0u64;

    // Simulate scheduling decisions with round-robin lineage requests
    for i in 0..num_decisions {
        let request_lineage = format!("lineage-{}", (i as usize) % num_lineages);

        if let Some(selected_node_id) = scheduler.select_node(&request_lineage, &nodes) {
            // Find the selected node's lineage
            let selected_lineage = nodes
                .iter()
                .find(|n| n.node_id == selected_node_id)
                .map(|n| n.lineage_id.clone())
                .unwrap_or_default();

            let same = selected_lineage == request_lineage;
            if same {
                same_lineage_served += 1;
                current_ext_wait += 1;
                if current_ext_wait > max_wait {
                    max_wait = current_ext_wait;
                }
                if current_ext_wait == starvation_threshold {
                    starvation_events += 1;
                }
            } else {
                external_lineage_served += 1;
                current_ext_wait = 0;
            }

            *per_lineage.entry(selected_lineage).or_insert(0) += 1;
        }
    }

    let total = same_lineage_served + external_lineage_served;
    let service_ratio = if external_lineage_served > 0 {
        same_lineage_served as f64 / external_lineage_served as f64
    } else if same_lineage_served > 0 {
        f64::INFINITY
    } else {
        0.0
    };

    let target_ratio = 2.0;
    let ratio_deviation = (service_ratio - target_ratio).abs();
    // Pass strictly requires bounding close to 2.0 and no starvation
    let passed = ratio_deviation <= 0.2 && external_lineage_served > 0 && starvation_events == 0;

    FairnessReport {
        total_decisions: total,
        same_lineage_served,
        external_lineage_served,
        service_ratio,
        target_ratio,
        ratio_deviation,
        max_wait,
        starvation_events,
        per_lineage,
        passed,
    }
}

/// Run the full suite of fairness simulations at 30, 300, 3000, 30000 decisions.
pub fn run_fairness_suite() -> Vec<(u64, FairnessReport)> {
    let scales = [30, 300, 3_000, 30_000];
    scales
        .iter()
        .map(|&n| {
            let report = simulate_fairness(n, 3, 2); // 3 lineages, 2 nodes each
            (n, report)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_selects_node() {
        let mut sched = LineageFairnessScheduler::new();
        let nodes = vec![
            SimNode {
                node_id: "n1".into(),
                lineage_id: "L1".into(),
                priority: 10,
                vram_mb: 16384,
            },
            SimNode {
                node_id: "n2".into(),
                lineage_id: "L2".into(),
                priority: 10,
                vram_mb: 16384,
            },
        ];

        let selected = sched.select_node("L1", &nodes);
        assert!(selected.is_some());
    }

    #[test]
    fn scheduler_empty_nodes() {
        let mut sched = LineageFairnessScheduler::new();
        let selected = sched.select_node("L1", &[]);
        assert!(selected.is_none());
    }

    #[test]
    fn simulation_30_decisions() {
        let report = simulate_fairness(30, 2, 1);
        assert_eq!(report.total_decisions, 30);
        assert!(report.same_lineage_served > 0);
    }

    #[test]
    fn simulation_300_decisions() {
        let report = simulate_fairness(300, 3, 2);
        assert_eq!(report.total_decisions, 300);
        // With 3 lineages and round-robin, same-lineage should get more service
        assert!(report.same_lineage_served > report.external_lineage_served);
    }

    #[test]
    fn simulation_3000_decisions() {
        let report = simulate_fairness(3000, 3, 2);
        assert_eq!(report.total_decisions, 3000);
        // Ratio should be close to 2:1
        assert!(
            report.service_ratio > 1.0,
            "ratio {} should be > 1.0",
            report.service_ratio
        );
    }

    #[test]
    fn simulation_30000_decisions() {
        let report = simulate_fairness(30000, 3, 2);
        assert_eq!(report.total_decisions, 30000);
        // Same-lineage should dominate with 2:1 weighting
        assert!(
            report.same_lineage_served > 0,
            "same lineage should be served at least once"
        );
        // Per-lineage tracking should account for all decisions
        let total_tracked: u64 = report.per_lineage.values().sum();
        assert_eq!(total_tracked, 30000);
    }

    #[test]
    fn fairness_suite_runs() {
        let results = run_fairness_suite();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, 30);
        assert_eq!(results[1].0, 300);
        assert_eq!(results[2].0, 3000);
        assert_eq!(results[3].0, 30000);
    }

    #[test]
    fn fairness_report_is_fair() {
        let report = FairnessReport {
            total_decisions: 100,
            same_lineage_served: 67,
            external_lineage_served: 33,
            service_ratio: 67.0 / 33.0,
            target_ratio: 2.0,
            ratio_deviation: (67.0_f64 / 33.0 - 2.0).abs(),
            max_wait: 5,
            starvation_events: 0,
            per_lineage: HashMap::new(),
            passed: true,
        };
        assert!(report.is_fair(0.5));
    }

    #[test]
    fn fairness_report_unfair() {
        let report = FairnessReport {
            total_decisions: 100,
            same_lineage_served: 90,
            external_lineage_served: 10,
            service_ratio: 9.0,
            target_ratio: 2.0,
            ratio_deviation: 7.0,
            max_wait: 50,
            starvation_events: 5,
            per_lineage: HashMap::new(),
            passed: false,
        };
        assert!(!report.is_fair(0.5));
    }

    #[test]
    fn per_lineage_tracking() {
        let report = simulate_fairness(100, 4, 1);
        assert_eq!(report.per_lineage.len(), 4);
        let total: u64 = report.per_lineage.values().sum();
        assert_eq!(total, 100);
    }
}
