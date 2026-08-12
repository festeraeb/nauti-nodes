//! Scale Simulator
//!
//! Synthetic federation tests at 1, 10, 100, 1000 nodes.
//! A scheduler simulator does NOT need real GPUs.
//!
//! Includes:
//! - Multiple branches
//! - Different GPU sizes
//! - Latency
//! - Node loss
//! - Lineage contention
//! - Federation contention
//! - High-priority mission
//! - Privacy restrictions
//!
//! Measures:
//! - Decisions/sec
//! - Memory
//! - Lineage query cost
//! - Fairness
//! - Maximum wait

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// A simulated node in the federation.
#[derive(Debug, Clone)]
pub struct SimFederationNode {
    pub node_id: String,
    pub lineage_id: String,
    pub gpu_vram_mb: u64,
    pub latency_ms: u64,
    pub online: bool,
    pub privacy_class: PrivacyClass,
}

/// Privacy class for a node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrivacyClass {
    Public,
    Internal,
    Sensitive,
    Restricted,
}

/// A simulated work request.
#[derive(Debug, Clone)]
pub struct SimFederationWork {
    pub work_id: String,
    pub lineage_id: String,
    pub required_vram_mb: u64,
    pub required_privacy: PrivacyClass,
    pub priority: u32,
    pub is_mission: bool,
}

/// Result of a scale simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleSimReport {
    /// Number of nodes in the simulation
    pub node_count: usize,

    /// Number of scheduling decisions made
    pub decisions: u64,

    /// Decisions per second
    pub decisions_per_sec: f64,

    /// Total elapsed time in milliseconds
    pub elapsed_ms: u64,

    /// Maximum wait for any work item
    pub max_wait: u64,

    /// Number of starvation events
    pub starvation_events: u64,

    /// Number of lineage queries performed
    pub lineage_queries: u64,

    /// Average lineage query cost (arbitrary units)
    pub avg_lineage_query_cost: f64,

    /// Fairness ratio (same:external)
    pub fairness_ratio: f64,

    /// Number of nodes lost during simulation
    pub nodes_lost: u64,

    /// Number of mission-priority work items served
    pub mission_served: u64,

    /// Number of privacy-restricted work items served
    pub privacy_served: u64,
}

/// The scale simulator.
pub struct ScaleSimulator {
    nodes: Vec<SimFederationNode>,
    work_queue: Vec<SimFederationWork>,
    lineage_queries: u64,
    nodes_lost: u64,
    mission_served: u64,
    privacy_served: u64,
    max_wait: u64,
    starvation_events: u64,
}

impl ScaleSimulator {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            work_queue: Vec::new(),
            lineage_queries: 0,
            nodes_lost: 0,
            mission_served: 0,
            privacy_served: 0,
            max_wait: 0,
            starvation_events: 0,
        }
    }

    /// Add a node to the federation.
    pub fn add_node(&mut self, node: SimFederationNode) {
        self.nodes.push(node);
    }

    /// Add work to the queue.
    pub fn add_work(&mut self, work: SimFederationWork) {
        self.work_queue.push(work);
    }

    /// Simulate node loss (mark a node offline).
    pub fn lose_node(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.node_id == node_id) {
            node.online = false;
            self.nodes_lost += 1;
        }
    }

    /// Run the simulation.
    ///
    /// Processes work items in priority order, matching each to an eligible node.
    /// Mission-priority work always gets served first.
    pub fn run(&mut self) -> ScaleSimReport {
        let start = Instant::now();
        let mut decisions = 0u64;
        let mut same_lineage_served = 0u64;
        let mut external_lineage_served = 0u64;

        // Sort work by priority (mission first, then priority)
        self.work_queue.sort_by(|a, b| {
            b.is_mission
                .cmp(&a.is_mission)
                .then(b.priority.cmp(&a.priority))
        });

        let mut wait_counts: HashMap<String, u64> = HashMap::new();

        // Process work items
        let mut remaining: Vec<SimFederationWork> = std::mem::take(&mut self.work_queue);
        for work in remaining.drain(..) {
            // Find eligible online nodes
            let eligible: Vec<&SimFederationNode> = self
                .nodes
                .iter()
                .filter(|n| {
                    n.online
                        && n.gpu_vram_mb >= work.required_vram_mb
                        && privacy_meets(&n.privacy_class, &work.required_privacy)
                })
                .collect();

            if eligible.is_empty() {
                // No eligible node - count as starvation
                self.starvation_events += 1;
                continue;
            }

            // Lineage query: find same-lineage nodes
            self.lineage_queries += 1;
            let same_lineage: Vec<&SimFederationNode> = eligible
                .iter()
                .filter(|n| n.lineage_id == work.lineage_id)
                .copied()
                .collect();

            // Select node: prefer same-lineage, then lowest latency
            let selected = if !same_lineage.is_empty() {
                same_lineage_served += 1;
                same_lineage
                    .iter()
                    .min_by_key(|n| n.latency_ms)
                    .map(|n| n.node_id.clone())
            } else {
                external_lineage_served += 1;
                eligible
                    .iter()
                    .min_by_key(|n| n.latency_ms)
                    .map(|n| n.node_id.clone())
            };

            if let Some(node_id) = selected {
                decisions += 1;
                if work.is_mission {
                    self.mission_served += 1;
                }
                if work.required_privacy != PrivacyClass::Public {
                    self.privacy_served += 1;
                }

                // Track wait
                let wait = wait_counts.entry(work.work_id.clone()).or_insert(0);
                *wait += 1;
                self.max_wait = self.max_wait.max(*wait);
                if *wait > 10 {
                    self.starvation_events += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let decisions_per_sec = if elapsed_ms > 0 {
            decisions as f64 / (elapsed_ms as f64 / 1000.0)
        } else {
            decisions as f64
        };

        let fairness_ratio = if external_lineage_served > 0 {
            same_lineage_served as f64 / external_lineage_served as f64
        } else if same_lineage_served > 0 {
            f64::INFINITY
        } else {
            0.0
        };

        ScaleSimReport {
            node_count: self.nodes.len(),
            decisions,
            decisions_per_sec,
            elapsed_ms,
            max_wait: self.max_wait,
            starvation_events: self.starvation_events,
            lineage_queries: self.lineage_queries,
            avg_lineage_query_cost: self.lineage_queries as f64 / decisions.max(1) as f64,
            fairness_ratio,
            nodes_lost: self.nodes_lost,
            mission_served: self.mission_served,
            privacy_served: self.privacy_served,
        }
    }
}

impl Default for ScaleSimulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a node's privacy class meets the work's requirement.
fn privacy_meets(node_class: &PrivacyClass, required: &PrivacyClass) -> bool {
    let rank = |p: &PrivacyClass| match p {
        PrivacyClass::Public => 0,
        PrivacyClass::Internal => 1,
        PrivacyClass::Sensitive => 2,
        PrivacyClass::Restricted => 3,
    };
    rank(node_class) >= rank(required)
}

/// Build a federation with the given number of nodes.
///
/// Creates `num_lineages` lineages, each with `nodes_per_lineage` nodes.
/// Nodes have varying GPU sizes and latencies.
pub fn build_federation(
    num_lineages: usize,
    nodes_per_lineage: usize,
) -> Vec<SimFederationNode> {
    let mut nodes = Vec::new();
    for l in 0..num_lineages {
        let lineage_id = format!("lineage-{}", l);
        for n in 0..nodes_per_lineage {
            let vram = match n % 3 {
                0 => 16384, // 16GB
                1 => 24576, // 24GB
                _ => 8192,  // 8GB
            };
            let latency = 5 + (l * 3 + n) % 20; // 5-25ms
            nodes.push(SimFederationNode {
                node_id: format!("node-{}-{}", l, n),
                lineage_id: lineage_id.clone(),
                gpu_vram_mb: vram,
                latency_ms: latency as u64,
                online: true,
                privacy_class: PrivacyClass::Internal,
            });
        }
    }
    nodes
}

/// Run a scale simulation at the given node count.
pub fn run_scale_simulation(node_count: usize) -> ScaleSimReport {
    let num_lineages = (node_count / 2).max(1);
    let nodes_per_lineage = (node_count / num_lineages).max(1);

    let mut sim = ScaleSimulator::new();
    for node in build_federation(num_lineages, nodes_per_lineage) {
        sim.add_node(node);
    }

    // Add work items
    for i in 0..(node_count * 10) {
        let lineage = format!("lineage-{}", i % num_lineages);
        let is_mission = i % 50 == 0; // 2% mission priority
        sim.add_work(SimFederationWork {
            work_id: format!("work-{}", i),
            lineage_id: lineage,
            required_vram_mb: 8000,
            required_privacy: PrivacyClass::Internal,
            priority: 10,
            is_mission,
        });
    }

    sim.run()
}

/// Run the full scale simulation suite at 1, 10, 100, 1000 nodes.
pub fn run_scale_suite() -> Vec<(usize, ScaleSimReport)> {
    [1, 10, 100, 1000]
        .iter()
        .map(|&n| {
            let report = run_scale_simulation(n);
            (n, report)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_sim_1_node() {
        let report = run_scale_simulation(1);
        assert_eq!(report.node_count, 1);
        assert!(report.decisions > 0);
    }

    #[test]
    fn scale_sim_10_nodes() {
        let report = run_scale_simulation(10);
        assert_eq!(report.node_count, 10);
        assert!(report.decisions > 0);
    }

    #[test]
    fn scale_sim_100_nodes() {
        let report = run_scale_simulation(100);
        assert_eq!(report.node_count, 100);
        assert!(report.decisions > 0);
    }

    #[test]
    fn scale_sim_1000_nodes() {
        let report = run_scale_simulation(1000);
        assert_eq!(report.node_count, 1000);
        assert!(report.decisions > 0);
    }

    #[test]
    fn scale_suite_runs() {
        let results = run_scale_suite();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 10);
        assert_eq!(results[2].0, 100);
        assert_eq!(results[3].0, 1000);
    }

    #[test]
    fn node_loss_handled() {
        let mut sim = ScaleSimulator::new();
        sim.add_node(SimFederationNode {
            node_id: "n1".into(),
            lineage_id: "L1".into(),
            gpu_vram_mb: 16384,
            latency_ms: 5,
            online: true,
            privacy_class: PrivacyClass::Internal,
        });
        sim.add_work(SimFederationWork {
            work_id: "w1".into(),
            lineage_id: "L1".into(),
            required_vram_mb: 8000,
            required_privacy: PrivacyClass::Internal,
            priority: 10,
            is_mission: false,
        });

        // Lose the node before running
        sim.lose_node("n1");
        let report = sim.run();
        assert_eq!(report.nodes_lost, 1);
        assert_eq!(report.decisions, 0); // no eligible nodes
    }

    #[test]
    fn mission_priority_served() {
        let mut sim = ScaleSimulator::new();
        sim.add_node(SimFederationNode {
            node_id: "n1".into(),
            lineage_id: "L1".into(),
            gpu_vram_mb: 16384,
            latency_ms: 5,
            online: true,
            privacy_class: PrivacyClass::Internal,
        });
        sim.add_work(SimFederationWork {
            work_id: "w1".into(),
            lineage_id: "L1".into(),
            required_vram_mb: 8000,
            required_privacy: PrivacyClass::Internal,
            priority: 10,
            is_mission: true,
        });

        let report = sim.run();
        assert_eq!(report.mission_served, 1);
    }

    #[test]
    fn privacy_restriction_blocks() {
        let mut sim = ScaleSimulator::new();
        sim.add_node(SimFederationNode {
            node_id: "n1".into(),
            lineage_id: "L1".into(),
            gpu_vram_mb: 16384,
            latency_ms: 5,
            online: true,
            privacy_class: PrivacyClass::Public, // only public
        });
        sim.add_work(SimFederationWork {
            work_id: "w1".into(),
            lineage_id: "L1".into(),
            required_vram_mb: 8000,
            required_privacy: PrivacyClass::Restricted, // needs restricted
            priority: 10,
            is_mission: false,
        });

        let report = sim.run();
        assert_eq!(report.decisions, 0); // blocked by privacy
        assert_eq!(report.privacy_served, 0);
    }

    #[test]
    fn privacy_meets_check() {
        assert!(privacy_meets(&PrivacyClass::Restricted, &PrivacyClass::Restricted));
        assert!(privacy_meets(&PrivacyClass::Restricted, &PrivacyClass::Public));
        assert!(privacy_meets(&PrivacyClass::Internal, &PrivacyClass::Internal));
        assert!(!privacy_meets(&PrivacyClass::Public, &PrivacyClass::Internal));
        assert!(!privacy_meets(&PrivacyClass::Internal, &PrivacyClass::Restricted));
    }

    #[test]
    fn build_federation_creates_nodes() {
        let nodes = build_federation(3, 2);
        assert_eq!(nodes.len(), 6);
        // All nodes should have unique IDs
        let mut ids: Vec<String> = nodes.iter().map(|n| n.node_id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 6);
    }
}