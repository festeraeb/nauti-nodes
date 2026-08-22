//! Types for validated closed-loop engineering.

use crate::hygiene::HygieneCycleReport;
use crate::injector::{
    AgentIntent, ContextPackage, OutcomeType, PolicyRecommendation, RetrievalPlan,
};
use serde::{Deserialize, Serialize};

/// An engineering task dispatched to the closed-loop engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineeringTask {
    pub task_id: String,
    pub repository_id: String,
    pub intent: AgentIntent,
    pub error_diagnostic: Option<String>,
    pub target_files: Vec<String>,
    pub initiator_node_id: String,
}

/// Simulated or observed result of code change execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixResult {
    pub success: bool,
    pub commit_hash: String,
    pub outcome: OutcomeType,
    pub tokens_consumed: usize,
}

/// Comprehensive outcome of an end-to-end closed-loop execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedLoopResult {
    pub task_id: String,
    pub repository_id: String,
    pub context_package: ContextPackage,
    pub retrieval_plan: RetrievalPlan,
    pub policy_recommendation: PolicyRecommendation,
    pub execution_success: bool,
    pub fixing_commit: Option<String>,
    pub outcome_type: OutcomeType,
    pub contribution_receipt_id: Option<String>,
    pub hygiene_report: Option<HygieneCycleReport>,
    pub total_latency_ms: u64,
}
