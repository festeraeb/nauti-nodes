use crate::index::symbol::SymbolKind;
use serde::{Deserialize, Serialize};

/// Specific retrieval action planned by the injector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RetrievalAction {
    /// Retrieve symbol definition by name and optional kind.
    QuerySymbol {
        qualified_name: String,
        kind: Option<SymbolKind>,
    },
    /// Retrieve symbols that call or reference the target.
    QueryCallers { target_symbol: String },
    /// Retrieve past compiler incidents for an error code or affected symbol.
    QueryCompilerIncidents {
        error_code: Option<String>,
        symbol_id: Option<String>,
    },
    /// Retrieve unit or integration tests covering the target symbol.
    QueryTests { target_symbol: String },
    /// Perform semantic vector search.
    QueryVectorSimilarity { query: String, top_k: usize },
    /// Deliberately skip retrieval when context is already sufficient or irrelevant.
    SkipRetrieval { reason: String },
}

/// Metadata and budgeting weight for an individual action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionWeight {
    /// Priority order (higher is executed earlier).
    pub priority: u32,
    /// Estimated token budget allocated for results of this action.
    pub token_budget: usize,
    /// Expected relevance weight between 0.0 and 1.0.
    pub relevance_score: f64,
}

impl Default for ActionWeight {
    fn default() -> Self {
        Self {
            priority: 50,
            token_budget: 1024,
            relevance_score: 0.8,
        }
    }
}

/// Execution strategy origin of a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStrategy {
    /// Authoritative baseline multi-stage hybrid search.
    AuthoritativeHybrid,
    /// Distilled outcome-trained lightweight policy.
    DistilledPolicy,
    /// Executed in shadow mode alongside ground truth.
    ShadowEvaluated,
}

/// A structured sequence of retrieval actions forming an anticipatory injection plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalPlan {
    pub plan_id: String,
    pub intent_summary: String,
    pub actions: Vec<(RetrievalAction, ActionWeight)>,
    pub total_token_budget: usize,
    pub confidence_score: f64,
    pub strategy: PlanStrategy,
    pub created_at_ms: u64,
}

impl RetrievalPlan {
    pub fn new(
        plan_id: impl Into<String>,
        intent_summary: impl Into<String>,
        strategy: PlanStrategy,
    ) -> Self {
        Self {
            plan_id: plan_id.into(),
            intent_summary: intent_summary.into(),
            actions: Vec::new(),
            total_token_budget: 4096,
            confidence_score: 0.5,
            strategy,
            created_at_ms: crate::types::now_ms(),
        }
    }

    pub fn add_action(&mut self, action: RetrievalAction, weight: ActionWeight) {
        self.actions.push((action, weight));
    }
}
