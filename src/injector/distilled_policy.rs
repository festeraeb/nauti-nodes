use crate::index::hybrid::RetrievalResult;
use crate::injector::action::{ActionWeight, PlanStrategy, RetrievalAction, RetrievalPlan};
use crate::injector::feedback::OutcomeRecord;
use crate::injector::intent::AgentIntent;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Recommendation for whether to promote distilled policy to fast-path execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyRecommendation {
    /// Confidence and overlap are high; safe to execute fast-path without full hybrid search.
    PromoteToFastPath,
    /// Keep running in shadow mode alongside ground truth.
    KeepInShadow,
    /// Low accuracy/confidence; policy retrain or fallback to authoritative retrieval required.
    RetrainRequired,
}

/// Shadow evaluation report comparing distilled policy with authoritative retrieval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowEvaluationReport {
    pub intent_summary: String,
    pub overlap_ratio: f64,
    pub distilled_latency_saved_est_ms: u64,
    pub confidence_score: f64,
    pub recommendation: PolicyRecommendation,
}

/// Outcome-trained distilled policy for fast anticipatory planning.
pub struct DistilledPolicy {
    /// Action category weights adjusted via outcome feedback (scaled by 1000 for atomic updates).
    action_weights: Arc<DashMap<String, AtomicU64>>,
    /// Total evaluations performed.
    evaluation_counter: AtomicU64,
    /// Confidence threshold to recommend fast-path promotion.
    promotion_confidence_threshold: f64,
    /// Overlap threshold to recommend fast-path promotion.
    promotion_overlap_threshold: f64,
}

impl Default for DistilledPolicy {
    fn default() -> Self {
        Self::new(0.75, 0.70)
    }
}

impl DistilledPolicy {
    pub fn new(promotion_confidence_threshold: f64, promotion_overlap_threshold: f64) -> Self {
        let action_weights = Arc::new(DashMap::new());
        // Default prior weights (800 = 0.80)
        action_weights.insert("QuerySymbol".into(), AtomicU64::new(850));
        action_weights.insert("QueryCallers".into(), AtomicU64::new(750));
        action_weights.insert("QueryCompilerIncidents".into(), AtomicU64::new(900));
        action_weights.insert("QueryTests".into(), AtomicU64::new(800));
        action_weights.insert("QueryVectorSimilarity".into(), AtomicU64::new(650));
        action_weights.insert("SkipRetrieval".into(), AtomicU64::new(500));

        Self {
            action_weights,
            evaluation_counter: AtomicU64::new(0),
            promotion_confidence_threshold,
            promotion_overlap_threshold,
        }
    }

    /// Plans a sequence of retrieval actions based on the current agent intent.
    pub fn plan_for_intent(&self, intent: &AgentIntent) -> RetrievalPlan {
        let summary = intent.summary();
        let plan_id = format!("plan-{}", uuid::Uuid::new_v4());
        let mut plan = RetrievalPlan::new(plan_id, &summary, PlanStrategy::DistilledPolicy);

        match intent {
            AgentIntent::InspectSymbol { qualified_name } => {
                let sym_w = self.get_action_weight("QuerySymbol");
                plan.add_action(
                    RetrievalAction::QuerySymbol {
                        qualified_name: qualified_name.clone(),
                        kind: None,
                    },
                    ActionWeight {
                        priority: 100,
                        token_budget: 1536,
                        relevance_score: sym_w,
                    },
                );

                let callers_w = self.get_action_weight("QueryCallers");
                plan.add_action(
                    RetrievalAction::QueryCallers {
                        target_symbol: qualified_name.clone(),
                    },
                    ActionWeight {
                        priority: 80,
                        token_budget: 1024,
                        relevance_score: callers_w,
                    },
                );

                let tests_w = self.get_action_weight("QueryTests");
                plan.add_action(
                    RetrievalAction::QueryTests {
                        target_symbol: qualified_name.clone(),
                    },
                    ActionWeight {
                        priority: 60,
                        token_budget: 1024,
                        relevance_score: tests_w,
                    },
                );
            }
            AgentIntent::InspectFile { path } => {
                plan.add_action(
                    RetrievalAction::QuerySymbol {
                        qualified_name: path.clone(),
                        kind: None,
                    },
                    ActionWeight {
                        priority: 100,
                        token_budget: 2048,
                        relevance_score: self.get_action_weight("QuerySymbol"),
                    },
                );
            }
            AgentIntent::FindCallers { symbol } => {
                plan.add_action(
                    RetrievalAction::QueryCallers {
                        target_symbol: symbol.clone(),
                    },
                    ActionWeight {
                        priority: 100,
                        token_budget: 2048,
                        relevance_score: self.get_action_weight("QueryCallers"),
                    },
                );
                plan.add_action(
                    RetrievalAction::QuerySymbol {
                        qualified_name: symbol.clone(),
                        kind: None,
                    },
                    ActionWeight {
                        priority: 70,
                        token_budget: 1024,
                        relevance_score: self.get_action_weight("QuerySymbol"),
                    },
                );
            }
            AgentIntent::ResolveCompilerError {
                error_code,
                diagnostic: _,
            } => {
                plan.add_action(
                    RetrievalAction::QueryCompilerIncidents {
                        error_code: error_code.clone(),
                        symbol_id: None,
                    },
                    ActionWeight {
                        priority: 100,
                        token_budget: 2048,
                        relevance_score: self.get_action_weight("QueryCompilerIncidents"),
                    },
                );
            }
            AgentIntent::InvestigateTestFailure {
                test_name,
                output: _,
            } => {
                plan.add_action(
                    RetrievalAction::QueryTests {
                        target_symbol: test_name.clone(),
                    },
                    ActionWeight {
                        priority: 100,
                        token_budget: 2048,
                        relevance_score: self.get_action_weight("QueryTests"),
                    },
                );
                plan.add_action(
                    RetrievalAction::QuerySymbol {
                        qualified_name: test_name.clone(),
                        kind: None,
                    },
                    ActionWeight {
                        priority: 80,
                        token_budget: 1024,
                        relevance_score: self.get_action_weight("QuerySymbol"),
                    },
                );
            }
            AgentIntent::VerifyInvariant { description } => {
                plan.add_action(
                    RetrievalAction::QueryVectorSimilarity {
                        query: description.clone(),
                        top_k: 5,
                    },
                    ActionWeight {
                        priority: 90,
                        token_budget: 2048,
                        relevance_score: self.get_action_weight("QueryVectorSimilarity"),
                    },
                );
            }
            AgentIntent::PrepareReview { changed_files } => {
                for file in changed_files {
                    plan.add_action(
                        RetrievalAction::QuerySymbol {
                            qualified_name: file.clone(),
                            kind: None,
                        },
                        ActionWeight {
                            priority: 90,
                            token_budget: 1536,
                            relevance_score: self.get_action_weight("QuerySymbol"),
                        },
                    );
                }
                plan.add_action(
                    RetrievalAction::QueryCompilerIncidents {
                        error_code: None,
                        symbol_id: None,
                    },
                    ActionWeight {
                        priority: 60,
                        token_budget: 1024,
                        relevance_score: self.get_action_weight("QueryCompilerIncidents"),
                    },
                );
            }
        }

        // Compute average confidence from constituent actions
        let total_score: f64 = plan.actions.iter().map(|(_, w)| w.relevance_score).sum();
        if !plan.actions.is_empty() {
            plan.confidence_score = total_score / (plan.actions.len() as f64);
        }

        plan
    }

    /// Updates internal policy weights based on real outcome feedback.
    pub fn update_from_feedback(&self, record: &OutcomeRecord) {
        let reward = record.outcome_type.reward_weight();
        let learning_rate = 0.05;

        for (action, _) in &record.plan.actions {
            let key = match action {
                RetrievalAction::QuerySymbol { .. } => "QuerySymbol",
                RetrievalAction::QueryCallers { .. } => "QueryCallers",
                RetrievalAction::QueryCompilerIncidents { .. } => "QueryCompilerIncidents",
                RetrievalAction::QueryTests { .. } => "QueryTests",
                RetrievalAction::QueryVectorSimilarity { .. } => "QueryVectorSimilarity",
                RetrievalAction::SkipRetrieval { .. } => "SkipRetrieval",
            };

            let entry = self
                .action_weights
                .entry(key.into())
                .or_insert_with(|| AtomicU64::new(500));

            let current = entry.load(Ordering::Relaxed) as f64 / 1000.0;
            let updated = (current + learning_rate * reward).clamp(0.05, 0.99);
            entry.store((updated * 1000.0) as u64, Ordering::Relaxed);
        }
    }

    /// Evaluates distilled plan in shadow mode against ground truth authoritative results.
    pub fn shadow_evaluate(
        &self,
        distilled_plan: &RetrievalPlan,
        distilled_results: &[RetrievalResult],
        ground_truth_results: &[RetrievalResult],
    ) -> ShadowEvaluationReport {
        self.evaluation_counter.fetch_add(1, Ordering::Relaxed);

        let distilled_ids: std::collections::HashSet<_> =
            distilled_results.iter().map(|r| &r.symbol_id).collect();
        let ground_truth_ids: std::collections::HashSet<_> =
            ground_truth_results.iter().map(|r| &r.symbol_id).collect();

        let intersection_count = distilled_ids.intersection(&ground_truth_ids).count();
        let union_count = distilled_ids.union(&ground_truth_ids).count();

        let overlap_ratio = if union_count == 0 {
            1.0
        } else {
            intersection_count as f64 / union_count as f64
        };

        let confidence = distilled_plan.confidence_score;
        let recommendation = if confidence >= self.promotion_confidence_threshold
            && overlap_ratio >= self.promotion_overlap_threshold
        {
            PolicyRecommendation::PromoteToFastPath
        } else if confidence >= 0.40 {
            PolicyRecommendation::KeepInShadow
        } else {
            PolicyRecommendation::RetrainRequired
        };

        // Estimate ~15ms saved per avoided full hybrid search
        let distilled_latency_saved_est_ms =
            if recommendation == PolicyRecommendation::PromoteToFastPath {
                15
            } else {
                0
            };

        ShadowEvaluationReport {
            intent_summary: distilled_plan.intent_summary.clone(),
            overlap_ratio,
            distilled_latency_saved_est_ms,
            confidence_score: confidence,
            recommendation,
        }
    }

    fn get_action_weight(&self, key: &str) -> f64 {
        if let Some(entry) = self.action_weights.get(key) {
            entry.load(Ordering::Relaxed) as f64 / 1000.0
        } else {
            0.5
        }
    }
}
