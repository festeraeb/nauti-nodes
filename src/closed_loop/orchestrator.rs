//! Closed-loop engineering orchestrator unifying indexing, distilled planning, anticipatory injection, outcome feedback, contribution accounting, and evidence hygiene.

use super::types::{ClosedLoopResult, EngineeringTask, FixResult};
use crate::contribution::ContributionLedger;
use crate::hygiene::{EvidenceHygieneEngine, HygienePolicy, ScalableStorageManager};
use crate::index::compiler_memory::{CompilerIncident, CompilerMemoryStore};
use crate::index::storage::StructuralStorage;
use crate::injector::{
    AgentIntent, AnticipatoryInjector, ContextPackage, DeliveryMode, DistilledPolicy,
    OutcomeFeedbackStore, OutcomeRecord, OutcomeType,
};
use crate::types::now_ms;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

/// Orchestrates full validated closed-loop engineering workflows.
pub struct ClosedLoopEngine {
    storage: Arc<StructuralStorage>,
    compiler_mem: Arc<CompilerMemoryStore>,
    feedback_store: Arc<OutcomeFeedbackStore>,
    policy: Arc<DistilledPolicy>,
    injector: Arc<AnticipatoryInjector>,
    hygiene_engine: Arc<EvidenceHygieneEngine>,
    storage_manager: Arc<ScalableStorageManager>,
    contribution_ledger: Arc<ContributionLedger>,
}

impl ClosedLoopEngine {
    /// Constructs a new ClosedLoopEngine with all required subsystems.
    pub fn new(
        storage: Arc<StructuralStorage>,
        compiler_mem: Arc<CompilerMemoryStore>,
        feedback_store: Arc<OutcomeFeedbackStore>,
        policy: Arc<DistilledPolicy>,
        injector: Arc<AnticipatoryInjector>,
        contribution_ledger: Arc<ContributionLedger>,
    ) -> Self {
        let hygiene_policy = HygienePolicy::default();
        let hygiene_engine = Arc::new(EvidenceHygieneEngine::new(hygiene_policy.clone()));
        let storage_manager = Arc::new(ScalableStorageManager::new(hygiene_policy));

        Self {
            storage,
            compiler_mem,
            feedback_store,
            policy,
            injector,
            hygiene_engine,
            storage_manager,
            contribution_ledger,
        }
    }

    /// Accessor for distilled policy.
    pub fn distilled_policy(&self) -> Arc<DistilledPolicy> {
        self.policy.clone()
    }

    /// Accessor for feedback store.
    pub fn feedback_store(&self) -> Arc<OutcomeFeedbackStore> {
        self.feedback_store.clone()
    }

    /// Accessor for compiler memory store.
    pub fn compiler_memory(&self) -> Arc<CompilerMemoryStore> {
        self.compiler_mem.clone()
    }

    /// Accessor for structural storage.
    pub fn storage(&self) -> Arc<StructuralStorage> {
        self.storage.clone()
    }

    /// Accessor for contribution ledger.
    pub fn contribution_ledger(&self) -> Arc<ContributionLedger> {
        self.contribution_ledger.clone()
    }

    /// Accessor for storage manager.
    pub fn storage_manager(&self) -> Arc<ScalableStorageManager> {
        self.storage_manager.clone()
    }

    /// Executes an autonomous end-to-end engineering cycle for a task.
    pub async fn execute_task(
        &self,
        task: &EngineeringTask,
        fix: Option<FixResult>,
        run_hygiene: bool,
    ) -> Result<ClosedLoopResult, crate::types::Error> {
        let start_time = Instant::now();

        // 1. Record compiler incident if diagnostic is provided and not already present
        let mut recorded_incident_id = None;
        if let Some(diag) = &task.error_diagnostic {
            let inc_id = format!("inc-{}", Uuid::new_v4());
            let error_code =
                if let AgentIntent::ResolveCompilerError { error_code, .. } = &task.intent {
                    error_code.clone().unwrap_or_else(|| "E_UNKNOWN".into())
                } else {
                    "E_UNKNOWN".into()
                };

            let incident = CompilerIncident {
                incident_id: inc_id.clone(),
                repository_id: task.repository_id.clone(),
                rustc_version: "1.80.0".into(),
                error_codes: vec![error_code],
                diagnostic: diag.clone(),
                affected_symbol_ids: vec![task.intent.target_symbol()],
                failing_commit: None,
                fixing_commit: None,
                verified_fixed: false,
                recorded_at_ms: now_ms(),
            };
            self.compiler_mem.record_incident(&incident)?;
            recorded_incident_id = Some(inc_id);
        }

        // 2. Generate Retrieval Plan via Distilled Policy
        let retrieval_plan = self.policy.plan_for_intent(&task.intent);
        let policy_recommendation = self.policy.recommend_policy_action(&task.intent);

        // 3. Execute Plan via Anticipatory Injector
        let retrieved_results = self.injector.execute_plan(&retrieval_plan).await;

        // 4. Construct Context Package
        let context_package =
            ContextPackage::build(&task.intent, retrieved_results, DeliveryMode::Inline);

        // 5. Evaluate execution / fix outcome
        let mut execution_success = false;
        let mut fixing_commit = None;
        let mut outcome_type = OutcomeType::ConsumedHelpful;
        let mut contribution_receipt_id = None;

        if let Some(fix_result) = fix {
            execution_success = fix_result.success;
            fixing_commit = Some(fix_result.commit_hash.clone());
            outcome_type = fix_result.outcome;

            // Record outcome feedback in durable store
            let outcome_record = OutcomeRecord {
                record_id: format!("out-{}", Uuid::new_v4()),
                intent_summary: task.intent.summary(),
                plan: retrieval_plan.clone(),
                outcome_type,
                tool_calls_count: 1,
                retrieval_wait_ms: 10,
                timestamp_ms: now_ms(),
            };
            self.feedback_store
                .record_outcome(&outcome_record)
                .map_err(|e| crate::types::Error::Internal(e.to_string()))?;

            // Update distilled policy atomic weights
            self.policy.update_from_feedback(&outcome_record);

            if execution_success {
                // Link fixing commit and mark verified fixed in compiler memory
                if let Some(inc_id) = recorded_incident_id {
                    self.compiler_mem
                        .mark_verified_fixed(&inc_id, &fix_result.commit_hash)?;
                }

                // Credit contribution accounting
                self.contribution_ledger
                    .update(&task.initiator_node_id, |record| {
                        record.record_completion(1.2, 0.8, 1024, 2048);
                        record.record_validation_success();
                    });

                contribution_receipt_id =
                    Some(format!("rcpt-{}-{}", task.task_id, fix_result.commit_hash));
            } else {
                // Record failure in contribution accounting
                self.contribution_ledger
                    .update(&task.initiator_node_id, |record| {
                        record.record_failure();
                    });
            }
        }

        // 6. Optional Evidence Hygiene Sweep
        let hygiene_report = if run_hygiene {
            Some(
                self.hygiene_engine
                    .run_hygiene_cycle(&self.storage, &self.compiler_mem)?,
            )
        } else {
            None
        };

        let total_latency_ms = start_time.elapsed().as_millis() as u64;

        info!(
            "Closed loop cycle completed for task {} (success: {}, latency: {}ms)",
            task.task_id, execution_success, total_latency_ms
        );

        Ok(ClosedLoopResult {
            task_id: task.task_id.clone(),
            repository_id: task.repository_id.clone(),
            context_package,
            retrieval_plan,
            policy_recommendation,
            execution_success,
            fixing_commit,
            outcome_type,
            contribution_receipt_id,
            hygiene_report,
            total_latency_ms,
        })
    }
}
