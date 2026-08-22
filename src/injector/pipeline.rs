//! Concurrent Anticipatory Context Injector pipeline.

use super::action::{RetrievalAction, RetrievalPlan};
use super::distilled_policy::{DistilledPolicy, ShadowEvaluationReport};
use super::feedback::{OutcomeFeedbackStore, OutcomeRecord};
use super::intent::AgentIntent;
use super::package::{ContextPackage, DeliveryMode};
use super::telemetry::InjectorTelemetry;
use crate::index::hybrid::RetrievalResult;
use crate::index::{CompilerMemoryStore, HybridRetriever, SymbolKind};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info};

struct IntentMessage {
    intent: AgentIntent,
    delivery_mode: DeliveryMode,
    response_tx: Option<tokio::sync::oneshot::Sender<ContextPackage>>,
}

/// Concurrent anticipatory context injector service.
pub struct AnticipatoryInjector {
    retriever: Arc<HybridRetriever>,
    compiler_memory: Arc<CompilerMemoryStore>,
    policy: Arc<DistilledPolicy>,
    feedback_store: Arc<OutcomeFeedbackStore>,
    telemetry: Arc<InjectorTelemetry>,
    intent_tx: mpsc::Sender<IntentMessage>,
    pending_packages: Arc<DashMap<DeliveryMode, Vec<ContextPackage>>>,
    cache_store: Arc<DashMap<String, ContextPackage>>,
}

impl AnticipatoryInjector {
    /// Spawns a new AnticipatoryInjector background service with default policy and in-memory feedback.
    pub fn spawn(
        retriever: Arc<HybridRetriever>,
        compiler_memory: Arc<CompilerMemoryStore>,
    ) -> Arc<Self> {
        let policy = Arc::new(DistilledPolicy::default());
        let feedback_store = Arc::new(
            OutcomeFeedbackStore::open("sqlite::memory:")
                .expect("Failed to open default in-memory outcome store"),
        );
        Self::spawn_with_policy(retriever, compiler_memory, policy, feedback_store)
    }

    /// Spawns a new AnticipatoryInjector with custom policy and outcome feedback store.
    pub fn spawn_with_policy(
        retriever: Arc<HybridRetriever>,
        compiler_memory: Arc<CompilerMemoryStore>,
        policy: Arc<DistilledPolicy>,
        feedback_store: Arc<OutcomeFeedbackStore>,
    ) -> Arc<Self> {
        let (intent_tx, mut intent_rx) = mpsc::channel::<IntentMessage>(128);
        let telemetry = InjectorTelemetry::new();
        let pending_packages = Arc::new(DashMap::new());
        let cache_store = Arc::new(DashMap::new());

        let injector = Arc::new(Self {
            retriever: retriever.clone(),
            compiler_memory: compiler_memory.clone(),
            policy: policy.clone(),
            feedback_store: feedback_store.clone(),
            telemetry: telemetry.clone(),
            intent_tx,
            pending_packages: pending_packages.clone(),
            cache_store: cache_store.clone(),
        });

        // Background worker loop for concurrent anticipatory retrieval
        let retriever_bg = retriever;
        let compiler_bg = compiler_memory;
        let pending_bg = pending_packages;
        let cache_bg = cache_store;

        tokio::spawn(async move {
            info!("Anticipatory injector background worker started");
            while let Some(msg) = intent_rx.recv().await {
                let start_time = Instant::now();
                let query = msg.intent.to_query_string();

                // 1. Query hybrid retriever
                let search_results = retriever_bg.query(&query, 10).await.unwrap_or_default();

                // 2. Extract compiler incidents
                let mut incidents = Vec::new();
                for r in &search_results {
                    incidents.extend(r.compiler_incidents.clone());
                }

                // If intent is specifically resolving a compiler error, query memory directly
                if let AgentIntent::ResolveCompilerError { ref error_code, .. } = msg.intent {
                    if let Some(code) = error_code {
                        if let Ok(found) = compiler_bg.find_by_error_code(code) {
                            for inc in found {
                                if !incidents.iter().any(|i| i.incident_id == inc.incident_id) {
                                    incidents.push(inc);
                                }
                            }
                        }
                    }
                }

                // 3. Find related tests
                let mut related_tests = Vec::new();
                for r in &search_results {
                    if r.kind == SymbolKind::Test {
                        related_tests.push(r.qualified_name.clone());
                    }
                }

                let elapsed_ms = start_time.elapsed().as_millis() as u64;
                let package = ContextPackage::new(
                    msg.intent.clone(),
                    msg.delivery_mode,
                    search_results,
                    incidents,
                    related_tests,
                    elapsed_ms,
                );

                debug!(
                    "Constructed context package {} for intent {:?} in {}ms",
                    package.package_id, msg.intent, elapsed_ms
                );

                if let Some(tx) = msg.response_tx {
                    let _ = tx.send(package.clone());
                }

                if msg.delivery_mode == DeliveryMode::Cache {
                    cache_bg.insert(query, package);
                } else {
                    let mut list = pending_bg.entry(msg.delivery_mode).or_default();
                    list.push(package);
                }
            }
        });

        injector
    }

    /// Generates a structured retrieval plan for an agent intent using the distilled policy.
    pub fn plan_intent(&self, intent: &AgentIntent) -> RetrievalPlan {
        self.policy.plan_for_intent(intent)
    }

    /// Executes the individual steps of a RetrievalPlan against storage and compiler memory.
    pub async fn execute_plan(&self, plan: &RetrievalPlan) -> Vec<RetrievalResult> {
        let mut results = Vec::new();

        for (action, weight) in &plan.actions {
            match action {
                RetrievalAction::QuerySymbol { qualified_name, .. } => {
                    if let Ok(mut found) = self.retriever.query(qualified_name, 5).await {
                        for r in &mut found {
                            r.score *= weight.relevance_score as f32;
                        }
                        results.extend(found);
                    }
                }
                RetrievalAction::QueryCallers { target_symbol } => {
                    let query = format!("callers of {target_symbol}");
                    if let Ok(mut found) = self.retriever.query(&query, 5).await {
                        for r in &mut found {
                            r.score *= (weight.relevance_score * 0.9) as f32;
                        }
                        results.extend(found);
                    }
                }
                RetrievalAction::QueryCompilerIncidents {
                    error_code,
                    symbol_id,
                } => {
                    if let Some(code) = error_code {
                        if let Ok(incidents) = self.compiler_memory.find_by_error_code(code) {
                            for inc in incidents {
                                results.push(RetrievalResult {
                                    symbol_id: inc.incident_id.clone(),
                                    repository_id: inc.repository_id.clone(),
                                    relative_path: "compiler_memory".into(),
                                    qualified_name: format!(
                                        "compiler_incident::{}",
                                        inc.incident_id
                                    ),
                                    kind: SymbolKind::Function,
                                    signature: None,
                                    doc_comment: None,
                                    body: inc.diagnostic.clone(),
                                    start_line: 1,
                                    end_line: 1,
                                    score: (0.95 * weight.relevance_score) as f32,
                                    match_reasons: vec!["compiler_incident_match".into()],
                                    raw_vector_score: None,
                                    compiler_incidents: vec![inc],
                                });
                            }
                        }
                    }
                    if let Some(sym_id) = symbol_id {
                        if let Ok(incidents) = self.compiler_memory.find_by_symbol_id(sym_id) {
                            for inc in incidents {
                                results.push(RetrievalResult {
                                    symbol_id: inc.incident_id.clone(),
                                    repository_id: inc.repository_id.clone(),
                                    relative_path: "compiler_memory".into(),
                                    qualified_name: format!(
                                        "compiler_incident::{}",
                                        inc.incident_id
                                    ),
                                    kind: SymbolKind::Function,
                                    signature: None,
                                    doc_comment: None,
                                    body: inc.diagnostic.clone(),
                                    start_line: 1,
                                    end_line: 1,
                                    score: (0.90 * weight.relevance_score) as f32,
                                    match_reasons: vec!["compiler_incident_match".into()],
                                    raw_vector_score: None,
                                    compiler_incidents: vec![inc],
                                });
                            }
                        }
                    }
                }
                RetrievalAction::QueryTests { target_symbol } => {
                    let query = format!("test {target_symbol}");
                    if let Ok(mut found) = self.retriever.query(&query, 5).await {
                        for r in &mut found {
                            if r.kind == SymbolKind::Test {
                                r.score *= (weight.relevance_score * 1.1) as f32;
                                results.push(r.clone());
                            }
                        }
                    }
                }
                RetrievalAction::QueryVectorSimilarity { query, top_k } => {
                    if let Ok(mut found) = self.retriever.query(query, *top_k).await {
                        for r in &mut found {
                            r.score *= weight.relevance_score as f32;
                        }
                        results.extend(found);
                    }
                }
                RetrievalAction::SkipRetrieval { .. } => {}
            }
        }

        // Deduplicate results by symbol_id, keeping highest score
        let mut deduped: std::collections::HashMap<String, RetrievalResult> =
            std::collections::HashMap::new();
        for r in results {
            deduped
                .entry(r.symbol_id.clone())
                .and_modify(|existing| {
                    if r.score > existing.score {
                        *existing = r.clone();
                    }
                })
                .or_insert(r);
        }

        let mut sorted: Vec<_> = deduped.into_values().collect();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted
    }

    /// Evaluates an agent intent in shadow mode, comparing distilled plan execution against authoritative retrieval.
    pub async fn shadow_evaluate_intent(&self, intent: &AgentIntent) -> ShadowEvaluationReport {
        let distilled_plan = self.plan_intent(intent);
        let distilled_results = self.execute_plan(&distilled_plan).await;

        let query = intent.to_query_string();
        let ground_truth = self.retriever.query(&query, 10).await.unwrap_or_default();

        self.policy
            .shadow_evaluate(&distilled_plan, &distilled_results, &ground_truth)
    }

    /// Records outcome feedback in the durable store and updates policy weights.
    pub fn record_outcome(&self, record: OutcomeRecord) -> Result<(), rusqlite::Error> {
        self.policy.update_from_feedback(&record);
        self.feedback_store.record_outcome(&record)
    }

    /// Emits a structured agent intent asynchronously without blocking the primary role.
    pub async fn emit_intent(
        &self,
        intent: AgentIntent,
        delivery_mode: DeliveryMode,
    ) -> anyhow::Result<()> {
        self.intent_tx
            .send(IntentMessage {
                intent,
                delivery_mode,
                response_tx: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to emit agent intent: {e}"))
    }

    /// Emits an intent and waits for the synchronously constructed package (e.g. for Inline mode).
    pub async fn emit_and_wait(
        &self,
        intent: AgentIntent,
        delivery_mode: DeliveryMode,
    ) -> anyhow::Result<ContextPackage> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let start = Instant::now();

        self.intent_tx
            .send(IntentMessage {
                intent,
                delivery_mode,
                response_tx: Some(tx),
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send intent: {e}"))?;

        let pkg = rx
            .await
            .map_err(|e| anyhow::anyhow!("Context package receiver dropped: {e}"))?;

        self.telemetry
            .record_retrieval_wait(start.elapsed().as_millis() as u64);
        Ok(pkg)
    }

    /// Consumes the next available package for the specified delivery mode.
    pub fn consume_next_package(&self, mode: DeliveryMode) -> Option<ContextPackage> {
        if let Some(mut list) = self.pending_packages.get_mut(&mode) {
            if !list.is_empty() {
                let mut pkg = list.remove(0);
                pkg.mark_consumed();
                self.telemetry.record_package_consumed();
                return Some(pkg);
            }
        }
        None
    }

    /// Fetches a cached context package by query key.
    pub fn fetch_cached_package(&self, query_key: &str) -> Option<ContextPackage> {
        if let Some(entry) = self.cache_store.get(query_key) {
            let mut pkg = entry.value().clone();
            pkg.mark_consumed();
            self.telemetry.record_package_consumed();
            Some(pkg)
        } else {
            None
        }
    }

    /// Accesses the underlying hybrid retriever.
    pub fn retriever(&self) -> Arc<HybridRetriever> {
        self.retriever.clone()
    }

    /// Accesses the compiler incident memory store.
    pub fn compiler_memory(&self) -> Arc<CompilerMemoryStore> {
        self.compiler_memory.clone()
    }

    /// Accesses the distilled planner policy.
    pub fn policy(&self) -> Arc<DistilledPolicy> {
        self.policy.clone()
    }

    /// Accesses the durable outcome feedback store.
    pub fn feedback_store(&self) -> Arc<OutcomeFeedbackStore> {
        self.feedback_store.clone()
    }

    /// Accesses the real performance telemetry tracker.
    pub fn telemetry(&self) -> Arc<InjectorTelemetry> {
        self.telemetry.clone()
    }
}
