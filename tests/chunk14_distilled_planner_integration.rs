use nauti_nodes::index::{
    CodeSymbol, CompilerIncident, CompilerMemoryStore, HybridRetriever, SimpleCodeTokenizer,
    StructuralStorage, SymbolChunk, SymbolKind,
};
use nauti_nodes::injector::{
    AgentIntent, AnticipatoryInjector, DistilledPolicy, OutcomeFeedbackStore, OutcomeRecord,
    OutcomeType, PlanStrategy, PolicyRecommendation, RetrievalAction,
};
use std::sync::Arc;

async fn setup_test_environment() -> (
    Arc<AnticipatoryInjector>,
    StructuralStorage,
    Arc<CompilerMemoryStore>,
    Arc<OutcomeFeedbackStore>,
) {
    let storage = StructuralStorage::open("sqlite::memory:").expect("storage");
    let compiler_mem =
        Arc::new(CompilerMemoryStore::open("sqlite::memory:").expect("compiler memory"));
    let feedback_store =
        Arc::new(OutcomeFeedbackStore::open("sqlite::memory:").expect("feedback store"));

    let sym_coord = CodeSymbol {
        symbol_id: "sym_coord".into(),
        repository_id: "nauti-nodes".into(),
        relative_path: "src/coordinator.rs".into(),
        language: "rust".into(),
        qualified_name: "run_coordinator".into(),
        kind: SymbolKind::Function,
        start_byte: 0,
        end_byte: 200,
        start_line: 1,
        end_line: 10,
        signature: Some("pub async fn run_coordinator(cfg: Config)".into()),
        parent_symbol_id: None,
        content_hash: "hash_c".into(),
        doc_comment: Some("Runs central coordinator.".into()),
        visibility: Some("pub".into()),
    };

    let sym_test = CodeSymbol {
        symbol_id: "sym_test_coord".into(),
        repository_id: "nauti-nodes".into(),
        relative_path: "tests/coordinator_test.rs".into(),
        language: "rust".into(),
        qualified_name: "test_coordinator_boot".into(),
        kind: SymbolKind::Test,
        start_byte: 0,
        end_byte: 150,
        start_line: 1,
        end_line: 8,
        signature: Some("async fn test_coordinator_boot()".into()),
        parent_symbol_id: None,
        content_hash: "hash_tc".into(),
        doc_comment: None,
        visibility: None,
    };

    let tokenizer = SimpleCodeTokenizer;
    let chunk1 = SymbolChunk::from_symbol(
        &sym_coord,
        "pub async fn run_coordinator(cfg: Config) {\n    info!(\"boot\");\n}\n",
        &tokenizer,
    );
    let chunk2 = SymbolChunk::from_symbol(
        &sym_test,
        "async fn test_coordinator_boot() {\n    assert!(true);\n}\n",
        &tokenizer,
    );

    storage
        .upsert_file_symbols(
            "nauti-nodes",
            "src/coordinator.rs",
            "h1",
            &[sym_coord.clone(), sym_test.clone()],
        )
        .expect("upsert symbols");
    storage.store_chunk(&chunk1, None).expect("store chunk1");
    storage.store_chunk(&chunk2, None).expect("store chunk2");

    let incident = CompilerIncident {
        incident_id: "inc-e0308".into(),
        repository_id: "nauti-nodes".into(),
        rustc_version: "1.80.0".into(),
        error_codes: vec!["E0308".into()],
        diagnostic: "mismatched types: expected `Config`, found `()`".into(),
        affected_symbol_ids: vec!["sym_coord".into()],
        failing_commit: Some("commit_fail".into()),
        fixing_commit: Some("commit_fix".into()),
        verified_fixed: true,
        recorded_at_ms: 1700000000000,
    };
    compiler_mem
        .record_incident(&incident)
        .expect("record incident");

    let retriever_storage = StructuralStorage::open("sqlite::memory:").expect("storage");
    retriever_storage
        .upsert_file_symbols(
            "nauti-nodes",
            "src/coordinator.rs",
            "h1",
            &[sym_coord.clone(), sym_test.clone()],
        )
        .expect("upsert symbols");
    retriever_storage
        .store_chunk(&chunk1, None)
        .expect("chunk1");
    retriever_storage
        .store_chunk(&chunk2, None)
        .expect("chunk2");

    let hybrid = Arc::new(HybridRetriever::new(
        retriever_storage,
        CompilerMemoryStore::open("sqlite::memory:").expect("compiler memory"),
        None,
    ));

    let policy = Arc::new(DistilledPolicy::new(0.70, 0.60));
    let injector = AnticipatoryInjector::spawn_with_policy(
        hybrid,
        compiler_mem.clone(),
        policy,
        feedback_store.clone(),
    );

    (injector, storage, compiler_mem, feedback_store)
}

#[tokio::test]
async fn test_distilled_plan_generation_across_intents() {
    let (injector, _, _, _) = setup_test_environment().await;

    // 1. InspectSymbol Intent
    let sym_intent = AgentIntent::InspectSymbol {
        qualified_name: "run_coordinator".into(),
    };
    let sym_plan = injector.plan_intent(&sym_intent);
    assert_eq!(sym_plan.strategy, PlanStrategy::DistilledPolicy);
    assert!(sym_plan.actions.len() >= 3);
    assert!(sym_plan
        .actions
        .iter()
        .any(|(a, _)| matches!(a, RetrievalAction::QuerySymbol { .. })));
    assert!(sym_plan
        .actions
        .iter()
        .any(|(a, _)| matches!(a, RetrievalAction::QueryCallers { .. })));
    assert!(sym_plan
        .actions
        .iter()
        .any(|(a, _)| matches!(a, RetrievalAction::QueryTests { .. })));

    // 2. ResolveCompilerError Intent
    let err_intent = AgentIntent::ResolveCompilerError {
        error_code: Some("E0308".into()),
        diagnostic: "mismatched types".into(),
    };
    let err_plan = injector.plan_intent(&err_intent);
    assert!(err_plan
        .actions
        .iter()
        .any(|(a, _)| matches!(a, RetrievalAction::QueryCompilerIncidents { .. })));

    // 3. PrepareReview Intent
    let rev_intent = AgentIntent::PrepareReview {
        changed_files: vec!["src/coordinator.rs".into()],
    };
    let rev_plan = injector.plan_intent(&rev_intent);
    assert!(rev_plan
        .actions
        .iter()
        .any(|(a, _)| matches!(a, RetrievalAction::QuerySymbol { .. })));
    assert!(rev_plan.confidence_score > 0.0);
}

#[tokio::test]
async fn test_outcome_feedback_store_persistence() {
    let (_, _, _, feedback_store) = setup_test_environment().await;

    let plan = nauti_nodes::injector::RetrievalPlan::new(
        "p1",
        "Inspect run_coordinator",
        PlanStrategy::DistilledPolicy,
    );

    let record1 = OutcomeRecord {
        record_id: "rec-1".into(),
        intent_summary: "Inspect run_coordinator".into(),
        plan: plan.clone(),
        outcome_type: OutcomeType::ConsumedHelpful,
        tool_calls_count: 0,
        retrieval_wait_ms: 12,
        timestamp_ms: 1000,
    };

    let record2 = OutcomeRecord {
        record_id: "rec-2".into(),
        intent_summary: "Inspect run_coordinator".into(),
        plan: plan.clone(),
        outcome_type: OutcomeType::IgnoredIrrelevant,
        tool_calls_count: 3,
        retrieval_wait_ms: 8,
        timestamp_ms: 2000,
    };

    feedback_store.record_outcome(&record1).expect("record 1");
    feedback_store.record_outcome(&record2).expect("record 2");

    let records = feedback_store.list_records(10).expect("list records");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record_id, "rec-2"); // Ordered DESC by timestamp
    assert_eq!(records[1].record_id, "rec-1");

    let success_ratio = feedback_store
        .get_success_ratio("Inspect run_coordinator")
        .expect("success ratio");
    assert_eq!(success_ratio, 0.5); // 1 helpful out of 2
}

#[tokio::test]
async fn test_policy_weight_adaptation_on_reinforcement() {
    let (injector, _, _, _) = setup_test_environment().await;

    let intent = AgentIntent::InspectSymbol {
        qualified_name: "run_coordinator".into(),
    };

    let initial_plan = injector.plan_intent(&intent);
    let initial_confidence = initial_plan.confidence_score;

    // Simulate 5 successful completions
    for i in 0..5 {
        let record = OutcomeRecord {
            record_id: format!("pos-{i}"),
            intent_summary: intent.summary(),
            plan: initial_plan.clone(),
            outcome_type: OutcomeType::ConsumedHelpful,
            tool_calls_count: 0,
            retrieval_wait_ms: 5,
            timestamp_ms: 1000 + i * 10,
        };
        injector.record_outcome(record).expect("record pos outcome");
    }

    let adapted_plan = injector.plan_intent(&intent);
    assert!(
        adapted_plan.confidence_score >= initial_confidence,
        "Positive feedback must increase or maintain plan confidence"
    );

    // Simulate negative failures
    for i in 0..10 {
        let record = OutcomeRecord {
            record_id: format!("neg-{i}"),
            intent_summary: intent.summary(),
            plan: initial_plan.clone(),
            outcome_type: OutcomeType::TestFailed,
            tool_calls_count: 5,
            retrieval_wait_ms: 5,
            timestamp_ms: 2000 + i * 10,
        };
        injector.record_outcome(record).expect("record neg outcome");
    }

    let penalised_plan = injector.plan_intent(&intent);
    assert!(
        penalised_plan.confidence_score < adapted_plan.confidence_score,
        "Negative feedback must decrease plan confidence"
    );
}

#[tokio::test]
async fn test_shadow_evaluation_against_hybrid_ground_truth() {
    let (injector, _, _, _) = setup_test_environment().await;

    let intent = AgentIntent::InspectSymbol {
        qualified_name: "run_coordinator".into(),
    };

    let report = injector.shadow_evaluate_intent(&intent).await;
    assert_eq!(report.intent_summary, intent.summary());
    assert!(report.overlap_ratio >= 0.0 && report.overlap_ratio <= 1.0);
    assert!(report.confidence_score > 0.0);

    // Initial policy starts in shadow or fast-path depending on overlap
    assert!(
        report.recommendation == PolicyRecommendation::KeepInShadow
            || report.recommendation == PolicyRecommendation::PromoteToFastPath
    );
}

#[tokio::test]
async fn test_plan_execution_and_result_deduplication() {
    let (injector, _, _, _) = setup_test_environment().await;

    let err_intent = AgentIntent::ResolveCompilerError {
        error_code: Some("E0308".into()),
        diagnostic: "mismatched types".into(),
    };

    let plan = injector.plan_intent(&err_intent);
    let results = injector.execute_plan(&plan).await;

    assert!(
        !results.is_empty(),
        "Plan execution must retrieve matching results"
    );

    // Ensure compiler incident was found and symbol was found
    assert!(results
        .iter()
        .any(|r| r.qualified_name.contains("run_coordinator")
            || r.qualified_name.contains("compiler_incident")));

    // Ensure no duplicate symbol IDs
    let mut ids = std::collections::HashSet::new();
    for r in &results {
        assert!(
            ids.insert(r.symbol_id.clone()),
            "Duplicate symbol ID in results: {}",
            r.symbol_id
        );
    }
}
