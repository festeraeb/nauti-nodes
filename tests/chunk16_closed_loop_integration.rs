//! Comprehensive integration tests for Chunk 16: Validated closed-loop engineering.

use nauti_nodes::closed_loop::{ClosedLoopEngine, EngineeringTask, FixResult};
use nauti_nodes::contribution::ContributionLedger;
use nauti_nodes::index::{
    CodeSymbol, CompilerMemoryStore, HybridRetriever, SimpleCodeTokenizer, StructuralStorage,
    SymbolChunk, SymbolKind,
};
use nauti_nodes::injector::{
    AgentIntent, AnticipatoryInjector, DistilledPolicy, OutcomeFeedbackStore, OutcomeType,
};
use std::sync::Arc;

async fn setup_closed_loop_engine() -> (
    Arc<ClosedLoopEngine>,
    Arc<StructuralStorage>,
    Arc<CompilerMemoryStore>,
    Arc<OutcomeFeedbackStore>,
    Arc<ContributionLedger>,
) {
    let storage = Arc::new(StructuralStorage::open("sqlite::memory:").expect("storage"));
    let compiler_mem =
        Arc::new(CompilerMemoryStore::open("sqlite::memory:").expect("compiler memory"));
    let feedback_store =
        Arc::new(OutcomeFeedbackStore::open("sqlite::memory:").expect("feedback store"));
    let contribution_ledger = Arc::new(ContributionLedger::new());

    let policy = Arc::new(DistilledPolicy::default());
    let retriever = Arc::new(HybridRetriever::new(
        (*storage).clone(),
        (*compiler_mem).clone(),
        None,
    ));
    let injector = AnticipatoryInjector::spawn_with_policy(
        retriever,
        compiler_mem.clone(),
        policy.clone(),
        feedback_store.clone(),
    );

    let engine = Arc::new(ClosedLoopEngine::new(
        storage.clone(),
        compiler_mem.clone(),
        feedback_store.clone(),
        policy,
        injector,
        contribution_ledger.clone(),
    ));

    (
        engine,
        storage,
        compiler_mem,
        feedback_store,
        contribution_ledger,
    )
}

fn seed_symbol(storage: &StructuralStorage, repo: &str, file: &str, name: &str) -> CodeSymbol {
    let sym = CodeSymbol {
        symbol_id: format!("sym_{name}"),
        repository_id: repo.into(),
        relative_path: file.into(),
        language: "rust".into(),
        qualified_name: name.into(),
        kind: SymbolKind::Function,
        start_byte: 0,
        end_byte: 100,
        start_line: 1,
        end_line: 10,
        signature: Some(format!("pub fn {name}()")),
        parent_symbol_id: None,
        content_hash: format!("hash_{name}"),
        doc_comment: Some(format!("Doc comment for {name}")),
        visibility: Some("pub".into()),
    };

    storage
        .upsert_file_symbols(repo, file, "filehash", std::slice::from_ref(&sym))
        .expect("upsert");

    let tokenizer = SimpleCodeTokenizer;
    let chunk = SymbolChunk::from_symbol(&sym, &format!("fn {name}() {{}}"), &tokenizer);
    storage.store_chunk(&chunk, None).expect("store chunk");

    sym
}

#[tokio::test]
async fn test_closed_loop_compiler_error_recovery_and_learning() {
    let (engine, storage, compiler_mem, feedback_store, contribution_ledger) =
        setup_closed_loop_engine().await;

    seed_symbol(&storage, "test-repo", "src/auth.rs", "verify_token");

    let task = EngineeringTask {
        task_id: "task-001".into(),
        repository_id: "test-repo".into(),
        intent: AgentIntent::ResolveCompilerError {
            error_code: Some("E0308".into()),
            diagnostic: "mismatched types: expected bool, found &str in verify_token".into(),
        },
        error_diagnostic: Some(
            "mismatched types: expected bool, found &str in verify_token".into(),
        ),
        target_files: vec!["src/auth.rs".into()],
        initiator_node_id: "node-gpu-01".into(),
    };

    let fix = FixResult {
        success: true,
        commit_hash: "commit_fix_100".into(),
        outcome: OutcomeType::ConsumedHelpful,
        tokens_consumed: 320,
    };

    let result = engine
        .execute_task(&task, Some(fix), true)
        .await
        .expect("execute task");

    // 1. Assert result fields
    assert_eq!(result.task_id, "task-001");
    assert!(result.execution_success);
    assert_eq!(result.fixing_commit.as_deref(), Some("commit_fix_100"));
    assert!(result.contribution_receipt_id.is_some());
    assert!(result.hygiene_report.is_some());

    // 2. Assert compiler memory was updated and verified fixed
    let incidents = compiler_mem.find_by_error_code("E0308").unwrap();
    assert_eq!(incidents.len(), 1);
    assert!(incidents[0].verified_fixed);
    assert_eq!(
        incidents[0].fixing_commit.as_deref(),
        Some("commit_fix_100")
    );

    // 3. Assert outcome feedback store recorded the helpful consumption
    let feedback = feedback_store.list_records(100).unwrap();
    assert_eq!(feedback.len(), 1);
    assert_eq!(feedback[0].outcome_type, OutcomeType::ConsumedHelpful);

    // 4. Assert contribution ledger credited node-gpu-01
    let node_contrib = contribution_ledger.get("node-gpu-01").unwrap();
    assert_eq!(node_contrib.completed_work_units, 1);
    assert_eq!(node_contrib.result_validation_success, 1);
    assert_eq!(node_contrib.failed_work_units, 0);
}

#[tokio::test]
async fn test_closed_loop_feature_implementation_and_failure_feedback() {
    let (engine, storage, _, feedback_store, contribution_ledger) =
        setup_closed_loop_engine().await;

    seed_symbol(&storage, "test-repo", "src/math.rs", "calculate_checksum");

    let task = EngineeringTask {
        task_id: "task-002".into(),
        repository_id: "test-repo".into(),
        intent: AgentIntent::InspectSymbol {
            qualified_name: "calculate_checksum".into(),
        },
        error_diagnostic: None,
        target_files: vec!["src/math.rs".into()],
        initiator_node_id: "node-gpu-02".into(),
    };

    let fix = FixResult {
        success: false,
        commit_hash: "failed_commit".into(),
        outcome: OutcomeType::CompileFailed,
        tokens_consumed: 150,
    };

    let result = engine
        .execute_task(&task, Some(fix), false)
        .await
        .expect("execute task");

    assert_eq!(result.task_id, "task-002");
    assert!(!result.execution_success);
    assert_eq!(result.outcome_type, OutcomeType::CompileFailed);
    assert!(result.contribution_receipt_id.is_none());

    // Assert outcome feedback recorded the failure
    let feedback = feedback_store.list_records(100).unwrap();
    assert_eq!(feedback.len(), 1);
    assert_eq!(feedback[0].outcome_type, OutcomeType::CompileFailed);

    // Assert contribution ledger recorded a failure
    let node_contrib = contribution_ledger.get("node-gpu-02").unwrap();
    assert_eq!(node_contrib.completed_work_units, 0);
    assert_eq!(node_contrib.failed_work_units, 1);
}

#[tokio::test]
async fn test_closed_loop_multi_iteration_policy_adaptation() {
    let (engine, storage, _, _, _) = setup_closed_loop_engine().await;

    seed_symbol(&storage, "test-repo", "src/state.rs", "AppState::new");

    let initial_weight = engine.distilled_policy().get_symbol_weight();

    // Run 5 successful iterations
    for i in 1..=5 {
        let task = EngineeringTask {
            task_id: format!("task-iter-{i}"),
            repository_id: "test-repo".into(),
            intent: AgentIntent::InspectSymbol {
                qualified_name: "AppState::new".into(),
            },
            error_diagnostic: None,
            target_files: vec!["src/state.rs".into()],
            initiator_node_id: "node-gpu-03".into(),
        };

        let fix = FixResult {
            success: true,
            commit_hash: format!("commit_ok_{i}"),
            outcome: OutcomeType::ConsumedHelpful,
            tokens_consumed: 100,
        };

        engine.execute_task(&task, Some(fix), false).await.unwrap();
    }

    let adapted_weight = engine.distilled_policy().get_symbol_weight();
    assert!(
        adapted_weight > initial_weight,
        "Policy weight should adapt upwards after positive reinforcement"
    );
}

#[tokio::test]
async fn test_closed_loop_evidence_hygiene_and_bundle_export_post_cycle() {
    let (engine, storage, compiler_mem, _, _) = setup_closed_loop_engine().await;

    seed_symbol(&storage, "repo-hygiene", "src/lib.rs", "init_engine");

    let task = EngineeringTask {
        task_id: "task-hygiene".into(),
        repository_id: "repo-hygiene".into(),
        intent: AgentIntent::ResolveCompilerError {
            error_code: Some("E0412".into()),
            diagnostic: "cannot find type `Config` in /home/cesarops/dev/repo/src/lib.rs".into(),
        },
        error_diagnostic: Some(
            "cannot find type `Config` in /home/cesarops/dev/repo/src/lib.rs with key sk-999999999999999999999999999".into(),
        ),
        target_files: vec!["src/lib.rs".into()],
        initiator_node_id: "node-gpu-04".into(),
    };

    let fix = FixResult {
        success: true,
        commit_hash: "commit_clean_01".into(),
        outcome: OutcomeType::ConsumedHelpful,
        tokens_consumed: 200,
    };

    let result = engine
        .execute_task(&task, Some(fix), true)
        .await
        .expect("execute task");

    // Assert hygiene report sanitized secret & user path
    assert!(result.hygiene_report.is_some());
    let hygiene_rep = result.hygiene_report.unwrap();
    assert!(hygiene_rep.sanitized_incidents_count >= 1);

    // Export verified evidence bundle
    let bundle = engine
        .storage_manager()
        .export_bundle("repo-hygiene", &storage, &compiler_mem)
        .expect("export bundle");

    assert_eq!(bundle.repository_id, "repo-hygiene");
    assert!(bundle.verify_integrity());
    assert_eq!(bundle.symbols.len(), 1);
    assert_eq!(bundle.incidents.len(), 1);
    assert!(bundle.incidents[0].verified_fixed);
}

#[tokio::test]
async fn test_closed_loop_concurrent_task_execution() {
    let (engine, storage, _, feedback_store, contribution_ledger) =
        setup_closed_loop_engine().await;

    for i in 0..10 {
        seed_symbol(
            &storage,
            "repo-concurrency",
            &format!("src/mod_{i}.rs"),
            &format!("fn_{i}"),
        );
    }

    let mut handles = Vec::new();
    for i in 0..10 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let task = EngineeringTask {
                task_id: format!("concurrent-task-{i}"),
                repository_id: "repo-concurrency".into(),
                intent: AgentIntent::InspectSymbol {
                    qualified_name: format!("fn_{i}"),
                },
                error_diagnostic: None,
                target_files: vec![format!("src/mod_{i}.rs")],
                initiator_node_id: format!("node-worker-{}", i % 3),
            };

            let fix = FixResult {
                success: true,
                commit_hash: format!("commit_{i}"),
                outcome: OutcomeType::ConsumedHelpful,
                tokens_consumed: 100,
            };

            engine_clone.execute_task(&task, Some(fix), false).await
        });
        handles.push(handle);
    }

    for h in handles {
        let res = h.await.expect("join handle").expect("task execution");
        assert!(res.execution_success);
    }

    // Verify feedback store received all 10 records
    let outcomes = feedback_store.list_records(100).unwrap();
    assert_eq!(outcomes.len(), 10);

    // Verify all 3 worker nodes received contributions
    for w in 0..3 {
        let rec = contribution_ledger
            .get(&format!("node-worker-{w}"))
            .unwrap();
        assert!(rec.completed_work_units >= 3);
    }
}
