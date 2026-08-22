//! Integration test suite for Chunk 13: Live Anticipatory Context Injector.

use nauti_nodes::index::{
    CodeSymbol, CompilerIncident, CompilerMemoryStore, HybridRetriever, SimpleCodeTokenizer,
    StructuralStorage, SymbolChunk, SymbolKind,
};
use nauti_nodes::injector::{AgentIntent, AnticipatoryInjector, DeliveryMode};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test_environment() -> (
    Arc<AnticipatoryInjector>,
    Arc<StructuralStorage>,
    Arc<CompilerMemoryStore>,
) {
    let storage = Arc::new(StructuralStorage::open("sqlite::memory:").expect("open storage"));
    let compiler_mem =
        Arc::new(CompilerMemoryStore::open("sqlite::memory:").expect("open compiler mem"));

    // Index sample codebase symbols
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

    storage
        .upsert_file_symbols(
            "nauti-nodes",
            "src/coordinator.rs",
            "h1",
            &[sym_coord.clone()],
        )
        .expect("upsert sym_coord");
    storage
        .upsert_file_symbols(
            "nauti-nodes",
            "tests/coordinator_test.rs",
            "h2",
            &[sym_test.clone()],
        )
        .expect("upsert sym_test");

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

    storage.store_chunk(&chunk1, None).expect("store chunk1");
    storage.store_chunk(&chunk2, None).expect("store chunk2");

    // Record past compiler error for run_coordinator
    let incident = CompilerIncident {
        incident_id: "inc-rustc-888".into(),
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

    // Also populate retriever's internal storage
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

    let injector = AnticipatoryInjector::spawn(hybrid, compiler_mem.clone());
    (injector, storage, compiler_mem)
}

#[tokio::test]
async fn test_inline_intent_and_synchronous_wait() {
    let (injector, _, _) = setup_test_environment().await;

    let intent = AgentIntent::InspectSymbol {
        qualified_name: "run_coordinator".into(),
    };

    let package = injector
        .emit_and_wait(intent.clone(), DeliveryMode::Inline)
        .await
        .expect("emit and wait inline package");

    assert_eq!(package.delivery_mode, DeliveryMode::Inline);
    assert_eq!(package.intent, intent);
    assert!(!package.retrieved_results.is_empty());
    assert_eq!(
        package.retrieved_results[0].qualified_name,
        "run_coordinator"
    );

    let telem = injector.telemetry().snapshot();
    assert_eq!(telem.packages_consumed, 0);
}

#[tokio::test]
async fn test_concurrent_next_delivery_mode() {
    let (injector, _, _) = setup_test_environment().await;

    let intent = AgentIntent::InspectFile {
        path: "src/coordinator.rs".into(),
    };

    // Emit asynchronously without blocking
    injector
        .emit_intent(intent.clone(), DeliveryMode::Next)
        .await
        .expect("emit intent");

    // Wait a brief moment for the background worker to finish constructing the package
    sleep(Duration::from_millis(50)).await;

    let consumed = injector
        .consume_next_package(DeliveryMode::Next)
        .expect("consume package");

    assert_eq!(consumed.delivery_mode, DeliveryMode::Next);
    assert!(consumed.consumed);

    let telem = injector.telemetry().snapshot();
    assert_eq!(telem.packages_consumed, 1);
}

#[tokio::test]
async fn test_review_packet_construction() {
    let (injector, _, _) = setup_test_environment().await;

    let intent = AgentIntent::PrepareReview {
        changed_files: vec!["src/coordinator.rs".into()],
    };

    let package = injector
        .emit_and_wait(intent.clone(), DeliveryMode::Review)
        .await
        .expect("review package");

    assert_eq!(package.delivery_mode, DeliveryMode::Review);
    assert!(!package.retrieved_results.is_empty());
}

#[tokio::test]
async fn test_cache_delivery_mode() {
    let (injector, _, _) = setup_test_environment().await;

    let intent = AgentIntent::VerifyInvariant {
        description: "run_coordinator".into(),
    };

    injector
        .emit_intent(intent.clone(), DeliveryMode::Cache)
        .await
        .expect("emit cache intent");

    sleep(Duration::from_millis(50)).await;

    let cached = injector
        .fetch_cached_package("run_coordinator")
        .expect("fetch cached package");

    assert_eq!(cached.delivery_mode, DeliveryMode::Cache);
    assert!(cached.consumed);

    let telem = injector.telemetry().snapshot();
    assert_eq!(telem.packages_consumed, 1);
}

#[tokio::test]
async fn test_telemetry_accounting_real_metrics() {
    let (injector, _, _) = setup_test_environment().await;
    let telem = injector.telemetry();

    telem.record_tool_call();
    telem.record_tool_call();
    telem.record_failed_iteration();
    telem.record_package_ignored();
    telem.complete_task();

    let snap = telem.snapshot();
    assert_eq!(snap.tool_calls, 2);
    assert_eq!(snap.failed_compile_test_iterations, 1);
    assert_eq!(snap.packages_ignored, 1);
    assert_eq!(snap.packages_consumed, 0);
}
