//! Integration test suite for Chunk 12: Wayfinder Rust Structural Index & Sidecar Storage.

use nauti_nodes::index::{
    CodeSymbol, CompilerIncident, CompilerMemoryStore, HttpEmbedder, HybridRetriever,
    IncrementalIndexer, RustParser, SimpleCodeTokenizer, StructuralStorage, SymbolChunk,
    SymbolKind,
};
use std::path::Path;

#[tokio::test]
async fn test_structural_parsing_and_symbol_extraction() {
    let source = r#"
/// Core runtime trait for local or distributed models.
pub trait ModelRuntime: Send + Sync {
    /// Dispatches an inference request.
    async fn infer(&self, prompt: &str) -> anyhow::Result<String>;
}

/// LlamaServer implementation.
pub struct LlamaServerRuntime {
    pub port: u16,
}

impl ModelRuntime for LlamaServerRuntime {
    async fn infer(&self, prompt: &str) -> anyhow::Result<String> {
        Ok(format!("output for {}", prompt))
    }
}

#[tokio::test]
async fn test_runtime_execution() {
    assert!(true);
}
"#;

    let symbols = RustParser::parse_file("nauti-nodes", "src/runtime.rs", source);
    assert!(!symbols.is_empty(), "symbols should be extracted");

    let trait_sym = symbols
        .iter()
        .find(|s| s.kind == SymbolKind::Trait)
        .expect("trait symbol");
    assert_eq!(trait_sym.qualified_name, "ModelRuntime");
    assert_eq!(
        trait_sym.doc_comment.as_deref(),
        Some("Core runtime trait for local or distributed models.")
    );

    let struct_sym = symbols
        .iter()
        .find(|s| s.kind == SymbolKind::Struct)
        .expect("struct symbol");
    assert_eq!(struct_sym.qualified_name, "LlamaServerRuntime");

    let method_sym = symbols
        .iter()
        .find(|s| s.kind == SymbolKind::Method)
        .expect("method symbol");
    assert_eq!(method_sym.qualified_name, "LlamaServerRuntime::infer");
    assert!(method_sym.parent_symbol_id.is_some());

    let test_sym = symbols
        .iter()
        .find(|s| s.kind == SymbolKind::Test)
        .expect("test symbol");
    assert_eq!(test_sym.qualified_name, "test_runtime_execution");
}

#[tokio::test]
async fn test_symbol_chunking_and_token_count() {
    let sym = CodeSymbol {
        symbol_id: "test-chunk-sym-1".into(),
        repository_id: "nauti-nodes".into(),
        relative_path: "src/engine.rs".into(),
        language: "rust".into(),
        qualified_name: "Engine::step".into(),
        kind: SymbolKind::Method,
        start_byte: 0,
        end_byte: 120,
        start_line: 1,
        end_line: 4,
        signature: Some("pub fn step(&mut self)".into()),
        parent_symbol_id: None,
        content_hash: "hash_step".into(),
        doc_comment: Some("Executes one engine step.".into()),
        visibility: Some("pub".into()),
    };

    let source = "pub fn step(&mut self) {\n    self.tick += 1;\n    self.process();\n}\n";
    let tokenizer = SimpleCodeTokenizer;
    let chunk = SymbolChunk::from_symbol(&sym, source, &tokenizer);

    assert_eq!(chunk.symbol_id, "test-chunk-sym-1");
    assert_eq!(chunk.qualified_name, "Engine::step");
    assert!(chunk.token_count >= 1);
    assert_eq!(chunk.start_line, 1);
    assert_eq!(chunk.end_line, 4);
}

#[tokio::test]
async fn test_compiler_memory_and_fix_linking() {
    let mem = CompilerMemoryStore::open("sqlite::memory:").expect("open compiler memory");

    let incident = CompilerIncident {
        incident_id: "inc-rustc-404".into(),
        repository_id: "nauti-nodes".into(),
        rustc_version: "rustc 1.80.0".into(),
        error_codes: vec!["E0382".into()],
        diagnostic: "use of moved value: `stream`".into(),
        affected_symbol_ids: vec!["sym_stream_handler".into()],
        failing_commit: Some("abc123f".into()),
        fixing_commit: None,
        verified_fixed: false,
        recorded_at_ms: 1700000000000,
    };

    mem.record_incident(&incident).expect("record incident");

    // Search by error code
    let results = mem.find_by_error_code("E0382").expect("find by error code");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].incident_id, "inc-rustc-404");
    assert!(!results[0].verified_fixed);

    // Mark fixed
    mem.mark_verified_fixed("inc-rustc-404", "def456c")
        .expect("mark verified fixed");

    let updated = mem
        .find_by_symbol_id("sym_stream_handler")
        .expect("find by symbol");
    assert_eq!(updated.len(), 1);
    assert!(updated[0].verified_fixed);
    assert_eq!(updated[0].fixing_commit.as_deref(), Some("def456c"));
}

#[tokio::test]
async fn test_storage_and_hybrid_retrieval() {
    let storage = StructuralStorage::open("sqlite::memory:").expect("open storage");
    let compiler_mem = CompilerMemoryStore::open("sqlite::memory:").expect("open compiler mem");

    // Index sample symbols
    let sym1 = CodeSymbol {
        symbol_id: "sym_coordinator".into(),
        repository_id: "nauti-nodes".into(),
        relative_path: "src/coordinator.rs".into(),
        language: "rust".into(),
        qualified_name: "run_coordinator".into(),
        kind: SymbolKind::Function,
        start_byte: 0,
        end_byte: 100,
        start_line: 1,
        end_line: 5,
        signature: Some("pub async fn run_coordinator(cfg: Config)".into()),
        parent_symbol_id: None,
        content_hash: "hash_coord".into(),
        doc_comment: Some("Runs coordinator service.".into()),
        visibility: Some("pub".into()),
    };

    let sym2 = CodeSymbol {
        symbol_id: "sym_worker".into(),
        repository_id: "nauti-nodes".into(),
        relative_path: "src/worker.rs".into(),
        language: "rust".into(),
        qualified_name: "run_worker".into(),
        kind: SymbolKind::Function,
        start_byte: 0,
        end_byte: 100,
        start_line: 1,
        end_line: 5,
        signature: Some("pub async fn run_worker(cfg: Config)".into()),
        parent_symbol_id: None,
        content_hash: "hash_worker".into(),
        doc_comment: Some("Runs inference worker.".into()),
        visibility: Some("pub".into()),
    };

    storage
        .upsert_file_symbols("nauti-nodes", "src/coordinator.rs", "h1", &[sym1.clone()])
        .expect("upsert sym1");
    storage
        .upsert_file_symbols("nauti-nodes", "src/worker.rs", "h2", &[sym2.clone()])
        .expect("upsert sym2");

    let tokenizer = SimpleCodeTokenizer;
    let chunk1 = SymbolChunk::from_symbol(
        &sym1,
        "pub async fn run_coordinator(cfg: Config) {\n    // start\n}\n",
        &tokenizer,
    );
    let chunk2 = SymbolChunk::from_symbol(
        &sym2,
        "pub async fn run_worker(cfg: Config) {\n    // start\n}\n",
        &tokenizer,
    );

    storage.store_chunk(&chunk1, None).expect("store chunk1");
    storage.store_chunk(&chunk2, None).expect("store chunk2");

    // Link a compiler incident to sym_worker
    let inc = CompilerIncident {
        incident_id: "inc-err-1".into(),
        repository_id: "nauti-nodes".into(),
        rustc_version: "1.80.0".into(),
        error_codes: vec!["E0277".into()],
        diagnostic: "the trait `Send` is not implemented for `RefCell`".into(),
        affected_symbol_ids: vec!["sym_worker".into()],
        failing_commit: Some("c1".into()),
        fixing_commit: Some("c2".into()),
        verified_fixed: true,
        recorded_at_ms: 1700000000000,
    };
    compiler_mem.record_incident(&inc).expect("record incident");

    // Initialize real embedder client (checks availability)
    let embedder = HttpEmbedder::default_local();
    let is_emb_online = embedder.is_available().await;

    let retriever = HybridRetriever::new(
        storage,
        compiler_mem,
        if is_emb_online {
            Some(Box::new(embedder))
        } else {
            None
        },
    );

    // Exact symbol query: "run_coordinator"
    let results = retriever
        .query("run_coordinator", 5)
        .await
        .expect("query run_coordinator");
    assert!(!results.is_empty());
    assert_eq!(results[0].qualified_name, "run_coordinator");
    assert!(results[0].score >= 100.0);
    assert!(results[0]
        .match_reasons
        .contains(&"exact_qualified_name_match".to_string()));

    // Compiler error query: "E0277"
    let inc_results = retriever.query("E0277", 5).await.expect("query E0277");
    assert!(!inc_results.is_empty());
    assert_eq!(inc_results[0].symbol_id, "sym_worker");
    assert!(!inc_results[0].compiler_incidents.is_empty());
}

#[tokio::test]
async fn test_incremental_indexer_on_real_codebase() {
    let storage = StructuralStorage::open("sqlite::memory:").expect("open storage");
    let indexer = IncrementalIndexer::new(storage, None);

    let src_dir = Path::new("src");
    let count = indexer
        .index_directory("nauti-nodes", src_dir)
        .await
        .expect("index src directory");

    assert!(
        count > 10,
        "should have indexed structural symbols from src/"
    );
}
