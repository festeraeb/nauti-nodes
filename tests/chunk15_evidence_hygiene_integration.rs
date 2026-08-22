//! Comprehensive integration tests for Chunk 15: Great Idea #87 - Evidence hygiene & scalable storage.

use nauti_nodes::hygiene::{
    EvidenceHygieneEngine, EvidenceSanitizer, HygienePolicy, ScalableStorageManager,
};
use nauti_nodes::index::{
    CodeSymbol, CompilerIncident, CompilerMemoryStore, SimpleCodeTokenizer,
    StructuralStorage, SymbolChunk, SymbolKind,
};
use tempfile::NamedTempFile;

fn create_sample_symbol(id: &str, name: &str, file: &str, line: usize) -> CodeSymbol {
    CodeSymbol {
        symbol_id: id.into(),
        repository_id: "test-repo".into(),
        relative_path: file.into(),
        language: "rust".into(),
        qualified_name: name.into(),
        kind: SymbolKind::Function,
        start_byte: 0,
        end_byte: 100,
        start_line: line,
        end_line: line + 5,
        signature: Some(format!("pub fn {name}()")),
        parent_symbol_id: None,
        content_hash: format!("hash_{id}"),
        doc_comment: Some(format!("Doc for {name}")),
        visibility: Some("pub".into()),
    }
}

#[test]
fn test_evidence_sanitizer_removes_secrets_and_paths() {
    let sanitizer = EvidenceSanitizer::new();

    // 1. Test bearer tokens and OpenAI/GitHub API keys
    let raw_diag = "Error in /home/cesarops/projects/nauti-nodes/src/main.rs: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xyz failed with key sk-1234567890abcdef1234567890abcdef and token ghp_abcdefghijklmnopqrstuvwxyz012345";
    let sanitized = sanitizer.sanitize_text(raw_diag);

    assert!(!sanitized.contains("/home/cesarops/"));
    assert!(sanitized.contains("~/"));
    assert!(!sanitized.contains("sk-1234567890abcdef1234567890abcdef"));
    assert!(sanitized.contains("[REDACTED_OPENAI_KEY]"));
    assert!(!sanitized.contains("ghp_abcdefghijklmnopqrstuvwxyz012345"));
    assert!(sanitized.contains("[REDACTED_GITHUB_TOKEN]"));
    assert!(!sanitized.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"));
    assert!(sanitized.contains("[REDACTED_BEARER_TOKEN]"));

    // 2. Test symbol sanitization
    let mut sym = create_sample_symbol("s1", "auth", "src/auth.rs", 10);
    sym.doc_comment = Some("Connects to Google API using AIzaSyD-1234567890abcdefghijklmnopqrst".into());
    let sanitized_sym = sanitizer.sanitize_symbol(&sym);

    assert!(!sanitized_sym.doc_comment.as_ref().unwrap().contains("AIzaSyD"));
    assert!(sanitized_sym.doc_comment.as_ref().unwrap().contains("[REDACTED_GOOGLE_API_KEY]"));
}

#[test]
fn test_hygiene_cycle_orphan_purging_and_ttl_expiration() {
    let storage = StructuralStorage::open("sqlite::memory:").expect("storage");
    let compiler_mem = CompilerMemoryStore::open("sqlite::memory:").expect("compiler memory");

    let sym1 = create_sample_symbol("s1", "foo", "src/foo.rs", 1);
    let sym2 = create_sample_symbol("s2", "bar", "src/bar.rs", 1);

    storage
        .upsert_file_symbols("test-repo", "src/foo.rs", "h1", &[sym1.clone()])
        .expect("upsert sym1");
    storage
        .upsert_file_symbols("test-repo", "src/bar.rs", "h2", &[sym2.clone()])
        .expect("upsert sym2");

    let tokenizer = SimpleCodeTokenizer;
    let chunk1 = SymbolChunk::from_symbol(&sym1, "fn foo() {}", &tokenizer);
    let chunk2 = SymbolChunk::from_symbol(&sym2, "fn bar() {}", &tokenizer);

    // Also inject an orphaned chunk referencing deleted "s_deleted"
    let mut chunk_orphan = SymbolChunk::from_symbol(&sym1, "fn deleted() {}", &tokenizer);
    chunk_orphan.chunk_id = "chunk_orphan".into();
    chunk_orphan.symbol_id = "s_deleted".into();

    storage.store_chunk(&chunk1, None).expect("store c1");
    storage.store_chunk(&chunk2, None).expect("store c2");
    storage.store_chunk(&chunk_orphan, None).expect("store orphan");

    assert_eq!(storage.count_chunks().unwrap(), 3);

    // Add unverified incident with stale timestamp
    let old_incident = CompilerIncident {
        incident_id: "inc_old".into(),
        repository_id: "test-repo".into(),
        rustc_version: "1.80".into(),
        error_codes: vec!["E0308".into()],
        diagnostic: "mismatched types in /home/user/code/foo.rs with key sk-999999999999999999999999999".into(),
        affected_symbol_ids: vec!["s1".into()],
        failing_commit: Some("c1".into()),
        fixing_commit: None,
        verified_fixed: false,
        recorded_at_ms: 1000, // Very old
    };
    compiler_mem.record_incident(&old_incident).expect("record old");

    // Add verified incident (must NOT be purged regardless of age)
    let verified_incident = CompilerIncident {
        incident_id: "inc_verified".into(),
        repository_id: "test-repo".into(),
        rustc_version: "1.80".into(),
        error_codes: vec!["E0382".into()],
        diagnostic: "borrow of moved value".into(),
        affected_symbol_ids: vec!["s2".into()],
        failing_commit: Some("c2".into()),
        fixing_commit: Some("fix_c2".into()),
        verified_fixed: true,
        recorded_at_ms: 1000,
    };
    compiler_mem.record_incident(&verified_incident).expect("record verified");

    let policy = HygienePolicy::strict_test_policy();
    let engine = EvidenceHygieneEngine::new(policy);

    let report = engine.run_hygiene_cycle(&storage, &compiler_mem).expect("hygiene cycle");

    // Assert orphan was purged
    assert_eq!(report.orphaned_chunks_purged, 1);
    assert_eq!(storage.count_chunks().unwrap(), 2);

    // Assert stale unverified was expired, verified was preserved
    assert_eq!(report.stale_incidents_expired, 1);
    assert_eq!(compiler_mem.count_incidents().unwrap(), 1);

    let remaining = compiler_mem.find_by_error_code("E0382").unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].verified_fixed);

    // Assert diagnostic was sanitized before purge
    assert_eq!(report.sanitized_incidents_count, 1);
}

#[test]
fn test_incident_deduplication_and_compaction() {
    let compiler_mem = CompilerMemoryStore::open("sqlite::memory:").expect("compiler memory");
    let storage = StructuralStorage::open("sqlite::memory:").expect("storage");

    // Record 3 duplicate incidents with same diagnostic and error code
    for i in 1..=3 {
        let incident = CompilerIncident {
            incident_id: format!("inc_dup_{i}"),
            repository_id: "test-repo".into(),
            rustc_version: "1.80".into(),
            error_codes: vec!["E0425".into()],
            diagnostic: "cannot find value `x` in this scope".into(),
            affected_symbol_ids: vec![format!("sym_{i}")],
            failing_commit: Some(format!("commit_{i}")),
            fixing_commit: None,
            verified_fixed: false,
            recorded_at_ms: 2000000000000 + i * 1000,
        };
        compiler_mem.record_incident(&incident).expect("record dup");
    }

    assert_eq!(compiler_mem.count_incidents().unwrap(), 3);

    let mut policy = HygienePolicy::default();
    policy.ttl_unverified_incidents_ms = 1_000_000_000_000; // Far future TTL
    let engine = EvidenceHygieneEngine::new(policy);

    let report = engine.run_hygiene_cycle(&storage, &compiler_mem).expect("hygiene cycle");

    assert_eq!(report.duplicate_incidents_compacted, 2);
    assert_eq!(compiler_mem.count_incidents().unwrap(), 1);

    let consolidated = compiler_mem.find_by_error_code("E0425").unwrap();
    assert_eq!(consolidated.len(), 1);
    // All 3 affected symbols merged
    assert_eq!(consolidated[0].affected_symbol_ids.len(), 3);
}

#[test]
fn test_tiered_storage_manager_and_disk_quota_enforcement() {
    let mut policy = HygienePolicy::default();
    policy.max_storage_quota_bytes = 50 * 1024 * 1024; // 50 MB

    let manager = ScalableStorageManager::new(policy);

    // Below quota
    let report_ok = manager.check_quota(20 * 1024 * 1024);
    assert!(!report_ok.quota_exceeded);
    assert_eq!(report_ok.action_taken, "WITHIN_STORAGE_QUOTA");

    // Exceeds quota
    let report_exceeded = manager.check_quota(80 * 1024 * 1024);
    assert!(report_exceeded.quota_exceeded);
    assert_eq!(
        report_exceeded.action_taken,
        "QUOTA_EXCEEDED_COLD_TIER_DEMOTION_TRIGGERED"
    );
}

#[test]
fn test_evidence_archive_bundle_export_import_cryptographic_verification() {
    let storage_src = StructuralStorage::open("sqlite::memory:").expect("storage src");
    let compiler_mem_src = CompilerMemoryStore::open("sqlite::memory:").expect("compiler mem src");

    let mut sym = create_sample_symbol("s_exp", "export_me", "src/export.rs", 10);
    sym.repository_id = "repo-alpha".into();
    storage_src
        .upsert_file_symbols("repo-alpha", "src/export.rs", "h_exp", &[sym.clone()])
        .expect("upsert");

    let tokenizer = SimpleCodeTokenizer;
    let chunk = SymbolChunk::from_symbol(&sym, "fn export_me() { println!(\"ok\"); }", &tokenizer);
    storage_src.store_chunk(&chunk, None).expect("chunk");

    let incident = CompilerIncident {
        incident_id: "inc_exp_1".into(),
        repository_id: "repo-alpha".into(),
        rustc_version: "1.80".into(),
        error_codes: vec!["E0599".into()],
        diagnostic: "no method named `foo` found".into(),
        affected_symbol_ids: vec!["s_exp".into()],
        failing_commit: Some("c_fail".into()),
        fixing_commit: Some("c_fix".into()),
        verified_fixed: true,
        recorded_at_ms: 1700000000000,
    };
    compiler_mem_src.record_incident(&incident).expect("record incident");

    let manager = ScalableStorageManager::new(HygienePolicy::default());

    // 1. Export Bundle
    let bundle = manager
        .export_bundle("repo-alpha", &storage_src, &compiler_mem_src)
        .expect("export bundle");

    assert_eq!(bundle.repository_id, "repo-alpha");
    assert_eq!(bundle.symbols.len(), 1);
    assert_eq!(bundle.chunks.len(), 1);
    assert_eq!(bundle.incidents.len(), 1);
    assert!(bundle.verify_integrity());

    // 2. Test File Save and Load
    let temp_file = NamedTempFile::new().expect("temp file");
    ScalableStorageManager::save_bundle_to_file(&bundle, temp_file.path()).expect("save bundle");

    let loaded_bundle = ScalableStorageManager::load_bundle_from_file(temp_file.path()).expect("load bundle");
    assert!(loaded_bundle.verify_integrity());
    assert_eq!(loaded_bundle.manifest_sha256, bundle.manifest_sha256);

    // 3. Test Import on fresh target databases
    let storage_dst = StructuralStorage::open("sqlite::memory:").expect("storage dst");
    let compiler_mem_dst = CompilerMemoryStore::open("sqlite::memory:").expect("compiler mem dst");

    let summary = manager
        .import_bundle(&loaded_bundle, &storage_dst, &compiler_mem_dst)
        .expect("import bundle");

    assert!(summary.verified_checksum);
    assert_eq!(summary.symbols_imported, 1);
    assert_eq!(summary.chunks_imported, 1);
    assert_eq!(summary.incidents_imported, 1);

    // Verify target storage has the imported data
    let found_symbols = storage_dst.find_by_qualified_name("export_me").unwrap();
    assert_eq!(found_symbols.len(), 1);
    assert_eq!(found_symbols[0].symbol_id, "s_exp");

    let found_incidents = compiler_mem_dst.find_by_error_code("E0599").unwrap();
    assert_eq!(found_incidents.len(), 1);
    assert_eq!(found_incidents[0].incident_id, "inc_exp_1");

    // 4. Test Tampered Bundle Rejection
    let mut tampered = loaded_bundle.clone();
    tampered.symbols[0].qualified_name = "tampered_symbol_name".into();
    assert!(!tampered.verify_integrity());

    let err = manager.import_bundle(&tampered, &storage_dst, &compiler_mem_dst);
    assert!(err.is_err());
}
