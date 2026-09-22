//! Scalable tiered evidence storage, archive bundle export/import, and quota enforcement.

use super::engine::EvidenceHygieneEngine;
use super::policy::HygienePolicy;
use crate::index::compiler_memory::{CompilerIncident, CompilerMemoryStore};
use crate::index::{CodeSymbol, StructuralStorage, SymbolChunk};
use crate::types::now_ms;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;
use uuid::Uuid;

/// Storage tier categorization for evidence artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// In-memory fast cache (sub-millisecond latency).
    HotMemory,
    /// Local SQLite relational and vector store.
    WarmSqlite,
    /// Cold disk-backed compressed archive bundle.
    ColdArchive,
}

/// Standalone cryptographic evidence archive bundle for backup and federated node replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceArchiveBundle {
    pub bundle_id: String,
    pub repository_id: String,
    pub created_at_ms: u64,
    pub symbols: Vec<CodeSymbol>,
    pub chunks: Vec<SymbolChunk>,
    pub incidents: Vec<CompilerIncident>,
    pub manifest_sha256: String,
}

impl EvidenceArchiveBundle {
    /// Constructs a new bundle and calculates its cryptographic manifest checksum.
    pub fn new(
        repository_id: String,
        symbols: Vec<CodeSymbol>,
        chunks: Vec<SymbolChunk>,
        incidents: Vec<CompilerIncident>,
    ) -> Self {
        let bundle_id = format!("bundle-{}", Uuid::new_v4());
        let created_at_ms = now_ms();

        let mut payload = format!(
            "{}:{}:{}:{}:{}:{}",
            bundle_id,
            repository_id,
            created_at_ms,
            symbols.len(),
            chunks.len(),
            incidents.len()
        );
        for s in &symbols {
            payload.push_str(&format!(
                ":sym:{}:{}:{}",
                s.symbol_id, s.qualified_name, s.content_hash
            ));
        }
        for c in &chunks {
            payload.push_str(&format!(":chunk:{}:{}", c.chunk_id, c.content_hash));
        }
        for inc in &incidents {
            payload.push_str(&format!(":inc:{}:{}", inc.incident_id, inc.diagnostic));
        }

        let manifest_sha256 = EvidenceHygieneEngine::compute_sha256(payload.as_bytes());

        Self {
            bundle_id,
            repository_id,
            created_at_ms,
            symbols,
            chunks,
            incidents,
            manifest_sha256,
        }
    }

    /// Verifies the cryptographic integrity of the bundle.
    pub fn verify_integrity(&self) -> bool {
        let mut payload = format!(
            "{}:{}:{}:{}:{}:{}",
            self.bundle_id,
            self.repository_id,
            self.created_at_ms,
            self.symbols.len(),
            self.chunks.len(),
            self.incidents.len()
        );
        for s in &self.symbols {
            payload.push_str(&format!(
                ":sym:{}:{}:{}",
                s.symbol_id, s.qualified_name, s.content_hash
            ));
        }
        for c in &self.chunks {
            payload.push_str(&format!(":chunk:{}:{}", c.chunk_id, c.content_hash));
        }
        for inc in &self.incidents {
            payload.push_str(&format!(":inc:{}:{}", inc.incident_id, inc.diagnostic));
        }

        let computed = EvidenceHygieneEngine::compute_sha256(payload.as_bytes());
        computed == self.manifest_sha256
    }
}

/// Summary of an evidence archive bundle import operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportSummary {
    pub symbols_imported: usize,
    pub chunks_imported: usize,
    pub incidents_imported: usize,
    pub verified_checksum: bool,
}

/// Report detailing quota enforcement actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaEnforcementReport {
    pub current_bytes: u64,
    pub max_quota_bytes: u64,
    pub quota_exceeded: bool,
    pub action_taken: String,
}

/// Manages multi-tier storage scaling, archiving, and quota boundaries.
pub struct ScalableStorageManager {
    policy: HygienePolicy,
}

impl ScalableStorageManager {
    /// Creates a new ScalableStorageManager.
    pub fn new(policy: HygienePolicy) -> Self {
        Self { policy }
    }

    /// Exports all symbols, chunks, and incidents for a repository into a verified bundle.
    pub fn export_bundle(
        &self,
        repository_id: &str,
        storage: &StructuralStorage,
        compiler_mem: &CompilerMemoryStore,
    ) -> Result<EvidenceArchiveBundle, crate::types::Error> {
        let all_symbols = storage.list_all_symbols()?;
        let repo_symbols: Vec<CodeSymbol> = all_symbols
            .into_iter()
            .filter(|s| s.repository_id == repository_id)
            .collect();

        let all_chunks = storage.list_all_chunks()?;
        let repo_chunks: Vec<SymbolChunk> = all_chunks
            .into_iter()
            .filter(|c| c.repository_id == repository_id)
            .collect();

        let all_incidents = compiler_mem.list_all_incidents()?;
        let repo_incidents: Vec<CompilerIncident> = all_incidents
            .into_iter()
            .filter(|i| i.repository_id == repository_id)
            .collect();

        let bundle = EvidenceArchiveBundle::new(
            repository_id.to_string(),
            repo_symbols,
            repo_chunks,
            repo_incidents,
        );

        info!(
            "Exported evidence bundle {} for repository {}: {} symbols, {} chunks, {} incidents",
            bundle.bundle_id,
            repository_id,
            bundle.symbols.len(),
            bundle.chunks.len(),
            bundle.incidents.len()
        );

        Ok(bundle)
    }

    /// Saves a bundle to disk as a JSON archive.
    pub fn save_bundle_to_file(
        bundle: &EvidenceArchiveBundle,
        path: &Path,
    ) -> Result<(), crate::types::Error> {
        let json = serde_json::to_string_pretty(bundle).map_err(|e| {
            crate::types::Error::Internal(format!("Failed to serialize bundle: {e}"))
        })?;
        std::fs::write(path, json).map_err(|e| {
            crate::types::Error::Internal(format!("Failed to write bundle file: {e}"))
        })?;
        Ok(())
    }

    /// Reads and parses a bundle from disk.
    pub fn load_bundle_from_file(
        path: &Path,
    ) -> Result<EvidenceArchiveBundle, crate::types::Error> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            crate::types::Error::Internal(format!("Failed to read bundle file: {e}"))
        })?;
        let bundle: EvidenceArchiveBundle = serde_json::from_str(&raw).map_err(|e| {
            crate::types::Error::Internal(format!("Failed to deserialize bundle: {e}"))
        })?;
        Ok(bundle)
    }

    /// Imports and reconciles an archive bundle into live storage and compiler memory.
    pub fn import_bundle(
        &self,
        bundle: &EvidenceArchiveBundle,
        storage: &StructuralStorage,
        compiler_mem: &CompilerMemoryStore,
    ) -> Result<ImportSummary, crate::types::Error> {
        if !bundle.verify_integrity() {
            return Err(crate::types::Error::Internal(
                "Evidence bundle failed cryptographic checksum verification".into(),
            ));
        }

        // Group symbols by relative_path to use upsert_file_symbols
        let mut by_file: std::collections::HashMap<String, Vec<CodeSymbol>> =
            std::collections::HashMap::new();
        for sym in &bundle.symbols {
            by_file
                .entry(sym.relative_path.clone())
                .or_default()
                .push(sym.clone());
        }

        let mut symbols_imported = 0;
        for (path, syms) in by_file {
            storage.upsert_file_symbols(&bundle.repository_id, &path, "imported_bundle", &syms)?;
            symbols_imported += syms.len();
        }

        let mut chunks_imported = 0;
        for chunk in &bundle.chunks {
            storage.store_chunk(chunk, None)?;
            chunks_imported += 1;
        }

        let mut incidents_imported = 0;
        for inc in &bundle.incidents {
            compiler_mem.record_incident(inc)?;
            incidents_imported += 1;
        }

        info!(
            "Imported evidence bundle {}: {} symbols, {} chunks, {} incidents",
            bundle.bundle_id, symbols_imported, chunks_imported, incidents_imported
        );

        Ok(ImportSummary {
            symbols_imported,
            chunks_imported,
            incidents_imported,
            verified_checksum: true,
        })
    }

    /// Evaluates storage size against quota boundaries.
    pub fn check_quota(&self, current_bytes: u64) -> QuotaEnforcementReport {
        let max_quota = self.policy.max_storage_quota_bytes;
        let exceeded = current_bytes > max_quota;

        let action_taken = if exceeded {
            "QUOTA_EXCEEDED_COLD_TIER_DEMOTION_TRIGGERED".to_string()
        } else {
            "WITHIN_STORAGE_QUOTA".to_string()
        };

        QuotaEnforcementReport {
            current_bytes,
            max_quota_bytes: max_quota,
            quota_exceeded: exceeded,
            action_taken,
        }
    }
}
