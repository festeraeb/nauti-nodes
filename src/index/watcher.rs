//! Incremental code indexing and file watching support.

use super::chunk::{SimpleCodeTokenizer, SymbolChunk};
use super::embedder::Embedder;
use super::parser::RustParser;
use super::storage::StructuralStorage;
use super::symbol::CodeSymbol;
use std::path::Path;

/// High-level coordinator for indexing Rust source trees.
pub struct IncrementalIndexer {
    storage: StructuralStorage,
    embedder: Option<Box<dyn Embedder>>,
    tokenizer: SimpleCodeTokenizer,
}

impl IncrementalIndexer {
    pub fn new(storage: StructuralStorage, embedder: Option<Box<dyn Embedder>>) -> Self {
        Self {
            storage,
            embedder,
            tokenizer: SimpleCodeTokenizer,
        }
    }

    /// Indexes a single file into the structural index.
    pub async fn index_file(
        &self,
        repo: &str,
        relative_path: &str,
        source_code: &str,
    ) -> Result<usize, crate::types::Error> {
        let content_hash = CodeSymbol::compute_content_hash(source_code);

        // 1. Parse structural symbols
        let symbols = RustParser::parse_file(repo, relative_path, source_code);
        let sym_count = symbols.len();

        // 2. Persist symbols to relational storage
        self.storage
            .upsert_file_symbols(repo, relative_path, &content_hash, &symbols)?;

        // 3. Build symbol chunks
        let chunks: Vec<SymbolChunk> = symbols
            .iter()
            .map(|s| SymbolChunk::from_symbol(s, source_code, &self.tokenizer))
            .collect();

        // 4. Compute embeddings if real embedder is active
        for chunk in &chunks {
            let embedding = if let Some(ref emb) = self.embedder {
                emb.embed(&chunk.body).await.ok()
            } else {
                None
            };
            self.storage.store_chunk(chunk, embedding.as_deref())?;
        }

        Ok(sym_count)
    }

    /// Indexes an entire directory tree recursively.
    pub async fn index_directory(
        &self,
        repo: &str,
        root_dir: &Path,
    ) -> Result<usize, crate::types::Error> {
        let mut total_symbols = 0;
        let mut dirs_to_visit = vec![root_dir.to_path_buf()];

        while let Some(current_dir) = dirs_to_visit.pop() {
            if let Ok(entries) = std::fs::read_dir(&current_dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        // Ignore hidden dirs and target
                        if !name.starts_with('.') && name != "target" {
                            dirs_to_visit.push(path);
                        }
                    } else if path.is_file()
                        && path.extension().and_then(|s| s.to_str()) == Some("rs")
                    {
                        if let Ok(rel) = path.strip_prefix(root_dir) {
                            let rel_str = rel.to_string_lossy().to_string();
                            if let Ok(content) = std::fs::read_to_string(&path) {
                                if let Ok(count) = self.index_file(repo, &rel_str, &content).await {
                                    total_symbols += count;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(total_symbols)
    }
}
