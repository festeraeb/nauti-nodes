//! Wayfinder Rust-native structural indexing pipeline.
//!
//! Provides structural parsing, stable symbol IDs, token accounting,
//! real vector embedder integration, SQLite/sidecar storage, compiler incident memory,
//! and multi-stage hybrid retrieval.

pub mod chunk;
pub mod compiler_memory;
pub mod embedder;
pub mod hybrid;
pub mod parser;
pub mod storage;
pub mod symbol;
pub mod watcher;

pub use chunk::{FastTokenizer, SimpleCodeTokenizer, SymbolChunk};
pub use compiler_memory::{CompilerIncident, CompilerMemoryStore};
pub use embedder::{Embedder, HttpEmbedder};
pub use hybrid::{HybridRetriever, RetrievalResult};
pub use parser::RustParser;
pub use storage::StructuralStorage;
pub use symbol::{CodeSymbol, SymbolKind};
pub use watcher::IncrementalIndexer;
