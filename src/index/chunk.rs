//! Symbol-level chunking and token counting for structural retrieval.

use super::symbol::{CodeSymbol, SymbolKind};
use serde::{Deserialize, Serialize};

/// Fast tokenizer abstraction.
pub trait FastTokenizer: Send + Sync {
    fn count(&self, text: &str) -> anyhow::Result<usize>;
    fn tokenize(&self, text: &str) -> anyhow::Result<Vec<u32>>;
}

/// Default word / whitespace / code token counter.
#[derive(Debug, Clone, Default)]
pub struct SimpleCodeTokenizer;

impl FastTokenizer for SimpleCodeTokenizer {
    fn count(&self, text: &str) -> anyhow::Result<usize> {
        // Approximate BPE token count: ~4 characters or split by whitespace/punctuation
        let words = text.split_whitespace().count();
        let chars = text.len();
        let approx = (words + (chars / 4)) / 2;
        Ok(approx.max(1))
    }

    fn tokenize(&self, text: &str) -> anyhow::Result<Vec<u32>> {
        // Simple deterministic token mapping for testing/accounting
        let mut tokens = Vec::new();
        for word in text.split_whitespace() {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(word, &mut hasher);
            use std::hash::Hasher;
            tokens.push((hasher.finish() & 0xFFFFFFFF) as u32);
        }
        Ok(tokens)
    }
}

/// A chunk representing an exact symbol unit for retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolChunk {
    pub chunk_id: String,
    pub symbol_id: String,
    pub repository_id: String,
    pub relative_path: String,
    pub qualified_name: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc_comment: Option<String>,
    pub body: String,
    pub start_line: usize,
    pub end_line: usize,
    pub token_count: usize,
    pub content_hash: String,
}

impl SymbolChunk {
    /// Builds a chunk from a CodeSymbol and the corresponding file source.
    pub fn from_symbol(symbol: &CodeSymbol, source: &str, tokenizer: &dyn FastTokenizer) -> Self {
        let lines: Vec<&str> = source.lines().collect();
        let body = if symbol.start_line > 0 && symbol.start_line <= lines.len() {
            let end = symbol.end_line.min(lines.len());
            lines[symbol.start_line - 1..end].join("\n")
        } else {
            symbol.signature.clone().unwrap_or_default()
        };

        let token_count = tokenizer.count(&body).unwrap_or(1);
        let content_hash = CodeSymbol::compute_content_hash(&body);
        let chunk_id = format!("{}:{}", symbol.symbol_id, &content_hash[..8]);

        Self {
            chunk_id,
            symbol_id: symbol.symbol_id.clone(),
            repository_id: symbol.repository_id.clone(),
            relative_path: symbol.relative_path.clone(),
            qualified_name: symbol.qualified_name.clone(),
            kind: symbol.kind,
            signature: symbol.signature.clone(),
            doc_comment: symbol.doc_comment.clone(),
            body,
            start_line: symbol.start_line,
            end_line: symbol.end_line,
            token_count,
            content_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_chunk_creation() {
        let sym = CodeSymbol {
            symbol_id: "test-sym-1".into(),
            repository_id: "nauti-nodes".into(),
            relative_path: "src/lib.rs".into(),
            language: "rust".into(),
            qualified_name: "run_coordinator".into(),
            kind: SymbolKind::Function,
            start_byte: 0,
            end_byte: 50,
            start_line: 1,
            end_line: 2,
            signature: Some("pub fn run_coordinator()".into()),
            parent_symbol_id: None,
            content_hash: "hash123".into(),
            doc_comment: None,
            visibility: Some("pub".into()),
        };

        let src = "pub fn run_coordinator() {\n    // coordinate\n}\n";
        let tokenizer = SimpleCodeTokenizer;
        let chunk = SymbolChunk::from_symbol(&sym, src, &tokenizer);

        assert_eq!(chunk.symbol_id, "test-sym-1");
        assert_eq!(chunk.qualified_name, "run_coordinator");
        assert!(chunk.token_count > 0);
    }
}
