//! Structural code symbol representations for Wayfinder Rust indexing.

use serde::{Deserialize, Serialize};

/// Categorization of AST symbols extracted from source files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolKind {
    Module,
    Struct,
    Enum,
    EnumVariant,
    Trait,
    Impl,
    Function,
    Method,
    Const,
    Static,
    TypeAlias,
    Macro,
    Test,
    Field,
}

impl SymbolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Module => "module",
            Self::Struct => "struct",
            Self::Enum => "enum",
            Self::EnumVariant => "variant",
            Self::Trait => "trait",
            Self::Impl => "impl",
            Self::Function => "function",
            Self::Method => "method",
            Self::Const => "const",
            Self::Static => "static",
            Self::TypeAlias => "type_alias",
            Self::Macro => "macro",
            Self::Test => "test",
            Self::Field => "field",
        }
    }
}

/// A structurally parsed symbol in a codebase with source spans and identity hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeSymbol {
    pub symbol_id: String,
    pub repository_id: String,
    pub relative_path: String,
    pub language: String,
    pub qualified_name: String,
    pub kind: SymbolKind,
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub signature: Option<String>,
    pub parent_symbol_id: Option<String>,
    pub content_hash: String,
    pub doc_comment: Option<String>,
    pub visibility: Option<String>,
}

impl CodeSymbol {
    /// Computes a deterministic, stable symbol ID based on structural identity.
    pub fn compute_symbol_id(
        repo: &str,
        path: &str,
        qualified_name: &str,
        kind: SymbolKind,
    ) -> String {
        let raw = format!("{repo}::{path}::{qualified_name}::{:?}", kind);
        let hash = ring::digest::digest(&ring::digest::SHA256, raw.as_bytes());
        hex::encode(&hash.as_ref()[..16])
    }

    /// Computes a SHA256 hex hash of code text.
    pub fn compute_content_hash(content: &str) -> String {
        let hash = ring::digest::digest(&ring::digest::SHA256, content.as_bytes());
        hex::encode(&hash.as_ref()[..16])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_id_stability() {
        let id1 = CodeSymbol::compute_symbol_id(
            "nauti-nodes",
            "src/state.rs",
            "AppState::new",
            SymbolKind::Method,
        );
        let id2 = CodeSymbol::compute_symbol_id(
            "nauti-nodes",
            "src/state.rs",
            "AppState::new",
            SymbolKind::Method,
        );
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 32);
    }
}
