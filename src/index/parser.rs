//! Structural Rust code parser for symbol extraction.

use super::symbol::{CodeSymbol, SymbolKind};

/// Parser that inspects Rust source text and extracts structural symbols.
pub struct RustParser;

impl RustParser {
    /// Extracts all top-level and nested structural code symbols from a Rust source file.
    pub fn parse_file(repo: &str, relative_path: &str, source: &str) -> Vec<CodeSymbol> {
        let mut symbols = Vec::new();
        let lines: Vec<&str> = source.lines().collect();

        let mut current_impl: Option<(String, String)> = None; // (impl_target, symbol_id)
        let mut impl_brace_depth: i32 = 0;
        let mut current_brace_depth: i32 = 0;
        let mut doc_accumulator: Vec<String> = Vec::new();
        let mut pending_test = false;

        let mut byte_offset = 0;

        for (line_idx, line) in lines.iter().enumerate() {
            let line_num = line_idx + 1;
            let trimmed = line.trim();
            let start_byte = byte_offset;
            let end_byte = byte_offset + line.len();
            byte_offset = end_byte + 1; // account for newline

            // Collect doc comments
            if trimmed.starts_with("///") {
                doc_accumulator.push(trimmed.trim_start_matches("///").trim().to_string());
                continue;
            } else if trimmed.starts_with("#[test]") || trimmed.starts_with("#[tokio::test]") {
                pending_test = true;
                continue;
            } else if trimmed.starts_with("//") || trimmed.is_empty() {
                continue;
            }

            let doc_comment = if !doc_accumulator.is_empty() {
                let doc = doc_accumulator.join("\n");
                doc_accumulator.clear();
                Some(doc)
            } else {
                None
            };

            // Track brace depths
            let open_braces = line.chars().filter(|&c| c == '{').count() as i32;
            let close_braces = line.chars().filter(|&c| c == '}').count() as i32;

            // Check for module declarations
            if trimmed.starts_with("pub mod ") || trimmed.starts_with("mod ") {
                if let Some(mod_name) = extract_identifier(trimmed, &["pub mod ", "mod "]) {
                    let kind = SymbolKind::Module;
                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &mod_name, kind);
                    symbols.push(CodeSymbol {
                        symbol_id,
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name: mod_name.clone(),
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: None,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: extract_visibility(trimmed),
                    });
                }
            }
            // Check for struct declarations
            else if trimmed.contains("struct ") && !trimmed.starts_with("//") {
                if let Some(struct_name) = extract_struct_or_enum_name(trimmed, "struct ") {
                    let kind = SymbolKind::Struct;
                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &struct_name, kind);
                    symbols.push(CodeSymbol {
                        symbol_id,
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name: struct_name.clone(),
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: None,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: extract_visibility(trimmed),
                    });
                }
            }
            // Check for enum declarations
            else if trimmed.contains("enum ") && !trimmed.starts_with("//") {
                if let Some(enum_name) = extract_struct_or_enum_name(trimmed, "enum ") {
                    let kind = SymbolKind::Enum;
                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &enum_name, kind);
                    symbols.push(CodeSymbol {
                        symbol_id,
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name: enum_name.clone(),
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: None,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: extract_visibility(trimmed),
                    });
                }
            }
            // Check for trait declarations
            else if trimmed.contains("trait ") && !trimmed.starts_with("//") {
                if let Some(trait_name) = extract_struct_or_enum_name(trimmed, "trait ") {
                    let kind = SymbolKind::Trait;
                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &trait_name, kind);
                    symbols.push(CodeSymbol {
                        symbol_id,
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name: trait_name.clone(),
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: None,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: extract_visibility(trimmed),
                    });
                }
            }
            // Check for impl blocks
            else if trimmed.starts_with("impl ") || trimmed.starts_with("pub impl ") {
                if let Some(impl_target) = extract_impl_target(trimmed) {
                    let kind = SymbolKind::Impl;
                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &impl_target, kind);
                    symbols.push(CodeSymbol {
                        symbol_id: symbol_id.clone(),
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name: format!("impl {}", impl_target),
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: None,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: None,
                    });
                    current_impl = Some((impl_target, symbol_id));
                    impl_brace_depth = current_brace_depth;
                }
            }
            // Check for function or method declarations
            else if (trimmed.contains("fn ") || trimmed.starts_with("async fn "))
                && !trimmed.starts_with("//")
            {
                if let Some(fn_name) = extract_fn_name(trimmed) {
                    let is_test = pending_test
                        || fn_name.starts_with("test_")
                        || relative_path.starts_with("tests/");
                    pending_test = false;

                    let (kind, qualified_name, parent_id) = if is_test {
                        (SymbolKind::Test, fn_name.clone(), None)
                    } else if let Some((ref target, ref p_id)) = current_impl {
                        (
                            SymbolKind::Method,
                            format!("{}::{}", target, fn_name),
                            Some(p_id.clone()),
                        )
                    } else {
                        (SymbolKind::Function, fn_name.clone(), None)
                    };

                    let symbol_id =
                        CodeSymbol::compute_symbol_id(repo, relative_path, &qualified_name, kind);
                    symbols.push(CodeSymbol {
                        symbol_id,
                        repository_id: repo.to_string(),
                        relative_path: relative_path.to_string(),
                        language: "rust".to_string(),
                        qualified_name,
                        kind,
                        start_byte,
                        end_byte,
                        start_line: line_num,
                        end_line: line_num,
                        signature: Some(line.to_string()),
                        parent_symbol_id: parent_id,
                        content_hash: CodeSymbol::compute_content_hash(line),
                        doc_comment: doc_comment.clone(),
                        visibility: extract_visibility(trimmed),
                    });
                }
            }

            current_brace_depth += open_braces - close_braces;
            if current_brace_depth <= impl_brace_depth && current_impl.is_some() {
                current_impl = None;
            }
        }

        symbols
    }
}

fn extract_visibility(line: &str) -> Option<String> {
    if line.starts_with("pub(crate)") {
        Some("pub(crate)".to_string())
    } else if line.starts_with("pub ") {
        Some("pub".to_string())
    } else {
        None
    }
}

fn extract_identifier(line: &str, prefixes: &[&str]) -> Option<String> {
    for prefix in prefixes {
        if let Some(rest) = line.strip_prefix(prefix) {
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !ident.is_empty() {
                return Some(ident);
            }
        }
    }
    None
}

fn extract_struct_or_enum_name(line: &str, keyword: &str) -> Option<String> {
    if let Some(idx) = line.find(keyword) {
        let rest = &line[idx + keyword.len()..];
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '<' || *c == '>')
            .collect();
        let clean = ident.split('<').next()?.trim().to_string();
        if !clean.is_empty() {
            return Some(clean);
        }
    }
    None
}

fn extract_impl_target(line: &str) -> Option<String> {
    // "impl ModelManager {" -> "ModelManager"
    // "impl RuntimeBackend for LlamaServerBackend {" -> "LlamaServerBackend"
    let clean = line
        .trim_start_matches("pub ")
        .trim_start_matches("impl ")
        .trim_end_matches('{')
        .trim();
    if clean.contains(" for ") {
        let parts: Vec<&str> = clean.split(" for ").collect();
        if let Some(target) = parts.get(1) {
            let t = target.split('<').next()?.trim().to_string();
            return Some(t);
        }
    }
    let t = clean.split('<').next()?.trim().to_string();
    if !t.is_empty() {
        Some(t)
    } else {
        None
    }
}

fn extract_fn_name(line: &str) -> Option<String> {
    if let Some(idx) = line.find("fn ") {
        let rest = &line[idx + 3..];
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !ident.is_empty() {
            return Some(ident);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rust_file() {
        let code = r#"
/// AppState holds shared coordinator memory.
pub struct AppState {
    pub config: Config,
}

impl AppState {
    /// Creates a new AppState instance.
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[test]
fn test_app_state_creation() {
    assert!(true);
}
"#;
        let symbols = RustParser::parse_file("nauti-nodes", "src/state.rs", code);
        assert_eq!(symbols.len(), 4);

        let struct_sym = symbols
            .iter()
            .find(|s| s.kind == SymbolKind::Struct)
            .unwrap();
        assert_eq!(struct_sym.qualified_name, "AppState");
        assert_eq!(
            struct_sym.doc_comment.as_deref(),
            Some("AppState holds shared coordinator memory.")
        );

        let method_sym = symbols
            .iter()
            .find(|s| s.kind == SymbolKind::Method)
            .unwrap();
        assert_eq!(method_sym.qualified_name, "AppState::new");
        assert!(method_sym.parent_symbol_id.is_some());

        let test_sym = symbols.iter().find(|s| s.kind == SymbolKind::Test).unwrap();
        assert_eq!(test_sym.qualified_name, "test_app_state_creation");
    }
}
