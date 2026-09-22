//! Structured agent intent representations for anticipatory context retrieval.

use serde::{Deserialize, Serialize};

/// Explicit structured intent emitted by Thinker / Coder / Reviewer roles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum AgentIntent {
    /// Agent intends to inspect or edit a specific file.
    InspectFile { path: String },

    /// Agent intends to inspect or modify a symbol by qualified name.
    InspectSymbol { qualified_name: String },

    /// Agent intends to find callers or usages of a symbol.
    FindCallers { symbol: String },

    /// Agent intends to resolve a specific compiler error.
    ResolveCompilerError {
        error_code: Option<String>,
        diagnostic: String,
    },

    /// Agent intends to investigate a failed test.
    InvestigateTestFailure { test_name: String, output: String },

    /// Agent intends to verify an architectural or system invariant.
    VerifyInvariant { description: String },

    /// Agent intends to prepare a code review for modified files.
    PrepareReview { changed_files: Vec<String> },
}

impl AgentIntent {
    /// Generates a retrieval query string representing the intent.
    pub fn to_query_string(&self) -> String {
        match self {
            Self::InspectFile { path } => path.clone(),
            Self::InspectSymbol { qualified_name } => qualified_name.clone(),
            Self::FindCallers { symbol } => symbol.clone(),
            Self::ResolveCompilerError {
                error_code,
                diagnostic,
            } => {
                if let Some(code) = error_code {
                    format!("{} {}", code, diagnostic)
                } else {
                    diagnostic.clone()
                }
            }
            Self::InvestigateTestFailure { test_name, .. } => test_name.clone(),
            Self::VerifyInvariant { description } => description.clone(),
            Self::PrepareReview { changed_files } => changed_files.join(" "),
        }
    }

    /// Provides a human-readable summary of the intent.
    pub fn summary(&self) -> String {
        match self {
            Self::InspectFile { path } => format!("Inspect file {}", path),
            Self::InspectSymbol { qualified_name } => format!("Inspect symbol {}", qualified_name),
            Self::FindCallers { symbol } => format!("Find callers of {}", symbol),
            Self::ResolveCompilerError {
                error_code,
                diagnostic,
            } => {
                if let Some(code) = error_code {
                    format!("Resolve compiler error [{}] {}", code, diagnostic)
                } else {
                    format!("Resolve compiler error {}", diagnostic)
                }
            }
            Self::InvestigateTestFailure { test_name, .. } => {
                format!("Investigate test failure {}", test_name)
            }
            Self::VerifyInvariant { description } => {
                format!("Verify invariant {}", description)
            }
            Self::PrepareReview { changed_files } => {
                format!("Prepare review for {} files", changed_files.len())
            }
        }
    }

    /// Returns the static string name of the intent variant.
    pub fn intent_name(&self) -> &'static str {
        match self {
            Self::InspectFile { .. } => "InspectFile",
            Self::InspectSymbol { .. } => "InspectSymbol",
            Self::FindCallers { .. } => "FindCallers",
            Self::ResolveCompilerError { .. } => "ResolveCompilerError",
            Self::InvestigateTestFailure { .. } => "InvestigateTestFailure",
            Self::VerifyInvariant { .. } => "VerifyInvariant",
            Self::PrepareReview { .. } => "PrepareReview",
        }
    }

    /// Returns the primary target symbol or identifier for the intent.
    pub fn target_symbol(&self) -> String {
        match self {
            Self::InspectFile { path } => path.clone(),
            Self::InspectSymbol { qualified_name } => qualified_name.clone(),
            Self::FindCallers { symbol } => symbol.clone(),
            Self::ResolveCompilerError { error_code, .. } => error_code
                .clone()
                .unwrap_or_else(|| "compiler_error".into()),
            Self::InvestigateTestFailure { test_name, .. } => test_name.clone(),
            Self::VerifyInvariant { description } => description.clone(),
            Self::PrepareReview { changed_files } => {
                changed_files.first().cloned().unwrap_or_default()
            }
        }
    }
}
