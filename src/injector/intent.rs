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
}
