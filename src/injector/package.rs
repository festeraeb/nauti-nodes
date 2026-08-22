//! Context package definitions and delivery modes for anticipatory retrieval.

use crate::index::{CompilerIncident, RetrievalResult};
use crate::injector::intent::AgentIntent;
use serde::{Deserialize, Serialize};

/// Delivery mode specifying when and where the context package is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    /// Injected immediately into the active prompt / turn.
    Inline,
    /// Queued for consumption at the next turn.
    Next,
    /// Structured specifically for the Reviewer role prior to review.
    Review,
    /// Pre-computed and cached in the background for potential future retrieval.
    Cache,
}

/// A structured bundle of retrieved code context, symbols, and diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPackage {
    pub package_id: String,
    pub intent: AgentIntent,
    pub delivery_mode: DeliveryMode,
    pub retrieved_results: Vec<RetrievalResult>,
    pub compiler_incidents: Vec<CompilerIncident>,
    pub related_tests: Vec<String>,
    pub created_at_ms: u64,
    pub retrieval_duration_ms: u64,
    pub consumed: bool,
}

impl ContextPackage {
    /// Builds a new context package.
    pub fn new(
        intent: AgentIntent,
        delivery_mode: DeliveryMode,
        retrieved_results: Vec<RetrievalResult>,
        compiler_incidents: Vec<CompilerIncident>,
        related_tests: Vec<String>,
        retrieval_duration_ms: u64,
    ) -> Self {
        let package_id = uuid::Uuid::new_v4().to_string();
        let created_at_ms = crate::types::now_ms();

        Self {
            package_id,
            intent,
            delivery_mode,
            retrieved_results,
            compiler_incidents,
            related_tests,
            created_at_ms,
            retrieval_duration_ms,
            consumed: false,
        }
    }

    /// Marks the package as consumed by the primary agent role.
    pub fn mark_consumed(&mut self) {
        self.consumed = true;
    }
}
