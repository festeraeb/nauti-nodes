//! Validated Closed-Loop Engineering subsystem (Chunk 16).

pub mod orchestrator;
pub mod types;

pub use orchestrator::ClosedLoopEngine;
pub use types::{ClosedLoopResult, EngineeringTask, FixResult};
