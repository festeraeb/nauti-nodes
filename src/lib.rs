//! NautiInferer v4 — distributed inference control plane.

pub mod adapter;
pub mod api;
pub mod config;
pub mod coordinator;
pub mod fleet;
pub mod job_runner;
pub mod jobs;
pub mod node;
pub mod scheduler;
pub mod state;
pub mod types;
pub mod worker;

pub use config::{Config, RuntimeMode};
pub use types::errors::Error;

pub use coordinator::run_coordinator;
pub use worker::run_worker;
