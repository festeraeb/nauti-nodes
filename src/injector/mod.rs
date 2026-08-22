//! Anticipatory context injector for concurrent retrieval.
//!
//! Exposes agent intent models, context packages, real non-synthetic telemetry,
//! and the concurrent anticipatory retrieval pipeline.

pub mod intent;
pub mod package;
pub mod pipeline;
pub mod telemetry;

pub use intent::AgentIntent;
pub use package::{ContextPackage, DeliveryMode};
pub use pipeline::AnticipatoryInjector;
pub use telemetry::{InjectorTelemetry, InjectorTelemetrySnapshot};
