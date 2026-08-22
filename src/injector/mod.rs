//! Anticipatory context injector for concurrent retrieval.
//!
//! Exposes agent intent models, context packages, real non-synthetic telemetry,
//! outcome feedback tracking, distilled planning policy, and the concurrent anticipatory retrieval pipeline.

pub mod action;
pub mod distilled_policy;
pub mod feedback;
pub mod intent;
pub mod package;
pub mod pipeline;
pub mod telemetry;

pub use action::{ActionWeight, PlanStrategy, RetrievalAction, RetrievalPlan};
pub use distilled_policy::{DistilledPolicy, PolicyRecommendation, ShadowEvaluationReport};
pub use feedback::{OutcomeFeedbackStore, OutcomeRecord, OutcomeType};
pub use intent::AgentIntent;
pub use package::{ContextPackage, DeliveryMode};
pub use pipeline::AnticipatoryInjector;
pub use telemetry::{InjectorTelemetry, InjectorTelemetrySnapshot};
