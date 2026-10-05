//! Rust port of the Fleet F-series planner primitives.
//!
//! Python (`cesaropsserver/fleet/`) is the reference; see
//! `fleet/MILESTONES.md`. Each port carries the same contract as its Python
//! original, including the rule that an unsatisfiable ask must surface as
//! `None` / an explicit error rather than an empty-but-successful value.
//!
//! Port map:
//!   002b scope     → [`scope`]      (F2 — Scope.LOCAL / Scope.FLEET)
//!   002e planner   → [`planner`]    (F5 — swappable FleetPlanner trait)
//!   002c escalate  → [`crate::scheduler::escalation`] (F3 — hard-constraint escalation)

pub mod merge;
pub mod planner;
pub mod scope;

pub use merge::{merge_snapshots, AwarenessSnapshot, GpuAwareness};

pub use planner::{
    FederatedPerNodePlanner, FleetPlanner, MonolithicMoEPlanner, PlanFragmentSpec,
    PlannerError, PlannerKind,
};
pub use scope::{hosts_in_scope, gpus_in_scope, HasHostKey, Scope};