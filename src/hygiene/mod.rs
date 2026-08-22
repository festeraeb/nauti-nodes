//! Evidence hygiene, privacy sanitization, and scalable storage module (Great Idea #87).

pub mod engine;
pub mod policy;
pub mod sanitizer;
pub mod tiering;

pub use engine::{EvidenceHygieneEngine, HygieneCycleReport};
pub use policy::HygienePolicy;
pub use sanitizer::EvidenceSanitizer;
pub use tiering::{
    EvidenceArchiveBundle, ImportSummary, QuotaEnforcementReport, ScalableStorageManager,
    StorageTier,
};
