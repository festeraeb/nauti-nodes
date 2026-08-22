//! Retention, aging, and quota configuration for evidence hygiene.

use serde::{Deserialize, Serialize};

/// Policy governing evidence hygiene, retention TTLs, and storage limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HygienePolicy {
    /// Time-to-live in milliseconds for unverified compiler incidents (default: 14 days).
    pub ttl_unverified_incidents_ms: u64,

    /// Maximum number of unverified incidents retained per compiler error code.
    pub max_unverified_per_error_code: usize,

    /// Maximum disk footprint quota in bytes for SQLite databases before tiering/purging.
    pub max_storage_quota_bytes: u64,

    /// Whether to purge orphaned symbol chunks during hygiene cycle.
    pub enable_orphan_purge: bool,

    /// Whether to compact duplicate compiler incidents into canonical entries.
    pub enable_deduplication: bool,

    /// Whether to execute SQLite vacuum / defragmentation during hygiene cycle.
    pub enable_auto_vacuum: bool,
}

impl Default for HygienePolicy {
    fn default() -> Self {
        Self {
            ttl_unverified_incidents_ms: 14 * 24 * 60 * 60 * 1000, // 14 days
            max_unverified_per_error_code: 50,
            max_storage_quota_bytes: 500 * 1024 * 1024, // 500 MB
            enable_orphan_purge: true,
            enable_deduplication: true,
            enable_auto_vacuum: true,
        }
    }
}

impl HygienePolicy {
    /// Creates a strict test policy with short TTLs and smaller quotas.
    pub fn strict_test_policy() -> Self {
        Self {
            ttl_unverified_incidents_ms: 1000, // 1 second
            max_unverified_per_error_code: 2,
            max_storage_quota_bytes: 10 * 1024 * 1024, // 10 MB
            enable_orphan_purge: true,
            enable_deduplication: true,
            enable_auto_vacuum: true,
        }
    }
}
