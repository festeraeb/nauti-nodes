//! Persistent authoritative operational state (Chunk 7).
//!
//! SQLite-backed store for the state that MUST survive coordinator restart
//! to keep the ownership invariant:
//!
//! - known node identity + last known lifecycle + observed hardware
//! - active Forge leases and their transition timestamps
//! - operational event journal (bounded)
//! - token reservation ledger (no double charge / no double refund)
//! - checkpoint registry (job / node / lease association)
//!
//! Schema changes are additive; RC1 databases (`api_quotas`, `revoked_nodes`)
//! are preserved untouched. PIDs / process handles are NOT persisted as truth:
//! a pre-restart PID is observational evidence, not proof of liveness.

use crate::eligibility::{LeaseState, RuntimeState};
use crate::types::{now_ms, Error, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

/// Current schema version. Bump only for explicit migrations.
pub const OPS_SCHEMA_VERSION: u32 = 2;

/// Default bounded retention for the operational event journal.
pub const DEFAULT_EVENT_RETENTION: u64 = 20_000;

// ─────────────────────────────────────────────────────────────────────────────
// Records
// ─────────────────────────────────────────────────────────────────────────────

/// Persisted lease row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub lease_id: String,
    pub mission_id: String,
    pub node_id: String,
    pub device_id: Option<String>,
    pub state: LeaseState,
    pub vram_mb: u64,
    pub priority: u32,
    pub allow_preemption: bool,
    pub requested_at_ms: u64,
    pub scheduler_excluded_at_ms: Option<u64>,
    pub runtime_exited_at_ms: Option<u64>,
    pub gpu_released_at_ms: Option<u64>,
    pub granted_at_ms: Option<u64>,
    pub releasing_at_ms: Option<u64>,
    pub released_at_ms: Option<u64>,
    pub recovery_started_at_ms: Option<u64>,
    pub runtime_ready_at_ms: Option<u64>,
    pub scheduler_eligible_at_ms: Option<u64>,
    pub first_inference_at_ms: Option<u64>,
    pub failure_reason: Option<String>,
    pub updated_at_ms: u64,
}

/// Persisted node operational snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStateRow {
    pub node_id: String,
    pub public_key_hex: String,
    pub addr: String,
    pub inference_url: String,
    pub role: String,
    pub gpu_name: String,
    pub vram_mb: u32,
    pub models_json: String,
    pub lifecycle_state: String,
    pub health_state: String,
    pub ownership: String,
    pub runtime_state: String,
    pub reconciliation_status: String,
    pub claim_json: String,
    pub observed_json: Option<String>,
    pub last_heartbeat_ms: Option<u64>,
    pub last_state_change_ms: u64,
    pub failure_reason: String,
    pub updated_at_ms: u64,
}

/// Typed operational event kinds (audit journal, not a log replacement).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpsEventKind {
    CoordinatorStarted,
    NodeJoined,
    NodeOffline,
    NodeReconnected,
    NodeWithdrawn,
    NodeRevoked,
    NodeUnrevoked,
    HeartbeatStale,
    HardwareReconciled,
    HardwareMismatch,
    LeaseRequested,
    SchedulerExcluded,
    JobCheckpointed,
    RuntimeExited,
    GpuReleased,
    LeaseGranted,
    LeaseReleased,
    LeaseConflict,
    LeaseStale,
    RecoveryStarted,
    RuntimeReady,
    RuntimeFailed,
    NodeEligible,
    JobResumed,
    JobRequeued,
    JobCompleted,
    JobFailed,
    JobPreempted,
    RevocationApplied,
    AccountingSettled,
    AccountingRefunded,
    OwnershipConflictBlocked,
    DuplicateRequest,
    StateRecovered,
}

impl OpsEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CoordinatorStarted => "COORDINATOR_STARTED",
            Self::NodeJoined => "NODE_JOINED",
            Self::NodeOffline => "NODE_OFFLINE",
            Self::NodeReconnected => "NODE_RECONNECTED",
            Self::NodeWithdrawn => "NODE_WITHDRAWN",
            Self::NodeRevoked => "NODE_REVOKED",
            Self::NodeUnrevoked => "NODE_UNREVOKED",
            Self::HeartbeatStale => "HEARTBEAT_STALE",
            Self::HardwareReconciled => "HARDWARE_RECONCILED",
            Self::HardwareMismatch => "HARDWARE_MISMATCH",
            Self::LeaseRequested => "LEASE_REQUESTED",
            Self::SchedulerExcluded => "SCHEDULER_EXCLUDED",
            Self::JobCheckpointed => "JOB_CHECKPOINTED",
            Self::RuntimeExited => "RUNTIME_EXITED",
            Self::GpuReleased => "GPU_RELEASED",
            Self::LeaseGranted => "LEASE_GRANTED",
            Self::LeaseReleased => "LEASE_RELEASED",
            Self::LeaseConflict => "LEASE_CONFLICT",
            Self::LeaseStale => "LEASE_STALE",
            Self::RecoveryStarted => "RECOVERY_STARTED",
            Self::RuntimeReady => "RUNTIME_READY",
            Self::RuntimeFailed => "RUNTIME_FAILED",
            Self::NodeEligible => "NODE_ELIGIBLE",
            Self::JobResumed => "JOB_RESUMED",
            Self::JobRequeued => "JOB_REQUEUED",
            Self::JobCompleted => "JOB_COMPLETED",
            Self::JobFailed => "JOB_FAILED",
            Self::JobPreempted => "JOB_PREEMPTED",
            Self::RevocationApplied => "REVOCATION_APPLIED",
            Self::AccountingSettled => "ACCOUNTING_SETTLED",
            Self::AccountingRefunded => "ACCOUNTING_REFUNDED",
            Self::OwnershipConflictBlocked => "OWNERSHIP_CONFLICT_BLOCKED",
            Self::DuplicateRequest => "DUPLICATE_REQUEST",
            Self::StateRecovered => "STATE_RECOVERED",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "COORDINATOR_STARTED" => Self::CoordinatorStarted,
            "NODE_JOINED" => Self::NodeJoined,
            "NODE_OFFLINE" => Self::NodeOffline,
            "NODE_RECONNECTED" => Self::NodeReconnected,
            "NODE_WITHDRAWN" => Self::NodeWithdrawn,
            "NODE_REVOKED" => Self::NodeRevoked,
            "NODE_UNREVOKED" => Self::NodeUnrevoked,
            "HEARTBEAT_STALE" => Self::HeartbeatStale,
            "HARDWARE_RECONCILED" => Self::HardwareReconciled,
            "HARDWARE_MISMATCH" => Self::HardwareMismatch,
            "LEASE_REQUESTED" => Self::LeaseRequested,
            "SCHEDULER_EXCLUDED" => Self::SchedulerExcluded,
            "JOB_CHECKPOINTED" => Self::JobCheckpointed,
            "RUNTIME_EXITED" => Self::RuntimeExited,
            "GPU_RELEASED" => Self::GpuReleased,
            "LEASE_GRANTED" => Self::LeaseGranted,
            "LEASE_RELEASED" => Self::LeaseReleased,
            "LEASE_CONFLICT" => Self::LeaseConflict,
            "LEASE_STALE" => Self::LeaseStale,
            "RECOVERY_STARTED" => Self::RecoveryStarted,
            "RUNTIME_READY" => Self::RuntimeReady,
            "RUNTIME_FAILED" => Self::RuntimeFailed,
            "NODE_ELIGIBLE" => Self::NodeEligible,
            "JOB_RESUMED" => Self::JobResumed,
            "JOB_REQUEUED" => Self::JobRequeued,
            "JOB_COMPLETED" => Self::JobCompleted,
            "JOB_FAILED" => Self::JobFailed,
            "JOB_PREEMPTED" => Self::JobPreempted,
            "REVOCATION_APPLIED" => Self::RevocationApplied,
            "ACCOUNTING_SETTLED" => Self::AccountingSettled,
            "ACCOUNTING_REFUNDED" => Self::AccountingRefunded,
            "OWNERSHIP_CONFLICT_BLOCKED" => Self::OwnershipConflictBlocked,
            "DUPLICATE_REQUEST" => Self::DuplicateRequest,
            "STATE_RECOVERED" => Self::StateRecovered,
            _ => return None,
        })
    }
}

/// A journal event row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsEvent {
    pub seq: u64,
    pub ts_ms: u64,
    pub kind: String,
    pub node_id: Option<String>,
    pub lease_id: Option<String>,
    pub job_id: Option<String>,
    pub mission_id: Option<String>,
    pub detail: String,
    pub result_state: String,
}

/// Token reservation ledger row (idempotent accounting boundary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationRow {
    pub job_id: String,
    pub api_key: String,
    pub reserved_tokens: u64,
    pub used_tokens: u64,
    pub released_tokens: u64,
    /// ACTIVE | SETTLED | REFUNDED | PREEMPTED
    pub state: String,
    pub created_at_ms: u64,
    pub settled_at_ms: Option<u64>,
}

/// Chunk 8B: durable per-resource fence row (highest accepted authority).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FenceRow {
    pub resource_id: String,
    pub epoch: u64,
    pub authority_coordinator_id: String,
    pub updated_at_ms: u64,
}

/// Chunk 9: durable delegated job row (exactly-once across failover).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegatedJobRow {
    pub job_id: String,
    pub origin_coordinator_id: String,
    pub executing_coordinator_id: Option<String>,
    /// PENDING | RUNNING | COMPLETED | FAILED | CANCELLED
    pub state: String,
    pub ownership_epoch: u64,
    pub created_at_ms: u64,
    pub completed_at_ms: Option<u64>,
}

/// Checkpoint registry row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRow {
    pub job_id: String,
    pub node_id: String,
    pub lease_id: Option<String>,
    pub mission_id: Option<String>,
    pub checkpoint_ref: String,
    pub metadata_json: String,
    pub created_at_ms: u64,
    pub consumed_at_ms: Option<u64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Store
// ─────────────────────────────────────────────────────────────────────────────

pub struct OpsStore {
    conn: Mutex<Connection>,
    event_retention: u64,
}

fn open_conn(db_url: &str) -> Result<Connection> {
    let conn = if db_url == ":memory:" || db_url == "sqlite::memory:" {
        Connection::open_in_memory()?
    } else {
        let path = db_url.strip_prefix("sqlite:").unwrap_or(db_url);
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        Connection::open(path)?
    };
    // Multiple connections share the DB file (quota/revocation/lineage stores);
    // give writers time instead of failing instantly.
    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(|e| Error::Internal(format!("busy_timeout: {e}")))?;
    Ok(conn)
}

impl OpsStore {
    /// Open (or create/migrate) the operational store.
    pub fn open(db_url: &str) -> Result<Self> {
        Self::open_with_retention(db_url, DEFAULT_EVENT_RETENTION)
    }

    pub fn open_with_retention(db_url: &str, event_retention: u64) -> Result<Self> {
        let conn = open_conn(db_url)?;
        Self::from_connection(conn, event_retention)
    }

    pub fn from_connection(conn: Connection, event_retention: u64) -> Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ops_schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS forge_leases (
                lease_id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                device_id TEXT,
                state TEXT NOT NULL,
                vram_mb INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 0,
                allow_preemption INTEGER NOT NULL DEFAULT 1,
                requested_at_ms INTEGER NOT NULL,
                scheduler_excluded_at_ms INTEGER,
                runtime_exited_at_ms INTEGER,
                gpu_released_at_ms INTEGER,
                granted_at_ms INTEGER,
                releasing_at_ms INTEGER,
                released_at_ms INTEGER,
                recovery_started_at_ms INTEGER,
                runtime_ready_at_ms INTEGER,
                scheduler_eligible_at_ms INTEGER,
                first_inference_at_ms INTEGER,
                failure_reason TEXT,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_lease_mission_node
                ON forge_leases(mission_id, node_id);
            CREATE INDEX IF NOT EXISTS idx_lease_node_state
                ON forge_leases(node_id, state);
            CREATE TABLE IF NOT EXISTS node_state (
                node_id TEXT PRIMARY KEY,
                public_key_hex TEXT NOT NULL DEFAULT '',
                addr TEXT NOT NULL DEFAULT '',
                inference_url TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT '',
                gpu_name TEXT NOT NULL DEFAULT '',
                vram_mb INTEGER NOT NULL DEFAULT 0,
                models_json TEXT NOT NULL DEFAULT '[]',
                lifecycle_state TEXT NOT NULL DEFAULT 'OFFLINE',
                health_state TEXT NOT NULL DEFAULT 'OFFLINE',
                ownership TEXT NOT NULL DEFAULT 'UNAVAILABLE',
                runtime_state TEXT NOT NULL DEFAULT 'UNKNOWN',
                reconciliation_status TEXT NOT NULL DEFAULT 'UNVERIFIED',
                claim_json TEXT NOT NULL DEFAULT '{}',
                observed_json TEXT,
                last_heartbeat_ms INTEGER,
                last_state_change_ms INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT NOT NULL DEFAULT '',
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ops_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                node_id TEXT,
                lease_id TEXT,
                job_id TEXT,
                mission_id TEXT,
                detail TEXT NOT NULL DEFAULT '',
                result_state TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_events_node ON ops_events(node_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_lease ON ops_events(lease_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_job ON ops_events(job_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_kind ON ops_events(kind, seq);
            CREATE TABLE IF NOT EXISTS token_reservations (
                job_id TEXT PRIMARY KEY,
                api_key TEXT NOT NULL,
                reserved_tokens INTEGER NOT NULL,
                used_tokens INTEGER NOT NULL DEFAULT 0,
                released_tokens INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at_ms INTEGER NOT NULL,
                settled_at_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_reservations_state
                ON token_reservations(state);
            CREATE TABLE IF NOT EXISTS checkpoints (
                job_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                lease_id TEXT,
                mission_id TEXT,
                checkpoint_ref TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_ms INTEGER NOT NULL,
                consumed_at_ms INTEGER
            );
            -- Chunk 8B: durable per-resource fencing fence. Highest accepted
            -- authority per resource; survives restarts so a fence can never roll back.
            CREATE TABLE IF NOT EXISTS federation_fence (
                resource_id TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL,
                authority_coordinator_id TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            -- Chunk 9: durable delegated job identity table (exactly-once).
            -- A duplicate arrival returns the current authoritative status/result.
            CREATE TABLE IF NOT EXISTS delegated_job (
                job_id TEXT PRIMARY KEY,
                origin_coordinator_id TEXT NOT NULL,
                executing_coordinator_id TEXT,
                state TEXT NOT NULL,
                ownership_epoch INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                completed_at_ms INTEGER
            );
            ",
        )
        .map_err(|e| Error::Internal(format!("ops schema: {e}")))?;

        let store = Self {
            conn: Mutex::new(conn),
            event_retention: event_retention.max(100),
        };
        store.record_schema_version()?;
        Ok(store)
    }

    fn record_schema_version(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO ops_schema_meta (key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![OPS_SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    pub fn schema_version(&self) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let v: String = conn
            .query_row(
                "SELECT value FROM ops_schema_meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| "0".to_string());
        Ok(v.parse().unwrap_or(0))
    }

    // ── Event journal ────────────────────────────────────────────────────

    /// Append an event and enforce bounded retention.
    pub fn record(
        &self,
        kind: OpsEventKind,
        node_id: Option<&str>,
        lease_id: Option<&str>,
        job_id: Option<&str>,
        mission_id: Option<&str>,
        detail: &str,
        result_state: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO ops_events (ts_ms, kind, node_id, lease_id, job_id, mission_id, detail, result_state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                now_ms(),
                kind.as_str(),
                node_id,
                lease_id,
                job_id,
                mission_id,
                detail,
                result_state
            ],
        )?;
        // Bounded journal: drop oldest beyond retention.
        conn.execute(
            "DELETE FROM ops_events WHERE seq <= (
                SELECT MAX(seq) - ?1 FROM ops_events
             )",
            params![self.event_retention as i64],
        )?;
        Ok(())
    }

    /// Query recent events with optional filters (node / lease / job / kind).
    pub fn events(
        &self,
        node_id: Option<&str>,
        lease_id: Option<&str>,
        job_id: Option<&str>,
        kind: Option<&str>,
        limit: u64,
    ) -> Result<Vec<OpsEvent>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut sql = String::from(
            "SELECT seq, ts_ms, kind, node_id, lease_id, job_id, mission_id, detail, result_state
             FROM ops_events WHERE 1=1",
        );
        let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(n) = node_id {
            sql.push_str(" AND node_id = ?");
            args.push(Box::new(n.to_string()));
        }
        if let Some(l) = lease_id {
            sql.push_str(" AND lease_id = ?");
            args.push(Box::new(l.to_string()));
        }
        if let Some(j) = job_id {
            sql.push_str(" AND job_id = ?");
            args.push(Box::new(j.to_string()));
        }
        if let Some(k) = kind {
            sql.push_str(" AND kind = ?");
            args.push(Box::new(k.to_string()));
        }
        sql.push_str(" ORDER BY seq DESC LIMIT ?");
        args.push(Box::new(limit.min(1000) as i64));

        let mut stmt = conn.prepare(&sql)?;
        let arg_refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
        let rows = stmt.query_map(arg_refs.as_slice(), |r| {
            Ok(OpsEvent {
                seq: r.get(0)?,
                ts_ms: r.get(1)?,
                kind: r.get(2)?,
                node_id: r.get(3)?,
                lease_id: r.get(4)?,
                job_id: r.get(5)?,
                mission_id: r.get(6)?,
                detail: r.get(7)?,
                result_state: r.get(8)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ── Leases ───────────────────────────────────────────────────────────

    pub fn insert_lease(&self, l: &LeaseRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO forge_leases (
                lease_id, mission_id, node_id, device_id, state, vram_mb, priority,
                allow_preemption, requested_at_ms, scheduler_excluded_at_ms,
                runtime_exited_at_ms, gpu_released_at_ms, granted_at_ms, releasing_at_ms,
                released_at_ms, recovery_started_at_ms, runtime_ready_at_ms,
                scheduler_eligible_at_ms, first_inference_at_ms, failure_reason, updated_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
            params![
                l.lease_id,
                l.mission_id,
                l.node_id,
                l.device_id,
                l.state.as_str(),
                l.vram_mb as i64,
                l.priority,
                l.allow_preemption,
                l.requested_at_ms,
                l.scheduler_excluded_at_ms,
                l.runtime_exited_at_ms,
                l.gpu_released_at_ms,
                l.granted_at_ms,
                l.releasing_at_ms,
                l.released_at_ms,
                l.recovery_started_at_ms,
                l.runtime_ready_at_ms,
                l.scheduler_eligible_at_ms,
                l.first_inference_at_ms,
                l.failure_reason,
                l.updated_at_ms
            ],
        )?;
        Ok(())
    }

    pub fn update_lease(&self, l: &LeaseRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "UPDATE forge_leases SET
                state = ?2, scheduler_excluded_at_ms = ?3, runtime_exited_at_ms = ?4,
                gpu_released_at_ms = ?5, granted_at_ms = ?6, releasing_at_ms = ?7,
                released_at_ms = ?8, recovery_started_at_ms = ?9, runtime_ready_at_ms = ?10,
                scheduler_eligible_at_ms = ?11, first_inference_at_ms = ?12,
                failure_reason = ?13, updated_at_ms = ?14
             WHERE lease_id = ?1",
            params![
                l.lease_id,
                l.state.as_str(),
                l.scheduler_excluded_at_ms,
                l.runtime_exited_at_ms,
                l.gpu_released_at_ms,
                l.granted_at_ms,
                l.releasing_at_ms,
                l.released_at_ms,
                l.recovery_started_at_ms,
                l.runtime_ready_at_ms,
                l.scheduler_eligible_at_ms,
                l.first_inference_at_ms,
                l.failure_reason,
                l.updated_at_ms
            ],
        )?;
        Ok(())
    }

    fn row_to_lease(r: &rusqlite::Row<'_>) -> rusqlite::Result<LeaseRecord> {
        let state_s: String = r.get(4)?;
        let allow: i64 = r.get(7)?;
        Ok(LeaseRecord {
            lease_id: r.get(0)?,
            mission_id: r.get(1)?,
            node_id: r.get(2)?,
            device_id: r.get(3)?,
            state: LeaseState::from_str(&state_s).unwrap_or(LeaseState::Stale),
            vram_mb: r.get::<_, i64>(5)? as u64,
            priority: r.get::<_, i64>(6)? as u32,
            allow_preemption: allow != 0,
            requested_at_ms: r.get::<_, i64>(8)? as u64,
            scheduler_excluded_at_ms: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
            runtime_exited_at_ms: r.get::<_, Option<i64>>(10)?.map(|v| v as u64),
            gpu_released_at_ms: r.get::<_, Option<i64>>(11)?.map(|v| v as u64),
            granted_at_ms: r.get::<_, Option<i64>>(12)?.map(|v| v as u64),
            releasing_at_ms: r.get::<_, Option<i64>>(13)?.map(|v| v as u64),
            released_at_ms: r.get::<_, Option<i64>>(14)?.map(|v| v as u64),
            recovery_started_at_ms: r.get::<_, Option<i64>>(15)?.map(|v| v as u64),
            runtime_ready_at_ms: r.get::<_, Option<i64>>(16)?.map(|v| v as u64),
            scheduler_eligible_at_ms: r.get::<_, Option<i64>>(17)?.map(|v| v as u64),
            first_inference_at_ms: r.get::<_, Option<i64>>(18)?.map(|v| v as u64),
            failure_reason: r.get(19)?,
            updated_at_ms: r.get::<_, i64>(20)? as u64,
        })
    }

    const LEASE_COLS: &'static str = "lease_id, mission_id, node_id, device_id, state, vram_mb,
        priority, allow_preemption, requested_at_ms, scheduler_excluded_at_ms,
        runtime_exited_at_ms, gpu_released_at_ms, granted_at_ms, releasing_at_ms,
        released_at_ms, recovery_started_at_ms, runtime_ready_at_ms,
        scheduler_eligible_at_ms, first_inference_at_ms, failure_reason, updated_at_ms";

    pub fn lease_by_id(&self, lease_id: &str) -> Result<Option<LeaseRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let sql = format!(
            "SELECT {} FROM forge_leases WHERE lease_id = ?1",
            Self::LEASE_COLS
        );
        match conn.query_row(&sql, params![lease_id], Self::row_to_lease) {
            Ok(l) => Ok(Some(l)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Latest lease for a (mission_id, node_id) pair, any state.
    pub fn lease_by_mission_node(
        &self,
        mission_id: &str,
        node_id: &str,
    ) -> Result<Option<LeaseRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let sql = format!(
            "SELECT {} FROM forge_leases WHERE mission_id = ?1 AND node_id = ?2
             ORDER BY requested_at_ms DESC LIMIT 1",
            Self::LEASE_COLS
        );
        match conn.query_row(&sql, params![mission_id, node_id], Self::row_to_lease) {
            Ok(l) => Ok(Some(l)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The active (non-terminal) lease currently holding a node, if any.
    pub fn active_lease_for_node(&self, node_id: &str) -> Result<Option<LeaseRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let sql = format!(
            "SELECT {} FROM forge_leases
             WHERE node_id = ?1 AND state IN (
                'LEASE_REQUESTED','DRAINING','LEASE_GRANTED','LEASED','RELEASING','RECOVERING'
             ) ORDER BY requested_at_ms DESC LIMIT 1",
            Self::LEASE_COLS
        );
        match conn.query_row(&sql, params![node_id], Self::row_to_lease) {
            Ok(l) => Ok(Some(l)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn all_active_leases(&self) -> Result<Vec<LeaseRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let sql = format!(
            "SELECT {} FROM forge_leases
             WHERE state IN (
                'LEASE_REQUESTED','DRAINING','LEASE_GRANTED','LEASED','RELEASING','RECOVERING'
             ) ORDER BY requested_at_ms ASC",
            Self::LEASE_COLS
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], Self::row_to_lease)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn all_leases(&self, limit: u64) -> Result<Vec<LeaseRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let sql = format!(
            "SELECT {} FROM forge_leases ORDER BY requested_at_ms DESC LIMIT ?1",
            Self::LEASE_COLS
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![limit.min(1000) as i64], Self::row_to_lease)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ── Node state ───────────────────────────────────────────────────────

    pub fn upsert_node(&self, n: &NodeStateRow) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO node_state (
                node_id, public_key_hex, addr, inference_url, role, gpu_name, vram_mb,
                models_json, lifecycle_state, health_state, ownership, runtime_state,
                reconciliation_status, claim_json, observed_json, last_heartbeat_ms,
                last_state_change_ms, failure_reason, updated_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
             ON CONFLICT(node_id) DO UPDATE SET
                public_key_hex = excluded.public_key_hex,
                addr = excluded.addr,
                inference_url = excluded.inference_url,
                role = excluded.role,
                gpu_name = excluded.gpu_name,
                vram_mb = excluded.vram_mb,
                models_json = excluded.models_json,
                lifecycle_state = excluded.lifecycle_state,
                health_state = excluded.health_state,
                ownership = excluded.ownership,
                runtime_state = excluded.runtime_state,
                reconciliation_status = excluded.reconciliation_status,
                claim_json = excluded.claim_json,
                observed_json = excluded.observed_json,
                last_heartbeat_ms = excluded.last_heartbeat_ms,
                last_state_change_ms = excluded.last_state_change_ms,
                failure_reason = excluded.failure_reason,
                updated_at_ms = excluded.updated_at_ms",
            params![
                n.node_id,
                n.public_key_hex,
                n.addr,
                n.inference_url,
                n.role,
                n.gpu_name,
                n.vram_mb,
                n.models_json,
                n.lifecycle_state,
                n.health_state,
                n.ownership,
                n.runtime_state,
                n.reconciliation_status,
                n.claim_json,
                n.observed_json,
                n.last_heartbeat_ms,
                n.last_state_change_ms,
                n.failure_reason,
                n.updated_at_ms
            ],
        )?;
        Ok(())
    }

    pub fn load_nodes(&self) -> Result<Vec<NodeStateRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(
            "SELECT node_id, public_key_hex, addr, inference_url, role, gpu_name, vram_mb,
                    models_json, lifecycle_state, health_state, ownership, runtime_state,
                    reconciliation_status, claim_json, observed_json, last_heartbeat_ms,
                    last_state_change_ms, failure_reason, updated_at_ms
             FROM node_state",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(NodeStateRow {
                node_id: r.get(0)?,
                public_key_hex: r.get(1)?,
                addr: r.get(2)?,
                inference_url: r.get(3)?,
                role: r.get(4)?,
                gpu_name: r.get(5)?,
                vram_mb: r.get::<_, i64>(6)? as u32,
                models_json: r.get(7)?,
                lifecycle_state: r.get(8)?,
                health_state: r.get(9)?,
                ownership: r.get(10)?,
                runtime_state: r.get(11)?,
                reconciliation_status: r.get(12)?,
                claim_json: r.get(13)?,
                observed_json: r.get(14)?,
                last_heartbeat_ms: r.get::<_, Option<i64>>(15)?.map(|v| v as u64),
                last_state_change_ms: r.get::<_, i64>(16)? as u64,
                failure_reason: r.get(17)?,
                updated_at_ms: r.get::<_, i64>(18)? as u64,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ── Token reservation ledger ─────────────────────────────────────────

    /// Record a fresh reservation. Fails if the job_id already has a ledger
    /// row (duplicate reservation guard).
    pub fn open_reservation(
        &self,
        job_id: &str,
        api_key: &str,
        reserved_tokens: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO token_reservations (job_id, api_key, reserved_tokens, state, created_at_ms)
             VALUES (?1, ?2, ?3, 'ACTIVE', ?4)",
            params![job_id, api_key, reserved_tokens as i64, now_ms()],
        )
        .map_err(|e| {
            Error::Internal(format!(
                "reservation ledger insert failed (duplicate job_id?): {e}"
            ))
        })?;
        Ok(())
    }

    /// Atomically settle a reservation exactly once.
    ///
    /// Returns true if THIS call performed the settlement. Duplicate
    /// completion signals return false and must not refund again.
    pub fn settle_reservation(
        &self,
        job_id: &str,
        used_tokens: u64,
        refund_tokens: u64,
        final_state: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let updated = conn.execute(
            "UPDATE token_reservations
             SET state = ?2, used_tokens = ?3, released_tokens = ?4, settled_at_ms = ?5
             WHERE job_id = ?1 AND state = 'ACTIVE'",
            params![
                job_id,
                final_state,
                used_tokens as i64,
                refund_tokens as i64,
                now_ms()
            ],
        )?;
        Ok(updated > 0)
    }

    pub fn reservation(&self, job_id: &str) -> Result<Option<ReservationRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        match conn.query_row(
            "SELECT job_id, api_key, reserved_tokens, used_tokens, released_tokens, state,
                    created_at_ms, settled_at_ms
             FROM token_reservations WHERE job_id = ?1",
            params![job_id],
            |r| {
                Ok(ReservationRow {
                    job_id: r.get(0)?,
                    api_key: r.get(1)?,
                    reserved_tokens: r.get::<_, i64>(2)? as u64,
                    used_tokens: r.get::<_, i64>(3)? as u64,
                    released_tokens: r.get::<_, i64>(4)? as u64,
                    state: r.get(5)?,
                    created_at_ms: r.get::<_, i64>(6)? as u64,
                    settled_at_ms: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                })
            },
        ) {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// All ACTIVE reservations (used at coordinator startup to refund
    /// reservations orphaned by a crash — no silent loss of reserved balance).
    pub fn active_reservations(&self) -> Result<Vec<ReservationRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(
            "SELECT job_id, api_key, reserved_tokens, used_tokens, released_tokens, state,
                    created_at_ms, settled_at_ms
             FROM token_reservations WHERE state = 'ACTIVE'",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ReservationRow {
                job_id: r.get(0)?,
                api_key: r.get(1)?,
                reserved_tokens: r.get::<_, i64>(2)? as u64,
                used_tokens: r.get::<_, i64>(3)? as u64,
                released_tokens: r.get::<_, i64>(4)? as u64,
                state: r.get(5)?,
                created_at_ms: r.get::<_, i64>(6)? as u64,
                settled_at_ms: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// All settled rows (for accounting audits / soak invariant checks).
    pub fn all_reservations(&self) -> Result<Vec<ReservationRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(
            "SELECT job_id, api_key, reserved_tokens, used_tokens, released_tokens, state,
                    created_at_ms, settled_at_ms
             FROM token_reservations",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ReservationRow {
                job_id: r.get(0)?,
                api_key: r.get(1)?,
                reserved_tokens: r.get::<_, i64>(2)? as u64,
                used_tokens: r.get::<_, i64>(3)? as u64,
                released_tokens: r.get::<_, i64>(4)? as u64,
                state: r.get(5)?,
                created_at_ms: r.get::<_, i64>(6)? as u64,
                settled_at_ms: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ── Checkpoints ──────────────────────────────────────────────────────

    pub fn record_checkpoint(
        &self,
        job_id: &str,
        node_id: &str,
        lease_id: Option<&str>,
        mission_id: Option<&str>,
        checkpoint_ref: &str,
        metadata_json: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO checkpoints (job_id, node_id, lease_id, mission_id, checkpoint_ref, metadata_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(job_id) DO UPDATE SET
                node_id = excluded.node_id,
                lease_id = excluded.lease_id,
                mission_id = excluded.mission_id,
                checkpoint_ref = excluded.checkpoint_ref,
                metadata_json = excluded.metadata_json,
                created_at_ms = excluded.created_at_ms",
            params![job_id, node_id, lease_id, mission_id, checkpoint_ref, metadata_json, now_ms()],
        )?;
        Ok(())
    }

    /// Mark a checkpoint consumed exactly once (resume idempotency).
    pub fn consume_checkpoint(&self, job_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let updated = conn.execute(
            "UPDATE checkpoints SET consumed_at_ms = ?2
             WHERE job_id = ?1 AND consumed_at_ms IS NULL",
            params![job_id, now_ms()],
        )?;
        Ok(updated > 0)
    }

    pub fn checkpoint(&self, job_id: &str) -> Result<Option<CheckpointRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        match conn.query_row(
            "SELECT job_id, node_id, lease_id, mission_id, checkpoint_ref, metadata_json,
                    created_at_ms, consumed_at_ms
             FROM checkpoints WHERE job_id = ?1",
            params![job_id],
            |r| {
                Ok(CheckpointRow {
                    job_id: r.get(0)?,
                    node_id: r.get(1)?,
                    lease_id: r.get(2)?,
                    mission_id: r.get(3)?,
                    checkpoint_ref: r.get(4)?,
                    metadata_json: r.get(5)?,
                    created_at_ms: r.get::<_, i64>(6)? as u64,
                    consumed_at_ms: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                })
            },
        ) {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn checkpoints_for_node(&self, node_id: &str) -> Result<Vec<CheckpointRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(
            "SELECT job_id, node_id, lease_id, mission_id, checkpoint_ref, metadata_json,
                    created_at_ms, consumed_at_ms
             FROM checkpoints WHERE node_id = ?1 AND consumed_at_ms IS NULL",
        )?;
        let rows = stmt.query_map(params![node_id], |r| {
            Ok(CheckpointRow {
                job_id: r.get(0)?,
                node_id: r.get(1)?,
                lease_id: r.get(2)?,
                mission_id: r.get(3)?,
                checkpoint_ref: r.get(4)?,
                metadata_json: r.get(5)?,
                created_at_ms: r.get::<_, i64>(6)? as u64,
                consumed_at_ms: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ── Chunk 8B: durable federation fences ─────────────────────────────

    /// The durable fence for a resource, if any.
    pub fn get_fence(&self, resource_id: &str) -> Result<Option<FenceRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        match conn.query_row(
            "SELECT resource_id, epoch, authority_coordinator_id, updated_at_ms
             FROM federation_fence WHERE resource_id = ?1",
            params![resource_id],
            |r| {
                Ok(FenceRow {
                    resource_id: r.get(0)?,
                    epoch: r.get::<_, i64>(1)? as u64,
                    authority_coordinator_id: r.get(2)?,
                    updated_at_ms: r.get::<_, i64>(3)? as u64,
                })
            },
        ) {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist a fence (used when a higher epoch is accepted). Callers must
    /// only invoke this after the fence gate decided ACCEPT.
    pub fn upsert_fence(&self, fence: &FenceRow) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO federation_fence (resource_id, epoch, authority_coordinator_id, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(resource_id) DO UPDATE SET
                epoch = excluded.epoch,
                authority_coordinator_id = excluded.authority_coordinator_id,
                updated_at_ms = excluded.updated_at_ms",
            params![
                fence.resource_id,
                fence.epoch as i64,
                fence.authority_coordinator_id,
                fence.updated_at_ms
            ],
        )?;
        Ok(())
    }

    // ── Chunk 9: durable delegated jobs ─────────────────────────────────

    /// Register a delegated job. Fails if the job_id already exists; the
    /// caller then returns the current authoritative status instead of
    /// executing twice.
    pub fn insert_delegated_job(&self, job: &DelegatedJobRow) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO delegated_job (
                job_id, origin_coordinator_id, executing_coordinator_id,
                state, ownership_epoch, created_at_ms, completed_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                job.job_id,
                job.origin_coordinator_id,
                job.executing_coordinator_id,
                job.state,
                job.ownership_epoch as i64,
                job.created_at_ms,
                job.completed_at_ms
            ],
        )?;
        Ok(())
    }

    /// The authoritative current record for a delegated job, if any.
    pub fn delegated_job(&self, job_id: &str) -> Result<Option<DelegatedJobRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        match conn.query_row(
            "SELECT job_id, origin_coordinator_id, executing_coordinator_id,
                    state, ownership_epoch, created_at_ms, completed_at_ms
             FROM delegated_job WHERE job_id = ?1",
            params![job_id],
            |r| {
                Ok(DelegatedJobRow {
                    job_id: r.get(0)?,
                    origin_coordinator_id: r.get(1)?,
                    executing_coordinator_id: r.get(2)?,
                    state: r.get(3)?,
                    ownership_epoch: r.get::<_, i64>(4)? as u64,
                    created_at_ms: r.get::<_, i64>(5)? as u64,
                    completed_at_ms: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                })
            },
        ) {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Update the authoritative state of a delegated job.
    pub fn update_delegated_job(&self, job: &DelegatedJobRow) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "UPDATE delegated_job SET
                executing_coordinator_id = ?2,
                state = ?3,
                completed_at_ms = ?4
             WHERE job_id = ?1",
            params![
                job.job_id,
                job.executing_coordinator_id,
                job.state,
                job.completed_at_ms
            ],
        )?;
        Ok(())
    }

    /// All durable fences (snapshot / catch-up + audit).
    pub fn all_fences(&self) -> Result<Vec<FenceRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(
            "SELECT resource_id, epoch, authority_coordinator_id, updated_at_ms
             FROM federation_fence ORDER BY resource_id ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(FenceRow {
                resource_id: r.get(0)?,
                epoch: r.get::<_, i64>(1)? as u64,
                authority_coordinator_id: r.get(2)?,
                updated_at_ms: r.get::<_, i64>(3)? as u64,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

/// Helper: runtime state string for persistence.
pub fn runtime_state_str(s: RuntimeState) -> &'static str {
    s.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_recorded() {
        let store = OpsStore::open("sqlite::memory:").unwrap();
        assert_eq!(store.schema_version().unwrap(), OPS_SCHEMA_VERSION);
    }

    #[test]
    fn lease_roundtrip_and_active_query() {
        let store = OpsStore::open("sqlite::memory:").unwrap();
        let lease = LeaseRecord {
            lease_id: "lease-1".into(),
            mission_id: "mission-1".into(),
            node_id: "node-1".into(),
            device_id: None,
            state: LeaseState::LeaseRequested,
            vram_mb: 16384,
            priority: 100,
            allow_preemption: true,
            requested_at_ms: now_ms(),
            scheduler_excluded_at_ms: None,
            runtime_exited_at_ms: None,
            gpu_released_at_ms: None,
            granted_at_ms: None,
            releasing_at_ms: None,
            released_at_ms: None,
            recovery_started_at_ms: None,
            runtime_ready_at_ms: None,
            scheduler_eligible_at_ms: None,
            first_inference_at_ms: None,
            failure_reason: None,
            updated_at_ms: now_ms(),
        };
        store.insert_lease(&lease).unwrap();
        assert!(store.active_lease_for_node("node-1").unwrap().is_some());

        let mut l2 = lease.clone();
        l2.state = LeaseState::Released;
        l2.released_at_ms = Some(now_ms());
        store.update_lease(&l2).unwrap();
        assert!(store.active_lease_for_node("node-1").unwrap().is_none());
        assert_eq!(
            store
                .lease_by_mission_node("mission-1", "node-1")
                .unwrap()
                .unwrap()
                .state,
            LeaseState::Released
        );
    }

    #[test]
    fn reservation_settle_is_idempotent() {
        let store = OpsStore::open("sqlite::memory:").unwrap();
        store.open_reservation("job-1", "key-a", 1000).unwrap();
        // Duplicate open must fail.
        assert!(store.open_reservation("job-1", "key-a", 1000).is_err());

        assert!(store
            .settle_reservation("job-1", 300, 700, "SETTLED")
            .unwrap());
        // Second settle (duplicate completion signal) must be a no-op.
        assert!(!store
            .settle_reservation("job-1", 300, 700, "SETTLED")
            .unwrap());

        let row = store.reservation("job-1").unwrap().unwrap();
        assert_eq!(row.state, "SETTLED");
        assert_eq!(row.used_tokens, 300);
        assert_eq!(row.released_tokens, 700);
    }

    #[test]
    fn event_journal_bounded() {
        let store = OpsStore::open_with_retention("sqlite::memory:", 100).unwrap();
        for i in 0..250 {
            store
                .record(
                    OpsEventKind::NodeJoined,
                    Some(&format!("n{i}")),
                    None,
                    None,
                    None,
                    "test",
                    "",
                )
                .unwrap();
        }
        let events = store.events(None, None, None, None, 1000).unwrap();
        assert!(
            events.len() <= 101,
            "journal must stay bounded, got {}",
            events.len()
        );
    }

    #[test]
    fn checkpoint_consume_once() {
        let store = OpsStore::open("sqlite::memory:").unwrap();
        store
            .record_checkpoint(
                "job-c",
                "node-c",
                Some("lease-c"),
                Some("mission-c"),
                "ref",
                "{}",
            )
            .unwrap();
        assert!(store.consume_checkpoint("job-c").unwrap());
        assert!(!store.consume_checkpoint("job-c").unwrap());
        let row = store.checkpoint("job-c").unwrap().unwrap();
        assert_eq!(row.lease_id.as_deref(), Some("lease-c"));
        assert!(row.consumed_at_ms.is_some());
    }
}
