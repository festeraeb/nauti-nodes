//! Chunk 11: provider-aware federation soak helpers.
//!
//! Resource observations and soak configuration for REAL control-plane
//! exercises. Counters must come from actual state transitions — never from
//! timer loops alone.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

/// Soak duration / cadence configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakConfig {
    pub duration: Duration,
    pub job_interval: Duration,
    pub lease_interval: Duration,
    pub failure_interval: Duration,
}

impl SoakConfig {
    /// Short CI soak (real operations, bounded).
    pub fn ci_short() -> Self {
        Self {
            duration: Duration::from_secs(5),
            job_interval: Duration::from_millis(200),
            lease_interval: Duration::from_millis(500),
            failure_interval: Duration::from_secs(1),
        }
    }

    /// Operator presets: 5m / 1h / 8h / 24h / 72h.
    pub fn operator(mode: &str) -> anyhow::Result<Self> {
        let secs = match mode {
            "5m" => 5 * 60,
            "1h" => 3600,
            "8h" => 8 * 3600,
            "24h" => 24 * 3600,
            "72h" => 72 * 3600,
            other => anyhow::bail!("unknown soak mode {other}"),
        };
        Ok(Self {
            duration: Duration::from_secs(secs),
            job_interval: Duration::from_millis(500),
            lease_interval: Duration::from_secs(2),
            failure_interval: Duration::from_secs(10),
        })
    }

    /// From `NAUTI_SOAK_DURATION_SECS` (default 5s CI).
    pub fn from_env() -> Self {
        let secs: u64 = std::env::var("NAUTI_SOAK_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        Self {
            duration: Duration::from_secs(secs),
            job_interval: Duration::from_millis(if secs <= 30 { 200 } else { 500 }),
            lease_interval: Duration::from_millis(if secs <= 30 { 500 } else { 2000 }),
            failure_interval: Duration::from_secs(if secs <= 30 { 1 } else { 10 }),
        }
    }
}

/// Real Linux process resource observation (no synthetic values).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceObservation {
    pub pid: u32,
    pub rss_bytes: Option<u64>,
    pub open_fds: Option<usize>,
    pub sqlite_db_bytes: Option<u64>,
    pub event_journal_rows: Option<u64>,
    pub observed_at_unix_ms: u64,
    pub notes: Vec<String>,
}

/// Read VmRSS from `/proc/{pid}/status` (Linux).
pub fn process_rss_bytes(pid: u32) -> anyhow::Result<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| anyhow::anyhow!("VmRSS missing"))?
                .parse()?;
            return Ok(kb * 1024);
        }
    }
    anyhow::bail!("VmRSS not found")
}

/// Count open file descriptors via `/proc/{pid}/fd`.
pub fn open_fd_count(pid: u32) -> anyhow::Result<usize> {
    Ok(std::fs::read_dir(format!("/proc/{pid}/fd"))?.count())
}

/// File size in bytes if path exists.
pub fn file_size_bytes(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

/// Observe current process resources + optional SQLite path.
pub fn observe_self(sqlite_path: Option<&Path>, event_rows: Option<u64>) -> ResourceObservation {
    let pid = std::process::id();
    let mut notes = Vec::new();
    let rss = match process_rss_bytes(pid) {
        Ok(v) => Some(v),
        Err(e) => {
            notes.push(format!("rss unavailable: {e}"));
            None
        }
    };
    let fds = match open_fd_count(pid) {
        Ok(v) => Some(v),
        Err(e) => {
            notes.push(format!("fd count unavailable: {e}"));
            None
        }
    };
    let db = sqlite_path.and_then(file_size_bytes);
    if sqlite_path.is_some() && db.is_none() {
        notes.push("sqlite path not readable".into());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    ResourceObservation {
        pid,
        rss_bytes: rss,
        open_fds: fds,
        sqlite_db_bytes: db,
        event_journal_rows: event_rows,
        observed_at_unix_ms: now,
        notes,
    }
}

/// Soak report payload written to disk after a real run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakReport {
    pub chunk: String,
    pub duration_secs: f64,
    pub config: SoakConfigJson,
    pub provider_decisions: Vec<String>,
    pub cases_executed: Vec<String>,
    pub integrity: crate::metrics::IntegritySnapshot,
    pub worker_gate: WorkerGateSnapshot,
    pub resource_start: ResourceObservation,
    pub resource_end: ResourceObservation,
    pub leases_active_end: usize,
    pub errors: Vec<String>,
    pub physical_gpu_used: bool,
    pub runtime_hardware_notes: Vec<String>,
    pub invariants_ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakConfigJson {
    pub duration_secs: u64,
    pub job_interval_ms: u64,
    pub lease_interval_ms: u64,
    pub failure_interval_ms: u64,
}

impl From<&SoakConfig> for SoakConfigJson {
    fn from(c: &SoakConfig) -> Self {
        Self {
            duration_secs: c.duration.as_secs(),
            job_interval_ms: c.job_interval.as_millis() as u64,
            lease_interval_ms: c.lease_interval.as_millis() as u64,
            failure_interval_ms: c.failure_interval.as_millis() as u64,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerGateSnapshot {
    pub commands_rejected_stale_epoch_total: u64,
    pub commands_rejected_authority_conflict_total: u64,
    pub duplicate_commands_blocked_total: u64,
    pub checkpoint_double_resume_blocked_total: u64,
}

impl SoakReport {
    pub fn write_json(&self, path: &Path) -> anyhow::Result<()> {
        let s = serde_json::to_string_pretty(self)?;
        std::fs::write(path, s)?;
        Ok(())
    }

    pub fn write_markdown(&self, path: &Path) -> anyhow::Result<()> {
        let mut md = String::new();
        md.push_str("# CHUNK 11 SOAK REPORT\n\n");
        md.push_str(&format!("- **duration_secs**: {:.3}\n", self.duration_secs));
        md.push_str(&format!("- **invariants_ok**: {}\n", self.invariants_ok));
        md.push_str(&format!(
            "- **physical_gpu_used**: {}\n",
            self.physical_gpu_used
        ));
        md.push_str("\n## Cases executed\n\n");
        for c in &self.cases_executed {
            md.push_str(&format!("- {c}\n"));
        }
        md.push_str("\n## Provider decisions\n\n");
        for d in &self.provider_decisions {
            md.push_str(&format!("- {d}\n"));
        }
        md.push_str("\n## Integrity counters\n\n");
        md.push_str(&format!(
            "```json\n{}\n```\n",
            serde_json::to_string_pretty(&self.integrity)?
        ));
        md.push_str("\n## Worker gate\n\n");
        md.push_str(&format!(
            "```json\n{}\n```\n",
            serde_json::to_string_pretty(&self.worker_gate)?
        ));
        md.push_str("\n## Resources\n\n");
        md.push_str(&format!(
            "- start RSS: {:?}\n- end RSS: {:?}\n- start FDs: {:?}\n- end FDs: {:?}\n- start DB: {:?}\n- end DB: {:?}\n",
            self.resource_start.rss_bytes,
            self.resource_end.rss_bytes,
            self.resource_start.open_fds,
            self.resource_end.open_fds,
            self.resource_start.sqlite_db_bytes,
            self.resource_end.sqlite_db_bytes,
        ));
        if !self.errors.is_empty() {
            md.push_str("\n## Errors\n\n");
            for e in &self.errors {
                md.push_str(&format!("- {e}\n"));
            }
        }
        md.push_str("\n## Runtime / hardware notes\n\n");
        for n in &self.runtime_hardware_notes {
            md.push_str(&format!("- {n}\n"));
        }
        std::fs::write(path, md)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_rss_self_readable_on_linux() {
        let pid = std::process::id();
        let rss = process_rss_bytes(pid).expect("VmRSS");
        assert!(rss > 0);
    }

    #[test]
    fn open_fd_self_readable_on_linux() {
        let pid = std::process::id();
        let n = open_fd_count(pid).expect("fd");
        assert!(n > 0);
    }

    #[test]
    fn soak_config_operator_modes() {
        assert_eq!(SoakConfig::operator("5m").unwrap().duration.as_secs(), 300);
        assert_eq!(SoakConfig::operator("1h").unwrap().duration.as_secs(), 3600);
        assert!(SoakConfig::operator("nope").is_err());
    }
}
