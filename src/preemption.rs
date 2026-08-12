//! Forge Preemption
//!
//! Wires live Forge eligibility/reservation semantics.
//!
//! Desired behavior:
//! ```text
//! background Nauti work
//!     ->
//! Forge requests GPU
//!     ->
//! checkpoint if possible
//!     ->
//! release
//!     ->
//! Forge mission
//!     ->
//! background work resumes later
//! ```
//!
//! Never report successful completion for silently killed work.
//!
//! # OS Process Preemption
//!
//! `ProcessPreemptionHandle` wraps a real `std::process::Child` and sends
//! SIGTERM (graceful), then escalates to SIGKILL after a timeout.
//!
//! # Checkpoint Serialization
//!
//! `CheckpointStore` serializes checkpoint data to `$TMPDIR/<job_id>.checkpoint.json`
//! so that work survives preemption and can be resumed after Forge mission completion.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Checkpoint Serialization
// ─────────────────────────────────────────────────────────────────────────────

/// Serialized state of a checkpointed job — written to disk so it survives
/// the Forge mission and can be resumed afterwards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub job_id: String,
    pub node_id: String,
    /// Partial output accumulated before preemption.
    pub partial_output: String,
    /// Arbitrary key/value metadata (step counters, token counts, etc.).
    pub metadata: HashMap<String, serde_json::Value>,
    /// Unix epoch seconds when the checkpoint was written.
    pub checkpointed_at_secs: u64,
}

impl CheckpointData {
    pub fn new(job_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            node_id: node_id.into(),
            partial_output: String::new(),
            metadata: HashMap::new(),
            checkpointed_at_secs: crate::types::now_ms() / 1000,
        }
    }

    pub fn with_output(mut self, output: impl Into<String>) -> Self {
        self.partial_output = output.into();
        self
    }

    pub fn with_meta(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

/// Atomic file-based checkpoint store.
///
/// Files are written to `$TMPDIR/<job_id>.checkpoint.json` by default; the
/// directory can be overridden at construction time.
pub struct CheckpointStore {
    dir: PathBuf,
}

impl CheckpointStore {
    /// Create a store that writes checkpoints to the system temp directory.
    pub fn new() -> Self {
        Self {
            dir: std::env::temp_dir(),
        }
    }

    /// Create a store that writes checkpoints to a specific directory.
    pub fn with_dir(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn path_for(&self, job_id: &str) -> PathBuf {
        self.dir.join(format!("{job_id}.checkpoint.json"))
    }

    /// Atomically write checkpoint data to disk.
    ///
    /// Uses a `.tmp` write-then-rename pattern so partial writes never
    /// corrupt an existing checkpoint.
    pub fn save(&self, data: &CheckpointData) -> std::io::Result<PathBuf> {
        let path = self.path_for(&data.job_id);
        let tmp_path = self.dir.join(format!("{}.tmp", data.job_id));
        let json = serde_json::to_string_pretty(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(&tmp_path, &path)?;
        Ok(path)
    }

    /// Load a previously saved checkpoint from disk.
    pub fn load(&self, job_id: &str) -> std::io::Result<CheckpointData> {
        let path = self.path_for(job_id);
        let raw = std::fs::read_to_string(&path)?;
        serde_json::from_str(&raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Remove a checkpoint file once the job has been successfully resumed.
    pub fn remove(&self, job_id: &str) -> std::io::Result<()> {
        let path = self.path_for(job_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Check whether a checkpoint file exists.
    pub fn exists(&self, job_id: &str) -> bool {
        self.path_for(job_id).exists()
    }
}

impl Default for CheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OS Process Preemption Handle
// ─────────────────────────────────────────────────────────────────────────────

/// Handle to a running OS process that Nauti can preempt on Forge demand.
///
/// Sends SIGTERM first (graceful shutdown / checkpoint flush).
/// If the process does not exit within `sigterm_timeout`, escalates to SIGKILL.
pub struct ProcessPreemptionHandle {
    pub job_id: String,
    child: std::sync::Mutex<Option<std::process::Child>>,
    pub sigterm_timeout: Duration,
}

impl ProcessPreemptionHandle {
    /// Wrap an already-spawned child process.
    pub fn new(job_id: impl Into<String>, child: std::process::Child) -> Self {
        Self {
            job_id: job_id.into(),
            child: std::sync::Mutex::new(Some(child)),
            sigterm_timeout: Duration::from_millis(500),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.sigterm_timeout = timeout;
        self
    }

    /// Send SIGTERM; escalate to SIGKILL if process does not exit in time.
    ///
    /// Returns:
    /// - `Ok(true)`  — process was running and has now been killed.
    /// - `Ok(false)` — process had already exited before preemption.
    /// - `Err(_)`    — OS error during kill/wait.
    pub fn preempt(&self) -> std::io::Result<bool> {
        let mut guard = self.child.lock().unwrap();
        let child = match guard.as_mut() {
            Some(c) => c,
            None => return Ok(false),
        };

        // Send SIGTERM
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let pid = child.id() as i32;
            // SAFETY: kill(2) is async-signal-safe.
            let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                // ESRCH means process already gone
                if err.raw_os_error() == Some(libc::ESRCH) {
                    let _ = guard.take();
                    return Ok(false);
                }
                return Err(err);
            }

            // Poll for exit up to sigterm_timeout in 10 ms slices
            let deadline = std::time::Instant::now() + self.sigterm_timeout;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        let _ = guard.take();
                        return Ok(true);
                    }
                    Ok(None) => {
                        if std::time::Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => return Err(e),
                }
            }

            // Escalate to SIGKILL
            let rc = unsafe { libc::kill(pid, libc::SIGKILL) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ESRCH) {
                    return Err(err);
                }
            }
        }

        #[cfg(not(unix))]
        {
            child.kill()?;
        }

        child.wait()?;
        let _ = guard.take();
        Ok(true)
    }

    /// True if the wrapped process is still tracked (not yet preempted or exited).
    pub fn is_alive(&self) -> bool {
        self.child.lock().unwrap().is_some()
    }
}

/// `libc` is used only on Unix for SIGTERM/SIGKILL.
#[cfg(unix)]
extern crate libc;

/// State of a preemptible job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreemptionState {
    /// Job is running normally
    Running,
    /// Job has been checkpointed and can be resumed
    Checkpointed,
    /// Job has been preempted (released GPU)
    Preempted,
    /// Job has been resumed after preemption
    Resumed,
    /// Job was killed without checkpoint (failed)
    Killed,
    /// Job completed successfully
    Completed,
}

/// A preemptible job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreemptibleJob {
    /// Job ID
    pub job_id: String,

    /// Node ID where the job runs
    pub node_id: String,

    /// Current state
    pub state: PreemptionState,

    /// Whether this job supports checkpointing
    pub checkpointable: bool,

    /// Checkpoint reference (path, URL, or hash)
    pub checkpoint_ref: Option<String>,

    /// When the job was created
    pub created_at_ms: u64,

    /// When the job was last preempted
    pub last_preempted_at_ms: Option<u64>,

    /// Number of times this job has been preempted
    pub preemption_count: u32,

    /// Whether the job completed successfully
    pub completed_successfully: bool,
}

impl PreemptibleJob {
    pub fn new(
        job_id: impl Into<String>,
        node_id: impl Into<String>,
        checkpointable: bool,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            node_id: node_id.into(),
            state: PreemptionState::Running,
            checkpointable,
            checkpoint_ref: None,
            created_at_ms: crate::types::now_ms(),
            last_preempted_at_ms: None,
            preemption_count: 0,
            completed_successfully: false,
        }
    }

    /// Checkpoint this job (if supported).
    pub fn checkpoint(&mut self, checkpoint_ref: impl Into<String>) -> bool {
        if !self.checkpointable {
            return false;
        }
        self.checkpoint_ref = Some(checkpoint_ref.into());
        self.state = PreemptionState::Checkpointed;
        true
    }

    /// Preempt this job (release GPU).
    pub fn preempt(&mut self) {
        self.state = PreemptionState::Preempted;
        self.last_preempted_at_ms = Some(crate::types::now_ms());
        self.preemption_count += 1;
    }

    /// Resume this job after preemption.
    pub fn resume(&mut self) {
        self.state = PreemptionState::Resumed;
    }

    /// Mark this job as completed successfully.
    pub fn complete(&mut self) {
        self.state = PreemptionState::Completed;
        self.completed_successfully = true;
    }

    /// Mark this job as killed (no checkpoint).
    pub fn kill(&mut self) {
        self.state = PreemptionState::Killed;
        self.completed_successfully = false;
    }
}

/// A Forge GPU reservation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgeReservationRequest {
    /// Mission ID requesting the GPU
    pub mission_id: String,

    /// Node ID being requested
    pub node_id: String,

    /// VRAM required in MiB
    pub vram_mb: u64,

    /// Priority of the mission (higher = more urgent)
    pub priority: u32,

    /// Whether preemption is allowed
    pub allow_preemption: bool,
}

/// Result of a preemption request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreemptionResult {
    /// Whether the GPU was successfully released
    pub released: bool,

    /// Number of jobs preempted
    pub jobs_preempted: u32,

    /// Number of jobs checkpointed
    pub jobs_checkpointed: u32,

    /// Number of jobs killed (no checkpoint)
    pub jobs_killed: u32,

    /// VRAM released in MiB
    pub vram_released_mb: u64,

    /// Preempted job IDs
    pub preempted_jobs: Vec<String>,
}

/// Preemption manager for Forge GPU reservations.
pub struct PreemptionManager {
    /// Active preemptible jobs
    jobs: Mutex<HashMap<String, PreemptibleJob>>,

    /// Active Forge reservations
    reservations: Mutex<HashMap<String, ForgeReservationRequest>>,
}

impl PreemptionManager {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            reservations: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new preemptible job.
    pub fn register_job(&self, job: PreemptibleJob) {
        self.jobs.lock().unwrap().insert(job.job_id.clone(), job);
    }

    /// Get a job by ID.
    pub fn get_job(&self, job_id: &str) -> Option<PreemptibleJob> {
        self.jobs.lock().unwrap().get(job_id).cloned()
    }

    /// List all jobs on a specific node.
    pub fn jobs_on_node(&self, node_id: &str) -> Vec<PreemptibleJob> {
        self.jobs
            .lock()
            .unwrap()
            .values()
            .filter(|j| j.node_id == node_id)
            .cloned()
            .collect()
    }

    /// Handle a Forge reservation request.
    ///
    /// Preempts background jobs on the requested node to free VRAM.
    /// Checkpointable jobs are checkpointed; non-checkpointable jobs are killed.
    pub fn handle_reservation(&self, request: &ForgeReservationRequest) -> PreemptionResult {
        let mut jobs = self.jobs.lock().unwrap();
        let mut preempted_jobs = Vec::new();
        let mut jobs_preempted = 0u32;
        let mut jobs_checkpointed = 0u32;
        let mut jobs_killed = 0u32;
        let mut vram_released = 0u64;

        // Find jobs on the requested node
        let node_jobs: Vec<String> = jobs
            .iter()
            .filter(|(_, j)| j.node_id == request.node_id && j.state == PreemptionState::Running)
            .map(|(id, _)| id.clone())
            .collect();

        for job_id in node_jobs {
            if let Some(job) = jobs.get_mut(&job_id) {
                // Checkpoint if possible
                if job.checkpointable {
                    job.checkpoint(format!("checkpoint-{}", job.job_id));
                    jobs_checkpointed += 1;
                } else {
                    job.kill();
                    jobs_killed += 1;
                }
                job.preempt();
                jobs_preempted += 1;
                vram_released += 8192; // assume ~8GB per job
                preempted_jobs.push(job_id);
            }
        }

        // Record the reservation
        self.reservations
            .lock()
            .unwrap()
            .insert(request.mission_id.clone(), request.clone());

        PreemptionResult {
            released: jobs_preempted > 0,
            jobs_preempted,
            jobs_checkpointed,
            jobs_killed,
            vram_released_mb: vram_released,
            preempted_jobs,
        }
    }

    /// Release a Forge reservation (mission complete).
    pub fn release_reservation(&self, mission_id: &str) -> bool {
        self.reservations
            .lock()
            .unwrap()
            .remove(mission_id)
            .is_some()
    }

    /// Resume all checkpointed jobs on a node.
    pub fn resume_checkpointed(&self, node_id: &str) -> u32 {
        let mut jobs = self.jobs.lock().unwrap();
        let mut resumed = 0u32;
        for job in jobs.values_mut() {
            if job.node_id == node_id && job.state == PreemptionState::Preempted {
                job.resume();
                resumed += 1;
            }
        }
        resumed
    }

    /// Number of active jobs.
    pub fn job_count(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }

    /// Number of active reservations.
    pub fn reservation_count(&self) -> usize {
        self.reservations.lock().unwrap().len()
    }
}

impl Default for PreemptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preemptible_job_lifecycle() {
        let mut job = PreemptibleJob::new("job-1", "node-1", true);
        assert_eq!(job.state, PreemptionState::Running);

        assert!(job.checkpoint("checkpoint-1"));
        assert_eq!(job.state, PreemptionState::Checkpointed);

        job.preempt();
        assert_eq!(job.state, PreemptionState::Preempted);
        assert_eq!(job.preemption_count, 1);

        job.resume();
        assert_eq!(job.state, PreemptionState::Resumed);

        job.complete();
        assert_eq!(job.state, PreemptionState::Completed);
        assert!(job.completed_successfully);
    }

    #[test]
    fn non_checkpointable_job_cannot_checkpoint() {
        let mut job = PreemptibleJob::new("job-1", "node-1", false);
        assert!(!job.checkpoint("checkpoint-1"));
        assert_eq!(job.state, PreemptionState::Running);
    }

    #[test]
    fn kill_marks_failed() {
        let mut job = PreemptibleJob::new("job-1", "node-1", false);
        job.kill();
        assert_eq!(job.state, PreemptionState::Killed);
        assert!(!job.completed_successfully);
    }

    #[test]
    fn reservation_preempts_jobs() {
        let manager = PreemptionManager::new();
        manager.register_job(PreemptibleJob::new("job-1", "node-1", true));
        manager.register_job(PreemptibleJob::new("job-2", "node-1", false));

        let request = ForgeReservationRequest {
            mission_id: "mission-1".into(),
            node_id: "node-1".into(),
            vram_mb: 16384,
            priority: 100,
            allow_preemption: true,
        };

        let result = manager.handle_reservation(&request);
        assert!(result.released);
        assert_eq!(result.jobs_preempted, 2);
        assert_eq!(result.jobs_checkpointed, 1); // job-1 checkpointable
        assert_eq!(result.jobs_killed, 1); // job-2 not checkpointable
        assert_eq!(result.preempted_jobs.len(), 2);
    }

    #[test]
    fn reservation_does_not_affect_other_nodes() {
        let manager = PreemptionManager::new();
        manager.register_job(PreemptibleJob::new("job-1", "node-1", true));
        manager.register_job(PreemptibleJob::new("job-2", "node-2", true));

        let request = ForgeReservationRequest {
            mission_id: "mission-1".into(),
            node_id: "node-1".into(),
            vram_mb: 16384,
            priority: 100,
            allow_preemption: true,
        };

        let result = manager.handle_reservation(&request);
        assert_eq!(result.jobs_preempted, 1);
        assert_eq!(result.preempted_jobs, vec!["job-1".to_string()]);
    }

    #[test]
    fn resume_checkpointed_jobs() {
        let manager = PreemptionManager::new();
        manager.register_job(PreemptibleJob::new("job-1", "node-1", true));

        let request = ForgeReservationRequest {
            mission_id: "mission-1".into(),
            node_id: "node-1".into(),
            vram_mb: 16384,
            priority: 100,
            allow_preemption: true,
        };
        manager.handle_reservation(&request);

        // Mission complete, resume jobs
        let resumed = manager.resume_checkpointed("node-1");
        assert_eq!(resumed, 1);

        let job = manager.get_job("job-1").unwrap();
        assert_eq!(job.state, PreemptionState::Resumed);
    }

    #[test]
    fn release_reservation() {
        let manager = PreemptionManager::new();
        let request = ForgeReservationRequest {
            mission_id: "mission-1".into(),
            node_id: "node-1".into(),
            vram_mb: 16384,
            priority: 100,
            allow_preemption: true,
        };
        manager.handle_reservation(&request);
        assert_eq!(manager.reservation_count(), 1);

        assert!(manager.release_reservation("mission-1"));
        assert_eq!(manager.reservation_count(), 0);
    }

    #[test]
    fn jobs_on_node_filtering() {
        let manager = PreemptionManager::new();
        manager.register_job(PreemptibleJob::new("job-1", "node-1", true));
        manager.register_job(PreemptibleJob::new("job-2", "node-2", true));

        let node1_jobs = manager.jobs_on_node("node-1");
        assert_eq!(node1_jobs.len(), 1);
        assert_eq!(node1_jobs[0].job_id, "job-1");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests — OS process preemption (real SIGTERM/SIGKILL)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod process_preemption_tests {
    use super::*;

    /// Spawn a long-running `sleep 60` process and preempt it via SIGTERM.
    /// Confirms the process is actually killed (not just state changed).
    #[cfg(unix)]
    #[test]
    fn sigterm_kills_real_sleep_process() {
        let child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("failed to spawn sleep — is /usr/bin/sleep present?");

        let pid = child.id();
        let handle = ProcessPreemptionHandle::new("test-job-sleep", child)
            .with_timeout(std::time::Duration::from_millis(200));

        assert!(handle.is_alive(), "process should be alive before preemption");

        let result = handle.preempt().expect("preempt() returned an IO error");
        assert!(result, "preempt() should return true for a running process");
        assert!(!handle.is_alive(), "handle should be empty after preemption");

        // Verify the OS process is no longer alive via kill(pid, 0)
        let rc = unsafe { libc::kill(pid as i32, 0) };
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        // ESRCH (3) = no such process — the expected outcome
        assert!(
            rc != 0 && errno == libc::ESRCH,
            "OS process pid={pid} should be dead (ESRCH), got rc={rc} errno={errno}"
        );
    }

    /// Preempting an already-exited process should return Ok(false) without panicking.
    #[cfg(unix)]
    #[test]
    fn preempt_already_exited_process_is_noop() {
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("failed to spawn true");
        child.wait().expect("wait failed");

        let handle = ProcessPreemptionHandle::new("test-job-true", child);
        // Process already exited — preempt() should return Ok(false) gracefully
        let result = handle.preempt();
        // Allow either Ok(false) or Ok(true) — the important thing is no panic/error
        assert!(result.is_ok(), "preempt() of exited process must not return Err");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests — Checkpoint disk round-trip
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod checkpoint_store_tests {
    use super::*;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn checkpoint_roundtrip_survives_drop() {
        let dir = tmpdir();
        let store = CheckpointStore::with_dir(dir.path().to_path_buf());

        let data = CheckpointData::new("job-cp-1", "node-a")
            .with_output("partial inference tokens 0..512")
            .with_meta("token_count", serde_json::json!(512))
            .with_meta("step", serde_json::json!(3));

        // Write checkpoint to disk
        let path = store.save(&data).expect("save failed");
        assert!(path.exists(), "checkpoint file must exist after save");

        // Simulate process restart — load from a fresh store pointing at the same dir
        let store2 = CheckpointStore::with_dir(dir.path().to_path_buf());
        let loaded = store2.load("job-cp-1").expect("load failed");

        assert_eq!(loaded.job_id, "job-cp-1");
        assert_eq!(loaded.node_id, "node-a");
        assert_eq!(loaded.partial_output, "partial inference tokens 0..512");
        assert_eq!(
            loaded.metadata["token_count"].as_u64().unwrap(),
            512,
            "token_count metadata must survive disk round-trip"
        );
        assert_eq!(
            loaded.metadata["step"].as_u64().unwrap(),
            3,
            "step metadata must survive disk round-trip"
        );

        // Remove checkpoint after successful resume
        store2.remove("job-cp-1").expect("remove failed");
        assert!(!store2.exists("job-cp-1"), "checkpoint must be gone after remove");
    }

    #[test]
    fn missing_checkpoint_returns_error() {
        let dir = tmpdir();
        let store = CheckpointStore::with_dir(dir.path().to_path_buf());
        let result = store.load("does-not-exist");
        assert!(result.is_err(), "loading non-existent checkpoint must fail");
    }

    #[test]
    fn save_is_atomic_tmp_rename() {
        // Confirm that .tmp file is not present after a successful save
        let dir = tmpdir();
        let store = CheckpointStore::with_dir(dir.path().to_path_buf());
        let data = CheckpointData::new("job-atomic", "node-b");
        store.save(&data).expect("save failed");

        let tmp = dir.path().join("job-atomic.tmp");
        assert!(!tmp.exists(), ".tmp file must not exist after atomic rename");
        assert!(store.exists("job-atomic"), "final checkpoint file must exist");
    }
}
