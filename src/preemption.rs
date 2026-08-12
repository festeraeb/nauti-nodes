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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

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
    pub fn new(job_id: impl Into<String>, node_id: impl Into<String>, checkpointable: bool) -> Self {
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
        self.reservations.lock().unwrap().remove(mission_id).is_some()
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