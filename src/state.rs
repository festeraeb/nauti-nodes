//! Application state: persistent operational state + lease authority.
//!
//! Chunk 7 additions:
//! - `metrics: Arc<FederationMetrics>` — Arc-shared so handler/task increments
//!   are not lost across AppState clones (the previous by-value clone snapshot
//!   was lossy).
//! - `ops: Arc<OpsStore>` — SQLite-backed persistent operational state store.
//! - `leases: Arc<LeaseManager>` — authoritative Forge lease state machine.
//! - `external_alpha_enabled: Arc<AtomicBool>` — operator kill switch.
//! - `identity: Option<Arc<CoordinatorIdentity>>` — Chunk 8: persistent
//!   coordinator identity + federation membership + authority epoch.
//!
//! Recovery rules (see `recover_operational_state`):
//! - Do NOT assume workers are immediately online: recovered nodes start
//!   OFFLINE with runtime Unknown and stale reconciliation. Fresh signed
//!   heartbeats / re-registration re-establish truth.
//! - Do NOT assume old runtime processes are alive: PIDs are never persisted
//!   as truth; runtime_state starts Unknown.
//! - Do NOT assume hardware is unchanged: MATCH/PARTIAL_MATCH reconciliations
//!   degrade to STALE until freshly re-observed.
//! - Active Forge lease ownership IS preserved (granted/leased/releasing/
//!   recovering); pre-grant leases are marked STALE.
//! - Orphaned token reservations are refunded exactly once.

use crate::config::Config;
use crate::eligibility::RuntimeState;
use crate::jobs::JobStore;
use crate::lease::LeaseManager;
use crate::lineage::LineageStore;
use crate::metrics::FederationMetrics;
use crate::node::auth::NonceTracker;
use crate::node::registry::NodeRegistry;
use crate::node::revocation::RevocationStore;
use crate::opsstore::{NodeStateRow, OpsEventKind, OpsStore};
use crate::preemption::PreemptionManager;
use crate::runtime::ModelManager;
use crate::scheduler::Scheduler;
use crate::types::{
    now_ms, NodeCapabilities, NodeLifecycleState, NodeMetadata, ReconciliationStatus,
    RegisteredHardwareClaim,
};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// ── Coordinator identity (Chunk 8) ────────────────────────────────────────

use crate::federation::authority::AuthorityMonitor;
use crate::federation::delegation::DelegationStore;
use crate::federation::envelope::FederationAuthenticator;
use crate::federation::fence::FenceGate;
use crate::federation::worker_gate::WorkerFenceState;
use crate::federation::PeerRegistry;
use crate::node::coordinator_identity::{CoordinatorIdentity, FederationState};
use crate::node::identity::NodeIdentity;
use crate::providers::ProviderRegistry;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub registry: Arc<NodeRegistry>,
    pub scheduler: Arc<Scheduler>,
    pub jobs: Arc<JobStore>,
    pub http: reqwest::Client,
    /// Chunk 8B: RwLock-guarded so catch-up / failover can swap the
    /// identity (epoch adoption) without restarting the process.
    pub identity: Arc<std::sync::RwLock<Option<Arc<CoordinatorIdentity>>>>,
    pub lineage: Option<Arc<LineageStore>>,
    /// Chunk 7: Arc-shared so handler/task increments are not lost across
    /// AppState clones (the previous by-value clone snapshot was lossy).
    pub metrics: Arc<FederationMetrics>,
    pub nonce_tracker: Arc<NonceTracker>,
    pub revocations: Arc<RevocationStore>,
    pub model_manager: Arc<ModelManager>,
    pub preemption: Arc<PreemptionManager>,
    pub external_alpha_enabled: Arc<AtomicBool>,
    /// Chunk 7: persistent operational state store (SQLite).
    pub ops: Arc<OpsStore>,
    /// Chunk 7: authoritative Forge lease state machine.
    pub leases: Arc<LeaseManager>,
    /// Chunk 8B: federation peer registry (seeded from config; filled in by
    /// real handshakes — never synthetic peer state).
    pub peers: Arc<PeerRegistry>,
    /// Chunk 8B.2: worker-side fenced command gate (durable).
    pub worker_gate: Arc<WorkerFenceState>,
    /// Chunk 8B.4: authority election / failover monitor.
    pub authority: Arc<AuthorityMonitor>,
    /// Chunk 9: durable delegated-job store (exactly-once across failover).
    pub delegation: Arc<DelegationStore>,
    /// Chunk 10: pluggable execution provider registry.
    pub providers: Arc<ProviderRegistry>,
}

impl AppState {
    pub fn new(config: Config) -> crate::types::Result<Self> {
        let registry = Arc::new(NodeRegistry::new());
        let scheduler = Arc::new(Scheduler::new(registry.clone(), &config.db_url)?);
        let revocations = Arc::new(RevocationStore::open(&config.db_url)?);
        let model_manager = Arc::new(ModelManager::new());
        let preemption = Arc::new(PreemptionManager::new());
        let external_alpha_enabled = Arc::new(AtomicBool::new(config.external_alpha_enabled));
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .map_err(|e| crate::types::Error::Internal(e.to_string()))?;

        // Chunk 7: persistent operational state + lease authority.
        let event_retention = std::env::var("NAUTI_EVENT_RETENTION")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(crate::opsstore::DEFAULT_EVENT_RETENTION);
        let ops = Arc::new(OpsStore::open_with_retention(
            &config.db_url,
            event_retention,
        )?);
        let leases = Arc::new(LeaseManager::new(ops.clone()));

        // Chunk 7: coordinator restart recovery — load persisted truth,
        // fail closed while ownership is uncertain.
        recover_operational_state(&registry, &ops, &leases, &scheduler)?;

        // Chunk 8: load or generate coordinator identity + federation state.
        let identity = load_coordinator_identity(&config)?;

        // Chunk 8B: keep the lease authority's fencing epoch in sync with
        // the coordinator identity so lease overlays are stamped with the
        // real authority epoch (never a hardcoded value).
        if let Some(id) = identity.as_ref() {
            leases.set_authority_epoch(id.authority_epoch.value());
        }

        // Chunk 8B: federation peers from configuration.
        let peers = Arc::new(PeerRegistry::seed_from_endpoints(&config.federation_peers));

        // Chunk 8B.2: worker-side fenced command gate. Trust anchors come
        // from configured coordinator keys; replay protection reuses the
        // existing NonceTracker primitive; revocation is the durable store.
        let jobs = Arc::new(JobStore::new());
        let worker_gate = Arc::new(WorkerFenceState::new(
            FederationAuthenticator::new(
                config.trusted_coordinator_keys.clone(),
                Arc::new(NonceTracker::new()),
                revocations.clone(),
            ),
            FenceGate::new(ops.clone()),
            leases.clone(),
            ops.clone(),
            jobs.clone(),
        ));

        // Chunk 8B.4: authority monitor keyed by the local coordinator id.
        let local_id = identity
            .as_ref()
            .map(|id| id.node_identity.node_id.clone())
            .unwrap_or_default();
        let authority = Arc::new(AuthorityMonitor::new(local_id, peers.clone()));

        // Chunk 9: durable delegated-job identity store (exactly-once).
        let delegation = Arc::new(DelegationStore::new(ops.clone()));

        // Chunk 10: pluggable execution provider registry.
        let providers = Arc::new(ProviderRegistry::new());

        Ok(Self {
            config,
            registry,
            scheduler,
            jobs,
            http,
            identity: Arc::new(std::sync::RwLock::new(identity)),
            lineage: None,
            metrics: Arc::new(FederationMetrics::new()),
            nonce_tracker: Arc::new(NonceTracker::new()),
            revocations,
            model_manager,
            preemption,
            external_alpha_enabled,
            ops,
            leases,
            peers,
            worker_gate,
            authority,
            delegation,
            providers,
        })
    }
}

// ── Coordinator identity helpers (Chunk 8) ────────────────────────────────

/// Build the coordinator identity for an already-loaded durable node
/// identity (Chunk 8B).
///
/// - `coordinator_identity_path = Some(path)`: federation membership state
///   is persisted at `path` and MUST wrap the durable node identity
///   (identity-split fails closed).
/// - `coordinator_identity_path = None`: in-memory federation layer only;
///   no file is written (single-coordinator mode, per the documented
///   contract in `Config`).
pub fn coordinator_identity_for(
    config: &Config,
    node_identity: &NodeIdentity,
) -> crate::types::Result<CoordinatorIdentity> {
    match config.coordinator_identity_path.as_deref() {
        Some(path) => {
            CoordinatorIdentity::load_or_create_wrapping(std::path::Path::new(path), node_identity)
        }
        None => Ok(CoordinatorIdentity::wrap(node_identity.clone())),
    }
}

/// Load or generate the coordinator identity according to the configured
/// contract (Chunk 8B):
///
/// - `NAUTI_COORDINATOR_IDENTITY_PATH` set: persist federation state there,
///   wrapping the durable node identity from `identity_path`.
/// - Not set: generate an in-memory identity. NO file is written. (The
///   previous fallback silently wrote `nauti-coordinator-identity.json`
///   into the working directory, violating the documented contract and
///   polluting test runs.)
fn load_coordinator_identity(
    config: &Config,
) -> crate::types::Result<Option<Arc<CoordinatorIdentity>>> {
    if config.coordinator_identity_path.is_some() {
        let node = NodeIdentity::load_or_create(std::path::Path::new(&config.identity_path))?;
        let identity = coordinator_identity_for(config, &node)?;
        return Ok(Some(Arc::new(identity)));
    }
    // In-memory only: fresh node identity, no persistence.
    let identity = CoordinatorIdentity::generate()?;
    Ok(Some(Arc::new(identity)))
}

/// Read-lock helper for the identity handle.
pub fn identity_snapshot(state: &AppState) -> Option<Arc<CoordinatorIdentity>> {
    state.identity.read().ok().and_then(|guard| guard.clone())
}

/// Chunk 8B.13: install an updated identity (catch-up / failover adoption).
pub fn install_identity(state: &AppState, identity: CoordinatorIdentity) {
    if let Ok(mut guard) = state.identity.write() {
        *guard = Some(Arc::new(identity));
    }
}

/// Shortcut: whether the local coordinator may issue ownership mutations.
/// Returns false if we are not a coordinator, not active, or revoked.
pub fn can_issue_mutations(state: &AppState) -> bool {
    identity_snapshot(state).map_or(false, |id| id.can_issue_mutations())
}

/// Shortcut: whether the local coordinator is an authority candidate.
pub fn is_authority_candidate(state: &AppState) -> bool {
    identity_snapshot(state).map_or(false, |id| id.is_authority_candidate())
}

/// Shortcut: this coordinator's fingerprint, if any.
pub fn coordinator_fingerprint(state: &AppState) -> Option<String> {
    identity_snapshot(state).map(|id| id.fingerprint())
}

/// Persist a node snapshot from live metadata (write-through helper).
pub fn persist_node(ops: &OpsStore, rt: &crate::types::NodeRuntime) {
    let md = match rt.metadata.read() {
        Ok(m) => m,
        Err(_) => return,
    };
    let ownership = crate::eligibility::derive_ownership(&md, rt.assigned_jobs_count());
    let row = NodeStateRow {
        node_id: md.id.clone(),
        public_key_hex: hex::encode(&md.public_key),
        addr: md.addr.clone(),
        inference_url: md.inference_url.clone(),
        role: md.role.clone(),
        gpu_name: md.gpu_name.clone(),
        vram_mb: md.capabilities.vram_mb,
        models_json: serde_json::to_string(&md.capabilities.models)
            .unwrap_or_else(|_| "[]".to_string()),
        lifecycle_state: md.lifecycle_state.as_str().to_string(),
        health_state: md.health_state.clone(),
        ownership: ownership.as_str().to_string(),
        runtime_state: md.runtime_state.as_str().to_string(),
        reconciliation_status: format!("{:?}", md.reconciliation.status),
        claim_json: serde_json::to_string(&md.claim).unwrap_or_else(|_| "{}".to_string()),
        observed_json: md
            .observed
            .as_ref()
            .and_then(|o| serde_json::to_string(o).ok()),
        last_heartbeat_ms: Some(rt.last_heartbeat_ms()),
        last_state_change_ms: md.last_state_change_ms,
        failure_reason: md.failure_reason.clone(),
        updated_at_ms: now_ms(),
    };
    let _ = ops.upsert_node(&row);
}

// ── Coordinator restart recovery ─────────────────────────────────────────

fn parse_lifecycle(s: &str) -> NodeLifecycleState {
    match s {
        "JOINING" => NodeLifecycleState::Joining,
        "VERIFYING" => NodeLifecycleState::Verifying,
        "ONLINE" => NodeLifecycleState::Online,
        "DEGRADED" => NodeLifecycleState::Degraded,
        "DRAINING" => NodeLifecycleState::Draining,
        "WITHDRAWN" => NodeLifecycleState::Withdrawn,
        "REVOKED" => NodeLifecycleState::Revoked,
        _ => NodeLifecycleState::Offline,
    }
}

fn parse_reconciliation(s: &str) -> ReconciliationStatus {
    match s {
        "MATCH" | "PARTIAL_MATCH" => {
            // Hardware truth must be freshly re-established after restart.
            // Fail closed until a new reconciliation happens.
            ReconciliationStatus::STALE
        }
        "MISMATCH" => ReconciliationStatus::MISMATCH,
        "STALE" => ReconciliationStatus::STALE,
        "OFFLINE" => ReconciliationStatus::OFFLINE,
        _ => ReconciliationStatus::UNVERIFIED,
    }
}

/// Rebuild in-memory operational state from persisted truth.
///
/// Recovery rules:
/// - Do NOT assume workers are immediately online: recovered nodes start
///   OFFLINE with runtime Unknown and stale reconciliation.
/// - Do NOT assume old runtime processes are alive: runtime_state starts Unknown.
/// - Do NOT assume hardware is unchanged: MATCH/PARTIAL_MATCH -> STALE.
/// - Active Forge lease ownership IS preserved; pre-grant leases are STALE.
/// - Orphaned token reservations are refunded exactly once.
pub fn recover_operational_state(
    registry: &NodeRegistry,
    ops: &OpsStore,
    leases: &LeaseManager,
    scheduler: &Scheduler,
) -> crate::types::Result<()> {
    let mut recovered_nodes = 0u64;

    for row in ops.load_nodes()? {
        let meta = recovered_metadata(&row);
        registry.register_node(meta);
        recovered_nodes += 1;
    }

    // Preserve active lease ownership; overlay RESERVED_FORGE on nodes.
    let kept = leases.restore_overlays(registry);

    // Refund orphaned reservations exactly once (no silent loss of reserved
    // balance, no double refund: settle_reservation is an atomic transition).
    let mut refunded = 0u64;
    for res in ops.active_reservations()? {
        let settled = ops
            .settle_reservation(&res.job_id, 0, res.reserved_tokens, "REFUNDED")
            .unwrap_or(false);
        if settled {
            let _ = scheduler.release_tokens(&res.api_key, res.reserved_tokens);
            let _ = ops.record(
                OpsEventKind::AccountingRefunded,
                None,
                None,
                Some(&res.job_id),
                None,
                &format!(
                    "orphaned reservation refunded after restart ({} tokens)",
                    res.reserved_tokens
                ),
                "REFUNDED",
            );
            refunded += 1;
        }
    }

    let _ = ops.record(
        OpsEventKind::CoordinatorStarted,
        None,
        None,
        None,
        None,
        &format!(
            "recovered nodes={recovered_nodes} active_leases={} refunded_reservations={refunded}",
            kept.len()
        ),
        "RECOVERED",
    );
    if recovered_nodes > 0 || !kept.is_empty() || refunded > 0 {
        let _ = ops.record(
            OpsEventKind::StateRecovered,
            None,
            None,
            None,
            None,
            "coordinator restart recovery completed (fail-closed until fresh truth)",
            "RECOVERED",
        );
    }
    tracing::info!(
        recovered_nodes,
        active_leases = kept.len(),
        refunded_reservations = refunded,
        "coordinator operational state recovered (fail-closed until fresh truth)"
    );
    if !kept.is_empty() {
        for l in &kept {
            tracing::warn!(
                lease_id = %l.lease_id,
                node_id = %l.node_id,
                state = l.state.as_str(),
                "active Forge lease preserved across restart; node remains excluded"
            );
        }
    }
    Ok(())
}

/// Build fail-closed metadata from a persisted node row.
pub fn recovered_metadata(row: &NodeStateRow) -> NodeMetadata {
    let lifecycle = parse_lifecycle(&row.lifecycle_state);
    // Terminal operator states survive restart; everything else must be
    // re-established by fresh heartbeats.
    let (lifecycle, health) = match lifecycle {
        NodeLifecycleState::Withdrawn => (NodeLifecycleState::Withdrawn, "WITHDRAWN".to_string()),
        NodeLifecycleState::Revoked => (NodeLifecycleState::Revoked, "REVOKED".to_string()),
        _ => (NodeLifecycleState::Offline, "OFFLINE".to_string()),
    };

    let claim: RegisteredHardwareClaim =
        serde_json::from_str(&row.claim_json).unwrap_or_else(|_| RegisteredHardwareClaim {
            gpu_name: row.gpu_name.clone(),
            vram_mb: row.vram_mb,
            models: Vec::new(),
        });
    let observed = row
        .observed_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok());
    let models: Vec<String> = serde_json::from_str(&row.models_json).unwrap_or_default();

    NodeMetadata {
        id: row.node_id.clone(),
        addr: row.addr.clone(),
        inference_url: row.inference_url.clone(),
        role: row.role.clone(),
        gpu_name: row.gpu_name.clone(),
        online: false,
        public_key: hex::decode(&row.public_key_hex).unwrap_or_default(),
        capabilities: NodeCapabilities {
            max_batch: 1,
            models: models.clone(),
            vram_mb: row.vram_mb,
            role: row.role.clone(),
        },
        claim,
        observed,
        reconciliation: crate::types::ReconciledHardwareState {
            status: parse_reconciliation(&row.reconciliation_status),
            fields: Vec::new(),
        },
        last_hardware_probe: None,
        health_state: health,
        lifecycle_state: lifecycle,
        // FAIL CLOSED: runtime truth must be re-established after restart.
        runtime_state: RuntimeState::Unknown,
        active_lease: None,
        last_state_change_ms: now_ms(),
        failure_reason: String::new(),
        assigned_jobs: 0,
    }
}

// ── Replicated state accessors ────────────────────────────────────────────

/// The current authority epoch, if we have a coordinator identity.
pub fn authority_epoch(state: &AppState) -> u64 {
    identity_snapshot(state).map_or(0, |id| id.authority_epoch.value())
}

/// When the current epoch was acquired (millis since unix epoch).
pub fn epoch_acquired_at_ms(state: &AppState) -> u64 {
    identity_snapshot(state).map_or(0, |id| id.epoch_acquired_at_ms)
}

/// Trusted peer public keys, if any.
pub fn trusted_peers(state: &AppState) -> Vec<String> {
    identity_snapshot(state)
        .map(|id| id.trusted_peers.clone())
        .unwrap_or_default()
}

/// Replication position, if any.
pub fn replication_position(state: &AppState) -> u64 {
    identity_snapshot(state).map_or(0, |id| id.replication_position)
}

/// Federation state, if any.
pub fn federation_state(state: &AppState) -> Option<FederationState> {
    identity_snapshot(state).map(|id| id.federation_state.clone())
}

// ── Replicated lease state ────────────────────────────────────────────────

/// Check whether a lease's authority epoch is current relative to the
/// local coordinator's epoch.  If the lease epoch is stale, the lease
/// is considered invalid and the node must not be scheduled.
pub fn is_lease_epoch_current(lease_epoch: u64, local_epoch: u64) -> bool {
    // Lease is current if its epoch >= local epoch.
    // (A stale coordinator with a lower epoch cannot grant valid leases.)
    lease_epoch >= local_epoch
}

// ── Fail-closed eligibility with fencing ─────────────────────────────────

/// Extended eligibility that also checks the ownership epoch fencing.
/// A node is eligible only if:
///   • any active lease on the node has an epoch >= the local coordinator's
///     epoch (fencing check runs FIRST so a stale coordinator's lease fails
///     with an explicit fencing error instead of a generic lease rejection),
///     AND
///   • it passes the standard evaluate_node predicate.
pub fn evaluate_node_with_fencing(
    rt: &crate::types::NodeRuntime,
    local_epoch: u64,
) -> crate::types::Result<()> {
    // Fencing check first: if there is an active lease on this node, its
    // epoch must be >= the local coordinator's epoch. This is a metadata
    // check; the actual lease store is consulted by the scheduler/handler
    // layer.
    {
        let md = rt
            .metadata
            .read()
            .map_err(|e| crate::types::Error::Internal(format!("read node metadata: {e}")))?;
        if let Some(lease) = md.active_lease.clone() {
            if !is_lease_epoch_current(lease.epoch, local_epoch) {
                return Err(crate::types::Error::Internal(format!(
                    "node has active lease {} with stale epoch {} < {} (stale coordinator)",
                    lease.lease_id, lease.epoch, local_epoch
                )));
            }
        }
    }

    // Standard eligibility.
    crate::eligibility::evaluate_node(rt, crate::types::now_ms(), 45_000)
        .map_err(|e| crate::types::Error::Internal(format!("node ineligible: {e}")))?;

    Ok(())
}
