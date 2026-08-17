use crate::config::Config;
use crate::eligibility::RuntimeState;
use crate::jobs::JobStore;
use crate::lease::LeaseManager;
use crate::lineage::LineageStore;
use crate::metrics::FederationMetrics;
use crate::node::auth::NonceTracker;
use crate::node::identity::NodeIdentity;
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
use tracing::{info, warn};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub registry: Arc<NodeRegistry>,
    pub scheduler: Arc<Scheduler>,
    pub jobs: Arc<JobStore>,
    pub http: reqwest::Client,
    pub identity: Option<Arc<NodeIdentity>>,
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

        Ok(Self {
            config,
            registry,
            scheduler,
            jobs: Arc::new(JobStore::new()),
            http,
            identity: None,
            lineage: None,
            metrics: Arc::new(FederationMetrics::new()),
            nonce_tracker: Arc::new(NonceTracker::new()),
            revocations,
            model_manager,
            preemption,
            external_alpha_enabled,
            ops,
            leases,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Coordinator restart recovery
// ─────────────────────────────────────────────────────────────────────────────

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
///   OFFLINE with runtime Unknown and stale reconciliation. Fresh signed
///   heartbeats / re-registration re-establish truth.
/// - Do NOT assume old runtime processes are alive: PIDs are never persisted
///   as truth; runtime_state starts Unknown.
/// - Do NOT assume hardware is unchanged: MATCH/PARTIAL_MATCH reconciliations
///   degrade to STALE until freshly re-observed.
/// - Active Forge lease ownership IS preserved (granted/leased/releasing/
///   recovering); pre-grant leases are marked STALE.
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
    info!(
        recovered_nodes,
        active_leases = kept.len(),
        refunded_reservations = refunded,
        "coordinator operational state recovered (fail-closed until fresh truth)"
    );
    if !kept.is_empty() {
        for l in &kept {
            warn!(
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
