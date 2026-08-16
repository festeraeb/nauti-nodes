use crate::config::Config;
use crate::jobs::JobStore;
use crate::lineage::LineageStore;
use crate::metrics::FederationMetrics;
use crate::node::auth::NonceTracker;
use crate::node::identity::NodeIdentity;
use crate::node::registry::NodeRegistry;
use crate::scheduler::Scheduler;
use crate::node::revocation::RevocationStore;
use crate::runtime::ModelManager;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub registry: Arc<NodeRegistry>,
    pub scheduler: Arc<Scheduler>,
    pub jobs: Arc<JobStore>,
    pub http: reqwest::Client,
    pub identity: Option<Arc<NodeIdentity>>,
    pub lineage: Option<Arc<LineageStore>>,
    pub metrics: FederationMetrics,
    pub nonce_tracker: Arc<NonceTracker>,
    pub revocations: Arc<RevocationStore>,
    pub model_manager: Arc<ModelManager>,
}

impl AppState {
    pub fn new(config: Config) -> crate::types::Result<Self> {
        let registry = Arc::new(NodeRegistry::new());
        let scheduler = Arc::new(Scheduler::new(registry.clone(), &config.db_url)?);
        let revocations = Arc::new(RevocationStore::open(&config.db_url)?);
        let model_manager = Arc::new(ModelManager::new());
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .map_err(|e| crate::types::Error::Internal(e.to_string()))?;
        Ok(Self {
            config,
            registry,
            scheduler,
            jobs: Arc::new(JobStore::new()),
            http,
            identity: None,
            lineage: None,
            metrics: FederationMetrics::new(),
            nonce_tracker: Arc::new(NonceTracker::new()),
            revocations,
            model_manager,
        })
    }
}
