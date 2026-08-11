use crate::config::Config;
use crate::jobs::JobStore;
use crate::node::registry::NodeRegistry;
use crate::scheduler::Scheduler;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub registry: Arc<NodeRegistry>,
    pub scheduler: Arc<Scheduler>,
    pub jobs: Arc<JobStore>,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(config: Config) -> crate::types::Result<Self> {
        let registry = Arc::new(NodeRegistry::new());
        let scheduler = Arc::new(Scheduler::new(registry.clone(), &config.db_url)?);
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
        })
    }
}
