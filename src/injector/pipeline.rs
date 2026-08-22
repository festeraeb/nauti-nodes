//! Concurrent Anticipatory Context Injector pipeline.

use super::intent::AgentIntent;
use super::package::{ContextPackage, DeliveryMode};
use super::telemetry::InjectorTelemetry;
use crate::index::{CompilerMemoryStore, HybridRetriever, SymbolKind};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info};

struct IntentMessage {
    intent: AgentIntent,
    delivery_mode: DeliveryMode,
    response_tx: Option<tokio::sync::oneshot::Sender<ContextPackage>>,
}

/// Concurrent anticipatory context injector service.
pub struct AnticipatoryInjector {
    retriever: Arc<HybridRetriever>,
    compiler_memory: Arc<CompilerMemoryStore>,
    telemetry: Arc<InjectorTelemetry>,
    intent_tx: mpsc::Sender<IntentMessage>,
    pending_packages: Arc<DashMap<DeliveryMode, Vec<ContextPackage>>>,
    cache_store: Arc<DashMap<String, ContextPackage>>,
}

impl AnticipatoryInjector {
    /// Spawns a new AnticipatoryInjector background service.
    pub fn spawn(
        retriever: Arc<HybridRetriever>,
        compiler_memory: Arc<CompilerMemoryStore>,
    ) -> Arc<Self> {
        let (intent_tx, mut intent_rx) = mpsc::channel::<IntentMessage>(128);
        let telemetry = InjectorTelemetry::new();
        let pending_packages = Arc::new(DashMap::new());
        let cache_store = Arc::new(DashMap::new());

        let injector = Arc::new(Self {
            retriever: retriever.clone(),
            compiler_memory: compiler_memory.clone(),
            telemetry: telemetry.clone(),
            intent_tx,
            pending_packages: pending_packages.clone(),
            cache_store: cache_store.clone(),
        });

        // Background worker loop for concurrent anticipatory retrieval
        let retriever_bg = retriever;
        let compiler_bg = compiler_memory;
        let pending_bg = pending_packages;
        let cache_bg = cache_store;

        tokio::spawn(async move {
            info!("Anticipatory injector background worker started");
            while let Some(msg) = intent_rx.recv().await {
                let start_time = Instant::now();
                let query = msg.intent.to_query_string();

                // 1. Query hybrid retriever
                let search_results = retriever_bg.query(&query, 10).await.unwrap_or_default();

                // 2. Extract compiler incidents
                let mut incidents = Vec::new();
                for r in &search_results {
                    incidents.extend(r.compiler_incidents.clone());
                }

                // If intent is specifically resolving a compiler error, query memory directly
                if let AgentIntent::ResolveCompilerError { ref error_code, .. } = msg.intent {
                    if let Some(code) = error_code {
                        if let Ok(found) = compiler_bg.find_by_error_code(code) {
                            for inc in found {
                                if !incidents.iter().any(|i| i.incident_id == inc.incident_id) {
                                    incidents.push(inc);
                                }
                            }
                        }
                    }
                }

                // 3. Find related tests
                let mut related_tests = Vec::new();
                for r in &search_results {
                    if r.kind == SymbolKind::Test {
                        related_tests.push(r.qualified_name.clone());
                    }
                }

                let elapsed_ms = start_time.elapsed().as_millis() as u64;
                let package = ContextPackage::new(
                    msg.intent.clone(),
                    msg.delivery_mode,
                    search_results,
                    incidents,
                    related_tests,
                    elapsed_ms,
                );

                debug!(
                    "Constructed context package {} for intent {:?} in {}ms",
                    package.package_id, msg.intent, elapsed_ms
                );

                if let Some(tx) = msg.response_tx {
                    let _ = tx.send(package.clone());
                }

                if msg.delivery_mode == DeliveryMode::Cache {
                    cache_bg.insert(query, package);
                } else {
                    let mut list = pending_bg.entry(msg.delivery_mode).or_default();
                    list.push(package);
                }
            }
        });

        injector
    }

    /// Emits a structured agent intent asynchronously without blocking the primary role.
    pub async fn emit_intent(
        &self,
        intent: AgentIntent,
        delivery_mode: DeliveryMode,
    ) -> anyhow::Result<()> {
        self.intent_tx
            .send(IntentMessage {
                intent,
                delivery_mode,
                response_tx: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to emit agent intent: {e}"))
    }

    /// Emits an intent and waits for the synchronously constructed package (e.g. for Inline mode).
    pub async fn emit_and_wait(
        &self,
        intent: AgentIntent,
        delivery_mode: DeliveryMode,
    ) -> anyhow::Result<ContextPackage> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let start = Instant::now();

        self.intent_tx
            .send(IntentMessage {
                intent,
                delivery_mode,
                response_tx: Some(tx),
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send intent: {e}"))?;

        let pkg = rx
            .await
            .map_err(|e| anyhow::anyhow!("Context package receiver dropped: {e}"))?;

        self.telemetry
            .record_retrieval_wait(start.elapsed().as_millis() as u64);
        Ok(pkg)
    }

    /// Consumes the next available package for the specified delivery mode.
    pub fn consume_next_package(&self, mode: DeliveryMode) -> Option<ContextPackage> {
        if let Some(mut list) = self.pending_packages.get_mut(&mode) {
            if !list.is_empty() {
                let mut pkg = list.remove(0);
                pkg.mark_consumed();
                self.telemetry.record_package_consumed();
                return Some(pkg);
            }
        }
        None
    }

    /// Fetches a cached context package by query key.
    pub fn fetch_cached_package(&self, query_key: &str) -> Option<ContextPackage> {
        if let Some(entry) = self.cache_store.get(query_key) {
            let mut pkg = entry.value().clone();
            pkg.mark_consumed();
            self.telemetry.record_package_consumed();
            Some(pkg)
        } else {
            None
        }
    }

    /// Accesses the underlying hybrid retriever.
    pub fn retriever(&self) -> Arc<HybridRetriever> {
        self.retriever.clone()
    }

    /// Accesses the compiler incident memory store.
    pub fn compiler_memory(&self) -> Arc<CompilerMemoryStore> {
        self.compiler_memory.clone()
    }

    /// Accesses the real performance telemetry tracker.
    pub fn telemetry(&self) -> Arc<InjectorTelemetry> {
        self.telemetry.clone()
    }
}
