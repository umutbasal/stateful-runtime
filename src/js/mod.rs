mod ops;
mod runtime;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::LoadedConfig;
use crate::store::{CollectionStore, Store, StoreOp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsRouteRequest {
    pub method: String,
    pub path: String,
    pub params: HashMap<String, String>,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Value,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsRouteResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct JsRuntimePool {
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    route_scripts: HashMap<String, String>,
    ingest_script: Option<(String, String)>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    cron_scripts: HashMap<String, String>,
    lifecycle_scripts: HashMap<String, (String, String)>,
    slots: Vec<Arc<Mutex<()>>>,
    next_slot: AtomicUsize,
    max_cpu_budget: Duration,
    memory_limit_bytes: usize,
}

impl JsRuntimePool {
    pub fn new(
        config: &LoadedConfig,
        store: Arc<Store>,
        collection_store: Arc<CollectionStore>,
    ) -> Result<Self> {
        let mut route_scripts = HashMap::new();
        for endpoint in &config.app.endpoints {
            let script_path = config.bundle_path.join(&endpoint.handler);
            let script = std::fs::read_to_string(&script_path).with_context(|| {
                format!(
                    "failed to load endpoint handler '{}'",
                    script_path.to_string_lossy()
                )
            })?;
            route_scripts.insert(endpoint.handler.clone(), script);
        }

        let ingest_script = Self::load_optional_ingest_script(&config.bundle_path)?;
        let query_scripts = Arc::new(Self::load_query_scripts(config)?);
        let cron_scripts = Self::load_cron_scripts(config)?;
        let lifecycle_scripts = Self::load_lifecycle_scripts(config)?;
        let pool_size = config.app.limits.js.pool_size.max(1);
        let slots = (0..pool_size).map(|_| Arc::new(Mutex::new(()))).collect();

        Ok(Self {
            store,
            collection_store,
            route_scripts,
            ingest_script,
            query_scripts,
            cron_scripts,
            lifecycle_scripts,
            slots,
            next_slot: AtomicUsize::new(0),
            max_cpu_budget: Duration::from_millis(config.app.limits.js.max_cpu_ms_per_request),
            memory_limit_bytes: config.app.limits.js.memory_limit_bytes,
        })
    }

    pub async fn execute_route(
        &self,
        handler_path: &str,
        request: JsRouteRequest,
    ) -> Result<JsRouteResponse> {
        let source = self
            .route_scripts
            .get(handler_path)
            .cloned()
            .ok_or_else(|| anyhow!("missing route handler '{handler_path}'"))?;

        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let _guard = self.slots[slot].lock().await;
        let store = self.store.clone();
        let collection_store = self.collection_store.clone();
        let query_scripts = self.query_scripts.clone();
        let handler_name = handler_path.to_string();
        let timeout = self.max_cpu_budget;
        let memory_limit_bytes = self.memory_limit_bytes;

        tokio::time::timeout(timeout, async move {
            runtime::execute_route_handler(
                store,
                collection_store,
                query_scripts,
                &handler_name,
                &source,
                &request,
                memory_limit_bytes,
            )
        })
        .await
        .map_err(|_| anyhow!("route execution exceeded CPU time budget"))?
    }

    pub async fn execute_ingest(
        &self,
        event_type: &str,
        payload: Value,
        context: IngestContext,
    ) -> Result<Vec<StoreOp>> {
        let Some((script_name, source)) = &self.ingest_script else {
            return Ok(Vec::new());
        };

        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let _guard = self.slots[slot].lock().await;
        let store = self.store.clone();
        let collection_store = self.collection_store.clone();
        let query_scripts = self.query_scripts.clone();
        let script_name = script_name.clone();
        let source = source.clone();
        let event_type = event_type.to_string();
        let timeout = self.max_cpu_budget;
        let memory_limit_bytes = self.memory_limit_bytes;

        tokio::time::timeout(timeout, async move {
            runtime::execute_ingest_handler(
                store,
                collection_store,
                query_scripts,
                &script_name,
                &source,
                &event_type,
                &payload,
                &context,
                memory_limit_bytes,
            )
        })
        .await
        .map_err(|_| anyhow!("ingest execution exceeded CPU time budget"))?
    }

    pub async fn execute_cron(&self, handler_path: &str, cron_name: &str) -> Result<Vec<StoreOp>> {
        let source = self
            .cron_scripts
            .get(handler_path)
            .cloned()
            .ok_or_else(|| anyhow!("missing cron handler '{handler_path}'"))?;

        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let _guard = self.slots[slot].lock().await;
        let store = self.store.clone();
        let collection_store = self.collection_store.clone();
        let query_scripts = self.query_scripts.clone();
        let cron_handler_name = handler_path.to_string();
        let cron_name = cron_name.to_string();
        let timeout = self.max_cpu_budget;
        let memory_limit_bytes = self.memory_limit_bytes;

        tokio::time::timeout(timeout, async move {
            runtime::execute_cron_handler(
                store,
                collection_store,
                query_scripts,
                &cron_handler_name,
                &source,
                &cron_name,
                memory_limit_bytes,
            )
        })
        .await
        .map_err(|_| anyhow!("cron execution exceeded CPU time budget"))?
    }

    pub async fn execute_query(&self, query_name: &str, params: Value) -> Result<Value> {
        let (script_name, source) = self
            .query_scripts
            .get(query_name)
            .cloned()
            .ok_or_else(|| anyhow!("missing query handler '{query_name}'"))?;

        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let _guard = self.slots[slot].lock().await;
        let store = self.store.clone();
        let collection_store = self.collection_store.clone();
        let query_scripts = self.query_scripts.clone();
        let timeout = self.max_cpu_budget;
        let memory_limit_bytes = self.memory_limit_bytes;

        tokio::time::timeout(timeout, async move {
            runtime::execute_query_handler(
                store,
                collection_store,
                query_scripts,
                &script_name,
                &source,
                &params,
                memory_limit_bytes,
            )
        })
        .await
        .map_err(|_| anyhow!("query execution exceeded CPU time budget"))?
    }

    pub async fn execute_lifecycle(&self, hook_name: &str) -> Result<Vec<StoreOp>> {
        let Some((script_name, source)) = self.lifecycle_scripts.get(hook_name).cloned() else {
            return Ok(Vec::new());
        };

        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let _guard = self.slots[slot].lock().await;
        let store = self.store.clone();
        let collection_store = self.collection_store.clone();
        let query_scripts = self.query_scripts.clone();
        let hook_name = hook_name.to_string();
        let timeout = self.max_cpu_budget;
        let memory_limit_bytes = self.memory_limit_bytes;

        tokio::time::timeout(timeout, async move {
            runtime::execute_lifecycle_handler(
                store,
                collection_store,
                query_scripts,
                &script_name,
                &source,
                &hook_name,
                memory_limit_bytes,
            )
        })
        .await
        .map_err(|_| anyhow!("lifecycle execution exceeded CPU time budget"))?
    }

    fn load_optional_ingest_script(bundle_path: &Path) -> Result<Option<(String, String)>> {
        let path = bundle_path.join("scripts/on_ingest.js");
        if !path.exists() {
            return Ok(None);
        }
        let source = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.to_string_lossy()))?;
        Ok(Some(("scripts/on_ingest.js".to_string(), source)))
    }

    fn load_query_scripts(config: &LoadedConfig) -> Result<HashMap<String, (String, String)>> {
        let mut scripts = HashMap::new();
        for query in &config.app.queries {
            let path = config.bundle_path.join(&query.handler);
            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.to_string_lossy()))?;
            scripts.insert(query.name.clone(), (query.handler.clone(), source));
        }
        Ok(scripts)
    }

    fn load_cron_scripts(config: &LoadedConfig) -> Result<HashMap<String, String>> {
        let mut scripts = HashMap::new();
        for cron in &config.app.crons {
            let path = config.bundle_path.join(&cron.handler);
            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.to_string_lossy()))?;
            scripts.insert(cron.handler.clone(), source);
        }
        Ok(scripts)
    }

    fn load_lifecycle_scripts(config: &LoadedConfig) -> Result<HashMap<String, (String, String)>> {
        let mut scripts = HashMap::new();
        let Some(lifecycle) = &config.app.lifecycle else {
            return Ok(scripts);
        };

        if let Some(on_init_path) = &lifecycle.on_init {
            let path = config.bundle_path.join(on_init_path);
            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.to_string_lossy()))?;
            scripts.insert("on_init".to_string(), (on_init_path.clone(), source));
        }
        if let Some(on_shutdown_path) = &lifecycle.on_shutdown {
            let path = config.bundle_path.join(on_shutdown_path);
            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.to_string_lossy()))?;
            scripts.insert(
                "on_shutdown".to_string(),
                (on_shutdown_path.clone(), source),
            );
        }

        Ok(scripts)
    }
}
