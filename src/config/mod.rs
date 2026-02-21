mod app;
mod schema;

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
pub use app::*;
pub use schema::*;

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub app: AppConfig,
    pub schema: SchemaConfig,
    pub bundle_path: PathBuf,
}

impl LoadedConfig {
    pub fn load(bundle_path: impl AsRef<Path>) -> Result<Self> {
        let bundle_path = bundle_path.as_ref().to_path_buf();
        let app_path = bundle_path.join("app.yaml");
        let schema_path = bundle_path.join("schema.yaml");

        let app_yaml = fs::read_to_string(&app_path).with_context(|| {
            format!(
                "failed to read app config at {}",
                app_path.to_string_lossy()
            )
        })?;
        let schema_yaml = fs::read_to_string(&schema_path).with_context(|| {
            format!(
                "failed to read schema config at {}",
                schema_path.to_string_lossy()
            )
        })?;

        let app: AppConfig =
            serde_yaml::from_str(&app_yaml).context("failed to parse app.yaml as YAML")?;
        let schema: SchemaConfig =
            serde_yaml::from_str(&schema_yaml).context("failed to parse schema.yaml as YAML")?;

        let loaded = Self {
            app,
            schema,
            bundle_path,
        };

        loaded.validate()?;
        Ok(loaded)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_app()?;
        self.validate_schema()?;
        self.validate_cross_references()?;
        Ok(())
    }

    fn validate_app(&self) -> Result<()> {
        if self.app.app.name.trim().is_empty() {
            bail!("app.name must not be empty");
        }
        if self.app.app.version.trim().is_empty() {
            bail!("app.version must not be empty");
        }
        if self.app.limits.max_concurrent_requests == 0 {
            bail!("limits.max_concurrent_requests must be > 0");
        }
        if self.app.limits.js.pool_size == 0 {
            bail!("limits.js.pool_size must be > 0");
        }
        if self.app.limits.js.max_cpu_ms_per_request == 0 {
            bail!("limits.js.max_cpu_ms_per_request must be > 0");
        }
        if self.app.limits.store.max_bytes == 0 {
            bail!("limits.store.max_bytes must be > 0");
        }
        if self.app.limits.max_request_bytes == 0 {
            bail!("limits.max_request_bytes must be > 0");
        }
        if self.app.limits.max_response_bytes == 0 {
            bail!("limits.max_response_bytes must be > 0");
        }
        if self.app.limits.store.max_entity_bytes > 0
            && self.app.limits.store.max_entity_bytes > self.app.limits.store.max_bytes
        {
            bail!("limits.store.max_entity_bytes cannot exceed limits.store.max_bytes");
        }
        if self.app.limits.store.soft_limit_percent > 100 {
            bail!("limits.store.soft_limit_percent must be within 0..=100");
        }

        let mut endpoint_names = HashSet::new();
        let mut endpoint_keys = HashSet::new();
        for endpoint in &self.app.endpoints {
            if endpoint.name.trim().is_empty() {
                bail!("endpoint name cannot be empty");
            }
            if !endpoint_names.insert(endpoint.name.clone()) {
                bail!("duplicate endpoint name '{}'", endpoint.name);
            }

            let method = endpoint.method.trim().to_uppercase();
            if method.is_empty() {
                bail!("endpoint '{}' has an empty method", endpoint.name);
            }
            if endpoint.path.trim().is_empty() {
                bail!("endpoint '{}' has an empty path", endpoint.name);
            }
            let key = format!("{} {}", method, endpoint.path);
            if !endpoint_keys.insert(key.clone()) {
                bail!("duplicate endpoint definition '{}'", key);
            }

            let handler_path = self.bundle_path.join(&endpoint.handler);
            if !handler_path.is_file() {
                bail!(
                    "endpoint '{}' handler is missing: {}",
                    endpoint.name,
                    handler_path.to_string_lossy()
                );
            }
        }

        let mut query_names = HashSet::new();
        for query in &self.app.queries {
            if query.name.trim().is_empty() {
                bail!("query name cannot be empty");
            }
            if !query_names.insert(query.name.clone()) {
                bail!("duplicate query name '{}'", query.name);
            }
            if query.handler.trim().is_empty() {
                bail!("query '{}' has an empty handler", query.name);
            }
            let handler_path = self.bundle_path.join(&query.handler);
            if !handler_path.is_file() {
                bail!(
                    "query '{}' handler is missing: {}",
                    query.name,
                    handler_path.to_string_lossy()
                );
            }
        }

        let mut cron_names = HashSet::new();
        for cron in &self.app.crons {
            if cron.name.trim().is_empty() {
                bail!("cron name cannot be empty");
            }
            if !cron_names.insert(cron.name.clone()) {
                bail!("duplicate cron name '{}'", cron.name);
            }
            if cron.interval_seconds == 0 {
                bail!("cron '{}' interval_seconds must be > 0", cron.name);
            }
            if cron.handler.trim().is_empty() {
                bail!("cron '{}' has an empty handler", cron.name);
            }
            let handler_path = self.bundle_path.join(&cron.handler);
            if !handler_path.is_file() {
                bail!(
                    "cron '{}' handler is missing: {}",
                    cron.name,
                    handler_path.to_string_lossy()
                );
            }
        }

        if let Some(lifecycle) = &self.app.lifecycle {
            if let Some(on_init) = &lifecycle.on_init {
                if on_init.trim().is_empty() {
                    bail!("lifecycle.on_init must not be empty when provided");
                }
                let init_path = self.bundle_path.join(on_init);
                if !init_path.is_file() {
                    bail!(
                        "lifecycle on_init script is missing: {}",
                        init_path.to_string_lossy()
                    );
                }
            }
            if let Some(on_shutdown) = &lifecycle.on_shutdown {
                if on_shutdown.trim().is_empty() {
                    bail!("lifecycle.on_shutdown must not be empty when provided");
                }
                let shutdown_path = self.bundle_path.join(on_shutdown);
                if !shutdown_path.is_file() {
                    bail!(
                        "lifecycle on_shutdown script is missing: {}",
                        shutdown_path.to_string_lossy()
                    );
                }
            }
        }

        for source in &self.app.ingestion {
            if source.source_type != "kafka" && source.source_type != "http" {
                bail!(
                    "unsupported ingestion type '{}'; expected 'kafka' or 'http'",
                    source.source_type
                );
            }
            if source.source_type == "kafka" && source.topics.is_empty() {
                bail!("kafka ingestion source must define at least one topic");
            }
        }

        Ok(())
    }

    fn validate_schema(&self) -> Result<()> {
        if self.schema.entities.is_empty() {
            bail!("schema must define at least one entity");
        }

        let mut entity_names = HashSet::new();
        for entity in &self.schema.entities {
            if entity.name.trim().is_empty() {
                bail!("entity name cannot be empty");
            }
            if !entity_names.insert(entity.name.clone()) {
                bail!("duplicate entity '{}'", entity.name);
            }
            if entity.primary_key.trim().is_empty() {
                bail!("entity '{}' primary_key cannot be empty", entity.name);
            }
            let mut field_names = HashSet::new();
            for field in &entity.fields {
                if field.name.trim().is_empty() {
                    bail!("entity '{}' contains a field with empty name", entity.name);
                }
                if !field_names.insert(field.name.clone()) {
                    bail!(
                        "entity '{}' has duplicate field '{}'",
                        entity.name,
                        field.name
                    );
                }
            }
            field_names.insert(entity.primary_key.clone());

            if entity.entity_type == EntityType::Collection {
                let Some(partition_key) = entity.partition_key.as_ref() else {
                    bail!(
                        "collection entity '{}' must define partition_key",
                        entity.name
                    );
                };
                if partition_key.trim().is_empty() {
                    bail!(
                        "collection entity '{}' partition_key cannot be empty",
                        entity.name
                    );
                }
                if !field_names.contains(partition_key) {
                    bail!(
                        "collection entity '{}' partition_key '{}' references unknown field",
                        entity.name,
                        partition_key
                    );
                }
                if let Some(order_by) = &entity.order_by {
                    if order_by.trim().is_empty() {
                        bail!(
                            "collection entity '{}' order_by cannot be empty",
                            entity.name
                        );
                    }
                    if !field_names.contains(order_by) {
                        bail!(
                            "collection entity '{}' order_by '{}' references unknown field",
                            entity.name,
                            order_by
                        );
                    }
                }
                if let Some(max_per_partition) = entity.max_per_partition {
                    if max_per_partition == 0 {
                        bail!(
                            "collection entity '{}' max_per_partition must be > 0",
                            entity.name
                        );
                    }
                }
            }

            let mut index_names = HashSet::new();
            for index in &entity.indexes {
                if index.name.trim().is_empty() {
                    bail!("entity '{}' has index with empty name", entity.name);
                }
                if !index_names.insert(index.name.clone()) {
                    bail!(
                        "entity '{}' has duplicate index '{}'",
                        entity.name,
                        index.name
                    );
                }
                if !field_names.contains(&index.field) {
                    bail!(
                        "entity '{}' index '{}' references unknown field '{}'",
                        entity.name,
                        index.name,
                        index.field
                    );
                }
            }
        }

        Ok(())
    }

    fn validate_cross_references(&self) -> Result<()> {
        let entities: HashSet<&str> = self
            .schema
            .entities
            .iter()
            .map(|entity| entity.name.as_str())
            .collect();

        for source in &self.app.ingestion {
            for topic in &source.topics {
                if !entities.contains(topic.entity.as_str()) {
                    bail!(
                        "topic '{}' references unknown entity '{}'",
                        topic.name,
                        topic.entity
                    );
                }
            }
        }

        Ok(())
    }
}

pub fn load_bundle(bundle_path: impl AsRef<Path>) -> Result<LoadedConfig> {
    LoadedConfig::load(bundle_path)
}
