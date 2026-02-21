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
