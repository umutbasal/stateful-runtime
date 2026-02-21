mod collection;
mod entity;
mod index;
pub mod retention;

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
pub use collection::{CollectionStore, CollectionStoreError};
use dashmap::{DashMap, DashSet};
pub use entity::{EntityRecord, StoreOp, StoreOpKind, Tombstone};
pub use index::IndexMeta;
use index::{index_value_to_key, IndexState};
use parking_lot::RwLock;
use serde_json::Value;
use thiserror::Error;

use crate::config::{EntitySchema, EntityType, IndexKind, SchemaConfig};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("entity type '{0}' is not defined in schema")]
    UnknownEntity(String),
    #[error("entity payload exceeds max_entity_bytes")]
    EntityTooLarge,
    #[error("store hard memory limit exceeded")]
    HardMemoryLimitExceeded,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RetentionSweepResult {
    pub evicted_entities: usize,
    pub cleaned_tombstones: usize,
    pub trimmed_collection_items: usize,
}

pub struct Store {
    entities: DashMap<String, DashMap<String, EntityRecord>>,
    tombstones: DashMap<String, Tombstone>,
    index_state: IndexState,
    entity_defs: HashMap<String, EntitySchema>,
    index_defs: HashMap<String, IndexMeta>,
    memory_bytes: AtomicUsize,
    max_bytes: usize,
    max_entity_bytes: usize,
    collection_store: Arc<CollectionStore>,
}

impl Store {
    pub fn new(schema: &SchemaConfig, max_bytes: usize, max_entity_bytes: usize) -> Self {
        let entities = DashMap::new();
        let index_state = IndexState::default();
        let mut entity_defs = HashMap::new();
        let mut index_defs = HashMap::new();
        let collection_store = Arc::new(CollectionStore::new(schema));

        for entity in &schema.entities {
            if entity.entity_type == EntityType::Collection {
                continue;
            }

            entities.insert(entity.name.clone(), DashMap::new());
            index_state.ensure_entity_indexes(entity);
            entity_defs.insert(entity.name.clone(), entity.clone());
            for index in &entity.indexes {
                let internal_name = Self::internal_index_name(&entity.name, &index.name);
                index_defs.insert(
                    internal_name,
                    IndexMeta {
                        entity_type: entity.name.clone(),
                        index_name: index.name.clone(),
                        field_name: index.field.clone(),
                        kind: index.kind.clone(),
                    },
                );
            }
        }

        Self {
            entities,
            tombstones: DashMap::new(),
            index_state,
            entity_defs,
            index_defs,
            memory_bytes: AtomicUsize::new(0),
            max_bytes,
            max_entity_bytes,
            collection_store,
        }
    }

    pub fn apply_ops(&self, ops: &[StoreOp]) -> Result<()> {
        for op in ops {
            match op.op {
                StoreOpKind::Upsert => {
                    if let Some(value) = &op.value {
                        self.upsert(&op.entity_type, &op.key, value.clone())?;
                    }
                }
                StoreOpKind::Delete => {
                    self.delete(&op.entity_type, &op.key)?;
                }
                StoreOpKind::Push => {
                    let value = op
                        .value
                        .as_ref()
                        .ok_or_else(|| anyhow!("push op requires a value payload"))?
                        .clone();
                    let item_id = Self::extract_collection_item_id(op, &value)
                        .ok_or_else(|| anyhow!("push op requires item_id or value.id"))?;
                    self.collection_store
                        .push(&op.entity_type, &op.key, &item_id, value)?;
                }
                StoreOpKind::RemoveItem => {
                    let item_id = op
                        .item_id
                        .as_deref()
                        .or_else(|| {
                            op.value.as_ref().and_then(|value| {
                                value.get("id").and_then(serde_json::Value::as_str)
                            })
                        })
                        .ok_or_else(|| anyhow!("remove_item op requires item_id"))?;
                    let _ = self
                        .collection_store
                        .remove(&op.entity_type, &op.key, item_id)?;
                }
            }
        }
        Ok(())
    }

    pub fn collection_store(&self) -> Arc<CollectionStore> {
        Arc::clone(&self.collection_store)
    }

    pub fn snapshot_collection_sizes(&self) -> HashMap<String, usize> {
        self.collection_store.snapshot_collection_sizes()
    }

    pub fn upsert(&self, entity_type: &str, key: &str, value: Value) -> Result<(), StoreError> {
        let schema = self
            .entity_defs
            .get(entity_type)
            .ok_or_else(|| StoreError::UnknownEntity(entity_type.to_string()))?;

        let approx_size = Self::estimate_entity_size(entity_type, key, &value);
        if self.max_entity_bytes > 0 && approx_size > self.max_entity_bytes {
            return Err(StoreError::EntityTooLarge);
        }

        let entity_map = self
            .entities
            .get(entity_type)
            .ok_or_else(|| StoreError::UnknownEntity(entity_type.to_string()))?;

        let previous = entity_map.get(key).map(|record| record.clone());
        let previous_size = previous
            .as_ref()
            .map(|record| Self::estimate_entity_size(entity_type, key, &record.value))
            .unwrap_or(0);

        let projected = self
            .memory_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(previous_size)
            .saturating_add(approx_size);
        if self.max_bytes > 0 && projected > self.max_bytes {
            return Err(StoreError::HardMemoryLimitExceeded);
        }

        if let Some(old_record) = previous {
            self.remove_from_indexes(&old_record.entity_type, &old_record.key, &old_record.value);
        }

        let now_ms = now_millis();
        let expires_at_ms = schema
            .retention
            .as_ref()
            .and_then(|retention| retention.ttl_seconds)
            .map(|ttl_seconds| now_ms.saturating_add(ttl_seconds.saturating_mul(1000)));

        let record = EntityRecord {
            entity_type: entity_type.to_string(),
            key: key.to_string(),
            value: value.clone(),
            updated_at_ms: now_ms,
            expires_at_ms,
        };

        entity_map.insert(key.to_string(), record);
        self.add_to_indexes(entity_type, key, &value);
        self.tombstones
            .remove(&Self::tombstone_key(entity_type, key));
        self.memory_bytes.store(projected, Ordering::Relaxed);
        Ok(())
    }

    pub fn delete(&self, entity_type: &str, key: &str) -> Result<(), StoreError> {
        let entity_map = self
            .entities
            .get(entity_type)
            .ok_or_else(|| StoreError::UnknownEntity(entity_type.to_string()))?;

        if let Some((_, old_record)) = entity_map.remove(key) {
            self.remove_from_indexes(&old_record.entity_type, &old_record.key, &old_record.value);
            let old_size = Self::estimate_entity_size(entity_type, key, &old_record.value);
            self.memory_bytes.fetch_sub(old_size, Ordering::Relaxed);
            self.tombstones.insert(
                Self::tombstone_key(entity_type, key),
                Tombstone {
                    entity_type: entity_type.to_string(),
                    key: key.to_string(),
                    deleted_at_ms: now_millis(),
                },
            );
        }

        Ok(())
    }

    pub fn get(&self, entity_type: &str, key: &str) -> Option<EntityRecord> {
        self.entities
            .get(entity_type)
            .and_then(|map| map.get(key).map(|entry| entry.clone()))
    }

    pub fn batch_get(&self, entity_type: &str, keys: &[String]) -> Vec<Option<EntityRecord>> {
        keys.iter().map(|key| self.get(entity_type, key)).collect()
    }

    pub fn index_lookup_keys(&self, index_name: &str, value: &str) -> Vec<String> {
        let resolved = match self.resolve_index_name(index_name) {
            Some(index) => index,
            None => return Vec::new(),
        };

        let index = match self.index_state.hash_indexes.get(&resolved) {
            Some(index) => index,
            None => return Vec::new(),
        };

        index
            .get(value)
            .map(|set| set.iter().map(|key| key.clone()).collect())
            .unwrap_or_default()
    }

    pub fn index_lookup_entities(
        &self,
        index_name: &str,
        value: &str,
        limit: usize,
    ) -> Vec<EntityRecord> {
        let resolved = match self.resolve_index_name(index_name) {
            Some(index) => index,
            None => return Vec::new(),
        };
        let Some(meta) = self.index_defs.get(&resolved) else {
            return Vec::new();
        };

        let keys = self.index_lookup_keys(index_name, value);
        keys.into_iter()
            .take(limit.max(1))
            .filter_map(|key| self.get(&meta.entity_type, &key))
            .collect()
    }

    pub fn range_lookup_keys(
        &self,
        index_name: &str,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        let resolved = match self.resolve_index_name(index_name) {
            Some(index) => index,
            None => return Vec::new(),
        };

        let tree_lock = match self.index_state.sorted_indexes.get(&resolved) {
            Some(lock) => lock.clone(),
            None => return Vec::new(),
        };
        let tree = tree_lock.read();

        let mut out = Vec::new();
        for (candidate, keys) in tree.iter() {
            if let Some(start) = start {
                if candidate.as_str() < start {
                    continue;
                }
            }
            if let Some(end) = end {
                if candidate.as_str() > end {
                    continue;
                }
            }
            for key in keys {
                out.push(key.clone());
                if limit > 0 && out.len() >= limit {
                    return out;
                }
            }
        }
        out
    }

    pub fn range_lookup_entities(
        &self,
        index_name: &str,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Vec<EntityRecord> {
        let resolved = match self.resolve_index_name(index_name) {
            Some(index) => index,
            None => return Vec::new(),
        };
        let Some(meta) = self.index_defs.get(&resolved) else {
            return Vec::new();
        };

        self.range_lookup_keys(index_name, start, end, limit)
            .into_iter()
            .filter_map(|key| self.get(&meta.entity_type, &key))
            .collect()
    }

    pub fn current_memory_bytes(&self) -> usize {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    pub fn max_memory_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn sweep_retention(&self, now_ms: u64, tombstone_ttl_seconds: u64) -> RetentionSweepResult {
        let mut expired = Vec::new();
        for entity_entry in self.entities.iter() {
            let entity_type = entity_entry.key().clone();
            for record in entity_entry.value().iter() {
                let should_evict = record
                    .value()
                    .expires_at_ms
                    .map(|expires_at| expires_at <= now_ms)
                    .unwrap_or(false);
                if should_evict {
                    expired.push((entity_type.clone(), record.key().clone()));
                }
            }
        }

        for (entity_type, key) in &expired {
            let _ = self.delete(entity_type, key);
        }

        let mut tombstones_to_remove = Vec::new();
        let tombstone_ttl_ms = tombstone_ttl_seconds.saturating_mul(1000);
        for tombstone in self.tombstones.iter() {
            if tombstone_ttl_seconds == 0 {
                tombstones_to_remove.push(tombstone.key().clone());
                continue;
            }
            if now_ms.saturating_sub(tombstone.deleted_at_ms) > tombstone_ttl_ms {
                tombstones_to_remove.push(tombstone.key().clone());
            }
        }

        for key in &tombstones_to_remove {
            self.tombstones.remove(key);
        }

        RetentionSweepResult {
            evicted_entities: expired.len(),
            cleaned_tombstones: tombstones_to_remove.len(),
            trimmed_collection_items: 0,
        }
    }

    pub fn snapshot_entity_counts(&self) -> HashMap<String, usize> {
        self.entities
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().len()))
            .collect()
    }

    pub fn snapshot_index_sizes(&self) -> HashMap<String, usize> {
        let mut out = HashMap::new();
        for hash_index in self.index_state.hash_indexes.iter() {
            let count = hash_index
                .value()
                .iter()
                .map(|value_set| value_set.value().len())
                .sum();
            out.insert(hash_index.key().clone(), count);
        }
        for sorted_index in self.index_state.sorted_indexes.iter() {
            let tree = sorted_index.value().read();
            let count = tree.values().map(BTreeSet::len).sum();
            out.insert(sorted_index.key().clone(), count);
        }
        out
    }

    pub fn list_tombstones(&self) -> Vec<Tombstone> {
        self.tombstones
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn extract_collection_item_id(op: &StoreOp, value: &Value) -> Option<String> {
        if let Some(item_id) = &op.item_id {
            if !item_id.is_empty() {
                return Some(item_id.clone());
            }
        }
        if let Some(item_id) = value.get("id").and_then(Value::as_str) {
            if !item_id.is_empty() {
                return Some(item_id.to_string());
            }
        }
        if let Some(item_id) = value.get("post_id").and_then(Value::as_i64) {
            return Some(item_id.to_string());
        }
        None
    }

    fn add_to_indexes(&self, entity_type: &str, key: &str, value: &Value) {
        let Some(schema) = self.entity_defs.get(entity_type) else {
            return;
        };

        for index in &schema.indexes {
            let internal_name = Self::internal_index_name(entity_type, &index.name);
            let field_key = index_value_to_key(value.get(&index.field));
            let Some(field_key) = field_key else {
                continue;
            };

            match index.kind {
                IndexKind::Hash | IndexKind::Membership => {
                    let values = self
                        .index_state
                        .hash_indexes
                        .entry(internal_name)
                        .or_insert_with(DashMap::new);
                    values
                        .entry(field_key)
                        .or_insert_with(DashSet::new)
                        .insert(key.to_string());
                }
                IndexKind::Sorted => {
                    let tree = self
                        .index_state
                        .sorted_indexes
                        .entry(internal_name)
                        .or_insert_with(|| Arc::new(RwLock::new(Default::default())));
                    tree.write()
                        .entry(field_key)
                        .or_insert_with(Default::default)
                        .insert(key.to_string());
                }
            }
        }
    }

    fn remove_from_indexes(&self, entity_type: &str, key: &str, value: &Value) {
        let Some(schema) = self.entity_defs.get(entity_type) else {
            return;
        };

        for index in &schema.indexes {
            let internal_name = Self::internal_index_name(entity_type, &index.name);
            let field_key = index_value_to_key(value.get(&index.field));
            let Some(field_key) = field_key else {
                continue;
            };

            match index.kind {
                IndexKind::Hash | IndexKind::Membership => {
                    if let Some(values) = self.index_state.hash_indexes.get(&internal_name) {
                        if let Some(set) = values.get(&field_key) {
                            set.remove(key);
                            if set.is_empty() {
                                values.remove(&field_key);
                            }
                        }
                    }
                }
                IndexKind::Sorted => {
                    if let Some(tree) = self.index_state.sorted_indexes.get(&internal_name) {
                        let mut tree = tree.write();
                        if let Some(keys) = tree.get_mut(&field_key) {
                            keys.remove(key);
                            if keys.is_empty() {
                                tree.remove(&field_key);
                            }
                        }
                    }
                }
            }
        }
    }

    fn resolve_index_name(&self, index_name: &str) -> Option<String> {
        if self.index_defs.contains_key(index_name) {
            return Some(index_name.to_string());
        }

        let mut matches = self
            .index_defs
            .keys()
            .filter(|name| name.ends_with(&format!(".{index_name}")))
            .cloned()
            .collect::<Vec<_>>();

        if matches.len() == 1 {
            matches.pop()
        } else {
            None
        }
    }

    fn estimate_entity_size(entity_type: &str, key: &str, value: &Value) -> usize {
        entity_type.len() + key.len() + serde_json::to_vec(value).map_or(0, |raw| raw.len()) + 64
    }

    fn internal_index_name(entity_type: &str, index_name: &str) -> String {
        format!("{entity_type}.{index_name}")
    }

    fn tombstone_key(entity_type: &str, key: &str) -> String {
        format!("{entity_type}:{key}")
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
