use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde_json::Value;
use thiserror::Error;

use crate::config::{EntityType, SchemaConfig};

#[derive(Debug, Clone)]
pub struct CollectionItem {
    pub id: String,
    pub value: Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CollectionDef {
    pub order_by: Option<String>,
    pub max_per_partition: Option<usize>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Error)]
pub enum CollectionStoreError {
    #[error("collection '{0}' is not defined in schema")]
    UnknownCollection(String),
    #[error("collection item id cannot be empty")]
    EmptyItemId,
}

pub struct CollectionStore {
    collections: DashMap<String, DashMap<String, VecDeque<CollectionItem>>>,
    defs: HashMap<String, CollectionDef>,
    memory_bytes: AtomicUsize,
}

impl CollectionStore {
    pub fn new(schema: &SchemaConfig) -> Self {
        let collections = DashMap::new();
        let mut defs = HashMap::new();

        for entity in &schema.entities {
            if entity.entity_type != EntityType::Collection {
                continue;
            }

            collections.insert(entity.name.clone(), DashMap::new());
            defs.insert(
                entity.name.clone(),
                CollectionDef {
                    order_by: entity.order_by.clone(),
                    max_per_partition: entity.max_per_partition,
                    ttl_seconds: entity
                        .retention
                        .as_ref()
                        .and_then(|retention| retention.ttl_seconds),
                },
            );
        }

        Self {
            collections,
            defs,
            memory_bytes: AtomicUsize::new(0),
        }
    }

    pub fn push(
        &self,
        collection_name: &str,
        partition_key: &str,
        item_id: &str,
        value: Value,
    ) -> Result<(), CollectionStoreError> {
        if item_id.trim().is_empty() {
            return Err(CollectionStoreError::EmptyItemId);
        }

        let def = self
            .defs
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;

        let mut removed_bytes = 0usize;
        let added_bytes = Self::estimate_item_size(partition_key, item_id, &value);
        let created_at_ms = Self::order_value_to_millis(def.order_by.as_deref(), &value);

        let mut partition_items = collection_partitions
            .entry(partition_key.to_string())
            .or_default();

        if let Some(existing_idx) = partition_items.iter().position(|item| item.id == item_id) {
            if let Some(existing) = partition_items.remove(existing_idx) {
                removed_bytes = removed_bytes.saturating_add(Self::estimate_item_size(
                    partition_key,
                    &existing.id,
                    &existing.value,
                ));
            }
        }

        partition_items.push_back(CollectionItem {
            id: item_id.to_string(),
            value,
            created_at_ms,
        });

        if def.order_by.is_some() {
            partition_items
                .make_contiguous()
                .sort_unstable_by_key(|item| item.created_at_ms);
        }

        if let Some(max_per_partition) = def.max_per_partition {
            while partition_items.len() > max_per_partition {
                if let Some(evicted) = partition_items.pop_front() {
                    removed_bytes = removed_bytes.saturating_add(Self::estimate_item_size(
                        partition_key,
                        &evicted.id,
                        &evicted.value,
                    ));
                }
            }
        }

        self.apply_memory_delta(added_bytes, removed_bytes);
        Ok(())
    }

    pub fn scan(
        &self,
        collection_name: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<Value>, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;

        let Some(items_ref) = collection_partitions.get(partition_key) else {
            return Ok(Vec::new());
        };

        let take_limit = if limit == 0 { usize::MAX } else { limit };
        Ok(items_ref
            .iter()
            .rev()
            .take(take_limit)
            .map(|item| item.value.clone())
            .collect())
    }

    pub fn multi_scan(
        &self,
        collection_name: &str,
        partition_keys: &[String],
        limit_per_partition: usize,
    ) -> Result<Vec<Value>, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;

        let take_limit = if limit_per_partition == 0 {
            usize::MAX
        } else {
            limit_per_partition
        };

        let mut combined = Vec::new();
        for partition_key in partition_keys {
            let Some(items_ref) = collection_partitions.get(partition_key) else {
                continue;
            };
            combined.extend(items_ref.iter().rev().take(take_limit).cloned());
        }

        combined.sort_unstable_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));
        Ok(combined.into_iter().map(|item| item.value).collect())
    }

    pub fn remove(
        &self,
        collection_name: &str,
        partition_key: &str,
        item_id: &str,
    ) -> Result<bool, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;

        let mut removed_bytes = 0usize;
        let mut removed = false;
        let mut partition_empty = false;

        if let Some(mut partition_items) = collection_partitions.get_mut(partition_key) {
            if let Some(existing_idx) = partition_items.iter().position(|item| item.id == item_id) {
                if let Some(existing) = partition_items.remove(existing_idx) {
                    removed = true;
                    removed_bytes = removed_bytes.saturating_add(Self::estimate_item_size(
                        partition_key,
                        &existing.id,
                        &existing.value,
                    ));
                }
            }

            partition_empty = partition_items.is_empty();
        }

        if partition_empty {
            collection_partitions.remove(partition_key);
        }

        self.apply_memory_delta(0, removed_bytes);
        Ok(removed)
    }

    pub fn len(
        &self,
        collection_name: &str,
        partition_key: &str,
    ) -> Result<usize, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;
        Ok(collection_partitions
            .get(partition_key)
            .map(|items| items.len())
            .unwrap_or(0))
    }

    pub fn trim_collection(
        &self,
        collection_name: &str,
        max_age_seconds: u64,
    ) -> Result<usize, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;

        let now_ms = now_millis();
        let max_age_ms = max_age_seconds.saturating_mul(1000);
        let mut removed_count = 0usize;
        let mut removed_bytes = 0usize;
        let mut empty_partitions = Vec::new();

        for mut partition in collection_partitions.iter_mut() {
            let partition_key = partition.key().clone();
            let items = partition.value_mut();

            if max_age_seconds == 0 {
                while let Some(evicted) = items.pop_front() {
                    removed_count = removed_count.saturating_add(1);
                    removed_bytes = removed_bytes.saturating_add(Self::estimate_item_size(
                        &partition_key,
                        &evicted.id,
                        &evicted.value,
                    ));
                }
            } else {
                while let Some(oldest) = items.front() {
                    if now_ms.saturating_sub(oldest.created_at_ms) <= max_age_ms {
                        break;
                    }
                    if let Some(evicted) = items.pop_front() {
                        removed_count = removed_count.saturating_add(1);
                        removed_bytes = removed_bytes.saturating_add(Self::estimate_item_size(
                            &partition_key,
                            &evicted.id,
                            &evicted.value,
                        ));
                    }
                }
            }

            if items.is_empty() {
                empty_partitions.push(partition_key);
            }
        }

        for partition_key in empty_partitions {
            collection_partitions.remove(&partition_key);
        }

        self.apply_memory_delta(0, removed_bytes);
        Ok(removed_count)
    }

    pub fn trim_expired(&self, collection_name: &str) -> Result<usize, CollectionStoreError> {
        let Some(def) = self.defs.get(collection_name) else {
            return Err(CollectionStoreError::UnknownCollection(
                collection_name.to_string(),
            ));
        };
        match def.ttl_seconds {
            Some(ttl_seconds) => self.trim_collection(collection_name, ttl_seconds),
            None => Ok(0),
        }
    }

    pub fn trim_all_expired(&self) -> usize {
        let mut total_trimmed = 0usize;
        for collection_name in self.defs.keys() {
            if let Ok(trimmed) = self.trim_expired(collection_name) {
                total_trimmed = total_trimmed.saturating_add(trimmed);
            }
        }
        total_trimmed
    }

    pub fn partition_count(&self, collection_name: &str) -> Result<usize, CollectionStoreError> {
        let collection_partitions = self
            .collections
            .get(collection_name)
            .ok_or_else(|| CollectionStoreError::UnknownCollection(collection_name.to_string()))?;
        Ok(collection_partitions.len())
    }

    pub fn snapshot_collection_sizes(&self) -> HashMap<String, usize> {
        self.collections
            .iter()
            .map(|entry| {
                let count = entry
                    .value()
                    .iter()
                    .map(|partition| partition.value().len())
                    .sum();
                (entry.key().clone(), count)
            })
            .collect()
    }

    pub fn current_memory_bytes(&self) -> usize {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    fn order_value_to_millis(order_by: Option<&str>, value: &Value) -> u64 {
        let Some(field) = order_by else {
            return now_millis();
        };
        let Some(raw_value) = value.get(field) else {
            return now_millis();
        };

        if let Some(v) = raw_value.as_u64() {
            return v;
        }
        if let Some(v) = raw_value.as_i64() {
            return v.max(0) as u64;
        }
        if let Some(v) = raw_value.as_str() {
            if let Ok(parsed) = v.parse::<u64>() {
                return parsed;
            }
        }

        now_millis()
    }

    fn estimate_item_size(partition_key: &str, item_id: &str, value: &Value) -> usize {
        partition_key.len()
            + item_id.len()
            + serde_json::to_vec(value).map_or(0, |raw| raw.len())
            + 64
    }

    fn apply_memory_delta(&self, added: usize, removed: usize) {
        if added >= removed {
            self.memory_bytes
                .fetch_add(added.saturating_sub(removed), Ordering::Relaxed);
            return;
        }

        let diff = removed.saturating_sub(added);
        let _ = self
            .memory_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(diff))
            });
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
