use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use serde_json::Value;

use crate::config::{EntitySchema, IndexKind};

#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub entity_type: String,
    pub index_name: String,
    pub field_name: String,
    pub kind: IndexKind,
}

#[derive(Debug, Default)]
pub struct IndexState {
    pub hash_indexes: DashMap<String, DashMap<String, DashSet<String>>>,
    pub sorted_indexes: DashMap<String, Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>>,
}

impl IndexState {
    pub fn ensure_entity_indexes(&self, entity: &EntitySchema) {
        for index in &entity.indexes {
            match index.kind {
                IndexKind::Hash | IndexKind::Membership => {
                    self.hash_indexes
                        .entry(index.name.clone())
                        .or_insert_with(DashMap::new);
                }
                IndexKind::Sorted => {
                    self.sorted_indexes
                        .entry(index.name.clone())
                        .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
                }
            }
        }
    }
}

pub fn index_value_to_key(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Null) => Some("null".to_string()),
        Some(Value::Bool(v)) => Some(v.to_string()),
        Some(Value::Number(v)) => Some(v.to_string()),
        Some(Value::String(v)) => Some(v.clone()),
        Some(Value::Array(v)) => {
            Some(serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()))
        }
        Some(Value::Object(v)) => {
            Some(serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string()))
        }
        None => None,
    }
}
