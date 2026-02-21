use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    #[default]
    Record,
    Collection,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaConfig {
    #[serde(default)]
    pub entities: Vec<EntitySchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySchema {
    pub name: String,
    pub primary_key: String,
    #[serde(default)]
    pub entity_type: EntityType,
    #[serde(default)]
    pub fields: Vec<FieldSchema>,
    #[serde(default)]
    pub indexes: Vec<IndexSchema>,
    #[serde(default)]
    pub partition_key: Option<String>,
    #[serde(default)]
    pub order_by: Option<String>,
    #[serde(default)]
    pub max_per_partition: Option<usize>,
    #[serde(default)]
    pub retention: Option<RetentionPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub field: String,
    #[serde(rename = "type")]
    pub kind: IndexKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    Hash,
    Sorted,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetentionPolicy {
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub max_entities: Option<usize>,
}
