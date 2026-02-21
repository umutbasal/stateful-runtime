use serde_json::Value;

#[derive(Debug, Clone)]
pub struct EntityRecord {
    pub entity_type: String,
    pub key: String,
    pub value: Value,
    pub updated_at_ms: u64,
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub entity_type: String,
    pub key: String,
    pub deleted_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct StoreOp {
    pub op: StoreOpKind,
    pub entity_type: String,
    pub key: String,
    pub item_id: Option<String>,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOpKind {
    Upsert,
    Delete,
    Push,
    RemoveItem,
}
