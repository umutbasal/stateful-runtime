use std::collections::HashMap;
use std::sync::Arc;

use deno_core::{op2, OpState};
use deno_error::JsErrorBox;
use serde_json::Value;

use crate::js::runtime;
use crate::store::{CollectionStore, Store};

#[op2]
#[string]
fn op_store_get(
    state: &mut OpState,
    #[string] entity_type: String,
    #[string] key: String,
) -> Result<String, JsErrorBox> {
    let store = state.borrow::<Arc<Store>>();
    let value = store
        .get(&entity_type, &key)
        .map(|record| record.value)
        .unwrap_or(Value::Null);
    serde_json::to_string(&value)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

#[op2(fast)]
fn op_store_upsert(
    state: &mut OpState,
    #[string] entity_type: String,
    #[string] key: String,
    #[string] value_json: String,
) -> Result<(), JsErrorBox> {
    let store = state.borrow::<Arc<Store>>();
    let value = serde_json::from_str::<Value>(&value_json)
        .map_err(|err| JsErrorBox::generic(format!("invalid JSON payload: {err}")))?;
    store
        .upsert(&entity_type, &key, value)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(())
}

#[op2(fast)]
fn op_store_delete(
    state: &mut OpState,
    #[string] entity_type: String,
    #[string] key: String,
) -> Result<(), JsErrorBox> {
    let store = state.borrow::<Arc<Store>>();
    store
        .delete(&entity_type, &key)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(())
}

#[op2]
#[string]
fn op_store_index_lookup(
    state: &mut OpState,
    #[string] index_name: String,
    #[string] value: String,
) -> Result<String, JsErrorBox> {
    let store = state.borrow::<Arc<Store>>();
    let records = store.index_lookup_entities(&index_name, &value, 10_000);
    let values = records
        .into_iter()
        .map(|record| record.value)
        .collect::<Vec<_>>();
    serde_json::to_string(&values)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

#[op2]
#[string]
fn op_store_range_lookup(
    state: &mut OpState,
    #[string] index_name: String,
    #[string] start: String,
    #[string] end: String,
    limit: u32,
) -> Result<String, JsErrorBox> {
    let store = state.borrow::<Arc<Store>>();
    let records = store.range_lookup_entities(
        &index_name,
        if start.is_empty() {
            None
        } else {
            Some(start.as_str())
        },
        if end.is_empty() {
            None
        } else {
            Some(end.as_str())
        },
        limit as usize,
    );
    let values = records
        .into_iter()
        .map(|record| record.value)
        .collect::<Vec<_>>();
    serde_json::to_string(&values)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

#[op2(fast)]
fn op_collection_push(
    state: &mut OpState,
    #[string] collection_name: String,
    #[string] partition_key: String,
    #[string] item_json: String,
) -> Result<(), JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let value = serde_json::from_str::<Value>(&item_json)
        .map_err(|err| JsErrorBox::generic(format!("invalid JSON payload: {err}")))?;
    let item_id = extract_item_id(&value)
        .ok_or_else(|| JsErrorBox::generic("collection item must include id or post_id"))?;
    collection_store
        .push(&collection_name, &partition_key, &item_id, value)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(())
}

#[op2]
#[string]
fn op_collection_scan(
    state: &mut OpState,
    #[string] collection_name: String,
    #[string] partition_key: String,
    limit: u32,
) -> Result<String, JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let items = collection_store
        .scan(&collection_name, &partition_key, limit as usize)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    serde_json::to_string(&items)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

#[op2]
#[string]
fn op_collection_multi_scan(
    state: &mut OpState,
    #[string] collection_name: String,
    #[string] partition_keys_json: String,
    limit_per_partition: u32,
) -> Result<String, JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let partition_keys = serde_json::from_str::<Vec<String>>(&partition_keys_json)
        .map_err(|err| JsErrorBox::generic(format!("invalid partition key list: {err}")))?;
    let items = collection_store
        .multi_scan(
            &collection_name,
            &partition_keys,
            limit_per_partition as usize,
        )
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    serde_json::to_string(&items)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

#[op2(fast)]
fn op_collection_remove(
    state: &mut OpState,
    #[string] collection_name: String,
    #[string] partition_key: String,
    #[string] item_id: String,
) -> Result<(), JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let _ = collection_store
        .remove(&collection_name, &partition_key, &item_id)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(())
}

#[op2(fast)]
fn op_collection_len(
    state: &mut OpState,
    #[string] collection_name: String,
    #[string] partition_key: String,
) -> Result<u32, JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let len = collection_store
        .len(&collection_name, &partition_key)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(len as u32)
}

#[op2(fast)]
fn op_collection_trim(
    state: &mut OpState,
    #[string] collection_name: String,
    max_age_seconds: u32,
) -> Result<u32, JsErrorBox> {
    let collection_store = state.borrow::<Arc<CollectionStore>>();
    let trimmed = collection_store
        .trim_collection(&collection_name, max_age_seconds as u64)
        .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    Ok(trimmed as u32)
}

#[op2]
#[string]
fn op_execute_query(
    state: &mut OpState,
    #[string] query_name: String,
    #[string] params_json: String,
) -> Result<String, JsErrorBox> {
    let store = state.borrow::<Arc<Store>>().clone();
    let collection_store = state.borrow::<Arc<CollectionStore>>().clone();
    let query_scripts = state
        .borrow::<Arc<HashMap<String, (String, String)>>>()
        .clone();
    let memory_limit_bytes = *state.borrow::<usize>();

    let (script_name, script_source) = query_scripts
        .get(&query_name)
        .cloned()
        .ok_or_else(|| JsErrorBox::generic(format!("query '{query_name}' not found")))?;

    let params = if params_json.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str::<Value>(&params_json)
            .map_err(|err| JsErrorBox::generic(format!("invalid query params JSON: {err}")))?
    };

    let result = runtime::execute_query_handler(
        store,
        collection_store,
        query_scripts,
        &script_name,
        &script_source,
        &params,
        memory_limit_bytes,
    )
    .map_err(|err| JsErrorBox::generic(err.to_string()))?;

    serde_json::to_string(&result)
        .map_err(|err| JsErrorBox::generic(format!("serialization failed: {err}")))
}

deno_core::extension!(
    stateful_runtime_ops,
    ops = [
        op_store_get,
        op_store_upsert,
        op_store_delete,
        op_store_index_lookup,
        op_store_range_lookup,
        op_collection_push,
        op_collection_scan,
        op_collection_multi_scan,
        op_collection_remove,
        op_collection_len,
        op_collection_trim,
        op_execute_query
    ],
    options = {
        store: Arc<Store>,
        collection_store: Arc<CollectionStore>,
        query_scripts: Arc<HashMap<String, (String, String)>>,
        memory_limit_bytes: usize
    },
    state = |state, options| {
        state.put(options.store);
        state.put(options.collection_store);
        state.put(options.query_scripts);
        state.put(options.memory_limit_bytes);
    }
);

pub fn build_extension(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    memory_limit_bytes: usize,
) -> deno_core::Extension {
    stateful_runtime_ops::init(store, collection_store, query_scripts, memory_limit_bytes)
}

fn extract_item_id(value: &Value) -> Option<String> {
    if let Some(id) = value.get("id").and_then(Value::as_str) {
        if !id.is_empty() {
            return Some(id.to_string());
        }
    }
    if let Some(post_id) = value.get("post_id").and_then(Value::as_i64) {
        return Some(post_id.to_string());
    }
    if let Some(post_id) = value.get("post_id").and_then(Value::as_str) {
        if !post_id.is_empty() {
            return Some(post_id.to_string());
        }
    }
    None
}
