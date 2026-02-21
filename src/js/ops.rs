use std::sync::Arc;

use deno_core::{op2, OpState};
use deno_error::JsErrorBox;
use serde_json::Value;

use crate::store::Store;

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

deno_core::extension!(
    stateful_runtime_ops,
    ops = [
        op_store_get,
        op_store_upsert,
        op_store_delete,
        op_store_index_lookup,
        op_store_range_lookup
    ],
    options = {
        store: Arc<Store>
    },
    state = |state, options| {
        state.put(options.store);
    }
);

pub fn build_extension(store: Arc<Store>) -> deno_core::Extension {
    stateful_runtime_ops::init(store)
}
