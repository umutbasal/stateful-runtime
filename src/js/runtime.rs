use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use deno_core::{serde_v8, v8, JsRuntime, RuntimeOptions};
use serde_json::Value;

use crate::js::ops;
use crate::js::{IngestContext, JsRouteRequest, JsRouteResponse};
use crate::store::{CollectionStore, Store, StoreOp, StoreOpKind};

pub fn execute_route_handler(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    script_name: &str,
    script_source: &str,
    request: &JsRouteRequest,
    memory_limit_bytes: usize,
) -> Result<JsRouteResponse> {
    let mut runtime = new_runtime(store, collection_store, query_scripts, memory_limit_bytes);
    bootstrap_runtime(&mut runtime)?;
    load_script(&mut runtime, script_name, script_source)?;

    let request_json = serde_json::to_string(request)?;
    let invocation = format!(
        r#"
(() => {{
  if (!globalThis.__stateful.routeHandler) {{
    throw new Error("route handler not registered");
  }}
  globalThis.__stateful.lastResult = globalThis.__stateful.routeHandler({request_json});
}})();
"#
    );
    runtime
        .execute_script("<route_invoke>", invocation)
        .context("failed to execute route handler")?;

    let result_handle = runtime
        .execute_script("<route_result>", "globalThis.__stateful.lastResult")
        .context("failed to read route handler result")?;
    let result_value = to_json_value(&mut runtime, result_handle)?;

    let response: JsRouteResponse = serde_json::from_value(result_value)
        .context("route handler returned an invalid response shape")?;
    Ok(response)
}

pub fn execute_ingest_handler(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    script_name: &str,
    script_source: &str,
    event_type: &str,
    payload: &Value,
    context: &IngestContext,
    memory_limit_bytes: usize,
) -> Result<Vec<StoreOp>> {
    let mut runtime = new_runtime(store, collection_store, query_scripts, memory_limit_bytes);
    bootstrap_runtime(&mut runtime)?;
    load_script(&mut runtime, script_name, script_source)?;

    let payload_json = serde_json::to_string(payload)?;
    let context_json = serde_json::to_string(context)?;
    let event_type_json = serde_json::to_string(event_type)?;
    let invocation = format!(
        r#"
(() => {{
  if (!globalThis.__stateful.ingestHandler) {{
    globalThis.__stateful.lastResult = [];
    return;
  }}
  globalThis.__stateful.lastResult = globalThis.__stateful.ingestHandler(
    {event_type_json},
    {payload_json},
    {context_json}
  );
}})();
"#
    );

    runtime
        .execute_script("<ingest_invoke>", invocation)
        .context("failed to execute ingest handler")?;
    let result_handle = runtime
        .execute_script("<ingest_result>", "globalThis.__stateful.lastResult")
        .context("failed to read ingest handler result")?;
    let result_value = to_json_value(&mut runtime, result_handle)?;

    parse_store_ops(result_value)
}

pub fn execute_cron_handler(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    script_name: &str,
    script_source: &str,
    cron_name: &str,
    memory_limit_bytes: usize,
) -> Result<Vec<StoreOp>> {
    let mut runtime = new_runtime(store, collection_store, query_scripts, memory_limit_bytes);
    bootstrap_runtime(&mut runtime)?;
    load_script(&mut runtime, script_name, script_source)?;

    let cron_name_json = serde_json::to_string(cron_name)?;
    let invocation = format!(
        r#"
(() => {{
  if (typeof on_cron !== "function") {{
    globalThis.__stateful.lastResult = [];
    return;
  }}
  const result = on_cron({cron_name_json});
  globalThis.__stateful.lastResult = Array.isArray(result) ? result : [];
}})();
"#
    );

    runtime
        .execute_script("<cron_invoke>", invocation)
        .context("failed to execute cron handler")?;
    let result_handle = runtime
        .execute_script("<cron_result>", "globalThis.__stateful.lastResult")
        .context("failed to read cron handler result")?;
    let result_value = to_json_value(&mut runtime, result_handle)?;
    parse_store_ops(result_value)
}

pub fn execute_query_handler(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    script_name: &str,
    script_source: &str,
    params: &Value,
    memory_limit_bytes: usize,
) -> Result<Value> {
    let mut runtime = new_runtime(store, collection_store, query_scripts, memory_limit_bytes);
    bootstrap_runtime(&mut runtime)?;
    load_script(&mut runtime, script_name, script_source)?;

    let params_json = serde_json::to_string(params)?;
    let invocation = format!(
        r#"
(() => {{
  if (typeof on_query !== "function") {{
    throw new Error("query handler not registered");
  }}
  globalThis.__stateful.lastResult = on_query({params_json});
}})();
"#
    );

    runtime
        .execute_script("<query_invoke>", invocation)
        .context("failed to execute query handler")?;
    let result_handle = runtime
        .execute_script("<query_result>", "globalThis.__stateful.lastResult")
        .context("failed to read query handler result")?;
    to_json_value(&mut runtime, result_handle)
}

pub fn execute_lifecycle_handler(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    script_name: &str,
    script_source: &str,
    hook_name: &str,
    memory_limit_bytes: usize,
) -> Result<Vec<StoreOp>> {
    let mut runtime = new_runtime(store, collection_store, query_scripts, memory_limit_bytes);
    bootstrap_runtime(&mut runtime)?;
    load_script(&mut runtime, script_name, script_source)?;

    let hook_name_json = serde_json::to_string(hook_name)?;
    let invocation = format!(
        r#"
(() => {{
  const hook = globalThis[{hook_name_json}];
  if (typeof hook !== "function") {{
    globalThis.__stateful.lastResult = [];
    return;
  }}
  const result = hook();
  globalThis.__stateful.lastResult = Array.isArray(result) ? result : [];
}})();
"#
    );

    runtime
        .execute_script("<lifecycle_invoke>", invocation)
        .context("failed to execute lifecycle handler")?;
    let result_handle = runtime
        .execute_script("<lifecycle_result>", "globalThis.__stateful.lastResult")
        .context("failed to read lifecycle handler result")?;
    let result_value = to_json_value(&mut runtime, result_handle)?;
    parse_store_ops(result_value)
}

fn new_runtime(
    store: Arc<Store>,
    collection_store: Arc<CollectionStore>,
    query_scripts: Arc<HashMap<String, (String, String)>>,
    memory_limit_bytes: usize,
) -> JsRuntime {
    let create_params = v8::Isolate::create_params().heap_limits(0, memory_limit_bytes);
    JsRuntime::new(RuntimeOptions {
        extensions: vec![ops::build_extension(
            store,
            collection_store,
            query_scripts,
            memory_limit_bytes,
        )],
        create_params: Some(create_params),
        ..Default::default()
    })
}

fn bootstrap_runtime(runtime: &mut JsRuntime) -> Result<()> {
    runtime.execute_script(
        "<bootstrap>",
        r#"
globalThis.__stateful = {
  routeHandler: null,
  ingestHandler: null,
  lastResult: null
};
"#,
    )?;
    Ok(())
}

fn load_script(runtime: &mut JsRuntime, script_name: &str, script_source: &str) -> Result<()> {
    let wrapped = format!(
        r#"
(() => {{
{script_source}

if (typeof route !== "undefined" && route && typeof route.handle === "function") {{
  globalThis.__stateful.routeHandler = route.handle;
}}

if (typeof on_ingest === "function") {{
  globalThis.__stateful.ingestHandler = on_ingest;
}}
}})();
"#
    );

    runtime
        .execute_script(script_name.to_string(), wrapped)
        .with_context(|| format!("failed to load script {script_name}"))?;
    Ok(())
}

fn to_json_value(runtime: &mut JsRuntime, value: v8::Global<v8::Value>) -> Result<Value> {
    deno_core::scope!(scope, runtime);
    let local = v8::Local::new(scope, value);
    let value: Value = serde_v8::from_v8(scope, local)?;
    Ok(value)
}

fn parse_store_ops(value: Value) -> Result<Vec<StoreOp>> {
    if value.is_null() {
        return Ok(Vec::new());
    }

    let entries = value
        .as_array()
        .ok_or_else(|| anyhow!("on_ingest must return an array of ops"))?;
    let mut ops = Vec::with_capacity(entries.len());

    for entry in entries {
        let object = entry
            .as_object()
            .ok_or_else(|| anyhow!("each op must be a JSON object"))?;
        let op = object
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("op.op must be present"))?;
        let entity_type = object
            .get("entity_type")
            .or_else(|| object.get("entityType"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("op.entity_type must be present"))?;
        let key = object
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("op.key must be present"))?;

        let op_kind = match op {
            "upsert" => StoreOpKind::Upsert,
            "delete" => StoreOpKind::Delete,
            "push" => StoreOpKind::Push,
            "remove_item" => StoreOpKind::RemoveItem,
            other => return Err(anyhow!("unsupported store op '{other}'")),
        };
        let item_id = object
            .get("item_id")
            .or_else(|| object.get("itemId"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let value = object.get("value").cloned();

        ops.push(StoreOp {
            op: op_kind,
            entity_type: entity_type.to_string(),
            key: key.to_string(),
            item_id,
            value,
        });
    }

    Ok(ops)
}
