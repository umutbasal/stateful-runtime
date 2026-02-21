use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use deno_core::{serde_v8, v8, JsRuntime, RuntimeOptions};
use serde_json::Value;

use crate::js::ops;
use crate::js::{IngestContext, JsRouteRequest, JsRouteResponse};
use crate::store::{Store, StoreOp, StoreOpKind};

pub fn execute_route_handler(
    store: Arc<Store>,
    script_name: &str,
    script_source: &str,
    request: &JsRouteRequest,
    memory_limit_bytes: usize,
) -> Result<JsRouteResponse> {
    let mut runtime = new_runtime(store, memory_limit_bytes);
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
    script_name: &str,
    script_source: &str,
    event_type: &str,
    payload: &Value,
    context: &IngestContext,
    memory_limit_bytes: usize,
) -> Result<Vec<StoreOp>> {
    let mut runtime = new_runtime(store, memory_limit_bytes);
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

fn new_runtime(store: Arc<Store>, memory_limit_bytes: usize) -> JsRuntime {
    let create_params = v8::Isolate::create_params().heap_limits(0, memory_limit_bytes);
    JsRuntime::new(RuntimeOptions {
        extensions: vec![ops::build_extension(store)],
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
            other => return Err(anyhow!("unsupported store op '{other}'")),
        };
        let value = object.get("value").cloned();

        ops.push(StoreOp {
            op: op_kind,
            entity_type: entity_type.to_string(),
            key: key.to_string(),
            value,
        });
    }

    Ok(ops)
}
