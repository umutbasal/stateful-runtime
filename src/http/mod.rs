pub mod admin;
pub mod health;
pub mod router;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::body::{to_bytes, Body};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, on};
use axum::{Json, Router};
use serde_json::Value;
use tracing::{error, warn};
use uuid::Uuid;

use crate::config::{EndpointConfig, LoadedConfig};
use crate::js::{JsRouteRequest, JsRuntimePool};
use crate::limits::{LimitError, RuntimeLimits};
use crate::metrics::SharedMetrics;
use crate::store::Store;

#[derive(Clone)]
pub struct RuntimeState {
    pub config: Arc<LoadedConfig>,
    pub store: Arc<Store>,
    pub js_pool: Arc<JsRuntimePool>,
    pub limits: Arc<RuntimeLimits>,
    pub metrics: SharedMetrics,
    pub health: health::HealthState,
}

pub fn build_router(state: RuntimeState) -> Result<Router> {
    let kafka_configured = state
        .config
        .app
        .ingestion
        .iter()
        .any(|source| source.source_type == "kafka");

    let ready_health = state.health.clone();
    let mut router = Router::new()
        .route("/healthz", get(health::health_handler))
        .route(
            "/readyz",
            get(move || health::ready_handler(ready_health.clone(), kafka_configured)),
        )
        .route("/metrics", get(admin::metrics_handler));

    if state.config.app.admin.enable_execute {
        router = router.route("/admin/execute", get(admin::execute_handler));
    }

    for endpoint in &state.config.app.endpoints {
        let endpoint_config = endpoint.clone();
        let method_filter = router::method_filter(&endpoint.method)?;
        router = router.route(
            &endpoint.path,
            on(
                method_filter,
                move |state: State<RuntimeState>,
                      params: Path<HashMap<String, String>>,
                      request: Request<Body>| async move {
                    handle_edge_request(state, params, request, endpoint_config.clone()).await
                },
            ),
        );
    }

    Ok(router.with_state(state))
}

async fn handle_edge_request(
    State(state): State<RuntimeState>,
    Path(params): Path<HashMap<String, String>>,
    request: Request<Body>,
    endpoint: EndpointConfig,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().as_str().to_string();
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    if let Err(err) = state.limits.check_request_rate(&endpoint.name) {
        return build_error_response(
            err,
            &request_id,
            &endpoint.name,
            &method,
            started_at,
            &state.metrics,
        );
    }
    let _permit = match state.limits.try_acquire_request_permit() {
        Ok(permit) => permit,
        Err(err) => {
            return build_error_response(
                err,
                &request_id,
                &endpoint.name,
                &method,
                started_at,
                &state.metrics,
            );
        }
    };
    if let Err(err) = state.limits.check_store_budget(&state.store) {
        return build_error_response(
            err,
            &request_id,
            &endpoint.name,
            &method,
            started_at,
            &state.metrics,
        );
    }

    let query = router::parse_query_string(request.uri().query());
    let headers = headers_to_map(request.headers());
    let path = request.uri().path().to_string();
    let (parts, body) = request.into_parts();
    let body_bytes = match to_bytes(body, state.config.app.limits.max_request_bytes).await {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!("invalid request body for {}: {}", endpoint.name, err);
            let response = (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "request body exceeds configured limit"})),
            )
                .into_response();
            state.metrics.observe_http(
                &endpoint.name,
                &method,
                response.status().as_u16(),
                started_at.elapsed(),
            );
            return with_request_id(response, &request_id);
        }
    };

    let body = if body_bytes.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice::<Value>(&body_bytes) {
            Ok(value) => value,
            Err(err) => {
                warn!("request body must be JSON for {}: {}", endpoint.name, err);
                let response = (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "request body must be valid JSON"})),
                )
                    .into_response();
                state.metrics.observe_http(
                    &endpoint.name,
                    &method,
                    response.status().as_u16(),
                    started_at.elapsed(),
                );
                return with_request_id(response, &request_id);
            }
        }
    };

    let js_request = JsRouteRequest {
        method: parts.method.as_str().to_string(),
        path,
        params,
        query,
        headers,
        body,
        request_id: request_id.clone(),
    };

    match state
        .js_pool
        .execute_route(&endpoint.handler, js_request)
        .await
    {
        Ok(js_response) => {
            let status = StatusCode::from_u16(js_response.status).unwrap_or(StatusCode::OK);
            let response_body = match serde_json::to_vec(&js_response.body) {
                Ok(payload) => payload,
                Err(err) => {
                    error!(
                        "failed to serialize JS response for {}: {}",
                        endpoint.name, err
                    );
                    return with_request_id(
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({"error": "failed to serialize response"})),
                        )
                            .into_response(),
                        &request_id,
                    );
                }
            };
            if response_body.len() > state.config.app.limits.max_response_bytes {
                let response = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": "response payload too large"})),
                )
                    .into_response();
                state.metrics.observe_http(
                    &endpoint.name,
                    &method,
                    response.status().as_u16(),
                    started_at.elapsed(),
                );
                return with_request_id(response, &request_id);
            }

            let mut response = Response::new(Body::from(response_body));
            *response.status_mut() = status;
            response.headers_mut().insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
            apply_custom_headers(response.headers_mut(), &js_response.headers);
            state.metrics.observe_http(
                &endpoint.name,
                &method,
                status.as_u16(),
                started_at.elapsed(),
            );
            state.metrics.sync_store_metrics(&state.store);
            with_request_id(response, &request_id)
        }
        Err(err) => {
            state.metrics.inc_js_error(&endpoint.name, "route");
            let status = if err.to_string().contains("time budget") {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            let response = (
                status,
                Json(serde_json::json!({ "error": err.to_string() })),
            )
                .into_response();
            state.metrics.observe_http(
                &endpoint.name,
                &method,
                response.status().as_u16(),
                started_at.elapsed(),
            );
            with_request_id(response, &request_id)
        }
    }
}

fn build_error_response(
    err: LimitError,
    request_id: &str,
    endpoint_name: &str,
    method: &str,
    started_at: Instant,
    metrics: &SharedMetrics,
) -> Response {
    let (status, message) = match err {
        LimitError::RequestRateExceeded => {
            (StatusCode::TOO_MANY_REQUESTS, "request rate limit exceeded")
        }
        LimitError::ConcurrencyExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "concurrency limit exceeded",
        ),
        LimitError::IngestionRateExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ingestion rate limit exceeded",
        ),
        LimitError::StoreSoftLimitExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "store memory soft limit exceeded",
        ),
        LimitError::StoreHardLimitExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "store memory hard limit exceeded",
        ),
    };
    let response = (status, Json(serde_json::json!({ "error": message }))).into_response();
    metrics.observe_http(endpoint_name, method, status.as_u16(), started_at.elapsed());
    with_request_id(response, request_id)
}

fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.to_string(), v.to_string()))
        })
        .collect()
}

fn apply_custom_headers(headers: &mut HeaderMap, custom_headers: &HashMap<String, String>) {
    for (key, value) in custom_headers {
        let Ok(name) = HeaderName::try_from(key.as_str()) else {
            continue;
        };
        let Ok(value) = HeaderValue::try_from(value.as_str()) else {
            continue;
        };
        headers.insert(name, value);
    }
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-request-id"), value);
    }
    response
}
