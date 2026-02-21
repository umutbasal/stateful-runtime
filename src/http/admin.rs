use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::http::RuntimeState;

pub async fn metrics_handler(State(state): State<RuntimeState>) -> impl IntoResponse {
    match state.metrics.render() {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("failed to render metrics: {err}"),
        )
            .into_response(),
    }
}

pub async fn execute_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "admin execute API is not enabled in the MVP runtime",
    )
}
