use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

#[derive(Clone, Default)]
pub struct HealthState {
    pub config_loaded: Arc<AtomicBool>,
    pub scripts_loaded: Arc<AtomicBool>,
    pub store_ready: Arc<AtomicBool>,
    pub kafka_ready: Arc<AtomicBool>,
}

impl HealthState {
    pub fn set_config_loaded(&self, value: bool) {
        self.config_loaded.store(value, Ordering::Relaxed);
    }

    pub fn set_scripts_loaded(&self, value: bool) {
        self.scripts_loaded.store(value, Ordering::Relaxed);
    }

    pub fn set_store_ready(&self, value: bool) {
        self.store_ready.store(value, Ordering::Relaxed);
    }

    pub fn set_kafka_ready(&self, value: bool) {
        self.kafka_ready.store(value, Ordering::Relaxed);
    }

    pub fn is_ready(&self, kafka_configured: bool) -> bool {
        self.config_loaded.load(Ordering::Relaxed)
            && self.scripts_loaded.load(Ordering::Relaxed)
            && self.store_ready.load(Ordering::Relaxed)
            && (!kafka_configured || self.kafka_ready.load(Ordering::Relaxed))
    }
}

#[derive(Serialize)]
pub struct HealthPayload {
    pub status: &'static str,
}

#[derive(Serialize)]
pub struct ReadinessPayload {
    pub ready: bool,
    pub config_loaded: bool,
    pub scripts_loaded: bool,
    pub store_ready: bool,
    pub kafka_ready: bool,
}

pub async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthPayload { status: "ok" }))
}

pub async fn ready_handler(health_state: HealthState, kafka_configured: bool) -> impl IntoResponse {
    let ready = health_state.is_ready(kafka_configured);
    let payload = ReadinessPayload {
        ready,
        config_loaded: health_state.config_loaded.load(Ordering::Relaxed),
        scripts_loaded: health_state.scripts_loaded.load(Ordering::Relaxed),
        store_ready: health_state.store_ready.load(Ordering::Relaxed),
        kafka_ready: health_state.kafka_ready.load(Ordering::Relaxed),
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(payload))
}
