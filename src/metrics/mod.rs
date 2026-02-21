use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use crate::store::{RetentionSweepResult, Store};

pub struct RuntimeMetrics {
    registry: Registry,
    http_requests_total: IntCounterVec,
    http_request_duration_seconds: HistogramVec,
    js_execution_duration_seconds: HistogramVec,
    js_errors_total: IntCounterVec,
    store_entities_total: IntGaugeVec,
    store_index_size: IntGaugeVec,
    store_evictions_total: IntCounter,
    ingestion_messages_total: IntCounterVec,
    ingestion_lag: IntGaugeVec,
    ingestion_errors_total: IntCounterVec,
}

impl RuntimeMetrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new_custom(Some("stateful_runtime".to_string()), None)?;

        let http_requests_total = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "Total HTTP requests by endpoint/method/status",
            ),
            &["endpoint", "method", "status"],
        )?;
        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request latency in seconds",
            ),
            &["endpoint", "method"],
        )?;
        let js_execution_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "js_execution_duration_seconds",
                "JS execution latency in seconds",
            ),
            &["handler", "kind"],
        )?;
        let js_errors_total = IntCounterVec::new(
            Opts::new("js_errors_total", "Total JS execution errors"),
            &["handler", "kind"],
        )?;
        let store_entities_total = IntGaugeVec::new(
            Opts::new("store_entities_total", "Entities in store by entity type"),
            &["entity_type"],
        )?;
        let store_index_size = IntGaugeVec::new(
            Opts::new("store_index_size", "Index key cardinality"),
            &["index_name"],
        )?;
        let store_evictions_total =
            IntCounter::new("store_evictions_total", "Retention-driven entity evictions")?;
        let ingestion_messages_total = IntCounterVec::new(
            Opts::new("ingestion_messages_total", "Messages processed by topic"),
            &["topic"],
        )?;
        let ingestion_lag = IntGaugeVec::new(
            Opts::new("ingestion_lag", "Kafka lag by topic and partition"),
            &["topic", "partition"],
        )?;
        let ingestion_errors_total = IntCounterVec::new(
            Opts::new("ingestion_errors_total", "Ingestion errors by topic"),
            &["topic"],
        )?;

        for collector in [
            Box::new(http_requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(http_request_duration_seconds.clone()),
            Box::new(js_execution_duration_seconds.clone()),
            Box::new(js_errors_total.clone()),
            Box::new(store_entities_total.clone()),
            Box::new(store_index_size.clone()),
            Box::new(store_evictions_total.clone()),
            Box::new(ingestion_messages_total.clone()),
            Box::new(ingestion_lag.clone()),
            Box::new(ingestion_errors_total.clone()),
        ] {
            registry.register(collector)?;
        }

        Ok(Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            js_execution_duration_seconds,
            js_errors_total,
            store_entities_total,
            store_index_size,
            store_evictions_total,
            ingestion_messages_total,
            ingestion_lag,
            ingestion_errors_total,
        })
    }

    pub fn observe_http(&self, endpoint: &str, method: &str, status: u16, duration: Duration) {
        self.http_requests_total
            .with_label_values(&[endpoint, method, &status.to_string()])
            .inc();
        self.http_request_duration_seconds
            .with_label_values(&[endpoint, method])
            .observe(duration.as_secs_f64());
    }

    pub fn observe_js_duration(&self, handler: &str, kind: &str, duration: Duration) {
        self.js_execution_duration_seconds
            .with_label_values(&[handler, kind])
            .observe(duration.as_secs_f64());
    }

    pub fn inc_js_error(&self, handler: &str, kind: &str) {
        self.js_errors_total
            .with_label_values(&[handler, kind])
            .inc();
    }

    pub fn observe_retention_sweep(&self, sweep: RetentionSweepResult) {
        if sweep.evicted_entities > 0 {
            self.store_evictions_total
                .inc_by(sweep.evicted_entities as u64);
        }
    }

    pub fn observe_ingestion_message(&self, topic: &str) {
        self.ingestion_messages_total
            .with_label_values(&[topic])
            .inc();
    }

    pub fn observe_ingestion_lag(&self, topic: &str, partition: i32, lag: i64) {
        self.ingestion_lag
            .with_label_values(&[topic, &partition.to_string()])
            .set(lag);
    }

    pub fn inc_ingestion_error(&self, topic: &str) {
        self.ingestion_errors_total
            .with_label_values(&[topic])
            .inc();
    }

    pub fn sync_store_metrics(&self, store: &Store) {
        for (entity_type, count) in store.snapshot_entity_counts() {
            self.store_entities_total
                .with_label_values(&[&entity_type])
                .set(count as i64);
        }
        for (index_name, count) in store.snapshot_index_sizes() {
            self.store_index_size
                .with_label_values(&[&index_name])
                .set(count as i64);
        }
    }

    pub fn render(&self) -> Result<String> {
        let metric_families = self.registry.gather();
        let mut out = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut out)?;
        Ok(String::from_utf8(out)?)
    }
}

pub type SharedMetrics = Arc<RuntimeMetrics>;
