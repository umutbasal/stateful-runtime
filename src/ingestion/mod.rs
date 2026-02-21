mod kafka;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
pub use kafka::KafkaTopicBinding;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::{IngestionSource, LoadedConfig};
use crate::js::JsRuntimePool;
use crate::limits::RuntimeLimits;
use crate::metrics::SharedMetrics;
use crate::store::Store;

#[derive(Clone, Default)]
pub struct IngestionHealth {
    pub kafka_ready: Arc<AtomicBool>,
}

impl IngestionHealth {
    pub fn is_ready(&self) -> bool {
        self.kafka_ready.load(Ordering::Relaxed)
    }
}

pub struct IngestionHandle {
    stop_tx: watch::Sender<bool>,
    join_handles: Vec<JoinHandle<()>>,
    pub health: IngestionHealth,
}

impl IngestionHandle {
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        for handle in self.join_handles {
            let _ = handle.await;
        }
    }
}

pub async fn start(
    config: Arc<LoadedConfig>,
    js_pool: Arc<JsRuntimePool>,
    store: Arc<Store>,
    limits: Arc<RuntimeLimits>,
    metrics: SharedMetrics,
) -> Result<Option<IngestionHandle>> {
    let kafka_sources: Vec<&IngestionSource> = config
        .app
        .ingestion
        .iter()
        .filter(|source| source.source_type == "kafka")
        .collect();
    if kafka_sources.is_empty() {
        return Ok(None);
    }

    let health = IngestionHealth::default();
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut join_handles = Vec::new();

    for source in kafka_sources {
        let topics = source
            .topics
            .iter()
            .map(|topic| KafkaTopicBinding {
                topic: topic.name.clone(),
                entity: topic.entity.clone(),
                event_type: if topic.event_type.is_empty() {
                    topic.name.clone()
                } else {
                    topic.event_type.clone()
                },
            })
            .collect::<Vec<_>>();

        let group_id = source
            .group_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", config.app.app.name, config.app.app.version));

        let brokers = if source.brokers.is_empty() {
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
        } else {
            source.brokers.join(",")
        };

        info!(
            "starting kafka consumer group_id={} brokers={} topics={}",
            group_id,
            brokers,
            topics.len()
        );

        join_handles.push(kafka::spawn_consumer(kafka::KafkaConsumerConfig {
            brokers,
            group_id,
            topics,
            max_in_flight: config.app.limits.max_concurrent_requests.max(1),
            stop_rx: stop_rx.clone(),
            health: health.kafka_ready.clone(),
            js_pool: js_pool.clone(),
            store: store.clone(),
            limits: limits.clone(),
            metrics: metrics.clone(),
        }));
    }

    Ok(Some(IngestionHandle {
        stop_tx,
        join_handles,
        health,
    }))
}
