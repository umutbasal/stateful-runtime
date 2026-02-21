use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::ClientConfig;
use rdkafka::TopicPartitionList;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::js::{IngestContext, JsRuntimePool};
use crate::limits::{LimitError, RuntimeLimits};
use crate::metrics::SharedMetrics;
use crate::store::Store;

#[derive(Debug, Clone)]
pub struct KafkaTopicBinding {
    pub topic: String,
    pub entity: String,
    pub event_type: String,
}

pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<KafkaTopicBinding>,
    pub max_in_flight: usize,
    pub stop_rx: watch::Receiver<bool>,
    pub health: Arc<AtomicBool>,
    pub js_pool: Arc<JsRuntimePool>,
    pub store: Arc<Store>,
    pub limits: Arc<RuntimeLimits>,
    pub metrics: SharedMetrics,
}

pub fn spawn_consumer(config: KafkaConsumerConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_consumer(config).await {
            error!("kafka consumer exited with error: {err:#}");
        }
    })
}

async fn run_consumer(mut config: KafkaConsumerConfig) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        .create()
        .context("failed to create Kafka consumer")?;
    let consumer = Arc::new(consumer);

    let topic_names: Vec<&str> = config
        .topics
        .iter()
        .map(|topic| topic.topic.as_str())
        .collect();
    consumer
        .subscribe(&topic_names)
        .context("failed to subscribe to Kafka topics")?;
    config.health.store(true, Ordering::Relaxed);
    info!(
        "kafka consumer ready group_id={} topics={}",
        config.group_id,
        topic_names.join(",")
    );

    let topic_bindings = config
        .topics
        .iter()
        .map(|binding| (binding.topic.clone(), binding.clone()))
        .collect::<HashMap<_, _>>();

    let semaphore = Arc::new(Semaphore::new(config.max_in_flight.max(1)));

    loop {
        tokio::select! {
            changed = config.stop_rx.changed() => {
                if changed.is_ok() && *config.stop_rx.borrow() {
                    break;
                }
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        let owned = message.detach();
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => continue,
                        };

                        let consumer = consumer.clone();
                        let topic_bindings = topic_bindings.clone();
                        let js_pool = config.js_pool.clone();
                        let store = config.store.clone();
                        let limits = config.limits.clone();
                        let metrics = config.metrics.clone();

                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(err) = process_message(
                                owned,
                                consumer,
                                topic_bindings,
                                js_pool,
                                store,
                                limits,
                                metrics,
                            )
                            .await
                            {
                                error!("failed to process Kafka message: {err:#}");
                            }
                        });
                    }
                    Err(err) => {
                        warn!("Kafka receive error: {err}");
                    }
                }
            }
        }
    }

    config.health.store(false, Ordering::Relaxed);
    Ok(())
}

async fn process_message(
    message: OwnedMessage,
    consumer: Arc<StreamConsumer>,
    topic_bindings: HashMap<String, KafkaTopicBinding>,
    js_pool: Arc<JsRuntimePool>,
    store: Arc<Store>,
    limits: Arc<RuntimeLimits>,
    metrics: SharedMetrics,
) -> Result<()> {
    let topic = message.topic().to_string();
    let partition = message.partition();
    let offset = message.offset();

    limits
        .check_ingestion_rate(&topic)
        .map_err(limit_err_to_anyhow)?;

    let payload = message.payload().ok_or_else(|| anyhow!("empty payload"))?;
    let json_payload: serde_json::Value =
        serde_json::from_slice(payload).context("failed to parse Kafka JSON payload")?;

    let binding = topic_bindings
        .get(&topic)
        .ok_or_else(|| anyhow!("topic '{topic}' missing from binding map"))?;

    let ops = js_pool
        .execute_ingest(
            &binding.event_type,
            json_payload,
            IngestContext {
                topic: topic.clone(),
                partition,
                offset,
            },
        )
        .await
        .context("ingest script execution failed")?;

    limits
        .check_store_budget(&store)
        .map_err(limit_err_to_anyhow)?;
    store.apply_ops(&ops).context("failed applying store ops")?;
    limits
        .check_store_budget(&store)
        .map_err(limit_err_to_anyhow)?;
    metrics.observe_ingestion_message(&topic);
    metrics.observe_ingestion_lag(&topic, partition, 0);

    commit_offset(&consumer, &topic, partition, offset + 1)?;
    Ok(())
}

fn commit_offset(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset))
        .with_context(|| {
            format!(
                "failed to stage commit offset topic={topic} partition={partition} offset={offset}"
            )
        })?;
    consumer
        .commit(&tpl, CommitMode::Async)
        .context("failed to commit Kafka offset")?;
    Ok(())
}

fn limit_err_to_anyhow(err: LimitError) -> anyhow::Error {
    anyhow!(err.to_string())
}
