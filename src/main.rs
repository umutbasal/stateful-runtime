use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::routing::get;
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use stateful_runtime::config::load_bundle;
use stateful_runtime::http::health::HealthState;
use stateful_runtime::http::{admin, build_router, RuntimeState};
use stateful_runtime::ingestion;
use stateful_runtime::js::JsRuntimePool;
use stateful_runtime::limits::{RuntimeLimits, StoreBudget};
use stateful_runtime::metrics::RuntimeMetrics;
use stateful_runtime::store::retention::spawn_retention_sweeper;
use stateful_runtime::store::Store;

#[derive(Debug, Parser)]
#[command(name = "stateful-runtime")]
#[command(about = "Stateful edge runtime for low-latency workloads")]
struct Cli {
    #[arg(long, env = "BUNDLE_PATH")]
    bundle_path: PathBuf,
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8080")]
    bind: String,
    #[arg(long, env = "METRICS_BIND", default_value = "0.0.0.0:9090")]
    metrics_bind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let health = HealthState::default();
    let config = Arc::new(load_bundle(&cli.bundle_path)?);
    health.set_config_loaded(true);

    let store = Arc::new(Store::new(
        &config.schema,
        config.app.limits.store.max_bytes,
        config.app.limits.store.max_entity_bytes,
    ));
    health.set_store_ready(true);

    let metrics = Arc::new(RuntimeMetrics::new()?);
    metrics.sync_store_metrics(&store);

    let limits = Arc::new(RuntimeLimits::new(
        config.app.limits.max_concurrent_requests,
        config.app.limits.rate_limits.query_rps,
        config.app.limits.rate_limits.ingest_rps,
        StoreBudget::from_hard_limit(
            config.app.limits.store.max_bytes,
            config.app.limits.store.soft_limit_percent,
        ),
    ));

    let js_pool = Arc::new(JsRuntimePool::new(&config, store.clone())?);
    health.set_scripts_loaded(true);

    let (sweep_tx, mut sweep_rx) = mpsc::unbounded_channel();
    let retention_handle = spawn_retention_sweeper(
        store.clone(),
        Duration::from_secs(1),
        config.app.limits.store.tombstone_ttl_seconds,
        Some(sweep_tx),
    );

    let retention_metrics = metrics.clone();
    tokio::spawn(async move {
        while let Some(sweep) = sweep_rx.recv().await {
            retention_metrics.observe_retention_sweep(sweep);
        }
    });

    let ingestion_handle = ingestion::start(
        config.clone(),
        js_pool.clone(),
        store.clone(),
        limits.clone(),
        metrics.clone(),
    )
    .await
    .context("failed starting ingestion")?;
    if let Some(handle) = &ingestion_handle {
        let health_state = health.clone();
        let ingestion_health = handle.health.clone();
        tokio::spawn(async move {
            loop {
                health_state.set_kafka_ready(ingestion_health.is_ready());
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    } else {
        health.set_kafka_ready(true);
    }

    let state = RuntimeState {
        config: config.clone(),
        store: store.clone(),
        js_pool: js_pool.clone(),
        limits: limits.clone(),
        metrics: metrics.clone(),
        health: health.clone(),
    };

    let app_router = build_router(state.clone())?;
    let app_listener = TcpListener::bind(&cli.bind)
        .await
        .with_context(|| format!("failed to bind edge HTTP listener on {}", cli.bind))?;

    let metrics_router = Router::new()
        .route("/metrics", get(admin::metrics_handler))
        .with_state(state);
    let metrics_listener = TcpListener::bind(&cli.metrics_bind)
        .await
        .with_context(|| format!("failed to bind metrics listener on {}", cli.metrics_bind))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut app_server = tokio::spawn(serve_with_shutdown(
        app_listener,
        app_router,
        shutdown_rx.clone(),
    ));
    let mut metrics_server = tokio::spawn(serve_with_shutdown(
        metrics_listener,
        metrics_router,
        shutdown_rx.clone(),
    ));

    info!(
        "runtime ready app={} version={} bind={} metrics_bind={}",
        config.app.app.name, config.app.app.version, cli.bind, cli.metrics_bind
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received shutdown signal");
        }
        result = &mut app_server => {
            match result {
                Ok(Ok(())) => info!("edge server exited"),
                Ok(Err(err)) => error!("edge server error: {err:#}"),
                Err(err) => error!("edge server task failed: {err}"),
            }
        }
        result = &mut metrics_server => {
            match result {
                Ok(Ok(())) => info!("metrics server exited"),
                Ok(Err(err)) => error!("metrics server error: {err:#}"),
                Err(err) => error!("metrics server task failed: {err}"),
            }
        }
    }

    let _ = shutdown_tx.send(true);

    if let Some(handle) = ingestion_handle {
        handle.stop().await;
    }
    retention_handle.stop().await;
    health.set_kafka_ready(false);
    health.set_scripts_loaded(false);
    health.set_store_ready(false);

    Ok(())
}

async fn serve_with_shutdown(
    listener: TcpListener,
    router: Router,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await?;
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .without_time()
        .init();
}
