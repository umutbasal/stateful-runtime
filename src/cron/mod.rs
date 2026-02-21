use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

use crate::config::LoadedConfig;
use crate::js::JsRuntimePool;
use crate::store::Store;

pub struct CronHandle {
    stop_tx: watch::Sender<bool>,
    join_handles: Vec<JoinHandle<()>>,
}

impl CronHandle {
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        for handle in self.join_handles {
            let _ = handle.await;
        }
    }
}

pub fn spawn_cron_tasks(
    config: Arc<LoadedConfig>,
    js_pool: Arc<JsRuntimePool>,
    store: Arc<Store>,
) -> Option<CronHandle> {
    if config.app.crons.is_empty() {
        return None;
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let mut join_handles = Vec::new();

    for cron in config.app.crons.clone() {
        let mut cron_stop_rx = stop_rx.clone();
        let cron_js_pool = js_pool.clone();
        let cron_store = store.clone();

        join_handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(cron.interval_seconds));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            info!(
                "cron task started name={} interval_seconds={} handler={}",
                cron.name, cron.interval_seconds, cron.handler
            );

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match cron_js_pool.execute_cron(&cron.handler, &cron.name).await {
                            Ok(ops) => {
                                if let Err(err) = cron_store.apply_ops(&ops) {
                                    error!("cron '{}' apply_ops failed: {err:#}", cron.name);
                                }
                            }
                            Err(err) => {
                                error!("cron '{}' execution failed: {err:#}", cron.name);
                            }
                        }
                    }
                    changed = cron_stop_rx.changed() => {
                        if changed.is_ok() && *cron_stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }));
    }

    Some(CronHandle {
        stop_tx,
        join_handles,
    })
}
