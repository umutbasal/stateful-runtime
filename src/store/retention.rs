use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::store::{now_millis, RetentionSweepResult, Store};

pub struct RetentionTaskHandle {
    stop_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

impl RetentionTaskHandle {
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join_handle.await;
    }
}

pub fn spawn_retention_sweeper(
    store: Arc<Store>,
    interval: Duration,
    tombstone_ttl_seconds: u64,
    sweep_events: Option<mpsc::UnboundedSender<RetentionSweepResult>>,
) -> RetentionTaskHandle {
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let join_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = store.sweep_retention(now_millis(), tombstone_ttl_seconds);
                    if let Some(events) = &sweep_events {
                        let _ = events.send(result);
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_ok() && *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    RetentionTaskHandle {
        stop_tx,
        join_handle,
    }
}
