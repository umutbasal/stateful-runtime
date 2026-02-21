mod rate_limiter;

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::store::Store;
pub use rate_limiter::RateLimiter;

#[derive(Debug, Error)]
pub enum LimitError {
    #[error("request rate limit exceeded")]
    RequestRateExceeded,
    #[error("ingestion rate limit exceeded")]
    IngestionRateExceeded,
    #[error("concurrency limit exceeded")]
    ConcurrencyExceeded,
    #[error("store memory soft limit exceeded")]
    StoreSoftLimitExceeded,
    #[error("store memory hard limit exceeded")]
    StoreHardLimitExceeded,
}

#[derive(Debug, Clone)]
pub struct StoreBudget {
    pub soft_limit_bytes: usize,
    pub hard_limit_bytes: usize,
}

impl StoreBudget {
    pub fn from_hard_limit(hard_limit_bytes: usize, soft_limit_percent: u8) -> Self {
        let soft_limit_bytes = if soft_limit_percent == 0 {
            hard_limit_bytes
        } else {
            hard_limit_bytes.saturating_mul(soft_limit_percent as usize) / 100
        };
        Self {
            soft_limit_bytes,
            hard_limit_bytes,
        }
    }
}

pub struct RuntimeLimits {
    request_limiter: RateLimiter,
    ingest_limiter: RateLimiter,
    request_semaphore: Arc<Semaphore>,
    store_budget: StoreBudget,
}

impl RuntimeLimits {
    pub fn new(
        max_concurrent_requests: usize,
        query_rps: usize,
        ingest_rps: usize,
        store_budget: StoreBudget,
    ) -> Self {
        Self {
            request_limiter: RateLimiter::new(query_rps),
            ingest_limiter: RateLimiter::new(ingest_rps),
            request_semaphore: Arc::new(Semaphore::new(max_concurrent_requests.max(1))),
            store_budget,
        }
    }

    pub fn check_request_rate(&self, endpoint_key: &str) -> Result<(), LimitError> {
        if self.request_limiter.allow(endpoint_key) {
            Ok(())
        } else {
            Err(LimitError::RequestRateExceeded)
        }
    }

    pub fn check_ingestion_rate(&self, stream_key: &str) -> Result<(), LimitError> {
        if self.ingest_limiter.allow(stream_key) {
            Ok(())
        } else {
            Err(LimitError::IngestionRateExceeded)
        }
    }

    pub fn try_acquire_request_permit(&self) -> Result<OwnedSemaphorePermit, LimitError> {
        self.request_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| LimitError::ConcurrencyExceeded)
    }

    pub fn check_store_budget(&self, store: &Store) -> Result<(), LimitError> {
        let usage = store.current_memory_bytes();
        if usage > self.store_budget.hard_limit_bytes {
            return Err(LimitError::StoreHardLimitExceeded);
        }
        if usage > self.store_budget.soft_limit_bytes {
            return Err(LimitError::StoreSoftLimitExceeded);
        }
        Ok(())
    }
}
