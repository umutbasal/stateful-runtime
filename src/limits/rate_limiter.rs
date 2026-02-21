use std::sync::Mutex;
use std::time::Instant;

use dashmap::DashMap;

#[derive(Debug)]
struct Bucket {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl Bucket {
    fn new(rps: usize) -> Self {
        let capacity = rps.max(1) as f64;
        Self {
            capacity,
            tokens: capacity,
            refill_per_second: capacity,
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct RateLimiter {
    default_rps: usize,
    buckets: DashMap<String, Mutex<Bucket>>,
}

impl RateLimiter {
    pub fn new(default_rps: usize) -> Self {
        Self {
            default_rps,
            buckets: DashMap::new(),
        }
    }

    pub fn allow(&self, key: &str) -> bool {
        if self.default_rps == 0 {
            return true;
        }

        let bucket = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| Mutex::new(Bucket::new(self.default_rps)));
        let mut bucket = bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        bucket.allow()
    }
}
