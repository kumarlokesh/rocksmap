//! Clock abstraction for TTL expiry.
//!
//! Expiry deadlines are wall-clock (UNIX epoch milliseconds): a deadline must survive process
//! restarts and be comparable across the foreground and background (compaction) threads, which
//! a monotonic `Instant` cannot do. The clock is injected behind [`Clock`] so expiry is
//! deterministically testable via [`ManualClock`].
//!
//! Wall-clock caveat: TTL timing follows the system clock, so NTP adjustments or manual clock
//! changes shift expiry. TTL is a retention/eviction mechanism, not a precise timer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of "now" in UNIX epoch milliseconds.
pub trait Clock: Send + Sync {
    /// Current time as milliseconds since the UNIX epoch.
    fn now_unix_millis(&self) -> u64;
}

/// The default clock, backed by the system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A manually controlled clock for deterministic tests.
///
/// Cloning shares the same underlying time, so a test can hold one handle and advance the clock
/// that a [`TtlRocksMap`](crate::TtlRocksMap) was opened with.
#[derive(Debug, Clone, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    /// Create a clock fixed at `start_millis`.
    pub fn new(start_millis: u64) -> Self {
        ManualClock(Arc::new(AtomicU64::new(start_millis)))
    }

    /// Set the current time to `millis`.
    pub fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }

    /// Advance the current time by `millis`.
    pub fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
