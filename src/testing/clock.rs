use crate::runtime::time::{Clock, Timestamp};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;

/// Paused clock / advance — Tokio `test-util` analog for retry delays.
/// Waiting tests do not need a timer; only retry-delay tests call [`FakeClock::advance`].
#[derive(Debug)]
pub struct FakeClock {
    now: Mutex<Timestamp>,
    paused: AtomicBool,
    tick: Notify,
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            now: Mutex::new(Timestamp(0)),
            paused: AtomicBool::new(true),
            tick: Notify::new(),
        }
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.tick.notify_waiters();
    }

    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().expect("fake clock");
        *now = now.saturating_add(duration);
        drop(now);
        self.tick.notify_waiters();
    }

    pub fn set(&self, ts: Timestamp) {
        *self.now.lock().expect("fake clock") = ts;
        self.tick.notify_waiters();
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        *self.now.lock().expect("fake clock")
    }

    async fn sleep(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        if !self.paused.load(Ordering::SeqCst) {
            tokio::time::sleep(duration).await;
            self.advance(duration);
            return;
        }
        let target = self.now().saturating_add(duration);
        loop {
            if self.now() >= target {
                return;
            }
            self.tick.notified().await;
        }
    }
}
