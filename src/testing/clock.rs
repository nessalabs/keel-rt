use crate::runtime::time::{Clock, Timestamp};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;

/// Paused clock / advance — Tokio `test-util` analog for retry delays.
/// Waiting tests do not need a timer; only retry-delay tests call [`FakeClock::advance`].
///
/// [`Self::live_sleeps`] counts in-flight [`Clock::sleep`](crate::Clock::sleep)
/// futures (Runtime `wait_until` + executor Delay). Drop of the handle
/// must bring this to 0.
#[derive(Debug)]
pub struct FakeClock {
    now: Mutex<Timestamp>,
    paused: AtomicBool,
    tick: Notify,
    live_sleeps: AtomicUsize,
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            now: Mutex::new(Timestamp(0)),
            paused: AtomicBool::new(true),
            tick: Notify::new(),
            live_sleeps: AtomicUsize::new(0),
        }
    }

    /// In-flight [`Clock::sleep`](crate::Clock::sleep) futures. Runtime
    /// `wait_until` and `timeout_after` / Delay each hold one until the
    /// sleeper is dropped.
    pub fn live_sleeps(&self) -> usize {
        self.live_sleeps.load(Ordering::SeqCst)
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
        self.live_sleeps.fetch_add(1, Ordering::SeqCst);
        struct LiveSleep<'a>(&'a AtomicUsize);
        impl Drop for LiveSleep<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _live = LiveSleep(&self.live_sleeps);
        if !self.paused.load(Ordering::SeqCst) {
            tokio::time::sleep(duration).await;
            self.advance(duration);
            return;
        }
        let target = self.now().saturating_add(duration);
        loop {
            // Subscribe before re-checking now(): an advance between a
            // failed check and notified() would otherwise be lost.
            let notified = self.tick.notified();
            if self.now() >= target {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sleep_does_not_lose_advance_notify() {
        for i in 0..200 {
            let clock = Arc::new(FakeClock::new());
            let sleeper_clock = clock.clone();
            let sleeper = tokio::spawn(async move {
                sleeper_clock.sleep(Duration::from_millis(5)).await;
            });
            let advancer_clock = clock.clone();
            let advancer = tokio::spawn(async move {
                for _ in 0..20 {
                    advancer_clock.advance(Duration::from_millis(1));
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::timeout(Duration::from_secs(2), sleeper)
                .await
                .unwrap_or_else(|_| panic!("lost FakeClock wakeup on iter {i}"))
                .expect("sleeper join");
            advancer.await.expect("advancer join");
            assert_eq!(
                clock.live_sleeps(),
                0,
                "iter {i}: sleeper Drop must release live_sleeps"
            );
        }
    }
}
