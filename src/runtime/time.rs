use std::sync::Arc;
use std::time::Duration;

pub use crate::domain::time::Timestamp;

#[async_trait::async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    /// Executor-facing (`ExecutionContext::sleep`). Not the scheduler.
    async fn sleep(&self, duration: Duration);
    /// Runtime drive loop only: wait until Instant T. Apply never calls this.
    async fn wait_until(&self, when: Timestamp) {
        let now = self.now();
        if when <= now {
            return;
        }
        self.sleep(when.saturating_duration_since(now)).await;
    }
}

#[derive(Clone, Debug, Default)]
pub struct SystemClock;

#[async_trait::async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now_system()
    }

    async fn sleep(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        tokio::time::sleep(duration).await;
    }
}

pub type SharedClock = Arc<dyn Clock>;
