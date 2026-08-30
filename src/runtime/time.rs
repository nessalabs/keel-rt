use std::sync::Arc;
use std::time::Duration;

pub use crate::domain::time::Timestamp;

#[async_trait::async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn sleep(&self, duration: Duration);
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
