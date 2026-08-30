use crate::runtime::inject::{Event, EventRx};
use crate::runtime::time::{Clock, Timestamp};
use crate::domain::ids::NodeId;
use std::sync::Arc;

/// Park waits for the next injected event or a retry deadline.
/// Production uses mpsc + time. Tests pass a [`Clock`] (FakeClock) so they
/// do not need a real timer driver.
pub(crate) trait Park: Send {
    #[allow(async_fn_in_trait)]
    async fn recv(&mut self, next_timer: Option<(Timestamp, NodeId)>) -> Option<Event>;
}

pub(crate) struct ChannelPark {
    rx: EventRx,
    clock: Arc<dyn Clock>,
}

impl ChannelPark {
    pub(crate) fn new(rx: EventRx, clock: Arc<dyn Clock>) -> Self {
        Self { rx, clock }
    }
}

impl Park for ChannelPark {
    async fn recv(&mut self, next_timer: Option<(Timestamp, NodeId)>) -> Option<Event> {
        match next_timer {
            Some((when, node_id)) => {
                let now = self.clock.now();
                if when <= now {
                    return Some(Event::Timer { node_id });
                }
                let wait = when.saturating_duration_since(now);
                tokio::select! {
                    ev = self.rx.recv() => ev,
                    _ = self.clock.sleep(wait) => Some(Event::Timer { node_id }),
                }
            }
            None => self.rx.recv().await,
        }
    }
}

/// Test park: same channel, clock-driven timers. No epoll required.
#[allow(dead_code)]
pub(crate) type FakePark = ChannelPark;
