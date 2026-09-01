use crate::domain::ids::NodeId;
use crate::runtime::inject::{Event, EventRx};
use crate::runtime::time::{Clock, Timestamp};
use std::sync::Arc;

/// Waits for the next injected event or a snapshot deadline T.
/// Tests pass a [`Clock`] (FakeClock) so they do not need a real timer driver.
/// `when <= now` returns [`Event::Timer`] immediately (no wall sleep).
pub(crate) struct ChannelPark {
    rx: EventRx,
    clock: Arc<dyn Clock>,
}

impl ChannelPark {
    pub(crate) fn new(rx: EventRx, clock: Arc<dyn Clock>) -> Self {
        Self { rx, clock }
    }

    pub(crate) async fn recv(&mut self, next_timer: Option<(Timestamp, NodeId)>) -> Event {
        match next_timer {
            Some((when, node_id)) => {
                let now = self.clock.now();
                if when <= now {
                    return Event::Timer { node_id };
                }
                let wait = when.saturating_duration_since(now);
                // Inbox first, then timer. If the deadline is also due, the
                // next park sees `when <= now` and fires Timer without waiting.
                tokio::select! {
                    biased;
                    ev = self.rx.recv() => ev.unwrap_or(Event::Shutdown),
                    _ = self.clock.sleep(wait) => Event::Timer { node_id },
                }
            }
            None => self.rx.recv().await.unwrap_or(Event::Shutdown),
        }
    }
}
