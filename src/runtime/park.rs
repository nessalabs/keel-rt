use crate::domain::ids::NodeId;
use crate::runtime::inject::{Event, EventRx};
use crate::runtime::time::{Clock, Timestamp};
use std::sync::Arc;

/// Waits for the next injected event or a snapshot deadline T.
/// Tests pass a [`Clock`] (FakeClock) so they do not need a real timer driver.
/// `when <= now` still prefers the inbox (Cancel / Shutdown) over Timer.
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
                    // Same order as the select: inbox first. Returning Timer
                    // here without try_recv let a queued Cancel lose when T
                    // was already due (resume + cancel the same instant).
                    return match self.rx.try_recv() {
                        Ok(ev) => ev,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            Event::Shutdown
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                            Event::Timer { node_id }
                        }
                    };
                }
                let wait = when.saturating_duration_since(now);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::inject;
    use crate::runtime::time::SystemClock;

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_prefers_queued_cancel() {
        let (tx, rx) = inject::channel();
        let mut park = ChannelPark::new(rx, Arc::new(SystemClock));
        let _ = tx.send(inject::Event::Cancel);
        let ev = park.recv(Some((Timestamp(0), NodeId::new("a")))).await;
        assert!(
            matches!(ev, inject::Event::Cancel),
            "inbox must beat due Timer, got {ev:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_empty_inbox_is_timer() {
        let (_tx, rx) = inject::channel();
        let mut park = ChannelPark::new(rx, Arc::new(SystemClock));
        match park.recv(Some((Timestamp(0), NodeId::new("n")))).await {
            inject::Event::Timer { node_id } => assert_eq!(node_id.as_str(), "n"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_disconnected_inbox_is_shutdown() {
        let (tx, rx) = inject::channel();
        let mut park = ChannelPark::new(rx, Arc::new(SystemClock));
        drop(tx);
        assert!(matches!(
            park.recv(Some((Timestamp(0), NodeId::new("a")))).await,
            inject::Event::Shutdown
        ));
    }
}
