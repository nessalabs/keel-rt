use crate::domain::ids::{NodeId, NodeSlot, ResumeToken};
use crate::domain::outcome::{NodeOutcome, Resume};
use crate::domain::snapshot::ExecutionSnapshot;
use tokio::sync::{mpsc, oneshot};

/// Waker analog: everything that can wake the apply loop arrives as an [`Event`].
#[derive(Debug)]
pub(crate) enum Event {
    Start,
    NodeFinished {
        slot: NodeSlot,
        node_id: NodeId,
        attempt: u32,
        /// `Err` is a panic payload string from the execute task.
        result: Result<NodeOutcome, String>,
    },
    Resume {
        token: ResumeToken,
        resume: Resume,
        reply: oneshot::Sender<Result<(), crate::domain::state::ApplyError>>,
    },
    Cancel,
    Inspect {
        reply: oneshot::Sender<ExecutionSnapshot>,
    },
    Timer {
        node_id: NodeId,
    },
    ForceCancelBound,
    /// Handle dropped (after wait or cancel). Ends the apply loop.
    Shutdown,
}

/// Apply-loop inbox. Unbounded so execute tasks and handle ops never wait on
/// apply (a bounded channel can deadlock `resume` / `inspect`). Capacity is
/// implicit: in-flight executes ≤ concurrency, plus handle messages. See
/// `docs/adr/0001-unbounded-apply-inbox.md`.
///
/// Ownership: [`crate::runtime::handle::ExecutionHandle`] holds a sender clone
/// (Drop sends `Cancel` if not consumed, then always `Shutdown`). The
/// scheduler task holds the receiver (`ChannelPark`) and another sender
/// (cancel-bound timer). Each execute task holds a sender for `NodeFinished`.
/// Last sender drop closes the channel; `recv` then yields `Shutdown`.
pub(crate) type EventTx = mpsc::UnboundedSender<Event>;
pub(crate) type EventRx = mpsc::UnboundedReceiver<Event>;

pub(crate) fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}
