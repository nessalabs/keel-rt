use crate::domain::ids::{NodeId, ResumeToken};
use crate::domain::outcome::{NodeOutcome, Resume};
use crate::domain::snapshot::ExecutionSnapshot;
use tokio::sync::{mpsc, oneshot};

/// Waker analog: everything that can wake the apply loop arrives as an [`Event`].
#[derive(Debug)]
pub(crate) enum Event {
    Start,
    NodeFinished {
        node_id: NodeId,
        attempt: u32,
        result: Result<NodeOutcome, JoinKind>,
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

#[derive(Debug)]
pub(crate) enum JoinKind {
    Panic(String),
    Cancelled,
}

pub(crate) type EventTx = mpsc::UnboundedSender<Event>;
pub(crate) type EventRx = mpsc::UnboundedReceiver<Event>;

pub(crate) fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}
