use crate::domain::ids::{NodeId, NodeSlot, ResumeToken};
use crate::domain::outcome::{NodeOutcome, Resume};
use crate::domain::snapshot::ExecutionSnapshot;
use tokio::sync::{mpsc, oneshot};

/// Waker analog: everything that can wake the apply loop arrives as an [`Event`].
#[derive(Debug)]
pub(crate) enum Event {
    Start,
    /// Rebuild from a snapshot. Does not apply [`ApplyCmd::Start`].
    Restore,
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
    /// Same [`crate::domain::state::ApplyCmd::Cancel`] as handle Drop.
    /// [`None`] reply is fire-and-forget (handle cancel). [`Some`] waits
    /// for persist-then-emit so [`crate::Runtime::cancel`] is not Ok on
    /// persist Err.
    Cancel {
        reply: Option<oneshot::Sender<Result<(), crate::domain::state::ApplyError>>>,
    },
    Inspect {
        reply: oneshot::Sender<ExecutionSnapshot>,
    },
    Timer {
        node_id: NodeId,
    },
    ForceCancelBound,
    /// Extend the store lease. Not a domain apply.
    Heartbeat,
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
/// Runtime drive loop holds the receiver and another sender (cancel-bound
/// timer). Each execute task holds a sender for `NodeFinished`.
/// Last sender drop closes the channel; `recv` then yields `Shutdown`.
pub(crate) type EventTx = mpsc::UnboundedSender<Event>;
pub(crate) type EventRx = mpsc::UnboundedReceiver<Event>;

pub(crate) fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}

/// Inject Complete / Reinvoke into a live drive. Same path as
/// [`crate::ExecutionHandle::resume`].
pub(crate) async fn inject_resume(
    tx: &EventTx,
    token: ResumeToken,
    resume: Resume,
) -> Result<(), crate::domain::state::ApplyError> {
    let (reply, rx) = oneshot::channel();
    tx.send(Event::Resume {
        token,
        resume,
        reply,
    })
    .map_err(|_| crate::domain::state::ApplyError::Illegal("execution scheduler stopped".into()))?;
    rx.await.map_err(|_| {
        crate::domain::state::ApplyError::Illegal("execution scheduler stopped".into())
    })?
}

/// Inject Cancel into a live drive. Same [`Event::Cancel`] apply as
/// [`crate::ExecutionHandle::cancel`]; waits for persist-then-emit.
pub(crate) async fn inject_cancel(tx: &EventTx) -> Result<(), crate::domain::state::ApplyError> {
    let (reply, rx) = oneshot::channel();
    tx.send(Event::Cancel { reply: Some(reply) }).map_err(|_| {
        crate::domain::state::ApplyError::Illegal("execution scheduler stopped".into())
    })?;
    rx.await.map_err(|_| {
        crate::domain::state::ApplyError::Illegal("execution scheduler stopped".into())
    })?
}
