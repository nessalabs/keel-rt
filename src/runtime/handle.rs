use crate::domain::ids::ResumeToken;
use crate::domain::outcome::Resume;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::{ApplyError, ExecutionState};
use crate::runtime::inject::{Event, EventTx};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Live handle to one execution.
///
/// - [`wait`](Self::wait) — block until **terminal** (`Succeeded` / `Failed` /
///   `Cancelled` / `Completed`). Consumes the handle so Drop does not cancel.
/// - [`wait_stable`](Self::wait_stable) — HITL wait: returns on terminal **or**
///   [`Waiting`](crate::ExecutionState::Waiting). Waiting is not done; call
///   [`resume`](Self::resume) then `wait`.
/// - **Drop cancels** (JoinSet semantics). It does not detach. Hold the handle
///   (or call `wait`) until you mean to cancel.
///
/// Prefer [`crate::Runtime::run`] when you only need the final state.
#[must_use = "dropping ExecutionHandle cancels the execution"]
pub struct ExecutionHandle {
    pub(crate) tx: EventTx,
    pub(crate) cancel: CancellationToken,
    pub(crate) state: watch::Receiver<ExecutionState>,
    pub(crate) dropped: Arc<AtomicBool>,
    pub(crate) consumed: bool,
}

impl ExecutionHandle {
    pub async fn cancel(&self) {
        self.cancel.cancel();
        let _ = self.tx.send(Event::Cancel);
    }

    pub async fn resume(&self, token: ResumeToken, resume: Resume) -> Result<(), ApplyError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Event::Resume {
                token,
                resume,
                reply,
            })
            .map_err(|_| ApplyError::Illegal("execution scheduler stopped".into()))?;
        rx.await
            .map_err(|_| ApplyError::Illegal("execution scheduler stopped".into()))?
    }

    pub async fn inspect(&self) -> ExecutionSnapshot {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Event::Inspect { reply }).is_err() {
            return empty_snapshot();
        }
        rx.await.unwrap_or_else(|_| empty_snapshot())
    }

    /// Wait until the execution is terminal (`Succeeded`, `Failed`,
    /// `Cancelled`, or `Completed`). Consumes the handle so Drop does not cancel.
    ///
    /// Does **not** return on [`Waiting`](crate::ExecutionState::Waiting) — use
    /// [`wait_stable`](Self::wait_stable) for HITL.
    pub async fn wait(mut self) -> ExecutionState {
        self.consumed = true;
        loop {
            let current = *self.state.borrow();
            if current.is_terminal() {
                return current;
            }
            if self.state.changed().await.is_err() {
                return *self.state.borrow();
            }
        }
    }

    /// Wait until terminal **or** Waiting (HITL / executor yield).
    /// Waiting is not a successful finish — resume, then [`wait`](Self::wait).
    pub async fn wait_stable(&self) -> ExecutionState {
        let mut state = self.state.clone();
        loop {
            let current = *state.borrow();
            if current.is_terminal() || current == ExecutionState::Waiting {
                return current;
            }
            if state.changed().await.is_err() {
                return *state.borrow();
            }
        }
    }
}

impl Drop for ExecutionHandle {
    fn drop(&mut self) {
        if !self.consumed {
            self.dropped.store(true, Ordering::SeqCst);
            self.cancel.cancel();
            let _ = self.tx.send(Event::Cancel);
        }
        let _ = self.tx.send(Event::Shutdown);
    }
}

fn empty_snapshot() -> ExecutionSnapshot {
    use crate::domain::ids::{ExecutionId, WorkflowId};
    use crate::domain::snapshot::SCHEMA_VERSION;
    ExecutionSnapshot {
        schema_version: SCHEMA_VERSION,
        revision: 0,
        execution_id: ExecutionId::new(),
        workflow_id: WorkflowId::new("stopped"),
        state: ExecutionState::Cancelled,
        nodes: Default::default(),
        node_order: Vec::new(),
    }
}
