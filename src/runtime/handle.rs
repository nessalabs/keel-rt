use crate::domain::ids::{ExecutionId, ResumeToken};
use crate::domain::outcome::Resume;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::{ApplyError, ExecutionState};
use crate::runtime::inject::{self, Event, EventTx};
use crate::runtime::store::{ClaimError, OwnerId, StateStore};
use crate::runtime::time::Clock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Live drives on one Runtime: id → inbox so [`crate::Runtime::complete`]
/// can inject without a handle.
pub(crate) type ActiveSet = HashMap<ExecutionId, EventTx>;

/// Shared claim / forget_live gate used by [`crate::Runtime::complete`] and
/// [`ExecutionHandle::resume`]. One path — do not fork a second machine.
pub(crate) struct LeaseGate {
    store: Arc<dyn StateStore>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    active: Arc<Mutex<ActiveSet>>,
}

impl LeaseGate {
    pub(crate) fn new(
        store: Arc<dyn StateStore>,
        owner: OwnerId,
        clock: Arc<dyn Clock>,
        active: Arc<Mutex<ActiveSet>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            owner,
            clock,
            active,
        })
    }

    pub(crate) fn live_tx(&self, id: &ExecutionId) -> Option<EventTx> {
        self.active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }

    pub(crate) fn forget_live(&self, id: &ExecutionId) {
        self.active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id);
    }

    /// Refresh this owner's lease, or refuse and drop live_tx.
    pub(crate) async fn claim_or_forget(&self, id: &ExecutionId) -> Result<(), ClaimError> {
        match self.store.claim(id, &self.owner, self.clock.now()).await {
            Ok(_) => Ok(()),
            Err(ClaimError::ClaimedElsewhere) => {
                self.forget_live(id);
                Err(ClaimError::ClaimedElsewhere)
            }
            Err(e) => Err(e),
        }
    }
}

/// Live handle to one execution.
///
/// - [`wait`](Self::wait) — block until **terminal** (`Succeeded` / `Failed` /
///   `Cancelled` / `Completed`). Consumes the handle so Drop does not cancel.
/// - [`wait_stable`](Self::wait_stable) — returns on terminal **or**
///   [`Waiting`](crate::ExecutionState::Waiting). Waiting is not done; call
///   [`resume`](Self::resume) then `wait`.
/// - **Drop cancels** (JoinSet semantics). It does not detach. Hold the handle
///   (or call `wait`) until you mean to cancel.
///
/// Prefer [`crate::Runtime::run`] when you only need the final state.
#[must_use = "dropping ExecutionHandle cancels the execution"]
pub struct ExecutionHandle {
    pub(crate) execution_id: ExecutionId,
    pub(crate) tx: EventTx,
    pub(crate) cancel: CancellationToken,
    pub(crate) state: watch::Receiver<ExecutionState>,
    pub(crate) dropped: Arc<AtomicBool>,
    pub(crate) consumed: bool,
    /// Unregisters this id from [`crate::Runtime`] on Drop (structural cleanup).
    pub(crate) _active: ActiveGuard,
    pub(crate) lease: Arc<LeaseGate>,
}

/// One live handle per execution id on a Runtime. Drop removes the id.
pub(crate) struct ActiveGuard {
    id: ExecutionId,
    active: Arc<Mutex<ActiveSet>>,
}

impl ActiveGuard {
    pub(crate) fn new(id: ExecutionId, active: Arc<Mutex<ActiveSet>>) -> Self {
        Self { id, active }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut g = self.active.lock().unwrap_or_else(|p| p.into_inner());
        g.remove(&self.id);
    }
}

impl ExecutionHandle {
    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub async fn cancel(&self) {
        self.cancel.cancel();
        let _ = self.tx.send(Event::Cancel);
    }

    pub async fn resume(&self, token: ResumeToken, resume: Resume) -> Result<(), ApplyError> {
        match self.lease.claim_or_forget(token.execution_id()).await {
            Ok(()) => inject::inject_resume(&self.tx, token, resume).await,
            Err(ClaimError::ClaimedElsewhere) => {
                Err(ApplyError::Illegal("execution claimed elsewhere".into()))
            }
            Err(ClaimError::Store(s)) => Err(ApplyError::Illegal(s.to_string())),
        }
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
    /// [`wait_stable`](Self::wait_stable) then [`resume`](Self::resume).
    ///
    /// If the scheduler task dies without publishing a terminal state (bug in
    /// a port, e.g. `Clock::now` panics), this returns [`Cancelled`] — the same
    /// signal [`inspect`](Self::inspect) uses for a stopped run. It does not
    /// return a leftover `Created` / `Running` / `Waiting` watch value.
    pub async fn wait(mut self) -> ExecutionState {
        self.consumed = true;
        loop {
            let current = *self.state.borrow();
            if current.is_terminal() {
                return current;
            }
            if self.state.changed().await.is_err() {
                return state_after_watch_closed(*self.state.borrow());
            }
        }
    }

    /// Wait until terminal **or** Waiting (executor yield).
    /// Waiting is not a successful finish — resume, then [`wait`](Self::wait).
    pub async fn wait_stable(&self) -> ExecutionState {
        let mut state = self.state.clone();
        loop {
            let current = *state.borrow();
            if current.is_terminal() || current == ExecutionState::Waiting {
                return current;
            }
            if state.changed().await.is_err() {
                return state_after_watch_closed(*state.borrow());
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

fn state_after_watch_closed(current: ExecutionState) -> ExecutionState {
    if current.is_terminal() {
        current
    } else {
        ExecutionState::Cancelled
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
        definition_hash: crate::domain::ids::DefinitionHash::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::store::MemoryStore;
    use crate::runtime::time::SystemClock;

    #[test]
    fn execution_id_is_the_stored_id() {
        let id = ExecutionId::parse("exec-handle").unwrap();
        let (tx, _rx) = crate::runtime::inject::channel();
        let (state_tx, state_rx) = watch::channel(ExecutionState::Created);
        drop(state_tx);
        let handle = ExecutionHandle {
            execution_id: id.clone(),
            tx,
            cancel: CancellationToken::new(),
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: true,
            _active: ActiveGuard::new(id.clone(), Arc::new(Mutex::new(HashMap::new()))),
            lease: LeaseGate::new(
                Arc::new(MemoryStore::new()),
                OwnerId::new(),
                Arc::new(SystemClock),
                Arc::new(Mutex::new(HashMap::new())),
            ),
        };
        assert_eq!(handle.execution_id(), &id);
    }

    #[test]
    fn active_guard_recovers_from_poison_and_unregisters() {
        let id = ExecutionId::parse("exec-active").unwrap();
        let set = Arc::new(Mutex::new(HashMap::from([(
            id.clone(),
            crate::runtime::inject::channel().0,
        )])));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = set.lock().unwrap();
            panic!("poison active set");
        }));
        assert!(poisoned.is_err());
        {
            let guard = ActiveGuard::new(id.clone(), set.clone());
            drop(guard);
        }
        assert!(!set
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&id));
    }

    #[test]
    fn watch_closed_keeps_terminal_and_maps_live_to_cancelled() {
        assert_eq!(
            state_after_watch_closed(ExecutionState::Succeeded),
            ExecutionState::Succeeded
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Failed),
            ExecutionState::Failed
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Completed),
            ExecutionState::Completed
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Cancelled),
            ExecutionState::Cancelled
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Running),
            ExecutionState::Cancelled
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Created),
            ExecutionState::Cancelled
        );
        assert_eq!(
            state_after_watch_closed(ExecutionState::Waiting),
            ExecutionState::Cancelled
        );
    }
}
