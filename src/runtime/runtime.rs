use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, ExecutorId, NodeId, ResumeToken};
use crate::domain::outcome::{NodeOutcome, Recover, Resume};
use crate::domain::policy::{AcceptPolicy, Policy};
use crate::domain::snapshot::{ExecutionSnapshot, SnapshotError};
use crate::domain::state::{ApplyCmd, ApplyError, Execution, ExecutionState};
use crate::domain::time::Timestamp;
use crate::runtime::executor::{ExecutionContext, Executor, ExecutorRegistry, FunctionExecutor};
use crate::runtime::handle::{ActiveGuard, ActiveSet, ExecutionHandle, LeaseGate};
use crate::runtime::inject::{self, Event, EventRx, EventTx};
use crate::runtime::scheduler::Scheduler;
use crate::runtime::sink::{EventSink, NoopSink};
use crate::runtime::store::{ClaimError, MemoryStore, OwnerId, StateStore, StoreError};
use crate::runtime::time::{Clock, SystemClock};
use crate::runtime::wait::Wait;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

/// `Runtime::start` / `run` rejected the definition before any node ran.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StartError {
    #[error("unregistered executor id(s): {0}")]
    UnregisteredExecutors(UnregisteredExecutors),
}

/// `Runtime::resume` rejected the snapshot before any node ran.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResumeError {
    #[error("unknown execution")]
    UnknownExecution,
    #[error("execution is already active on this runtime")]
    AlreadyActive,
    #[error("execution claimed elsewhere")]
    ClaimedElsewhere,
    #[error("workflow definition missing for snapshot")]
    DefinitionMissing,
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("unregistered executor id(s): {0}")]
    UnregisteredExecutors(UnregisteredExecutors),
    #[error(transparent)]
    Store(#[from] StoreError),
    /// [`Recover::RetryFailed`] requires Failed or Completed-with-failures.
    #[error("execution is not Failed or Completed-with-failures")]
    NotFailed,
}

impl From<ClaimError> for ResumeError {
    fn from(e: ClaimError) -> Self {
        match e {
            ClaimError::ClaimedElsewhere => Self::ClaimedElsewhere,
            ClaimError::Store(s) => Self::Store(s),
        }
    }
}

/// [`Runtime::complete`] rejected the token or could not apply it.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CompleteError {
    #[error("unknown resume token")]
    UnknownToken,
    #[error("execution is cancelled")]
    Cancelled,
    #[error(transparent)]
    Apply(#[from] ApplyError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("unregistered executor id(s): {0}")]
    UnregisteredExecutors(UnregisteredExecutors),
    #[error("execution claimed elsewhere")]
    ClaimedElsewhere,
}

impl From<ClaimError> for CompleteError {
    fn from(e: ClaimError) -> Self {
        match e {
            ClaimError::ClaimedElsewhere => Self::ClaimedElsewhere,
            ClaimError::Store(s) => Self::Store(s),
        }
    }
}

/// Unknown [`ExecutorId`]s named by the definition. Display is a comma-separated list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisteredExecutors(pub Vec<ExecutorId>);

impl std::fmt::Display for UnregisteredExecutors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for id in &self.0 {
            if !first {
                f.write_str(", ")?;
            }
            first = false;
            f.write_str(id.as_str())?;
        }
        Ok(())
    }
}

/// Default **wall** time the Runtime drive waits before aborting execute
/// tasks that ignore cancel. Not driven by [`Clock`](crate::Clock) — a
/// paused test clock does not stretch this. Tests that wait for cancel
/// should budget at least this long; production can override via
/// [`RuntimeBuilder::cancel_bound`].
pub const DEFAULT_CANCEL_BOUND: Duration = Duration::from_millis(50);

/// Inbox vs snapshot deadline T (and optional lease heartbeat). This is
/// the only kernel waiter: domain/scheduler apply given `now` and never
/// sleep. When T is already due, prefer the inbox (Cancel / Shutdown) so
/// a queued cancel at the same instant as a due deadline does not dispatch.
/// Heartbeat shares one `wait_until` with the timer (earlier of the two).
async fn next_drive_event(
    rx: &mut EventRx,
    clock: &dyn Clock,
    next_timer: Option<(Timestamp, NodeId)>,
    heartbeat_at: Option<Timestamp>,
) -> Event {
    let now = clock.now();
    let timer_due = next_timer.as_ref().map(|(t, _)| *t <= now).unwrap_or(false);
    let hb_due = heartbeat_at.map(|h| h <= now).unwrap_or(false);
    if timer_due || hb_due {
        return match rx.try_recv() {
            Ok(ev) => ev,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Event::Shutdown,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                match (next_timer, heartbeat_at) {
                    (Some((t, _id)), Some(h)) if hb_due && (!timer_due || h < t) => {
                        Event::Heartbeat
                    }
                    (Some((_, id)), _) if timer_due => Event::Timer { node_id: id },
                    (_, Some(_)) => Event::Heartbeat,
                    _ => Event::Shutdown,
                }
            }
        };
    }
    match (next_timer, heartbeat_at) {
        (None, None) => rx.recv().await.unwrap_or(Event::Shutdown),
        (Some((t, id)), None) => {
            tokio::select! {
                biased;
                ev = rx.recv() => ev.unwrap_or(Event::Shutdown),
                _ = clock.wait_until(t) => Event::Timer { node_id: id },
            }
        }
        (None, Some(h)) => {
            tokio::select! {
                biased;
                ev = rx.recv() => ev.unwrap_or(Event::Shutdown),
                _ = clock.wait_until(h) => Event::Heartbeat,
            }
        }
        (Some((t, id)), Some(h)) => {
            tokio::select! {
                biased;
                ev = rx.recv() => ev.unwrap_or(Event::Shutdown),
                _ = clock.wait_until(h) => Event::Heartbeat,
                _ = clock.wait_until(t) => Event::Timer { node_id: id },
            }
        }
    }
}

/// Wall hang-bound sleeper. Dropped when the drive loop exits (Shutdown
/// or panic) so it cannot wake a dead execution. Not a Clock wait.
struct CancelBoundGuard {
    handle: Option<AbortHandle>,
}

impl CancelBoundGuard {
    fn new() -> Self {
        Self { handle: None }
    }

    fn arm(&mut self, tx: EventTx, bound: Duration) {
        if self.handle.is_some() {
            return;
        }
        let handle = tokio::spawn(async move {
            tokio::time::sleep(bound).await;
            let _ = tx.send(Event::ForceCancelBound);
        });
        self.handle = Some(handle.abort_handle());
    }
}

impl Drop for CancelBoundGuard {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

/// Runtime shell: wait (inbox, Clock::wait_until(T)), then tick apply.
/// Scheduler is not a sleeper.
async fn drive(
    mut scheduler: Scheduler,
    mut rx: EventRx,
    clock: Arc<dyn Clock>,
    cancel_bound: Duration,
    tx: EventTx,
    active: Arc<Mutex<ActiveSet>>,
    execution_id: ExecutionId,
) {
    let mut cancel_bound_guard = CancelBoundGuard::new();
    loop {
        let timer = scheduler.next_deadline();
        let heartbeat = scheduler.next_heartbeat();
        let event = next_drive_event(&mut rx, clock.as_ref(), timer, heartbeat).await;
        if matches!(event, Event::Cancel) {
            cancel_bound_guard.arm(tx.clone(), cancel_bound);
        }
        // Start/Heartbeat returning true is a lost claim — drop live_tx so
        // a later complete goes through the store/claim path, not inject.
        let drop_live = matches!(event, Event::Start | Event::Heartbeat);
        if scheduler.handle_event(event).await {
            if drop_live {
                active
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&execution_id);
            }
            break;
        }
    }
}

/// Runtime bundle. [`OnFailure`](crate::OnFailure) / [`Join`](crate::Join) are
/// **not** set here — they belong on [`WorkflowDefinition`](crate::WorkflowDefinition).
/// The library default remains [`OnFailure::FailExecution`](crate::OnFailure::FailExecution).
pub struct Runtime {
    store: Arc<dyn StateStore>,
    policy: Arc<dyn Policy>,
    sink: Arc<dyn EventSink>,
    registry: ExecutorRegistry,
    clock: Arc<dyn Clock>,
    concurrency: usize,
    cancel_bound: Duration,
    /// Live drives on this Runtime. Handle Drop unregisters.
    active: Arc<Mutex<ActiveSet>>,
    /// Drives started by [`Self::complete`] when the id was not already live.
    /// Kept so Drop of those handles cannot cancel a parked successor.
    owned: Arc<Mutex<Vec<ExecutionHandle>>>,
    /// Store lease owner. Two Runtimes never share this.
    owner: OwnerId,
    /// Shared with handles so resume and complete use one claim path.
    lease: Arc<LeaseGate>,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    /// Start one execution. Fails **before** spawn if any node's executor id
    /// is not registered — nothing runs.
    pub fn start(&self, definition: WorkflowDefinition) -> Result<ExecutionHandle, StartError> {
        if let Some(missing) = self.missing_executors(&definition) {
            return Err(StartError::UnregisteredExecutors(missing));
        }
        let (tx, rx) = inject::channel();
        let (state_tx, state_rx) = watch::channel(ExecutionState::Created);
        let cancel = CancellationToken::new();
        let scheduler = Scheduler::new(
            definition,
            self.policy.clone(),
            self.store.clone(),
            self.sink.clone(),
            self.registry.clone(),
            self.clock.clone(),
            tx.clone(),
            self.concurrency,
            cancel.clone(),
            state_tx,
            self.owner.clone(),
        );
        let execution_id = scheduler.execution_id();
        // Documented invariant: `ExecutionId::new` is unique on this Runtime.
        // `start_two_executions_claim_distinct_ids` pins it.
        let active = self
            .claim_active(&execution_id, tx.clone())
            .expect("ExecutionId::new is unique on this Runtime");
        let _ = tx.send(Event::Start);
        tokio::spawn(drive(
            scheduler,
            rx,
            self.clock.clone(),
            self.cancel_bound,
            tx.clone(),
            self.active.clone(),
            execution_id.clone(),
        ));
        Ok(ExecutionHandle {
            execution_id,
            tx,
            cancel,
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: false,
            _active: active,
            lease: self.lease.clone(),
        })
    }

    /// `start` + [`ExecutionHandle::wait`]. The simple path does not hold a handle
    /// (and therefore cannot accidentally Drop-cancel).
    pub async fn run(&self, definition: WorkflowDefinition) -> Result<ExecutionState, StartError> {
        Ok(self.start(definition)?.wait().await)
    }

    /// Rebuild from the store snapshot. At-least-once: a node that was Running
    /// is restored Ready and re-invoked (attempt + 1 at dispatch). Succeeded
    /// nodes never re-run. Failed stay Failed. `start` still always creates
    /// a new execution. Same as [`Self::resume_with`] `Recover::Continue`.
    pub async fn resume(&self, execution_id: &ExecutionId) -> Result<ExecutionHandle, ResumeError> {
        self.resume_with(execution_id, Recover::Continue).await
    }

    /// Resume a stored execution. [`Recover::Continue`] is [`Self::resume`].
    /// [`Recover::RetryFailed`] re-invokes Failed/TimedOut nodes after
    /// persisting the recovered snapshot (CAS still applies).
    /// [`ExecutionHandle::resume`] (token Complete / Reinvoke) is unchanged.
    pub async fn resume_with(
        &self,
        execution_id: &ExecutionId,
        recover: Recover,
    ) -> Result<ExecutionHandle, ResumeError> {
        let (tx, rx) = inject::channel();
        let Some(active) = self.claim_active(execution_id, tx.clone()) else {
            return Err(ResumeError::AlreadyActive);
        };
        match self
            .spawn_resume(execution_id, active, recover, tx, rx)
            .await
        {
            Ok(handle) => Ok(handle),
            Err(e) => Err(e),
        }
    }

    async fn spawn_resume(
        &self,
        execution_id: &ExecutionId,
        active: ActiveGuard,
        recover: Recover,
        tx: EventTx,
        rx: EventRx,
    ) -> Result<ExecutionHandle, ResumeError> {
        let snap = self
            .store
            .get(execution_id)
            .await?
            .ok_or(ResumeError::UnknownExecution)?;
        let definition = self
            .store
            .workflow_definition(execution_id)
            .await?
            .ok_or(ResumeError::DefinitionMissing)?;
        let mut exec = Execution::from_snapshot(definition, snap)?;
        if let Some(missing) = self.missing_executors(exec.definition()) {
            return Err(ResumeError::UnregisteredExecutors(missing));
        }
        let epoch = self
            .store
            .claim(execution_id, &self.owner, self.clock.now())
            .await?;
        exec.set_fence_epoch(epoch.0);
        if recover == Recover::RetryFailed {
            match exec.apply(
                ApplyCmd::RetryFailed,
                self.policy.as_ref(),
                self.clock.now(),
            ) {
                Ok(_) => {}
                Err(ApplyError::Illegal(_)) => {
                    let _ = self.store.release(execution_id, epoch).await;
                    return Err(ResumeError::NotFailed);
                }
                Err(e) => unreachable!("RetryFailed apply returns only Illegal, got {e}"),
            }
            // Persist recovered snapshot before dispatch. CAS still applies.
            self.store.persist(&exec).await?;
        }
        let (state_tx, state_rx) = watch::channel(exec.state());
        let cancel = CancellationToken::new();
        let scheduler = Scheduler::from_execution(
            exec,
            self.policy.clone(),
            self.store.clone(),
            self.sink.clone(),
            self.registry.clone(),
            self.clock.clone(),
            tx.clone(),
            self.concurrency,
            cancel.clone(),
            state_tx,
            self.owner.clone(),
        );
        let _ = tx.send(Event::Restore);
        tokio::spawn(drive(
            scheduler,
            rx,
            self.clock.clone(),
            self.cancel_bound,
            tx.clone(),
            self.active.clone(),
            execution_id.clone(),
        ));
        Ok(ExecutionHandle {
            execution_id: execution_id.clone(),
            tx,
            cancel,
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: false,
            _active: active,
            lease: self.lease.clone(),
        })
    }

    fn claim_active(&self, id: &ExecutionId, tx: EventTx) -> Option<ActiveGuard> {
        let mut g = self.active.lock().unwrap_or_else(|p| p.into_inner());
        if g.contains_key(id) {
            return None;
        }
        g.insert(id.clone(), tx);
        Some(ActiveGuard::new(id.clone(), self.active.clone()))
    }

    fn live_tx(&self, id: &ExecutionId) -> Option<EventTx> {
        self.lease.live_tx(id)
    }

    /// Snapshot for an execution this Runtime can see: live drive first
    /// (same [`ExecutionHandle::inspect`]), else the store. Unknown id is
    /// [`None`]. Does not take a new lease.
    pub async fn inspect(&self, execution_id: &ExecutionId) -> Option<ExecutionSnapshot> {
        if let Some(tx) = self.live_tx(execution_id) {
            let (reply, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Event::Inspect { reply });
            if let Ok(snap) = rx.await {
                return Some(snap);
            }
        }
        self.store.get(execution_id).await.unwrap_or(None)
    }

    /// Complete or reinvoke a Waiting node. Live drive: inject (no second
    /// scheduler) only if this process still holds the lease. A stolen
    /// lease is [`CompleteError::ClaimedElsewhere`] — do not inject.
    /// Otherwise load the snapshot, apply, persist, then drive.
    /// Does not revive [`ExecutionState::Cancelled`].
    pub async fn complete(&self, token: ResumeToken, resume: Resume) -> Result<(), CompleteError> {
        let id = token.execution_id();
        if let Some(tx) = self.live_tx(id) {
            match self.lease.claim_or_forget(id).await {
                Ok(()) => {
                    return self
                        .map_complete_apply(inject::inject_resume(&tx, token, resume).await);
                }
                Err(e) => return Err(e.into()),
            }
        }
        self.complete_from_store(token, resume).await
    }

    fn map_complete_apply(&self, r: Result<(), ApplyError>) -> Result<(), CompleteError> {
        match r {
            Ok(()) => Ok(()),
            Err(ApplyError::TokenMismatch) | Err(ApplyError::UnknownNode(_)) => {
                Err(CompleteError::UnknownToken)
            }
            Err(ApplyError::ResumeAfterCancel) => Err(CompleteError::Cancelled),
            Err(e) => Err(CompleteError::Apply(e)),
        }
    }

    async fn complete_from_store(
        &self,
        token: ResumeToken,
        resume: Resume,
    ) -> Result<(), CompleteError> {
        let id = token.execution_id().clone();
        let snap = self
            .store
            .get(&id)
            .await?
            .ok_or(CompleteError::UnknownToken)?;
        if snap.state == ExecutionState::Cancelled {
            return Err(CompleteError::Cancelled);
        }
        let definition = self
            .store
            .workflow_definition(&id)
            .await?
            .ok_or(CompleteError::UnknownToken)?;
        if let Some(missing) = self.missing_executors(&definition) {
            return Err(CompleteError::UnregisteredExecutors(missing));
        }
        let mut exec = Execution::from_snapshot(definition, snap)?;
        if exec.state() == ExecutionState::Cancelled {
            return Err(CompleteError::Cancelled);
        }
        let epoch = self.store.claim(&id, &self.owner, self.clock.now()).await?;
        exec.set_fence_epoch(epoch.0);
        let applied = exec.apply(
            ApplyCmd::Resume {
                token: token.clone(),
                resume: resume.clone(),
            },
            self.policy.as_ref(),
            self.clock.now(),
        );
        if let Err(e) = applied {
            let _ = self.store.release(&id, epoch).await;
            return self.map_complete_apply(Err(e));
        }
        if let Err(e) = self.store.persist(&exec).await {
            return Err(CompleteError::Store(e));
        }
        if exec.state().is_terminal() {
            let _ = self.store.release(&id, epoch).await;
            return Ok(());
        }
        if let Some(tx) = self.live_tx(&id) {
            return self.map_complete_apply(inject::inject_resume(&tx, token, resume).await);
        }
        let (tx, rx) = inject::channel();
        if let Some(active) = self.claim_active(&id, tx.clone()) {
            if let Ok(handle) = self
                .spawn_resume(&id, active, Recover::Continue, tx, rx)
                .await
            {
                self.owned
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(handle);
            }
            return Ok(());
        }
        if let Some(tx) = self.live_tx(&id) {
            return self.map_complete_apply(inject::inject_resume(&tx, token, resume).await);
        }
        Ok(())
    }

    fn missing_executors(&self, definition: &WorkflowDefinition) -> Option<UnregisteredExecutors> {
        let mut seen = HashSet::new();
        let mut missing = Vec::new();
        for n in definition.nodes() {
            if !seen.insert(n.executor_id.clone()) {
                continue;
            }
            if self.registry.get(&n.executor_id).is_none() {
                missing.push(n.executor_id.clone());
            }
        }
        missing.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        if missing.is_empty() {
            None
        } else {
            Some(UnregisteredExecutors(missing))
        }
    }
}

pub struct RuntimeBuilder {
    store: Option<Arc<dyn StateStore>>,
    policy: Option<Arc<dyn Policy>>,
    sink: Option<Arc<dyn EventSink>>,
    registry: ExecutorRegistry,
    clock: Option<Arc<dyn Clock>>,
    concurrency: usize,
    cancel_bound: Duration,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        let mut registry = ExecutorRegistry::new();
        registry.register(Arc::new(Wait));
        Self {
            store: None,
            policy: None,
            sink: None,
            registry,
            clock: None,
            concurrency: 8,
            cancel_bound: DEFAULT_CANCEL_BOUND,
        }
    }
}

impl RuntimeBuilder {
    pub fn store(mut self, store: impl StateStore + 'static) -> Self {
        self.store = Some(Arc::new(store));
        self
    }

    pub fn store_arc(mut self, store: Arc<dyn StateStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn policy(mut self, policy: impl Policy + 'static) -> Self {
        self.policy = Some(Arc::new(policy));
        self
    }

    pub fn policy_arc(mut self, policy: Arc<dyn Policy>) -> Self {
        self.policy = Some(policy);
        self
    }

    pub fn sink(mut self, sink: impl EventSink + 'static) -> Self {
        self.sink = Some(Arc::new(sink));
        self
    }

    pub fn sink_arc(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    pub fn register(mut self, exec: impl Executor + 'static) -> Self {
        self.registry.register(Arc::new(exec));
        self
    }

    pub fn register_arc(mut self, exec: Arc<dyn Executor>) -> Self {
        self.registry.register(exec);
        self
    }

    /// Register a function without naming [`FunctionExecutor`].
    pub fn register_fn<F, Fut>(self, id: impl Into<crate::domain::ids::ExecutorId>, f: F) -> Self
    where
        F: Fn(ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NodeOutcome> + Send + 'static,
    {
        self.register(FunctionExecutor::new(id, f))
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn cancel_bound(mut self, bound: Duration) -> Self {
        self.cancel_bound = bound;
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn build(self) -> Runtime {
        let store = self.store.unwrap_or_else(|| Arc::new(MemoryStore::new()));
        let clock = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let owner = OwnerId::new();
        let active = Arc::new(Mutex::new(ActiveSet::new()));
        Runtime {
            store: store.clone(),
            policy: self.policy.unwrap_or_else(|| Arc::new(AcceptPolicy)),
            sink: self.sink.unwrap_or_else(|| Arc::new(NoopSink)),
            registry: self.registry,
            clock: clock.clone(),
            concurrency: self.concurrency,
            cancel_bound: self.cancel_bound,
            active: active.clone(),
            owned: Arc::new(Mutex::new(Vec::new())),
            owner: owner.clone(),
            lease: LeaseGate::new(store, owner, clock, active),
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Drop handle still cancels. Drop Runtime releases the store lease
        // so another Runtime may claim (engine-down / process death analog).
        self.store.release_owner_now(&self.owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::ResumeToken;
    use crate::runtime::sink::FnSink;
    use bytes::Bytes;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn tiny() -> WorkflowDefinition {
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_two_executions_claim_distinct_ids() {
        let rt = Runtime::builder()
            .register_fn("a", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let a = rt.start(tiny()).unwrap();
        let b = rt.start(tiny()).unwrap();
        assert_ne!(a.execution_id(), b.execution_id());
        assert_eq!(a.wait().await, ExecutionState::Succeeded);
        assert_eq!(b.wait().await, ExecutionState::Succeeded);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn active_mutex_poison_recovers_on_start() {
        let rt = Runtime::builder()
            .register_fn("a", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _g = rt.active.lock().unwrap();
            panic!("poison runtime active set");
        }));
        assert!(poisoned.is_err());
        let handle = rt.start(tiny()).expect("poisoned active set must recover");
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_prefers_queued_cancel() {
        let (tx, mut rx) = inject::channel();
        let _ = tx.send(Event::Cancel);
        let ev = next_drive_event(
            &mut rx,
            &SystemClock,
            Some((Timestamp(0), NodeId::new("a"))),
            None,
        )
        .await;
        assert!(
            matches!(ev, Event::Cancel),
            "inbox must beat due Timer, got {ev:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_empty_inbox_is_timer() {
        let (_tx, mut rx) = inject::channel();
        match next_drive_event(
            &mut rx,
            &SystemClock,
            Some((Timestamp(0), NodeId::new("n"))),
            None,
        )
        .await
        {
            Event::Timer { node_id } => assert_eq!(node_id.as_str(), "n"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_disconnected_inbox_is_shutdown() {
        let (tx, mut rx) = inject::channel();
        drop(tx);
        assert!(matches!(
            next_drive_event(
                &mut rx,
                &SystemClock,
                Some((Timestamp(0), NodeId::new("a"))),
                None,
            )
            .await,
            Event::Shutdown
        ));
    }

    struct PanicIfWaitUntil;

    #[async_trait::async_trait]
    impl Clock for PanicIfWaitUntil {
        fn now(&self) -> Timestamp {
            Timestamp(0)
        }
        async fn sleep(&self, _: Duration) {}
        async fn wait_until(&self, _: Timestamp) {
            panic!("due T must try_recv inbox, not wait_until");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_deadline_does_not_call_wait_until() {
        let (tx, mut rx) = inject::channel();
        let _ = tx.send(Event::Cancel);
        let ev = next_drive_event(
            &mut rx,
            &PanicIfWaitUntil,
            Some((Timestamp(0), NodeId::new("a"))),
            None,
        )
        .await;
        assert!(
            matches!(ev, Event::Cancel),
            "inbox must beat due Timer without wait_until, got {ev:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_unknown_id_is_none() {
        let rt = Runtime::builder().build();
        assert!(rt
            .inspect(&ExecutionId::parse("exec-missing").unwrap())
            .await
            .is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_live_wait_sees_token() {
        let rt = Runtime::builder().build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle.wait_stable())
            .await
            .unwrap();
        let snap = rt
            .inspect(handle.execution_id())
            .await
            .expect("live inspect");
        assert_eq!(snap.state, ExecutionState::Waiting);
        assert!(snap
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .is_some());
        handle.cancel().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_after_drop_is_cancelled() {
        let store = MemoryStore::new();
        let rt = Runtime::builder().store(store.clone()).build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        tokio::time::timeout(Duration::from_secs(5), handle.wait_stable())
            .await
            .unwrap();
        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(s) = store.get(&id).await.unwrap() {
                    if s.state == ExecutionState::Cancelled {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled");
        let snap = rt.inspect(&id).await.expect("store inspect");
        assert_eq!(snap.state, ExecutionState::Cancelled);
        assert!(snap
            .node(&NodeId::new("hold"))
            .and_then(|n| n.resume_token.clone())
            .is_none());
    }

    struct GetFails;

    #[async_trait::async_trait]
    impl StateStore for GetFails {
        async fn put(&self, _: &ExecutionSnapshot) -> Result<(), StoreError> {
            Ok(())
        }
        async fn get(&self, _: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
            Err(StoreError::Message("get fail".into()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_store_err_is_none() {
        let rt = Runtime::builder().store(GetFails).build();
        assert!(rt.inspect(&ExecutionId::new()).await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_dead_drive_falls_back_to_store() {
        let store = MemoryStore::new();
        let rt = Runtime::builder().store(store.clone()).build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle.wait_stable())
            .await
            .unwrap();
        let id = handle.execution_id().clone();
        let tx = rt.live_tx(&id).expect("live");
        let _ = tx.send(Event::Shutdown);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (reply, rx) = tokio::sync::oneshot::channel();
                match rt
                    .live_tx(&id)
                    .expect("still registered")
                    .send(Event::Inspect { reply })
                {
                    Ok(()) => {
                        if rx.await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drive gone");
        let snap = rt.inspect(&id).await.expect("store fallback");
        assert_eq!(snap.state, ExecutionState::Waiting);
        drop(handle);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inspect_after_node_waiting_matches_store_and_does_not_emit() {
        let store = MemoryStore::new();
        let seen = Arc::new(Mutex::new(Vec::<crate::Event>::new()));
        let log = seen.clone();
        let rt = Runtime::builder()
            .store(store.clone())
            .sink(FnSink(move |e: &crate::Event| {
                log.lock().unwrap().push(e.clone());
            }))
            .build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if seen
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|e| matches!(e, crate::Event::NodeWaiting { .. }))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("NodeWaiting announced");
        let announced = seen
            .lock()
            .unwrap()
            .iter()
            .find_map(|e| match e {
                crate::Event::NodeWaiting { token, .. } => Some(token.clone()),
                _ => None,
            })
            .expect("token on event");
        let live = rt.inspect(&id).await.expect("live");
        let stored = store.get(&id).await.unwrap().expect("persisted");
        assert_eq!(live.state, ExecutionState::Waiting);
        assert_eq!(stored.state, live.state);
        let live_tok = live
            .node(&NodeId::new("hold"))
            .and_then(|n| n.resume_token.clone());
        let store_tok = stored
            .node(&NodeId::new("hold"))
            .and_then(|n| n.resume_token.clone());
        assert_eq!(live_tok.as_ref(), Some(&announced));
        assert_eq!(store_tok, live_tok);
        let n = seen.lock().unwrap().len();
        let _ = rt.inspect(&id).await;
        assert_eq!(
            seen.lock().unwrap().len(),
            n,
            "inspect must not emit a public Event"
        );
        assert!(
            !seen
                .lock()
                .unwrap()
                .iter()
                .any(|e| format!("{e:?}").contains("Inspect")),
            "public Event must not grow an Inspect variant"
        );
        handle.cancel().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn complete_unknown_token_is_unknown() {
        let rt = Runtime::builder().build();
        let token = ResumeToken::issue(ExecutionId::new(), NodeId::new("hold"), 1);
        match rt
            .complete(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            )
            .await
        {
            Err(CompleteError::UnknownToken) => {}
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_heartbeat_empty_inbox_is_heartbeat() {
        let (_tx, mut rx) = inject::channel();
        assert!(matches!(
            next_drive_event(&mut rx, &SystemClock, None, Some(Timestamp(0))).await,
            Event::Heartbeat
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn due_heartbeat_prefers_queued_cancel() {
        let (tx, mut rx) = inject::channel();
        let _ = tx.send(Event::Cancel);
        let ev = next_drive_event(&mut rx, &SystemClock, None, Some(Timestamp(0))).await;
        assert!(
            matches!(ev, Event::Cancel),
            "inbox must beat due Heartbeat, got {ev:?}"
        );
    }

    struct JumpClock {
        now: std::sync::Mutex<Timestamp>,
        tick: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl Clock for JumpClock {
        fn now(&self) -> Timestamp {
            *self.now.lock().unwrap()
        }
        async fn sleep(&self, duration: Duration) {
            if duration.is_zero() {
                return;
            }
            let target = self.now().saturating_add(duration);
            loop {
                let notified = self.tick.notified();
                if self.now() >= target {
                    return;
                }
                notified.await;
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn future_heartbeat_waits_until_clock() {
        let (_tx, mut rx) = inject::channel();
        let clock = Arc::new(JumpClock {
            now: std::sync::Mutex::new(Timestamp(0)),
            tick: tokio::sync::Notify::new(),
        });
        let clock_c = clock.clone();
        let task = tokio::spawn(async move {
            next_drive_event(&mut rx, clock_c.as_ref(), None, Some(Timestamp(10))).await
        });
        *clock.now.lock().unwrap() = Timestamp(10);
        clock.tick.notify_waiters();
        assert!(matches!(task.await.unwrap(), Event::Heartbeat));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_earlier_than_timer_is_heartbeat() {
        let (_tx, mut rx) = inject::channel();
        let ev = next_drive_event(
            &mut rx,
            &SystemClock,
            Some((Timestamp(10_000), NodeId::new("a"))),
            Some(Timestamp(0)),
        )
        .await;
        assert!(
            matches!(ev, Event::Heartbeat),
            "earlier heartbeat must beat future timer, got {ev:?}"
        );
    }

    struct RejectClaim;

    #[async_trait::async_trait]
    impl StateStore for RejectClaim {
        async fn put(
            &self,
            _: &crate::domain::snapshot::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            Ok(())
        }
        async fn get(
            &self,
            _: &ExecutionId,
        ) -> Result<Option<crate::domain::snapshot::ExecutionSnapshot>, StoreError> {
            Ok(None)
        }
        async fn claim(
            &self,
            _: &ExecutionId,
            _: &OwnerId,
            _: Timestamp,
        ) -> Result<crate::runtime::store::LeaseEpoch, ClaimError> {
            Err(ClaimError::ClaimedElsewhere)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_claim_elsewhere_stops_drive() {
        let rt = Runtime::builder()
            .store(RejectClaim)
            .register_fn("a", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let handle = rt.start(tiny()).unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        drop(handle);
    }

    struct FailHeartbeat {
        inner: MemoryStore,
        claimed: std::sync::atomic::AtomicBool,
    }

    impl FailHeartbeat {
        fn new() -> Self {
            Self {
                inner: MemoryStore::new(),
                claimed: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateStore for FailHeartbeat {
        async fn put(
            &self,
            snap: &crate::domain::snapshot::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snap).await
        }
        async fn get(
            &self,
            id: &ExecutionId,
        ) -> Result<Option<crate::domain::snapshot::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            self.inner.persist(exec).await
        }
        async fn persist_with_events(
            &self,
            exec: &Execution,
            events: &[crate::domain::events::Event],
        ) -> Result<(), StoreError> {
            self.inner.persist_with_events(exec, events).await
        }
        async fn workflow_definition(
            &self,
            id: &ExecutionId,
        ) -> Result<Option<WorkflowDefinition>, StoreError> {
            self.inner.workflow_definition(id).await
        }
        async fn claim(
            &self,
            id: &ExecutionId,
            owner: &OwnerId,
            now: Timestamp,
        ) -> Result<crate::runtime::store::LeaseEpoch, ClaimError> {
            if self.claimed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(ClaimError::ClaimedElsewhere);
            }
            self.inner.claim(id, owner, now).await
        }
        async fn heartbeat(
            &self,
            _: &ExecutionId,
            _: crate::runtime::store::LeaseEpoch,
            _: Timestamp,
        ) -> Result<(), ClaimError> {
            Err(ClaimError::ClaimedElsewhere)
        }
        fn release_owner_now(&self, owner: &OwnerId) {
            self.inner.release_owner_now(owner);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_lost_lease_stops_drive_without_cancel() {
        let clock = Arc::new(JumpClock {
            now: std::sync::Mutex::new(Timestamp(0)),
            tick: tokio::sync::Notify::new(),
        });
        let rt = Runtime::builder()
            .store(FailHeartbeat::new())
            .clock(clock.clone())
            .build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
        *clock.now.lock().unwrap() =
            Timestamp(0).saturating_add(crate::runtime::store::DEFAULT_LEASE_TTL / 3);
        clock.tick.notify_waiters();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        drop(handle);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_is_registered_without_manual_executor() {
        let rt = Runtime::builder().build();
        let def = WorkflowDefinition::builder("wf")
            .node("hold", "wait")
            .build()
            .unwrap();
        let h = rt.start(def).unwrap();
        assert_eq!(h.wait_stable().await, ExecutionState::Waiting);
        h.cancel().await;
        assert_eq!(h.wait().await, ExecutionState::Cancelled);
    }
}
