use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, ExecutorId, NodeId};
use crate::domain::outcome::{NodeOutcome, Recover};
use crate::domain::policy::{AcceptPolicy, Policy};
use crate::domain::snapshot::SnapshotError;
use crate::domain::state::{ApplyCmd, Execution, ExecutionState};
use crate::domain::time::Timestamp;
use crate::runtime::executor::{ExecutionContext, Executor, ExecutorRegistry, FunctionExecutor};
use crate::runtime::handle::{ActiveGuard, ExecutionHandle};
use crate::runtime::inject::{self, Event, EventRx, EventTx};
use crate::runtime::scheduler::Scheduler;
use crate::runtime::sink::{EventSink, NoopSink};
use crate::runtime::store::{MemoryStore, StateStore, StoreError};
use crate::runtime::time::{Clock, SystemClock};
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

/// Inbox vs snapshot deadline T. This is the only kernel waiter:
/// domain/scheduler apply given `now` and never sleep. When T is already
/// due, prefer the inbox (Cancel / Shutdown) so a queued cancel at the
/// same instant as a due deadline does not dispatch.
async fn next_drive_event(
    rx: &mut EventRx,
    clock: &dyn Clock,
    next_timer: Option<(Timestamp, NodeId)>,
) -> Event {
    match next_timer {
        Some((when, node_id)) => {
            let now = clock.now();
            if when <= now {
                return match rx.try_recv() {
                    Ok(ev) => ev,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Event::Shutdown,
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Event::Timer { node_id },
                };
            }
            tokio::select! {
                biased;
                ev = rx.recv() => ev.unwrap_or(Event::Shutdown),
                _ = clock.wait_until(when) => Event::Timer { node_id },
            }
        }
        None => rx.recv().await.unwrap_or(Event::Shutdown),
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
) {
    let mut cancel_bound_guard = CancelBoundGuard::new();
    loop {
        let timer = scheduler.next_deadline();
        let event = next_drive_event(&mut rx, clock.as_ref(), timer).await;
        if matches!(event, Event::Cancel) {
            cancel_bound_guard.arm(tx.clone(), cancel_bound);
        }
        if scheduler.handle_event(event).await {
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
    /// Live execution ids on this Runtime. Handle Drop unregisters.
    active: Arc<Mutex<HashSet<ExecutionId>>>,
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
        );
        let execution_id = scheduler.execution_id();
        // Documented invariant: `ExecutionId::new` is unique on this Runtime.
        // `start_two_executions_claim_distinct_ids` pins it.
        let active = self
            .claim_active(&execution_id)
            .expect("ExecutionId::new is unique on this Runtime");
        let _ = tx.send(Event::Start);
        tokio::spawn(drive(
            scheduler,
            rx,
            self.clock.clone(),
            self.cancel_bound,
            tx.clone(),
        ));
        Ok(ExecutionHandle {
            execution_id,
            tx,
            cancel,
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: false,
            _active: active,
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
        let Some(active) = self.claim_active(execution_id) else {
            return Err(ResumeError::AlreadyActive);
        };
        match self.spawn_resume(execution_id, active, recover).await {
            Ok(handle) => Ok(handle),
            Err(e) => Err(e),
        }
    }

    async fn spawn_resume(
        &self,
        execution_id: &ExecutionId,
        active: ActiveGuard,
        recover: Recover,
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
        if recover == Recover::RetryFailed {
            if exec
                .apply(
                    ApplyCmd::RetryFailed,
                    self.policy.as_ref(),
                    self.clock.now(),
                )
                .is_err()
            {
                return Err(ResumeError::NotFailed);
            }
            // Persist recovered snapshot before dispatch. CAS still applies.
            self.store.persist(&exec).await?;
        }
        let (tx, rx) = inject::channel();
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
        );
        let _ = tx.send(Event::Restore);
        tokio::spawn(drive(
            scheduler,
            rx,
            self.clock.clone(),
            self.cancel_bound,
            tx.clone(),
        ));
        Ok(ExecutionHandle {
            execution_id: execution_id.clone(),
            tx,
            cancel,
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: false,
            _active: active,
        })
    }

    fn claim_active(&self, id: &ExecutionId) -> Option<ActiveGuard> {
        let mut g = self.active.lock().unwrap_or_else(|p| p.into_inner());
        if !g.insert(id.clone()) {
            return None;
        }
        Some(ActiveGuard::new(id.clone(), self.active.clone()))
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
        Self {
            store: None,
            policy: None,
            sink: None,
            registry: ExecutorRegistry::new(),
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
        Runtime {
            store: self.store.unwrap_or_else(|| Arc::new(MemoryStore::new())),
            policy: self.policy.unwrap_or_else(|| Arc::new(AcceptPolicy)),
            sink: self.sink.unwrap_or_else(|| Arc::new(NoopSink)),
            registry: self.registry,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            concurrency: self.concurrency,
            cancel_bound: self.cancel_bound,
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                Some((Timestamp(0), NodeId::new("a")))
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
        )
        .await;
        assert!(
            matches!(ev, Event::Cancel),
            "inbox must beat due Timer without wait_until, got {ev:?}"
        );
    }
}
