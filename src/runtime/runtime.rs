use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, ExecutorId};
use crate::domain::outcome::NodeOutcome;
use crate::domain::policy::{AcceptPolicy, Policy};
use crate::domain::snapshot::SnapshotError;
use crate::domain::state::{Execution, ExecutionState};
use crate::runtime::executor::{ExecutionContext, Executor, ExecutorRegistry, FunctionExecutor};
use crate::runtime::handle::{ActiveGuard, ExecutionHandle};
use crate::runtime::inject::{self, Event};
use crate::runtime::park::ChannelPark;
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

/// Default **wall** time the scheduler waits before aborting execute tasks
/// that ignore cancel. Not driven by [`Clock`](crate::Clock) — FakeClock
/// does not stretch this. Tests that wait for cancel should budget at least
/// this long; production can override via [`RuntimeBuilder::cancel_bound`].
pub const DEFAULT_CANCEL_BOUND: Duration = Duration::from_millis(50);

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
        let park = ChannelPark::new(rx, self.clock.clone());
        let scheduler = Scheduler::new(
            definition,
            self.policy.clone(),
            self.store.clone(),
            self.sink.clone(),
            self.registry.clone(),
            self.clock.clone(),
            park,
            tx.clone(),
            self.concurrency,
            cancel.clone(),
            state_tx,
            self.cancel_bound,
        );
        let execution_id = scheduler.execution_id();
        // Documented invariant: `ExecutionId::new` is unique on this Runtime.
        // `start_two_executions_claim_distinct_ids` pins it.
        let active = self
            .claim_active(&execution_id)
            .expect("ExecutionId::new is unique on this Runtime");
        let _ = tx.send(Event::Start);
        tokio::spawn(scheduler.run());
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
    /// nodes never re-run. `start` still always creates a new execution.
    pub async fn resume(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionHandle, ResumeError> {
        let Some(active) = self.claim_active(execution_id) else {
            return Err(ResumeError::AlreadyActive);
        };
        match self.spawn_resume(execution_id, active).await {
            Ok(handle) => Ok(handle),
            Err(e) => Err(e),
        }
    }

    async fn spawn_resume(
        &self,
        execution_id: &ExecutionId,
        active: ActiveGuard,
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
        let exec = Execution::from_snapshot(definition, snap)?;
        if let Some(missing) = self.missing_executors(exec.definition()) {
            return Err(ResumeError::UnregisteredExecutors(missing));
        }
        let (tx, rx) = inject::channel();
        let (state_tx, state_rx) = watch::channel(exec.state());
        let cancel = CancellationToken::new();
        let park = ChannelPark::new(rx, self.clock.clone());
        let scheduler = Scheduler::from_execution(
            exec,
            self.policy.clone(),
            self.store.clone(),
            self.sink.clone(),
            self.registry.clone(),
            self.clock.clone(),
            park,
            tx.clone(),
            self.concurrency,
            cancel.clone(),
            state_tx,
            self.cancel_bound,
        );
        let _ = tx.send(Event::Restore);
        tokio::spawn(scheduler.run());
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
            store: self
                .store
                .unwrap_or_else(|| Arc::new(MemoryStore::new())),
            policy: self
                .policy
                .unwrap_or_else(|| Arc::new(AcceptPolicy)),
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
}
