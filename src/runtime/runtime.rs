use crate::domain::definition::WorkflowDefinition;
use crate::domain::policy::{AcceptPolicy, Policy};
use crate::domain::state::ExecutionState;
use crate::runtime::executor::{Executor, ExecutorRegistry};
use crate::runtime::handle::ExecutionHandle;
use crate::runtime::inject::{self, Event};
use crate::runtime::park::ChannelPark;
use crate::runtime::scheduler::Scheduler;
use crate::runtime::sink::{EventSink, NoopSink};
use crate::runtime::store::{MemoryStore, StateStore};
use crate::runtime::time::{Clock, SystemClock};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Default time the scheduler waits before aborting execute tasks that ignore cancel.
/// Tests should use a timeout at least this large; production can override.
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
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    pub fn start(&self, definition: WorkflowDefinition) -> ExecutionHandle {
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
        let _ = tx.send(Event::Start);
        tokio::spawn(scheduler.run());
        ExecutionHandle {
            tx,
            cancel,
            state: state_rx,
            dropped: Arc::new(AtomicBool::new(false)),
            consumed: false,
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
        }
    }
}
