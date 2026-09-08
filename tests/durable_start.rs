//! Startup acceptance is separate from at-least-once executor effects.
use async_trait::async_trait;
use keel_rt::{
    ClaimError, DurableStartError, Execution, ExecutionId, ExecutionSnapshot, ExecutionState,
    FakeClock, InitializeError, LeaseEpoch, MemoryStore, NodeOutcome, NoopStore, OwnerId,
    ResumeError, Runtime, StateStore, StoreError, Timestamp, WorkflowDefinition,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(5);

fn definition() -> WorkflowDefinition {
    WorkflowDefinition::builder("startup")
        .node("a", "a")
        .build()
        .unwrap()
}

#[derive(Clone, Copy)]
enum Boundary {
    BeforeCommit,
    AfterCommit,
    Error,
    Panic,
}

// Explicit test double for a durable initialization port. MemoryStore itself
// deliberately does not implement this capability.
struct ControlledStore {
    inner: MemoryStore,
    boundary: Boundary,
    read_fault: AtomicUsize,
    entered: Notify,
    proceed: Notify,
    id: Mutex<Option<ExecutionId>>,
}

impl ControlledStore {
    fn new(boundary: Boundary) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(),
            boundary,
            read_fault: AtomicUsize::new(0),
            entered: Notify::new(),
            proceed: Notify::new(),
            id: Mutex::new(None),
        })
    }

    fn id(&self) -> ExecutionId {
        self.id.lock().unwrap().clone().unwrap()
    }
}

#[async_trait]
impl StateStore for ControlledStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        match self.read_fault.load(Ordering::SeqCst) {
            1 => Err(StoreError::Message("read failed".into())),
            2 => Ok(None),
            3 => {
                let mut snapshot = self.inner.get(id).await?.unwrap();
                snapshot.schema_version = u32::MAX;
                Ok(Some(snapshot))
            }
            _ => self.inner.get(id).await,
        }
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.inner.persist(exec).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
    async fn initialize(
        &self,
        exec: &Execution,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, InitializeError> {
        *self.id.lock().unwrap() = Some(exec.id().clone());
        match self.boundary {
            Boundary::Error => return Err(StoreError::Message("disk full".into()).into()),
            Boundary::Panic => panic!("initializer panicked"),
            Boundary::BeforeCommit => {
                self.entered.notify_one();
                self.proceed.notified().await;
            }
            Boundary::AfterCommit => {}
        }
        let epoch = self.inner.claim(exec.id(), owner, now).await.unwrap();
        let mut saved =
            Execution::from_snapshot(exec.definition().clone(), exec.snapshot()).unwrap();
        saved.set_fence_epoch(epoch.0);
        self.inner.persist(&saved).await?;
        if matches!(self.boundary, Boundary::AfterCommit) {
            self.entered.notify_one();
            self.proceed.notified().await;
        }
        Ok(epoch)
    }
    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        self.inner.claim(id, owner, now).await
    }
    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        self.inner.heartbeat(id, epoch, now).await
    }
    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.inner.release(id, epoch).await
    }
    fn release_owner_now(&self, owner: &OwnerId) {
        self.inner.release_owner_now(owner);
    }
}

fn runtime(
    store: Arc<ControlledStore>,
    clock: Arc<FakeClock>,
    runs: Arc<AtomicUsize>,
) -> Arc<Runtime> {
    let adapter: Arc<dyn StateStore> = store;
    Arc::new(
        Runtime::builder()
            .store(adapter)
            .clock(clock)
            .register_fn("a", move |_| {
                runs.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Default::default()) }
            })
            .build(),
    )
}

#[tokio::test]
async fn missing_executors_fail_before_initialization_and_memory_stores_are_unsupported() {
    let store = ControlledStore::new(Boundary::Panic);
    let adapter: Arc<dyn StateStore> = store.clone();
    let rt = Runtime::builder().store(adapter).build();
    let error = match rt.start_durable(definition()).await {
        Err(e) => e,
        Ok(_) => panic!("missing executor"),
    };
    assert!(matches!(error, DurableStartError::UnregisteredExecutors(_)));
    assert!(error.to_string().contains('a'));
    assert!(store.id.lock().unwrap().is_none());
    for adapter in [
        Arc::new(MemoryStore::new()) as Arc<dyn StateStore>,
        Arc::new(NoopStore),
    ] {
        let rt = Runtime::builder()
            .store(adapter)
            .register_fn("a", |_| async { panic!("unsupported store cannot launch") })
            .build();
        assert!(matches!(
            rt.start_durable(definition()).await,
            Err(DurableStartError::Initialization(
                InitializeError::Unsupported
            ))
        ));
    }
    assert_eq!(
        InitializeError::Unsupported.to_string(),
        "store does not support durable initialization"
    );
    assert_eq!(
        InitializeError::AlreadyExists.to_string(),
        "execution already exists"
    );
}

#[tokio::test]
async fn initialization_error_or_panic_never_launches_or_registers_a_live_execution() {
    for boundary in [Boundary::Error, Boundary::Panic] {
        let store = ControlledStore::new(boundary);
        let runs = Arc::new(AtomicUsize::new(0));
        let rt = runtime(store.clone(), Arc::new(FakeClock::new()), runs.clone());
        let error = match rt.start_durable(definition()).await {
            Err(e) => e,
            Ok(_) => panic!("initialization must fail"),
        };
        assert!(matches!(
            error,
            DurableStartError::Initialization(InitializeError::Store(_))
        ));
        assert!(!error.to_string().is_empty());
        assert!(matches!(
            rt.resume(&store.id()).await,
            Err(ResumeError::UnknownExecution)
        ));
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_precedes_acknowledgement_and_executor_launch() {
    for boundary in [Boundary::BeforeCommit, Boundary::AfterCommit] {
        let store = ControlledStore::new(boundary);
        let runs = Arc::new(AtomicUsize::new(0));
        let rt = runtime(store.clone(), Arc::new(FakeClock::new()), runs.clone());
        let starter = tokio::spawn({
            let rt = rt.clone();
            async move { rt.start_durable(definition()).await }
        });
        tokio::time::timeout(BOUND, store.entered.notified())
            .await
            .unwrap();
        assert!(!starter.is_finished(), "no ack before initializer returns");
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "no execution before commit/ack boundary"
        );
        assert_eq!(
            store.get(&store.id()).await.unwrap().is_some(),
            matches!(boundary, Boundary::AfterCommit)
        );
        assert!(
            matches!(
                rt.resume(&store.id()).await,
                Err(ResumeError::AlreadyActive)
            ),
            "recovery cannot launch the committed execution before startup returns"
        );
        store.proceed.notify_one();
        let handle = tokio::time::timeout(BOUND, starter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(store.get(handle.execution_id()).await.unwrap().is_some());
        assert!(store
            .workflow_definition(handle.execution_id())
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
            ExecutionState::Succeeded
        );
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_startup_preserves_committed_state_without_live_registration() {
    for boundary in [Boundary::BeforeCommit, Boundary::AfterCommit] {
        let store = ControlledStore::new(boundary);
        let clock = Arc::new(FakeClock::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let rt = runtime(store.clone(), clock.clone(), runs.clone());
        let starter = tokio::spawn({
            let rt = rt.clone();
            async move { rt.start_durable(definition()).await }
        });
        tokio::time::timeout(BOUND, store.entered.notified())
            .await
            .unwrap();
        starter.abort();
        assert!(matches!(starter.await, Err(e) if e.is_cancelled()));
        let id = store.id();
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        if matches!(boundary, Boundary::BeforeCommit) {
            assert!(matches!(
                rt.resume(&id).await,
                Err(ResumeError::UnknownExecution)
            ));
        } else {
            assert_eq!(
                store.get(&id).await.unwrap().unwrap().state,
                ExecutionState::Created
            );
            clock.advance(Duration::from_secs(31));
            assert_eq!(
                tokio::time::timeout(BOUND, rt.resume(&id).await.unwrap().wait())
                    .await
                    .unwrap(),
                ExecutionState::Succeeded
            );
            assert_eq!(runs.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn ownership_is_rechecked_before_a_delayed_durable_start_launches() {
    let store = ControlledStore::new(Boundary::AfterCommit);
    let clock = Arc::new(FakeClock::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let rt = runtime(store.clone(), clock.clone(), runs.clone());
    let starter = tokio::spawn({
        let rt = rt.clone();
        async move { rt.start_durable(definition()).await }
    });
    tokio::time::timeout(BOUND, store.entered.notified())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(31));
    let owner = OwnerId::new();
    store
        .claim(&store.id(), &owner, Timestamp(31_000))
        .await
        .unwrap();
    store.proceed.notify_one();
    let handle = starter.await.unwrap().unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Cancelled
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.get(&store.id()).await.unwrap().unwrap().state,
        ExecutionState::Created
    );
    assert!(
        matches!(
            rt.resume(&store.id()).await,
            Err(ResumeError::ClaimedElsewhere)
        ),
        "failed dispatch removes local registration"
    );
}

#[tokio::test]
async fn durable_dispatch_read_failures_do_not_execute_and_allow_recovery() {
    for fault in 1..=3 {
        let store = ControlledStore::new(Boundary::AfterCommit);
        let runs = Arc::new(AtomicUsize::new(0));
        let rt = runtime(store.clone(), Arc::new(FakeClock::new()), runs.clone());
        let starter = tokio::spawn({
            let rt = rt.clone();
            async move { rt.start_durable(definition()).await }
        });
        tokio::time::timeout(BOUND, store.entered.notified())
            .await
            .unwrap();
        store.read_fault.store(fault, Ordering::SeqCst);
        store.proceed.notify_one();
        let handle = starter.await.unwrap().unwrap();
        assert_eq!(
            tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
            ExecutionState::Cancelled
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        store.read_fault.store(0, Ordering::SeqCst);
        assert_eq!(
            tokio::time::timeout(BOUND, rt.resume(&store.id()).await.unwrap().wait())
                .await
                .unwrap(),
            ExecutionState::Succeeded
        );
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn durable_dispatch_reloads_a_cancelled_snapshot() {
    let store = ControlledStore::new(Boundary::AfterCommit);
    let clock = Arc::new(FakeClock::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let rt = runtime(store.clone(), clock.clone(), runs.clone());
    let starter = tokio::spawn({
        let rt = rt.clone();
        async move { rt.start_durable(definition()).await }
    });
    tokio::time::timeout(BOUND, store.entered.notified())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(31));
    runtime(store.clone(), clock, runs.clone())
        .cancel(&store.id())
        .await
        .unwrap();
    store.proceed.notify_one();
    let handle = starter.await.unwrap().unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Cancelled
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_stopped_durable_handle_preserves_its_live_successor() {
    for fault in 1..=4 {
        tokio::time::timeout(BOUND, async {
            let store = ControlledStore::new(Boundary::AfterCommit);
            let clock = Arc::new(FakeClock::new());
            let runs = Arc::new(AtomicUsize::new(0));
            let rt = Arc::new(
                Runtime::builder()
                    .store(store.clone() as Arc<dyn StateStore>)
                    .clock(clock.clone())
                    .register_fn("a", {
                        let runs = runs.clone();
                        move |ctx| {
                            runs.fetch_add(1, Ordering::SeqCst);
                            async move {
                                NodeOutcome::Waiting {
                                    token: ctx.resume_token,
                                }
                            }
                        }
                    })
                    .build(),
            );
            let starter = tokio::spawn({
                let rt = rt.clone();
                async move { rt.start_durable(definition()).await }
            });
            store.entered.notified().await;
            let foreign_epoch = if fault == 4 {
                clock.advance(Duration::from_secs(31));
                Some(
                    store
                        .claim(&store.id(), &OwnerId::new(), Timestamp(31_000))
                        .await
                        .unwrap(),
                )
            } else {
                store.read_fault.store(fault, Ordering::SeqCst);
                None
            };
            store.proceed.notify_one();
            let stale = starter.await.unwrap().unwrap();
            assert_eq!(stale.wait_stable().await, ExecutionState::Cancelled);
            store.read_fault.store(0, Ordering::SeqCst);
            if let Some(epoch) = foreign_epoch {
                store.release(&store.id(), epoch).await.unwrap();
            }
            let successor = rt.resume(&store.id()).await.unwrap();
            assert_eq!(successor.wait_stable().await, ExecutionState::Waiting);
            let token = successor
                .inspect()
                .await
                .node(&keel_rt::NodeId::new("a"))
                .unwrap()
                .resume_token
                .clone()
                .unwrap();
            drop(stale);
            assert!(
                matches!(
                    rt.resume(&store.id()).await,
                    Err(ResumeError::AlreadyActive)
                ),
                "stale handle unregistered its successor after startup fault {fault}"
            );
            rt.complete(
                token,
                keel_rt::Resume::Complete(NodeOutcome::Succeeded(Default::default())),
            )
            .await
            .unwrap();
            assert_eq!(successor.wait().await, ExecutionState::Succeeded);
            assert_eq!(runs.load(Ordering::SeqCst), 1);
        })
        .await
        .expect("bounded stale handle recovery");
    }
}
