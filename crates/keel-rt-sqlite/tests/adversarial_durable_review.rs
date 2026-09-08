//! Startup acceptance is separate from at-least-once executor effects.
use async_trait::async_trait;
use keel_rt::{
    ClaimError, Execution, ExecutionId, ExecutionSnapshot, ExecutionState, FakeClock,
    InitializeError, LeaseEpoch, NodeOutcome, OwnerId, Runtime, StateStore, StoreError, Timestamp,
    WorkflowDefinition,
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
}

// Explicit test double for a durable initialization port. MemoryStore itself
// deliberately does not implement this capability.
struct ControlledStore {
    inner: keel_rt_sqlite::SqliteStore,
    path: std::path::PathBuf,
    boundary: Boundary,
    entered: Notify,
    proceed: Notify,
    id: Mutex<Option<ExecutionId>>,
}

impl ControlledStore {
    fn new(boundary: Boundary) -> Arc<Self> {
        let path = std::env::temp_dir().join(format!("keel-adversarial-{}.db", ExecutionId::new()));
        Arc::new(Self {
            inner: keel_rt_sqlite::SqliteStore::open(&path).unwrap(),
            path,
            boundary,
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
        self.inner.get(id).await
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
            Boundary::BeforeCommit => {
                self.entered.notify_one();
                self.proceed.notified().await;
            }
            Boundary::AfterCommit => {}
        }
        let epoch = self.inner.initialize(exec, owner, now).await?;
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

impl Drop for ControlledStore {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

#[tokio::test]
async fn delayed_durable_start_must_not_rerun_nodes_completed_by_recovery() {
    tokio::time::timeout(BOUND, async {
        let store = ControlledStore::new(Boundary::AfterCommit);
        let clock = Arc::new(FakeClock::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let first = runtime(store.clone(), clock.clone(), runs.clone());
        let starter = tokio::spawn({
            let first = first.clone();
            async move { first.start_durable(definition()).await }
        });
        store.entered.notified().await;
        let id = store.id();
        clock.advance(Duration::from_secs(31));
        let second = runtime(store.clone(), clock.clone(), runs.clone());
        assert_eq!(
            second.resume(&id).await.unwrap().wait().await,
            ExecutionState::Succeeded
        );
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        store.proceed.notify_one();
        let stale = starter.await.unwrap().unwrap();
        let stale_result = stale.wait().await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "durably succeeded node reran; old start returned {stale_result:?}"
        );
    })
    .await
    .expect("bounded stale start test");
}

#[tokio::test]
async fn delayed_durable_start_must_not_resurrect_cancelled_execution() {
    tokio::time::timeout(BOUND, async {
        let store = ControlledStore::new(Boundary::AfterCommit);
        let clock = Arc::new(FakeClock::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let first = runtime(store.clone(), clock.clone(), runs.clone());
        let starter = tokio::spawn({
            let first = first.clone();
            async move { first.start_durable(definition()).await }
        });
        store.entered.notified().await;
        let id = store.id();
        clock.advance(Duration::from_secs(31));
        let second = runtime(store.clone(), clock.clone(), runs.clone());
        second.cancel(&id).await.unwrap();
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Cancelled
        );
        store.proceed.notify_one();
        let stale = starter.await.unwrap().unwrap();
        let result = stale.wait().await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "cancelled execution ran again; old start returned {result:?}"
        );
    })
    .await
    .expect("bounded cancellation takeover test");
}

#[tokio::test]
async fn attack_live_other_owner_blocks_delayed_start() {
    tokio::time::timeout(BOUND, async {
        let store = ControlledStore::new(Boundary::AfterCommit);
        let clock = Arc::new(FakeClock::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let first = runtime(store.clone(), clock.clone(), runs.clone());
        let starter = tokio::spawn({
            let first = first.clone();
            async move { first.start_durable(definition()).await }
        });
        store.entered.notified().await;
        clock.advance(Duration::from_secs(31));
        let other = OwnerId::new();
        let epoch = store
            .claim(&store.id(), &other, Timestamp(31_000))
            .await
            .unwrap();
        store.proceed.notify_one();
        let stale = starter.await.unwrap().unwrap();
        assert_eq!(stale.wait().await, ExecutionState::Cancelled);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        store.release(&store.id(), epoch).await.unwrap();
    })
    .await
    .expect("bounded live takeover test");
}

#[tokio::test]
async fn attack_abort_after_commit_repeatedly_recovers_without_losing_execution() {
    tokio::time::timeout(BOUND, async {
        for _ in 0..20 {
            let store = ControlledStore::new(Boundary::AfterCommit);
            let clock = Arc::new(FakeClock::new());
            let runs = Arc::new(AtomicUsize::new(0));
            let rt = runtime(store.clone(), clock.clone(), runs.clone());
            let starter = tokio::spawn({
                let rt = rt.clone();
                async move { rt.start_durable(definition()).await }
            });
            store.entered.notified().await;
            starter.abort();
            assert!(matches!(starter.await, Err(e) if e.is_cancelled()));
            assert_eq!(runs.load(Ordering::SeqCst), 0);
            assert_eq!(
                rt.resume(&store.id()).await.unwrap().wait().await,
                ExecutionState::Succeeded
            );
            for _ in 0..5 {
                assert_eq!(
                    rt.resume(&store.id()).await.unwrap().wait().await,
                    ExecutionState::Succeeded
                );
            }
            assert_eq!(runs.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .expect("bounded repeated recovery test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attack_durable_burst_fanout_and_terminal_recovery() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let store = ControlledStore::new(Boundary::BeforeCommit);
        let runs = Arc::new(AtomicUsize::new(0));
        let rt = Arc::new(
            Runtime::builder()
                .store(store.inner.clone())
                .register_fn("a", {
                    let runs = runs.clone();
                    move |_| {
                        let runs = runs.clone();
                        async move {
                            tokio::task::yield_now().await;
                            runs.fetch_add(1, Ordering::SeqCst);
                            NodeOutcome::Succeeded(Default::default())
                        }
                    }
                })
                .build(),
        );
        let mut builder = WorkflowDefinition::builder("burst");
        for i in 0..64 {
            builder = builder.node(format!("n{i}"), "a");
        }
        let def = builder.build().unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let rt = rt.clone();
            let def = def.clone();
            tasks.spawn(async move {
                let handle = rt.start_durable(def).await.unwrap();
                let id = handle.execution_id().clone();
                assert_eq!(handle.wait().await, ExecutionState::Succeeded);
                id
            });
        }
        let mut ids = std::collections::HashSet::new();
        while let Some(result) = tasks.join_next().await {
            assert!(ids.insert(result.unwrap()));
        }
        assert_eq!(runs.load(Ordering::SeqCst), 2048);
        for id in ids {
            let snap = store.get(&id).await.unwrap().unwrap();
            assert_eq!(snap.state, ExecutionState::Succeeded);
            assert_eq!(snap.nodes.len(), 64);
            assert_eq!(
                rt.resume(&id).await.unwrap().wait().await,
                ExecutionState::Succeeded
            );
        }
        assert_eq!(runs.load(Ordering::SeqCst), 2048);
    })
    .await
    .expect("bounded durable burst test");
}

#[tokio::test]
async fn released_successor_must_not_make_old_lease_epoch_valid_again() {
    tokio::time::timeout(BOUND, async {
        let store = ControlledStore::new(Boundary::BeforeCommit);
        let exec = Execution::new(definition());
        let first = OwnerId::new();
        let first_epoch = store
            .inner
            .initialize(&exec, &first, Timestamp(0))
            .await
            .unwrap();
        let second_epoch = store
            .claim(exec.id(), &OwnerId::new(), Timestamp(31_000))
            .await
            .unwrap();
        assert_ne!(first_epoch, second_epoch);
        store.release(exec.id(), second_epoch).await.unwrap();
        let third_epoch = store
            .claim(exec.id(), &OwnerId::new(), Timestamp(31_001))
            .await
            .unwrap();
        let stale_heartbeat = store
            .heartbeat(exec.id(), first_epoch, Timestamp(31_002))
            .await;
        assert_eq!(
            stale_heartbeat,
            Err(ClaimError::ClaimedElsewhere),
            "old epoch {first_epoch:?} became current again as {third_epoch:?}"
        );
    })
    .await
    .expect("bounded ABA fencing test");
}

#[test]
fn acknowledged_sqlite_start_must_not_rerun_recovered_succeeded_nodes() {
    let store = ControlledStore::new(Boundary::BeforeCommit);
    let clock = Arc::new(FakeClock::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let build = || {
        Runtime::builder()
            .store(store.inner.clone())
            .clock(clock.clone())
            .register_fn("a", {
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { NodeOutcome::Succeeded(Default::default()) }
                }
            })
            .build()
    };
    let first = build();
    let executor1 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // This is an acknowledged handle from an unmodified SqliteStore, with no wrapper around initialize.
    let handle = executor1
        .block_on(first.start_durable(definition()))
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(
        executor1.block_on(store.get(&id)).unwrap().unwrap().state,
        ExecutionState::Created
    );
    clock.advance(Duration::from_secs(31));
    let second = build();
    let executor2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    executor2.block_on(async {
        tokio::time::timeout(BOUND, async {
            assert_eq!(
                second.resume(&id).await.unwrap().wait().await,
                ExecutionState::Succeeded
            );
        })
        .await
        .unwrap();
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    executor1.block_on(async {
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap();
    });
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an acknowledged start reran a durably succeeded node after recovery"
    );
}

#[test]
fn acknowledged_start_must_preserve_succeeded_predecessor_after_waiting_takeover() {
    let store = ControlledStore::new(Boundary::BeforeCommit);
    let clock = Arc::new(FakeClock::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let build = || {
        Runtime::builder()
            .store(store.inner.clone())
            .clock(clock.clone())
            .register_fn("a", {
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { NodeOutcome::Succeeded(Default::default()) }
                }
            })
            .build()
    };
    let first = build();
    let executor1 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let def = WorkflowDefinition::builder("progress")
        .node("a", "a")
        .node("wait", "wait")
        .edge("a", "wait")
        .build()
        .unwrap();
    let handle = executor1.block_on(first.start_durable(def)).unwrap();
    let id = handle.execution_id().clone();
    clock.advance(Duration::from_secs(31));
    let second = build();
    let executor2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recovered = executor2.block_on(async {
        tokio::time::timeout(BOUND, async {
            let h = second.resume(&id).await.unwrap();
            assert_eq!(h.wait_stable().await, ExecutionState::Waiting);
            h
        })
        .await
        .unwrap()
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    clock.advance(Duration::from_secs(31));
    let stable = executor1.block_on(async {
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .unwrap()
    });
    drop(handle);
    drop(recovered);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "succeeded predecessor reran after Waiting successor lease expired, state={stable:?}"
    );
}

#[tokio::test]
async fn attack_initialization_write_failure_is_atomic_and_retryable() {
    tokio::time::timeout(BOUND, async {
        let store = ControlledStore::new(Boundary::BeforeCommit);
        let calls = Arc::new(AtomicUsize::new(0));
        let rt = Runtime::builder().store(store.inner.clone()).register_fn("a", {
            let calls = calls.clone(); move |_| {calls.fetch_add(1, Ordering::SeqCst); async {NodeOutcome::Succeeded(Default::default())}}
        }).build();
        let conn = rusqlite::Connection::open(&store.path).unwrap();
        for _ in 0..20 {
            conn.execute_batch("CREATE TRIGGER reject_node BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT, 'adversarial disk failure'); END;").unwrap();
            assert!(matches!(rt.start_durable(definition()).await, Err(keel_rt::DurableStartError::Initialization(InitializeError::Store(_)))));
            conn.execute_batch("DROP TRIGGER reject_node").unwrap();
        }
        for table in ["executions", "definitions", "nodes", "events"] {
            let count: usize = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r|r.get(0)).unwrap();
            assert_eq!(count, 0);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(rt.start_durable(definition()).await.unwrap().wait().await, ExecutionState::Succeeded);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }).await.expect("bounded injected failure test");
}

#[tokio::test]
async fn released_leases_reject_writes_and_cannot_release_successors() {
    for owner_cleanup in [false, true] {
        let store = ControlledStore::new(Boundary::BeforeCommit);
        let mut exec = Execution::new(definition());
        let owner = OwnerId::new();
        let epoch = store
            .inner
            .initialize(&exec, &owner, Timestamp(0))
            .await
            .unwrap();
        exec.set_fence_epoch(epoch.0);
        if owner_cleanup {
            store.inner.release_owner_now(&owner);
        } else {
            store.inner.release(exec.id(), epoch).await.unwrap();
        }
        assert_eq!(
            store.inner.heartbeat(exec.id(), epoch, Timestamp(1)).await,
            Err(ClaimError::ClaimedElsewhere)
        );
        assert!(matches!(
            store.inner.persist(&exec).await,
            Err(StoreError::StaleEpoch { .. })
        ));
        let next = store
            .inner
            .claim(exec.id(), &OwnerId::new(), Timestamp(2))
            .await
            .unwrap();
        assert!(next.0 > epoch.0);
        store.inner.release(exec.id(), epoch).await.unwrap();
        store
            .inner
            .heartbeat(exec.id(), next, Timestamp(3))
            .await
            .unwrap();
        assert!(matches!(
            store.inner.persist(&exec).await,
            Err(StoreError::StaleEpoch { .. })
        ));
    }
}
