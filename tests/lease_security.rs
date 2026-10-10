use async_trait::async_trait;
use keel_rt::{
    AcceptPolicy, ApplyCmd, ClaimError, Execution, ExecutionId, ExecutionSnapshot, ExecutionState,
    FakeClock, LeaseEpoch, MemoryStore, NodeId, NodeOutcome, NodeState, OwnerId, Resume, Runtime,
    StateStore, StoreError, Timestamp, WorkflowDefinition, DEFAULT_LEASE_TTL,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);

// A real persistence boundary that yields before committing. This exposes
// executor side effects that a fully synchronous MemoryStore would hide.
struct YieldBeforePersist(MemoryStore);
#[async_trait]
impl StateStore for YieldBeforePersist {
    async fn put(&self, s: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.0.put(s).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.0.get(id).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.0.workflow_definition(id).await
    }
    async fn persist(&self, e: &Execution) -> Result<(), StoreError> {
        tokio::task::yield_now().await;
        self.0.persist(e).await
    }
    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        self.0.claim(id, owner, now).await
    }
    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        self.0.heartbeat(id, epoch, now).await
    }
    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.0.release(id, epoch).await
    }
    fn release_owner_now(&self, owner: &OwnerId) {
        self.0.release_owner_now(owner);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn running_transition_is_stored_before_executor_invocation() {
    let store = MemoryStore::new();
    let check = store.clone();
    let rt = Runtime::builder()
        .store(YieldBeforePersist(store))
        .register_fn("check", move |ctx| {
            let check = check.clone();
            async move {
                let snapshot = check.get(&ctx.execution_id).await.unwrap().unwrap();
                assert!(matches!(
                    snapshot.node(&ctx.node_id).unwrap().state,
                    NodeState::Running { .. }
                ));
                NodeOutcome::succeeded(Vec::new())
            }
        })
        .build();
    assert_eq!(
        rt.run(
            WorkflowDefinition::builder("fenced")
                .node("a", "check")
                .build()
                .unwrap()
        )
        .await
        .unwrap(),
        ExecutionState::Succeeded
    );
}

async fn takeover_before_completion(heartbeat_due: bool) {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let predecessor_store = store.clone();
    let predecessor_clock = clock.clone();
    let next_calls = calls.clone();
    let rt = Runtime::builder()
        .store(YieldBeforePersist(store))
        .clock(clock)
        .register_fn("first", move |ctx| {
            let store = predecessor_store.clone();
            let clock = predecessor_clock.clone();
            async move {
                let takeover = Timestamp(31_000);
                store
                    .claim(&ctx.execution_id, &OwnerId::new(), takeover)
                    .await
                    .unwrap();
                if heartbeat_due {
                    clock.advance(DEFAULT_LEASE_TTL + Duration::from_secs(1));
                }
                NodeOutcome::succeeded(Vec::new())
            }
        })
        .register_fn("next", move |_| {
            next_calls.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::succeeded(Vec::new()) }
        })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("takeover")
                .node("a", "first")
                .node("b", "next")
                .edge("a", "b")
                .build()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Cancelled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn queued_completion_after_due_heartbeat_cannot_launch_successor() {
    takeover_before_completion(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stale_persist_stops_before_successor_without_due_heartbeat() {
    takeover_before_completion(false).await;
}

// Commit cancellation at the exact claim boundary. This uses the real memory
// store for ownership and state, making read-before-claim observable.
struct CancelAtClaim {
    inner: MemoryStore,
}
#[async_trait]
impl StateStore for CancelAtClaim {
    async fn put(&self, s: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(s).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
    async fn persist(&self, e: &Execution) -> Result<(), StoreError> {
        self.inner.persist(e).await
    }
    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        if let Some(snapshot) = self.inner.get(id).await? {
            let def = self.inner.workflow_definition(id).await?.unwrap();
            let mut e = Execution::from_snapshot(def, snapshot).unwrap();
            e.apply(ApplyCmd::Cancel, &AcceptPolicy, now).unwrap();
            self.inner.persist(&e).await?;
        }
        self.inner.claim(id, owner, now).await
    }
    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.inner.release(id, epoch).await
    }
}

async fn waiting() -> (MemoryStore, ExecutionId, keel_rt::ResumeToken) {
    let store = MemoryStore::new();
    let mut e = Execution::new(
        WorkflowDefinition::builder("waiting")
            .node("hold", "wait")
            .build()
            .unwrap(),
    );
    e.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    e.apply(
        ApplyCmd::StartNode {
            node_id: NodeId::new("hold"),
        },
        &AcceptPolicy,
        Timestamp(0),
    )
    .unwrap();
    let token = e
        .snapshot()
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    e.apply(
        ApplyCmd::FinishNode {
            node_id: NodeId::new("hold"),
            attempt: 1,
            outcome: Ok(NodeOutcome::Waiting {
                token: token.clone(),
            }),
        },
        &AcceptPolicy,
        Timestamp(0),
    )
    .unwrap();
    store.persist(&e).await.unwrap();
    (store, e.id().clone(), token)
}

#[tokio::test(flavor = "current_thread")]
async fn resume_reads_cancellation_committed_at_claim() {
    let (store, id, _) = waiting().await;
    let rt = Runtime::builder()
        .store(CancelAtClaim { inner: store })
        .build();
    let handle = rt.resume(&id).await.unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Cancelled
    );
}

#[tokio::test(flavor = "current_thread")]
async fn offline_complete_reads_cancellation_committed_at_claim() {
    let (store, _, token) = waiting().await;
    let rt = Runtime::builder()
        .store(CancelAtClaim { inner: store })
        .build();
    assert_eq!(
        rt.complete(token, Resume::Complete(NodeOutcome::succeeded(Vec::new())))
            .await,
        Err(keel_rt::CompleteError::Cancelled)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_equal_revision_requires_identical_content() {
    let (store, id, _) = waiting().await;
    let snapshot = store.get(&id).await.unwrap().unwrap();
    store.put(&snapshot).await.unwrap();
    let mut conflicting = snapshot.clone();
    conflicting.state = ExecutionState::Cancelled;
    assert_eq!(
        store.put(&conflicting).await,
        Err(StoreError::Conflict {
            revision: snapshot.revision
        })
    );
    let e = Execution::from_snapshot(
        store.workflow_definition(&id).await.unwrap().unwrap(),
        conflicting,
    )
    .unwrap();
    assert_eq!(
        store.persist(&e).await,
        Err(StoreError::Conflict {
            revision: snapshot.revision
        })
    );
}
