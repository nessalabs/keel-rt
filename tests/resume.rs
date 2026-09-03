//! Public-surface resume: MemoryStore in-process, not process death.
//! Crash-across-file tests live in the sibling store crate.
//!
//! `cargo test --test resume -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{disable, enable, FailingStore, FakeClock, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyCmd, ClaimError, CompleteError, Event, Execution, ExecutionContext,
    ExecutionId, ExecutionState, FnSink, Join, LeaseEpoch, MemoryStore, NodeId, NodeOutcome,
    NodeState, OnFailure, OwnerId, Recover, Resume, ResumeError, RetryPolicy, Runtime,
    SnapshotError, StateStore, StoreError, Timestamp, WorkflowDefinition, DEFAULT_CANCEL_BOUND,
    DEFAULT_LEASE_TTL, SCHEMA_VERSION,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(5);

async fn within<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(BOUND, f)
        .await
        .expect("resume test timed out")
}

fn succeed(id: &str) -> impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> {
    let _ = id;
    |_ctx: ExecutionContext| std::future::ready(NodeOutcome::Succeeded(Bytes::from_static(b"ok")))
}

#[tokio::test(flavor = "current_thread")]
async fn resume_unknown_id_is_unknown_execution() {
    let rt = Runtime::builder().register_fn("a", succeed("a")).build();
    let id = ExecutionId::parse("exec-missing").unwrap();
    match rt.resume(&id).await {
        Err(e) => assert_eq!(e, ResumeError::UnknownExecution),
        Ok(_) => panic!("expected UnknownExecution"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_of_live_start_is_already_active() {
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    match rt.resume(&id).await {
        Err(e) => assert_eq!(e, ResumeError::AlreadyActive),
        Ok(_) => panic!("expected AlreadyActive"),
    }
    handle.cancel().await;
    within(handle.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn shared_memory_store_second_runtime_resume_is_claimed_elsewhere() {
    let store = MemoryStore::new();
    let rt_a = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let handle = rt_a
        .start(
            WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    within(handle.wait_stable()).await;
    let rt_b = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    match rt_b.resume(&id).await {
        Err(ResumeError::ClaimedElsewhere) => {}
        Ok(_) => panic!("shared MemoryStore must fence the second Runtime"),
        Err(e) => panic!("expected ClaimedElsewhere, got {e}"),
    }
    handle.cancel().await;
    within(handle.wait()).await;
}

/// Heartbeat succeeds but does not extend the lease, so TTL steal can
/// happen while A's drive (and live_tx) is still running.
#[derive(Clone)]
struct NoRefreshHeartbeat {
    inner: MemoryStore,
}

#[async_trait::async_trait]
impl StateStore for NoRefreshHeartbeat {
    async fn put(&self, snapshot: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.inner.persist(exec).await
    }
    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
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
    ) -> Result<LeaseEpoch, ClaimError> {
        self.inner.claim(id, owner, now).await
    }
    async fn heartbeat(
        &self,
        _id: &ExecutionId,
        _epoch: LeaseEpoch,
        _now: Timestamp,
    ) -> Result<(), ClaimError> {
        Ok(())
    }
    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.inner.release(id, epoch).await
    }
    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        self.inner.release_now(id, epoch);
    }
    fn release_owner_now(&self, owner: &OwnerId) {
        self.inner.release_owner_now(owner);
    }
}

/// After TTL steal, A's live_tx must not inject Complete while B owns.
#[tokio::test(flavor = "current_thread")]
async fn live_complete_after_ttl_steal_is_claimed_elsewhere() {
    let store = NoRefreshHeartbeat {
        inner: MemoryStore::new(),
    };
    let clock = Arc::new(FakeClock::new());
    let next_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let next = next_runs.clone();
    let rt_a = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .register_fn("next", move |_ctx: ExecutionContext| {
            next.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = rt_a.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    clock.advance(DEFAULT_LEASE_TTL);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let rt_b = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .register_fn("next", succeed("next"))
        .build();
    let hb = rt_b
        .resume(&id)
        .await
        .expect("B claims after TTL while A handle is still live");
    match rt_a
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
    {
        Err(CompleteError::ClaimedElsewhere)
        | Err(CompleteError::Store(StoreError::StaleEpoch { .. })) => {}
        Err(CompleteError::UnknownToken) => panic!("must not be UnknownToken"),
        Ok(()) => panic!("live complete after steal must not be Ok"),
        Err(e) => panic!("expected ClaimedElsewhere / StaleEpoch, got {e}"),
    }
    assert_eq!(
        next_runs.load(Ordering::SeqCst),
        0,
        "A must not dispatch downstream after steal"
    );
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Waiting);
    assert!(matches!(
        snap.node(&NodeId::new("hold")).unwrap().state,
        NodeState::Waiting { .. }
    ));
    rt_b.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
    )
    .await
    .expect("owner B completes");
    assert_eq!(within(hb.wait()).await, ExecutionState::Succeeded);
    assert_eq!(next_runs.load(Ordering::SeqCst), 0);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Succeeded);
    assert_eq!(
        snap.node(&NodeId::new("next"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"ok"))
    );
    // Keep A's handle live until B finished; Drop now is Cancel on terminal.
    drop(handle);
}

/// After TTL steal, A's ExecutionHandle::resume must not inject Complete.
#[tokio::test(flavor = "current_thread")]
async fn handle_resume_after_ttl_steal_is_claimed_elsewhere() {
    let store = NoRefreshHeartbeat {
        inner: MemoryStore::new(),
    };
    let clock = Arc::new(FakeClock::new());
    let next_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let next = next_runs.clone();
    let rt_a = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .register_fn("next", move |_ctx: ExecutionContext| {
            next.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = rt_a.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    clock.advance(DEFAULT_LEASE_TTL);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let rt_b = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .register_fn("next", succeed("next"))
        .build();
    let hb = rt_b
        .resume(&id)
        .await
        .expect("B claims after TTL while A handle is still live");
    match handle
        .resume(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
    {
        Err(keel_rt::ApplyError::Illegal(s)) if s.contains("claimed elsewhere") => {}
        Err(keel_rt::ApplyError::Illegal(s)) if s.contains("persist failed") => {
            panic!("handle resume must refuse before inject, got persist failed")
        }
        Ok(()) => panic!("handle resume after steal must not be Ok"),
        Err(e) => panic!("expected ClaimedElsewhere equivalent, got {e}"),
    }
    assert_eq!(
        next_runs.load(Ordering::SeqCst),
        0,
        "A must not dispatch downstream after steal"
    );
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Waiting);
    assert!(matches!(
        snap.node(&NodeId::new("hold")).unwrap().state,
        NodeState::Waiting { .. }
    ));
    rt_b.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
    )
    .await
    .expect("owner B completes");
    assert_eq!(within(hb.wait()).await, ExecutionState::Succeeded);
    assert_eq!(next_runs.load(Ordering::SeqCst), 0);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Succeeded);
    drop(handle);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_after_wait_returns_terminal_without_re_running() {
    let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c = attempts.clone();
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", move |_ctx: ExecutionContext| {
            c.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "succeeded node must not re-run"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_does_not_survive_process_death() {
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    drop(rt);
    let rt = Runtime::builder()
        .store(MemoryStore::new())
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume(&id).await {
        Err(e) => assert_eq!(e, ResumeError::UnknownExecution),
        Ok(_) => panic!("a new MemoryStore must not invent history"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_reinvokes_running_and_skips_succeeded() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    let mut ex = Execution::new(def.clone());
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "a".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"A"))),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "b".into(),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();

    let a_attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let b_attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let ac = a_attempts.clone();
    let bc = b_attempts.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("ea", move |_ctx: ExecutionContext| {
            ac.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) }
        })
        .register_fn("eb", move |ctx: ExecutionContext| {
            bc.fetch_add(1, Ordering::SeqCst);
            let attempt = ctx.attempt;
            async move {
                assert_eq!(attempt, 2, "Running at crash re-invokes at attempt + 1");
                NodeOutcome::Succeeded(Bytes::from_static(b"B"))
            }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(a_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(b_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_keeps_waiting_token() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .node("b", "b")
        .edge("a", "b")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .register_fn("b", succeed("b"))
        .build();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    within(handle.wait_stable()).await;
    let token = store
        .get(&id)
        .await
        .unwrap()
        .unwrap()
        .node(&NodeId::new("a"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    drop(handle);
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    handle
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
        )
        .await
        .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_retry_ready_does_not_fire_before_deadline() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = RetryPolicy::new(3, Duration::from_millis(50));
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "a".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Failed(keel_rt::NodeError::new("boom"))),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .policy(p)
        .register_fn("a", move |_ctx: ExecutionContext| {
            f.store(true, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    tokio::task::yield_now().await;
    assert!(
        !fired.load(Ordering::SeqCst),
        "retry Ready must wait for runnable_at"
    );
    clock.advance(Duration::from_millis(50));
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert!(fired.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "current_thread")]
async fn resume_failed_execution_stays_failed() {
    let store = MemoryStore::new();
    let run = within(
        WorkflowTest::new()
            .store(store.clone())
            .node("a", ScriptedExecutor::new("a").fail("boom"))
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    let id = run.snapshot().await.execution_id.clone();
    drop(run);
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_without_definition_is_definition_missing() {
    struct SnapOnly {
        inner: MemoryStore,
    }
    #[async_trait::async_trait]
    impl StateStore for SnapOnly {
        async fn put(&self, s: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            self.inner.put(s).await
        }
        async fn get(
            &self,
            id: &ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            self.inner.persist(exec).await
        }
        async fn workflow_definition(
            &self,
            _id: &ExecutionId,
        ) -> Result<Option<WorkflowDefinition>, StoreError> {
            Ok(None)
        }
    }
    let store = SnapOnly {
        inner: MemoryStore::new(),
    };
    let exec = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap(),
    );
    store.persist(&exec).await.unwrap();
    let id = exec.id().clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume(&id).await {
        Err(e) => assert_eq!(e, ResumeError::DefinitionMissing),
        Ok(_) => panic!("expected DefinitionMissing"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_schema_mismatch_is_snapshot_error() {
    struct BadSchema {
        inner: MemoryStore,
    }
    #[async_trait::async_trait]
    impl StateStore for BadSchema {
        async fn put(&self, s: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            self.inner.put(s).await
        }
        async fn get(
            &self,
            id: &ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            Ok(self.inner.get(id).await?.map(|mut s| {
                s.schema_version = 99;
                s
            }))
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
    }
    let store = BadSchema {
        inner: MemoryStore::new(),
    };
    let exec = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap(),
    );
    store.persist(&exec).await.unwrap();
    let id = exec.id().clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume(&id).await {
        Err(ResumeError::Snapshot(SnapshotError::SchemaMismatch {
            found: 99,
            expected: SCHEMA_VERSION,
        })) => {}
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("expected resume error"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_unregistered_executor_fails_closed() {
    let store = MemoryStore::new();
    let exec = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "missing")
            .build()
            .unwrap(),
    );
    store.persist(&exec).await.unwrap();
    let id = exec.id().clone();
    let rt = Runtime::builder().store(store).build();
    match rt.resume(&id).await {
        Err(ResumeError::UnregisteredExecutors(_)) => {}
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("expected resume error"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_store_get_error_is_store() {
    struct Boom;
    #[async_trait::async_trait]
    impl StateStore for Boom {
        async fn put(&self, _: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            Ok(())
        }
        async fn get(
            &self,
            _: &ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            Err(StoreError::Message("get boom".into()))
        }
    }
    let rt = Runtime::builder()
        .store(Boom)
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume(&ExecutionId::parse("exec-1").unwrap()).await {
        Err(ResumeError::Store(StoreError::Message(m))) if m.contains("get boom") => {}
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("expected resume error"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_store_definition_error_is_store() {
    struct BoomDef;
    #[async_trait::async_trait]
    impl StateStore for BoomDef {
        async fn put(&self, _: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            Ok(())
        }
        async fn get(
            &self,
            _: &ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            Ok(Some(
                Execution::new(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .snapshot(),
            ))
        }
        async fn workflow_definition(
            &self,
            _: &ExecutionId,
        ) -> Result<Option<WorkflowDefinition>, StoreError> {
            Err(StoreError::Message("def boom".into()))
        }
    }
    let rt = Runtime::builder()
        .store(BoomDef)
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume(&ExecutionId::parse("exec-1").unwrap()).await {
        Err(ResumeError::Store(StoreError::Message(m))) if m.contains("def boom") => {}
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("expected resume error"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_created_snapshot_starts_sources() {
    let store = MemoryStore::new();
    let exec = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap(),
    );
    store.persist(&exec).await.unwrap();
    let id = exec.id().clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_diamond_join_waits_for_both_sides() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("src", "src")
        .node("sum", "sum")
        .node("crit", "crit")
        .node("writer", "writer")
        .edge("src", "sum")
        .edge("src", "crit")
        .edge("sum", "writer")
        .edge("crit", "writer")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    for id in ["src", "sum"] {
        ex.apply(ApplyCmd::StartNode { node_id: id.into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: id.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .unwrap();
    }
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "crit".into(),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let writer = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let w = writer.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("src", succeed("src"))
        .register_fn("sum", succeed("sum"))
        .register_fn("crit", succeed("crit"))
        .register_fn("writer", move |_ctx: ExecutionContext| {
            w.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"w")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(writer.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_fail_subtree_keeps_failed_pages_and_runs_reducer() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("p1", "p1")
        .node("p2", "p2")
        .node("red", "red")
        .join("red", Join::AllDone)
        .edge("p1", "red")
        .edge("p2", "red")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "p1".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "p1".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Failed(keel_rt::NodeError::new("page"))),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "p2".into(),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let p1 = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c1 = p1.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("p1", move |_ctx: ExecutionContext| {
            c1.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"no")) }
        })
        .register_fn("p2", succeed("p2"))
        .register_fn("red", succeed("red"))
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);
    assert_eq!(p1.load(Ordering::SeqCst), 0, "failed page must not re-run");
}

#[tokio::test(flavor = "current_thread")]
async fn resume_error_display_names_the_case() {
    assert!(ResumeError::UnknownExecution
        .to_string()
        .contains("unknown"));
    assert!(ResumeError::AlreadyActive.to_string().contains("already"));
    assert!(ResumeError::ClaimedElsewhere
        .to_string()
        .contains("elsewhere"));
    assert!(ResumeError::DefinitionMissing
        .to_string()
        .contains("definition"));
    assert!(ResumeError::NotFailed.to_string().contains("Failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn persist_then_emit_still_holds_after_resume_terminal() {
    let store = MemoryStore::new();
    let seen = Arc::new(AtomicBool::new(false));
    let flag = seen.clone();
    let sink = FnSink(move |e: &Event| {
        if matches!(e, Event::ExecutionSucceeded { .. }) {
            flag.store(true, Ordering::SeqCst);
        }
    });
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .sink(sink)
        .register_fn("a", succeed("a"))
        .build();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    seen.store(false, Ordering::SeqCst);
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert!(
        !seen.load(Ordering::SeqCst),
        "terminal resume must not re-announce ExecutionSucceeded"
    );
}

fn fail_fast_diamond() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .node("c", "ec")
        .node("d", "ed")
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .build()
        .unwrap()
}

/// Fail-fast A→{B,C}→D, B Failed, C cancelled while Running. `resume` stays
/// Failed; `resume_with(RetryFailed)` re-runs B; C and D were Cancelled and
/// run after preds Succeeded; A is not re-run.
#[tokio::test(flavor = "current_thread")]
async fn resume_stays_failed_resume_with_retry_failed_reruns_b_only() {
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("ea", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"A"))
        })
        .register_fn("eb", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("boom")
        })
        .register(ScriptedExecutor::new("ec").hang(false))
        .register_fn("ed", |_ctx: ExecutionContext| async {
            panic!("D must not run on fail-fast")
        })
        .build();
    let handle = rt.start(fail_fast_diamond()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert!(matches!(
        snap.node(&NodeId::new("b")).unwrap().state,
        NodeState::Failed
    ));
    assert!(matches!(
        snap.node(&NodeId::new("c")).unwrap().state,
        NodeState::Cancelled
    ));
    assert!(matches!(
        snap.node(&NodeId::new("d")).unwrap().state,
        NodeState::Cancelled
    ));

    let a_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let b_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let d_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (ac, bc, cc, dc) = (
        a_runs.clone(),
        b_runs.clone(),
        c_runs.clone(),
        d_runs.clone(),
    );
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("ea", move |_ctx: ExecutionContext| {
            ac.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) }
        })
        .register_fn("eb", move |_ctx: ExecutionContext| {
            bc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"B")) }
        })
        .register_fn("ec", move |_ctx: ExecutionContext| {
            cc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"C")) }
        })
        .register_fn("ed", move |_ctx: ExecutionContext| {
            dc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"D")) }
        })
        .build();
    let stay = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(stay.wait()).await, ExecutionState::Failed);
    assert_eq!(a_runs.load(Ordering::SeqCst), 0);
    assert_eq!(b_runs.load(Ordering::SeqCst), 0);

    let h = within(rt.resume_with(&id, Recover::RetryFailed))
        .await
        .unwrap();
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
    assert_eq!(a_runs.load(Ordering::SeqCst), 0, "A must not re-run");
    assert_eq!(b_runs.load(Ordering::SeqCst), 1, "B is the Failed retry");
    assert_eq!(
        c_runs.load(Ordering::SeqCst),
        1,
        "Cancelled C runs after recover"
    );
    assert_eq!(
        d_runs.load(Ordering::SeqCst),
        1,
        "Cancelled D runs after B and C"
    );
}

/// FailSubtree + AllDone: failed page retried; succeeded pages not; reducer
/// already Succeeded so it does not re-run (waits only if it was Cancelled).
#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_fail_subtree_all_done_retries_failed_page() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("p1", "e")
        .node("p2", "e")
        .node("join", "j")
        .edge("p1", "join")
        .edge("p2", "join")
        .join("join", Join::AllDone)
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("e", |ctx: ExecutionContext| async move {
            if ctx.node_id.as_str() == "p1" {
                NodeOutcome::failed("page")
            } else {
                NodeOutcome::Succeeded(Bytes::from_static(b"p2"))
            }
        })
        .register_fn("j", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"join"))
        })
        .build();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);

    let p1 = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let p2 = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (c1, c2) = (p1.clone(), p2.clone());
    let join = ScriptedExecutor::new("j").succeed(Bytes::from_static(b"join2"));
    let rt = Runtime::builder()
        .store(store)
        .register_fn("e", move |ctx: ExecutionContext| {
            if ctx.node_id.as_str() == "p1" {
                c1.fetch_add(1, Ordering::SeqCst);
            } else {
                c2.fetch_add(1, Ordering::SeqCst);
            }
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .register(join.clone())
        .build();
    let h = within(rt.resume_with(&id, Recover::RetryFailed))
        .await
        .unwrap();
    assert_eq!(
        within(h.wait()).await,
        ExecutionState::Succeeded,
        "retried page Succeeded; no Failed/Cancelled remain"
    );
    assert_eq!(p1.load(Ordering::SeqCst), 1, "failed page retried");
    assert_eq!(p2.load(Ordering::SeqCst), 0, "succeeded page not re-run");
    assert_eq!(
        join.attempts().len(),
        1,
        "AllDone reducer re-runs once after the retried pred Succeeded"
    );
    let inputs = join.last_inputs().expect("join ran");
    assert_eq!(
        inputs.get(&NodeId::new("p1")).map(|b| b.as_ref()),
        Some(b"ok".as_slice()),
        "join inputs_for includes p1's new Bytes"
    );
}

/// FailSubtree + AllDone: p1 Failed, p2 Succeeded, join itself Failed
/// (Completed). RetryFailed must not Ready the join before remain is
/// recounted — p1 is no longer terminal, so the join waits. On the bug,
/// join run count hits 1 immediately alongside hanging p1.
#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_failed_all_done_join_waits_for_retried_pred() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("p1", "p1")
        .node("p2", "p2")
        .node("join", "j")
        .edge("p1", "join")
        .edge("p2", "join")
        .join("join", Join::AllDone)
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("p1", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("page")
        })
        .register_fn("p2", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"p2"))
        })
        .register_fn("j", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("join")
        })
        .build();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);

    let p1 = ScriptedExecutor::new("p1").hang(false);
    let p2_runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c2 = p2_runs.clone();
    let join = ScriptedExecutor::new("j").succeed(Bytes::from_static(b"joined"));
    let rt = Runtime::builder()
        .store(store)
        .register(p1.clone())
        .register_fn("p2", move |_ctx: ExecutionContext| {
            c2.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"p2")) }
        })
        .register(join.clone())
        .build();
    let h = within(rt.resume_with(&id, Recover::RetryFailed))
        .await
        .unwrap();
    within(p1.wait_until_hanging()).await;
    assert_eq!(
        join.attempts().len(),
        0,
        "join must not run while p1 is in flight"
    );
    let snap = h.inspect().await;
    assert!(
        matches!(
            snap.node(&NodeId::new("join")).unwrap().state,
            NodeState::Pending
        ),
        "AllDone join stays Pending until the retried pred is terminal again, got {:?}",
        snap.node(&NodeId::new("join")).unwrap().state
    );
    p1.release();
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
    assert_eq!(p2_runs.load(Ordering::SeqCst), 0, "p2 must not re-run");
    assert_eq!(
        join.attempts().len(),
        1,
        "join runs once after p1 Succeeded"
    );
    let inputs = join.last_inputs().expect("join ran");
    assert_eq!(
        inputs.get(&NodeId::new("p1")).map(|b| b.as_ref()),
        Some(b"released".as_slice()),
        "join must see p1's new Bytes"
    );
}

/// Time-sensitive product: after Failed, `start` is a new id and every node runs.
#[tokio::test(flavor = "current_thread")]
async fn start_after_failed_is_new_id_and_reruns_all_nodes() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("boom")
        })
        .build();
    let h = rt.start(def.clone()).unwrap();
    let failed_id = h.execution_id().clone();
    assert_eq!(within(h.wait()).await, ExecutionState::Failed);

    let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c = runs.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", move |_ctx: ExecutionContext| {
            c.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let h = rt.start(def).unwrap();
    assert_ne!(h.execution_id(), &failed_id);
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

/// Live handle owns the id: RetryFailed is AlreadyActive, not Recover.
#[tokio::test(flavor = "current_thread")]
async fn hitl_live_handle_retry_failed_is_already_active() {
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    tokio::task::yield_now().await;
    let snap = handle.inspect().await;
    let token = snap
        .node(&NodeId::new("a"))
        .unwrap()
        .resume_token
        .clone()
        .expect("kernel token");
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::AlreadyActive) => {}
        Ok(_) => panic!("live handle owns the id"),
        Err(e) => panic!("live handle owns the id, got {e}"),
    }
    handle
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
        )
        .await
        .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_on_succeeded_is_not_failed() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", succeed("a"))
        .build();
    let h = rt.start(def).unwrap();
    let id = h.execution_id().clone();
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::NotFailed) => {}
        Ok(_) => panic!("expected NotFailed"),
        Err(e) => panic!("{e}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_on_waiting_is_not_failed() {
    let store = MemoryStore::new();
    let id = {
        let rt = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |ctx: ExecutionContext| async move {
                NodeOutcome::Waiting {
                    token: ctx.resume_token,
                }
            })
            .build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        within(async {
            loop {
                if let Some(snap) = store.get(&id).await.unwrap() {
                    if matches!(
                        snap.node(&NodeId::new("a")).unwrap().state,
                        NodeState::Waiting { .. }
                    ) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        std::mem::forget(handle);
        drop(rt);
        id
    };
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::NotFailed) => {}
        Ok(_) => panic!("Waiting is HITL"),
        Err(e) => panic!("Waiting is HITL, got {e}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_on_cancelled_is_not_failed() {
    let store = MemoryStore::new();
    let hang = ScriptedExecutor::new("a").hang(false);
    let rt = Runtime::builder()
        .store(store.clone())
        .register(hang.clone())
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    within(hang.wait_until_hanging()).await;
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::NotFailed) => {}
        Ok(_) => panic!("user-Cancelled is not RetryFailed"),
        Err(e) => panic!("user-Cancelled is NotFailed, got {e}"),
    }
}

/// max_attempts=1 Accepts the first fail. Recover resets attempt; a new
/// Runtime with max_attempts=2 must Retry the next fail (fresh budget),
/// not Accept because leftover attempt was already max.
#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_resets_retry_policy_budget() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .policy(RetryPolicy::new(1, Duration::ZERO))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("boom")
        })
        .build();
    let h = rt.start(def).unwrap();
    let id = h.execution_id().clone();
    assert_eq!(within(h.wait()).await, ExecutionState::Failed);

    let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c = attempts.clone();
    let rt = Runtime::builder()
        .store(store)
        .policy(RetryPolicy::new(2, Duration::ZERO))
        .register_fn("a", move |_ctx: ExecutionContext| {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n == 1 {
                    NodeOutcome::failed("again")
                } else {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }
        })
        .build();
    let h = within(rt.resume_with(&id, Recover::RetryFailed))
        .await
        .unwrap();
    assert_eq!(
        within(h.wait()).await,
        ExecutionState::Succeeded,
        "fresh budget: first fail after Recover must Retry, not Accept"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_persist_err_leaves_failed_then_retry_works() {
    let store = Arc::new(FailingStore::fail_on_nth_put(0));
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store_arc(store.clone() as Arc<dyn StateStore>)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("boom")
        })
        .build();
    let h = rt.start(def).unwrap();
    let id = h.execution_id().clone();
    assert_eq!(within(h.wait()).await, ExecutionState::Failed);
    enable("store.put", 1);
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::Store(_)) => {}
        Ok(_) => panic!("recover persist Err must surface"),
        Err(e) => panic!("expected Store, got {e}"),
    }
    disable("store.put");
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Failed);
    assert!(matches!(
        snap.node(&NodeId::new("a")).unwrap().state,
        NodeState::Failed
    ));
    drop(rt);
    let rt = Runtime::builder()
        .store(store.inner().clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let h = within(rt.resume_with(&id, Recover::RetryFailed))
        .await
        .unwrap();
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_with_retry_failed_on_waiting_then_continue_keeps_token() {
    let store = MemoryStore::new();
    let id = {
        let rt = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |ctx: ExecutionContext| async move {
                NodeOutcome::Waiting {
                    token: ctx.resume_token,
                }
            })
            .build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        within(async {
            loop {
                if let Some(snap) = store.get(&id).await.unwrap() {
                    if matches!(
                        snap.node(&NodeId::new("a")).unwrap().state,
                        NodeState::Waiting { .. }
                    ) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        std::mem::forget(handle);
        drop(rt);
        id
    };
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", succeed("a"))
        .build();
    match rt.resume_with(&id, Recover::RetryFailed).await {
        Err(ResumeError::NotFailed) => {}
        Ok(_) => panic!("Waiting is token resume"),
        Err(e) => panic!("Waiting is token resume, got {e}"),
    }
    let h = within(rt.resume(&id)).await.unwrap();
    let snap = h.inspect().await;
    assert!(matches!(
        snap.node(&NodeId::new("a")).unwrap().state,
        NodeState::Waiting { .. }
    ));
    let token = snap
        .node(&NodeId::new("a"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    h.resume(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
    )
    .await
    .unwrap();
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
}

fn wait_then_next() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .node("next", "next")
        .edge("hold", "next")
        .build()
        .unwrap()
}

/// Builtin `wait`, no manual register; second task `complete` (not handle.resume).
#[tokio::test(flavor = "current_thread")]
async fn complete_from_second_task_unblocks_wait_and_downstream_sees_bytes() {
    let store = MemoryStore::new();
    let rt = Arc::new(
        Runtime::builder()
            .store(store.clone())
            .register_fn("next", succeed("next"))
            .build(),
    );
    let handle = rt.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let rt2 = rt.clone();
    let task = tokio::spawn(async move {
        rt2.complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
    });
    within(task).await.expect("join").expect("complete");
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(
        snap.node(&NodeId::new("next"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"ok"))
    );
    assert_eq!(
        snap.node(&NodeId::new("hold"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"gate"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn complete_unknown_token_errors() {
    let rt = Runtime::builder().build();
    let token = keel_rt::ResumeToken::issue(
        ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
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
async fn complete_after_drop_handle_does_not_revive() {
    let store = MemoryStore::new();
    let id;
    let token;
    {
        let rt = Runtime::builder().store(store.clone()).build();
        let handle = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        id = handle.execution_id().clone();
        within(handle.wait_stable()).await;
        token = handle
            .inspect()
            .await
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        drop(handle);
        within(async {
            loop {
                if let Some(s) = store.get(&id).await.unwrap() {
                    if s.state == ExecutionState::Cancelled {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
    let rt = Runtime::builder().store(store.clone()).build();
    match rt
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        )
        .await
    {
        Err(CompleteError::Cancelled) => {}
        other => panic!("{other:?}"),
    }
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Cancelled);
}

/// Drop of a **terminal** handle (Cancel + Shutdown, consumed=false) must
/// not poison duplicate complete. Live-park drop stays Cancelled (above).
#[tokio::test(flavor = "current_thread")]
async fn complete_after_drop_of_terminal_handle_is_duplicate_noop() {
    let rt = Runtime::builder().build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    within(handle.wait_stable()).await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let resume = Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate")));
    rt.complete(token.clone(), resume.clone())
        .await
        .expect("first");
    within(async {
        loop {
            if handle.inspect().await.state == ExecutionState::Succeeded {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    drop(handle);
    rt.complete(token, resume)
        .await
        .expect("duplicate after terminal Drop is noop, not Apply");
    assert_eq!(
        rt.inspect(&id).await.unwrap().state,
        ExecutionState::Succeeded
    );
}

#[tokio::test(flavor = "current_thread")]
async fn complete_from_store_after_engine_down_unblocks_wait() {
    let store = MemoryStore::new();
    let (id, token) = {
        let rt = Runtime::builder()
            .store(store.clone())
            .register_fn("next", succeed("next"))
            .build();
        let handle = rt.start(wait_then_next()).unwrap();
        let id = handle.execution_id().clone();
        within(async {
            loop {
                if let Some(snap) = store.get(&id).await.unwrap() {
                    if matches!(
                        snap.node(&NodeId::new("hold")).unwrap().state,
                        NodeState::Waiting { .. }
                    ) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let token = store
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        std::mem::forget(handle);
        drop(rt);
        (id, token)
    };
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("next", succeed("next"))
        .build();
    rt.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if let Some(snap) = store.get(&id).await.unwrap() {
                if snap.state == ExecutionState::Succeeded {
                    assert_eq!(
                        snap.node(&NodeId::new("next"))
                            .and_then(|n| n.output.clone()),
                        Some(Bytes::from_static(b"ok"))
                    );
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn complete_failed_uses_fail_fast() {
    let rt = Runtime::builder()
        .register_fn("next", succeed("next"))
        .build();
    let handle = rt.start(wait_then_next()).unwrap();
    within(handle.wait_stable()).await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    rt.complete(token, Resume::Complete(NodeOutcome::failed("no")))
        .await
        .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn complete_duplicate_is_noop() {
    let rt = Runtime::builder().build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let outcome = NodeOutcome::Succeeded(Bytes::from_static(b"once"));
    rt.complete(token.clone(), Resume::Complete(outcome.clone()))
        .await
        .unwrap();
    rt.complete(token, Resume::Complete(outcome)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn complete_wrong_nonce_is_unknown_token() {
    let rt = Runtime::builder().build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let real = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let fake = keel_rt::ResumeToken::issue(
        real.execution_id().clone(),
        real.node_id().clone(),
        real.attempt(),
    );
    match rt
        .complete(
            fake,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
    {
        Err(CompleteError::UnknownToken) => {}
        other => panic!("{other:?}"),
    }
    handle.cancel().await;
    within(handle.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn complete_store_path_missing_executor_is_unregistered() {
    let store = MemoryStore::new();
    let token = {
        let rt = Runtime::builder()
            .store(store.clone())
            .register_fn("next", succeed("next"))
            .build();
        let handle = rt.start(wait_then_next()).unwrap();
        let id = handle.execution_id().clone();
        within(async {
            loop {
                if let Some(s) = store.get(&id).await.unwrap() {
                    if matches!(
                        s.node(&NodeId::new("hold")).unwrap().state,
                        NodeState::Waiting { .. }
                    ) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let token = store
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        std::mem::forget(handle);
        drop(rt);
        token
    };
    let rt = Runtime::builder().store(store).build();
    match rt
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
    {
        Err(CompleteError::UnregisteredExecutors(u)) => {
            assert!(u.to_string().contains("next"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn complete_store_persist_err_is_store() {
    let store = Arc::new(FailingStore::fail_on_nth_put(0));
    let rt = Runtime::builder()
        .store_arc(store.clone() as Arc<dyn StateStore>)
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    std::mem::forget(handle);
    drop(rt);
    enable("store.put", 1);
    let rt = Runtime::builder()
        .store_arc(store.clone() as Arc<dyn StateStore>)
        .build();
    match rt
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
    {
        Err(CompleteError::Store(_)) => {}
        other => panic!("{other:?}"),
    }
    let snap = store.get(token.execution_id()).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Waiting);
    assert!(matches!(
        snap.node(&NodeId::new("hold")).unwrap().state,
        NodeState::Waiting { .. }
    ));
    disable("store.put");
    let id = token.execution_id().clone();
    rt.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.state == ExecutionState::Succeeded {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn live_complete_persist_err_is_not_ok() {
    let store = Arc::new(FailingStore::fail_on_nth_put(0));
    let rt = Runtime::builder()
        .store_arc(store.clone() as Arc<dyn StateStore>)
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    enable("store.put", 1);
    match rt
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
    {
        Err(CompleteError::Apply(_)) => {}
        other => panic!("complete Ok only after persist Ok, got {other:?}"),
    }
    disable("store.put");
    rt.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn complete_reinvoke_from_store_runs_wait_again() {
    let store = MemoryStore::new();
    let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (id, token) = {
        let c = runs.clone();
        let rt = Runtime::builder()
            .store(store.clone())
            .register_fn("wait", move |ctx: ExecutionContext| {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if n == 1 {
                        NodeOutcome::Waiting {
                            token: ctx.resume_token,
                        }
                    } else {
                        NodeOutcome::Succeeded(Bytes::from_static(b"second"))
                    }
                }
            })
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
        within(async {
            loop {
                if let Some(s) = store.get(&id).await.unwrap() {
                    if matches!(
                        s.node(&NodeId::new("hold")).unwrap().state,
                        NodeState::Waiting { .. }
                    ) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let token = store
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        std::mem::forget(handle);
        drop(rt);
        (id, token)
    };
    let c = runs.clone();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("wait", move |ctx: ExecutionContext| {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n == 1 {
                    NodeOutcome::Waiting {
                        token: ctx.resume_token,
                    }
                } else {
                    NodeOutcome::Succeeded(Bytes::from_static(b"second"))
                }
            }
        })
        .build();
    rt.complete(token, Resume::Reinvoke).await.unwrap();
    within(async {
        loop {
            if let Some(snap) = store.get(&id).await.unwrap() {
                if snap.state == ExecutionState::Succeeded {
                    assert_eq!(
                        snap.node(&NodeId::new("hold"))
                            .and_then(|n| n.output.clone()),
                        Some(Bytes::from_static(b"second"))
                    );
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn complete_schema_mismatch_is_snapshot() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "hold".into(),
        },
        &p,
        now,
    )
    .unwrap();
    let token = ex.resume_token(&NodeId::new("hold")).unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "hold".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Waiting {
                token: token.clone(),
            }),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let mut snap = store.get(ex.id()).await.unwrap().unwrap();
    snap.schema_version = 99;
    snap.revision += 1;
    store.put(&snap).await.unwrap();
    let rt = Runtime::builder().store(store).build();
    match rt
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
    {
        Err(CompleteError::Snapshot(SnapshotError::SchemaMismatch { found: 99, .. })) => {}
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn complete_token_from_a_does_not_apply_to_b() {
    let rt = Runtime::builder().build();
    let ha = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    let hb = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(ha.wait_stable()).await;
    within(hb.wait_stable()).await;
    let token_a = ha
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    let token_b = hb
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    rt.complete(
        token_a.clone(),
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"a"))),
    )
    .await
    .unwrap();
    within(ha.wait()).await;
    assert_eq!(hb.inspect().await.state, ExecutionState::Waiting);
    let mut forged = serde_json::to_value(&token_a).unwrap();
    forged["execution_id"] = serde_json::json!(token_b.execution_id().as_str());
    let forged: keel_rt::ResumeToken = serde_json::from_value(forged).unwrap();
    match rt
        .complete(
            forged,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
    {
        Err(CompleteError::UnknownToken) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(hb.inspect().await.state, ExecutionState::Waiting);
    hb.cancel().await;
    within(hb.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn wait_is_waiting_not_ready_t_and_clock_does_not_complete() {
    let clock = Arc::new(FakeClock::new());
    let rt = Runtime::builder().clock(clock.clone()).build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let snap = handle.inspect().await;
    match &snap.node(&NodeId::new("hold")).unwrap().state {
        NodeState::Waiting { .. } => {}
        other => panic!("wait must be Waiting, not {other:?}"),
    }
    clock.advance(Duration::from_secs(3600));
    tokio::task::yield_now().await;
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    within(handle.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn complete_while_fail_fast_already_cancelled_wait() {
    let release = Arc::new(AtomicBool::new(false));
    let r = release.clone();
    let rt = Runtime::builder()
        .register_fn("boom", move |_ctx: ExecutionContext| {
            let r = r.clone();
            async move {
                while !r.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                NodeOutcome::failed("boom")
            }
        })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .node("boom", "boom")
                .build()
                .unwrap(),
        )
        .unwrap();
    let token = within(async {
        loop {
            let snap = handle.inspect().await;
            if let Some(t) = snap
                .node(&NodeId::new("hold"))
                .and_then(|n| n.resume_token.clone())
            {
                if matches!(
                    snap.node(&NodeId::new("hold")).unwrap().state,
                    NodeState::Waiting { .. }
                ) {
                    return t;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    release.store(true, Ordering::SeqCst);
    within(async {
        loop {
            let snap = handle.inspect().await;
            if let Some(n) = snap.node(&NodeId::new("hold")) {
                if matches!(n.state, NodeState::Cancelled) || snap.state.is_terminal() {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    match rt
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        )
        .await
    {
        Err(CompleteError::Cancelled) => {}
        other => panic!("{other:?}"),
    }
    within(handle.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn complete_256_wait_nodes_then_hang_bound_cancels() {
    let mut b = WorkflowDefinition::builder("wf");
    for i in 0..256 {
        b = b.node(format!("w{i}"), "wait");
    }
    let def = b.build().unwrap();
    let rt = Runtime::builder()
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .concurrency(32)
        .build();
    let handle = rt.start(def).unwrap();
    within(handle.wait_stable()).await;
    let snap = handle.inspect().await;
    let started = Instant::now();
    for i in 0..256 {
        let token = snap
            .node(&NodeId::new(format!("w{i}")))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        rt.complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"g"))),
        )
        .await
        .unwrap();
    }
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    eprintln!(
        "complete_256_wait_nodes elapsed_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0
    );

    let parked = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(parked.wait_stable()).await;
    let t0 = Instant::now();
    parked.cancel().await;
    assert_eq!(within(parked.wait()).await, ExecutionState::Cancelled);
    assert!(
        t0.elapsed() < DEFAULT_CANCEL_BOUND * 20,
        "hang bound must still cancel a parked wait, elapsed={:?}",
        t0.elapsed()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn guessed_sequential_nonces_do_not_complete() {
    let rt = Runtime::builder().build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    within(handle.wait_stable()).await;
    let real = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    for i in 1u128..=32 {
        let guess: keel_rt::ResumeToken = serde_json::from_value(serde_json::json!({
            "execution_id": real.execution_id().as_str(),
            "node_id": "hold",
            "attempt": 1,
            "nonce": format!("{i:032x}")
        }))
        .unwrap();
        match rt
            .complete(
                guess,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            )
            .await
        {
            Err(CompleteError::UnknownToken) => {}
            other => panic!("guess {i} {other:?}"),
        }
    }
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    within(handle.wait()).await;
}
