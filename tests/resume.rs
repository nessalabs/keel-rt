//! Public-surface resume: MemoryStore in-process, not process death.
//! Crash-across-file tests live in the sibling store crate.
//!
//! `cargo test --test resume -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Event, Execution, ExecutionContext, ExecutionId, ExecutionState,
    FnSink, Join, MemoryStore, NodeId, NodeOutcome, NodeState, OnFailure, Recover, Resume,
    ResumeError, RetryPolicy, Runtime, SnapshotError, StateStore, StoreError, Timestamp,
    WorkflowDefinition, SCHEMA_VERSION,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    let j = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (c1, c2, cj) = (p1.clone(), p2.clone(), j.clone());
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
        .register_fn("j", move |_ctx: ExecutionContext| {
            cj.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"join")) }
        })
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
        j.load(Ordering::SeqCst),
        0,
        "AllDone reducer already Succeeded; it does not re-run"
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

/// HITL Waiting uses `handle.resume(token)`, not `Recover::RetryFailed`.
#[tokio::test(flavor = "current_thread")]
async fn hitl_waiting_is_handle_resume_not_recover_retry_failed() {
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
