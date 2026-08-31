//! Resume / persist hunt that is not process death.
//! File-crash rows live in `keel-rt-sqlite` (`--test resume`).
//!
//! `cargo test --test adversarial resume -- --test-threads=1`

use crate::common::within;
use bytes::Bytes;
use keel_rt::{
    AcceptPolicy, ApplyCmd, Execution, ExecutionContext, ExecutionState, Join,
    MemoryStore, NodeId, NodeOutcome, ResumeError, Runtime, StateStore, StoreError, Timestamp,
    WorkflowDefinition,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

fn succeed(_id: &str) -> impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> {
    |_ctx: ExecutionContext| std::future::ready(NodeOutcome::Succeeded(Bytes::from_static(b"ok")))
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_resume_same_id_one_already_active() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let exec = Execution::new(def);
    store.persist(&exec).await.unwrap();
    let id = exec.id().clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", succeed("a"))
        .build();
    let (a, b) = tokio::join!(rt.resume(&id), rt.resume(&id));
    let oks = [&a, &b].iter().filter(|r| r.is_ok()).count();
    let actives = [&a, &b]
        .iter()
        .filter(|r| matches!(r, Err(ResumeError::AlreadyActive)))
        .count();
    assert_eq!(oks, 1, "one resume claims the execution");
    assert_eq!(actives, 1, "the other is AlreadyActive");
    let handle = a.ok().or(b.ok()).unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn persist_cas_then_drop_runtime_resume_keeps_terminal() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from(vec![7u8; 64]))),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded
    );
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", move |_ctx: ExecutionContext| {
            c.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"no")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn fat_bytes_resume_join_is_refcount() {
    let fat = Bytes::from(vec![9u8; 64 * 1024]);
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("fat", "fat")
        .node("join", "join")
        .edge("fat", "join")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "fat".into() }, &p, now)
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "fat".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(fat.clone())),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("fat", |_ctx: ExecutionContext| async {
            panic!("succeeded fat node must not re-run")
        })
        .register_fn("join", succeed("join"))
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let snap = store.get(&id).await.unwrap().unwrap();
    let out = snap
        .node(&NodeId::new("fat"))
        .and_then(|n| n.output.clone())
        .expect("fat output");
    assert_eq!(out.len(), fat.len());
    assert_eq!(
        out.as_ptr(),
        fat.as_ptr(),
        "MemoryStore snapshot keeps Bytes by refcount"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn drop_handle_after_running_persist_cancels_not_reinvoke() {
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("ea", succeed("ea"))
        .register(keel_rt::testing::ScriptedExecutor::new("eb").hang(false))
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    within(async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.node(&NodeId::new("b"))
                    .is_some_and(|n| matches!(n.state, keel_rt::NodeState::Running { .. }))
                {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    drop(handle);
    within(async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn stale_put_loses_on_memory_store() {
    let store = MemoryStore::new();
    let mut ex = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap(),
    );
    store.persist(&ex).await.unwrap();
    ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    store.persist(&ex).await.unwrap();
    let keep = store.get(ex.id()).await.unwrap().unwrap().revision;
    let mut older = store.get(ex.id()).await.unwrap().unwrap();
    older.revision = 0;
    assert_eq!(
        store.put(&older).await.unwrap_err(),
        StoreError::Stale {
            found: keep,
            attempted: 0
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fail_subtree_all_done_resume_runs_reducer_once() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .on_failure(keel_rt::OnFailure::FailSubtree)
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
    ex.apply(ApplyCmd::StartNode { node_id: "p1".into() }, &p, now)
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
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let red = Arc::new(AtomicU32::new(0));
    let c = red.clone();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("p1", |_ctx: ExecutionContext| async {
            panic!("failed page must not re-run")
        })
        .register_fn("p2", succeed("p2"))
        .register_fn("red", move |_ctx: ExecutionContext| {
            c.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"r")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);
    assert_eq!(red.load(Ordering::SeqCst), 1);
}
