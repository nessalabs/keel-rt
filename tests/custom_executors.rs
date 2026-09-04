//! Custom `impl Executor` types × kernel. Named killers for wait-token
//! steal, resume of Running without the adapter, id identity, fat Bytes,
//! cancel hang, FailSubtree permits, AllDone vs AllSucceeded, catalog cost.
//!
//! `cargo test --test custom_executors -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::RecordingSink;
use keel_rt::{
    AcceptPolicy, ApplyCmd, CompleteError, Event, Execution, ExecutionContext, ExecutionId,
    ExecutionState, Executor, ExecutorId, Join, MemoryStore, NodeError, NodeId, NodeOutcome,
    NodeState, OnFailure, Recover, Resume, ResumeError, ResumeToken, Runtime, StartError,
    StateStore, Timestamp, WorkflowDefinition, DEFAULT_CANCEL_BOUND, MAX_SINK_ERROR,
    MAX_SNAPSHOT_ERROR,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(5);

fn within<F: Future>(f: F) -> impl Future<Output = F::Output> {
    async { tokio::time::timeout(BOUND, f).await.expect("test bound") }
}

struct Notes;
impl Executor for Notes {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("research")
    }
    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"notes")) })
    }
}

struct ForgeWait;
impl Executor for ForgeWait {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("hold")
    }
    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async {
            NodeOutcome::Waiting {
                token: ResumeToken::issue(ExecutionId::new(), NodeId::new("hold"), 99),
            }
        })
    }
}

struct FatSrc {
    payload: Bytes,
}
impl Executor for FatSrc {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("fat")
    }
    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        let payload = self.payload.clone();
        Box::pin(async move { NodeOutcome::Succeeded(payload) })
    }
}

struct JoinCap {
    seen: Arc<Mutex<Option<(usize, usize)>>>,
}
impl Executor for JoinCap {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("join")
    }
    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        let seen = self.seen.clone();
        Box::pin(async move {
            let input = ctx
                .inputs
                .get(&NodeId::new("fat"))
                .cloned()
                .expect("fat pred");
            *seen.lock().expect("seen") = Some((input.as_ptr() as usize, input.len()));
            NodeOutcome::Succeeded(input)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn custom_waiting_forged_token_complete_is_mismatch_kernel_token_unblocks() {
    let rt = Runtime::builder().register(ForgeWait).build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "hold")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let snap = rt.inspect(&id).await.unwrap();
    let kernel = snap
        .node(&NodeId::new("hold"))
        .and_then(|n| n.resume_token.clone())
        .expect("kernel token");
    assert_eq!(kernel.attempt(), 1);
    assert_eq!(kernel.node_id(), &NodeId::new("hold"));
    let forged = ResumeToken::issue(id.clone(), NodeId::new("hold"), 1);
    assert_ne!(kernel.nonce(), forged.nonce());
    match rt
        .complete(
            forged,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
    {
        Err(CompleteError::UnknownToken) => {}
        other => panic!("forged complete must be UnknownToken, got {other:?}"),
    }
    rt.complete(
        kernel,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
    )
    .await
    .expect("kernel token unblocks");
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn custom_id_case_and_unicode_lookalike_are_unregistered() {
    let rt = Runtime::builder().register(Notes).build();
    let ids = rt.executor_ids();
    assert!(ids.iter().any(|id| id.as_str() == "research"), "{ids:?}");
    assert!(!ids.iter().any(|id| id.as_str() == "Research"), "{ids:?}");
    match rt.start(
        WorkflowDefinition::builder("wf")
            .node("n", "Research")
            .build()
            .unwrap(),
    ) {
        Err(StartError::UnregisteredExecutors(missing)) => {
            let names: Vec<&str> = missing.0.iter().map(|e| e.as_str()).collect();
            assert_eq!(names, vec!["Research"], "{names:?}");
        }
        Ok(_) => panic!("case mismatch must be unregistered, start succeeded"),
    }
    let lookalike = "r\u{0435}search";
    assert_ne!(lookalike, "research");
    match rt.start(
        WorkflowDefinition::builder("wf")
            .node("n", lookalike)
            .build()
            .unwrap(),
    ) {
        Err(StartError::UnregisteredExecutors(missing)) => {
            assert_eq!(missing.0[0].as_str(), lookalike);
        }
        Ok(_) => panic!("unicode lookalike must be unregistered, start succeeded"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_running_custom_without_adapter_is_unregistered_then_adapter_resumes() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("slow", "slow")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "slow".into(),
        },
        &p,
        now,
    )
    .unwrap();
    assert!(matches!(
        ex.snapshot().node(&NodeId::new("slow")).map(|n| &n.state),
        Some(NodeState::Running { .. })
    ));
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();

    let bare = Runtime::builder().store(store.clone()).build();
    match within(bare.resume(&id)).await {
        Err(ResumeError::UnregisteredExecutors(missing)) => {
            let names: Vec<&str> = missing.0.iter().map(|e| e.as_str()).collect();
            assert_eq!(names, vec!["slow"], "{names:?}");
        }
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("resume without the custom adapter must fail closed"),
    }
    let still = store.get(&id).await.unwrap().unwrap();
    assert!(
        matches!(
            still.node(&NodeId::new("slow")).map(|n| &n.state),
            Some(NodeState::Running { .. })
        ),
        "UnregisteredExecutors must not persist a converted snapshot: {:?}",
        still.node(&NodeId::new("slow")).map(|n| &n.state)
    );

    struct Done;
    impl Executor for Done {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("slow")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) })
        }
    }
    let with = Runtime::builder().store(store).register(Done).build();
    let handle = within(with.resume(&id)).await.expect("adapter present");
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn fat_bytes_custom_executor_join_is_refcount_not_copy() {
    let fat = Bytes::from(vec![9u8; 64 * 1024]);
    let seen = Arc::new(Mutex::new(None));
    let rt = Runtime::builder()
        .register(FatSrc {
            payload: fat.clone(),
        })
        .register(JoinCap { seen: seen.clone() })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("fat", "fat")
                .node("join", "join")
                .edge("fat", "join")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let out = rt
        .inspect(&id)
        .await
        .unwrap()
        .node(&NodeId::new("fat"))
        .and_then(|n| n.output.clone())
        .expect("fat output");
    let (ptr, len) = seen.lock().expect("seen").expect("join saw input");
    assert_eq!(len, fat.len());
    assert_eq!(out.len(), fat.len());
    assert_eq!(
        out.as_ptr() as usize,
        ptr,
        "custom-type join inputs must clone Bytes (refcount), not copy the buffer"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn custom_pending_cancel_is_cancelled_within_bound_never_succeeded() {
    struct Hang;
    impl Executor for Hang {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("hang")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { std::future::pending::<NodeOutcome>().await })
        }
    }
    let rt = Runtime::builder()
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .register(Hang)
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hang", "hang")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = rt.inspect(&id).await {
                if matches!(
                    s.node(&NodeId::new("hang")).map(|n| &n.state),
                    Some(NodeState::Running { .. })
                ) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Running");
    handle.cancel().await;
    let state = tokio::time::timeout(
        DEFAULT_CANCEL_BOUND + Duration::from_millis(200),
        handle.wait(),
    )
    .await
    .expect("cancel bound");
    assert_eq!(state, ExecutionState::Cancelled);
    let snap = rt.inspect(&id).await.unwrap();
    assert_eq!(snap.state, ExecutionState::Cancelled);
    assert!(!matches!(
        snap.node(&NodeId::new("hang")).map(|n| &n.state),
        Some(NodeState::Succeeded)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn custom_fail_subtree_releases_permit_sibling_runs() {
    struct Boom;
    impl Executor for Boom {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("boom")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::failed("boom") })
        }
    }
    struct Sib;
    impl Executor for Sib {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("sib")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"sib")) })
        }
    }
    let rt = Runtime::builder()
        .concurrency(1)
        .register(Boom)
        .register(Sib)
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .on_failure(OnFailure::FailSubtree)
                .node("boom", "boom")
                .node("sib", "sib")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);
    let snap = rt.inspect(&id).await.unwrap();
    assert!(matches!(
        snap.node(&NodeId::new("boom")).map(|n| &n.state),
        Some(NodeState::Failed)
    ));
    assert!(
        matches!(
            snap.node(&NodeId::new("sib")).map(|n| &n.state),
            Some(NodeState::Succeeded)
        ),
        "failed custom node must return its permit; sibling must not stay Pending/Running: {:?}",
        snap.node(&NodeId::new("sib")).map(|n| &n.state)
    );
    assert_eq!(snap.running_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn custom_all_done_runs_after_failed_pred_all_succeeded_does_not() {
    struct OkA;
    impl Executor for OkA {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("ok")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"a")) })
        }
    }
    struct Boom;
    impl Executor for Boom {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("boom")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::failed("boom") })
        }
    }
    struct JoinE {
        ran: Arc<AtomicBool>,
    }
    impl Executor for JoinE {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("join")
        }
        fn execute<'a>(
            &'a self,
            ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let ran = self.ran.clone();
            Box::pin(async move {
                ran.store(true, Ordering::SeqCst);
                NodeOutcome::Succeeded(Bytes::from(format!("{}", ctx.inputs.len())))
            })
        }
    }
    let ran_and = Arc::new(AtomicBool::new(false));
    let rt_and = Runtime::builder()
        .register(OkA)
        .register(Boom)
        .register(JoinE {
            ran: ran_and.clone(),
        })
        .build();
    let and_id = rt_and
        .start(
            WorkflowDefinition::builder("and")
                .on_failure(OnFailure::FailSubtree)
                .node("a", "ok")
                .node("b", "boom")
                .node("j", "join")
                .edge("a", "j")
                .edge("b", "j")
                .join("j", Join::AllSucceeded)
                .build()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(within(and_id.wait()).await, ExecutionState::Completed);
    assert!(
        !ran_and.load(Ordering::SeqCst),
        "AllSucceeded join must not run after a Failed pred"
    );

    let ran_done = Arc::new(AtomicBool::new(false));
    let rt_done = Runtime::builder()
        .register(OkA)
        .register(Boom)
        .register(JoinE {
            ran: ran_done.clone(),
        })
        .build();
    let done = rt_done
        .start(
            WorkflowDefinition::builder("done")
                .on_failure(OnFailure::FailSubtree)
                .node("a", "ok")
                .node("b", "boom")
                .node("j", "join")
                .edge("a", "j")
                .edge("b", "j")
                .join("j", Join::AllDone)
                .build()
                .unwrap(),
        )
        .unwrap();
    let done_id = done.execution_id().clone();
    assert_eq!(within(done.wait()).await, ExecutionState::Completed);
    assert!(
        ran_done.load(Ordering::SeqCst),
        "AllDone join must run after a Failed pred"
    );
    let out = rt_done
        .inspect(&done_id)
        .await
        .unwrap()
        .node(&NodeId::new("j"))
        .and_then(|n| n.output.clone());
    assert_eq!(out.as_deref(), Some(&b"1"[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn custom_failed_resume_with_retry_failed_rebumps_and_succeeds() {
    struct Flaky {
        n: Arc<AtomicU32>,
    }
    impl Executor for Flaky {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("flaky")
        }
        fn execute<'a>(
            &'a self,
            ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let n = self.n.fetch_add(1, Ordering::SeqCst);
            let attempt = ctx.attempt;
            Box::pin(async move {
                if n == 0 {
                    assert_eq!(attempt, 1);
                    NodeOutcome::failed("once")
                } else {
                    assert_eq!(
                        attempt, 1,
                        "RetryFailed resets attempt; dispatch bumps to 1"
                    );
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            })
        }
    }
    let n = Arc::new(AtomicU32::new(0));
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .register(Flaky { n: n.clone() })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "flaky")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    drop(rt);
    let rt2 = Runtime::builder()
        .store(store)
        .register(Flaky { n })
        .build();
    let h = within(rt2.resume_with(&id, Recover::RetryFailed))
        .await
        .expect("retry");
    assert_eq!(within(h.wait()).await, ExecutionState::Succeeded);
}

/// Custom Failed with a multi-MiB last_error must not land unbounded on the
/// live inspect snapshot or MemoryStore. Snapshot last_error is the short form.
#[tokio::test(flavor = "current_thread")]
async fn custom_failed_huge_last_error_is_capped_on_live_and_store_snapshot() {
    let huge = "x".repeat(MAX_SNAPSHOT_ERROR + 2 * 1024 * 1024);
    let h = huge.clone();
    let store = MemoryStore::new();
    struct Boom {
        msg: String,
    }
    impl Executor for Boom {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("boom")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let msg = self.msg.clone();
            Box::pin(async move { NodeOutcome::Failed(NodeError { message: msg }) })
        }
    }
    let rt = Runtime::builder()
        .store(store.clone())
        .register(Boom { msg: h })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "boom")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    let live = rt.inspect(&id).await.unwrap();
    let live_err = live
        .node(&NodeId::new("x"))
        .and_then(|n| n.last_error.clone())
        .expect("Failed last_error");
    assert!(
        live_err.message.len() <= MAX_SNAPSHOT_ERROR,
        "live snapshot last_error {} > MAX_SNAPSHOT_ERROR",
        live_err.message.len()
    );
    assert!(live_err.message.len() < huge.len());
    let prefix = live_err.message.trim_end_matches('\u{2026}');
    assert!(huge.starts_with(prefix));
    let stored = store.get(&id).await.unwrap().unwrap();
    let stored_err = stored
        .node(&NodeId::new("x"))
        .and_then(|n| n.last_error.clone())
        .expect("store last_error");
    assert_eq!(stored_err.message.len(), live_err.message.len());
    assert!(stored_err.message.len() <= MAX_SNAPSHOT_ERROR);
}

/// Pattern 3: snapshot stays short; EventSink keeps the full Failed message.
#[tokio::test(flavor = "current_thread")]
async fn custom_failed_full_error_emitted_to_sink_snapshot_stays_short() {
    let full = format!("detail-{}", "x".repeat(MAX_SNAPSHOT_ERROR + 2048));
    assert!(full.len() > MAX_SNAPSHOT_ERROR);
    assert!(full.len() <= MAX_SINK_ERROR);
    let sink = RecordingSink::new();
    let store = MemoryStore::new();
    struct Boom {
        msg: String,
    }
    impl Executor for Boom {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("boom")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let msg = self.msg.clone();
            Box::pin(async move { NodeOutcome::Failed(NodeError { message: msg }) })
        }
    }
    let rt = Runtime::builder()
        .store(store.clone())
        .sink(sink.clone())
        .register(Boom { msg: full.clone() })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "boom")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    let live = rt.inspect(&id).await.unwrap();
    let live_err = live
        .node(&NodeId::new("x"))
        .and_then(|n| n.last_error.clone())
        .expect("Failed last_error");
    assert!(
        live_err.message.len() <= MAX_SNAPSHOT_ERROR,
        "snapshot last_error {} > MAX_SNAPSHOT_ERROR",
        live_err.message.len()
    );
    assert_ne!(live_err.message, full);
    let stored = store.get(&id).await.unwrap().unwrap();
    let stored_err = stored
        .node(&NodeId::new("x"))
        .and_then(|n| n.last_error.clone())
        .expect("store last_error");
    assert!(stored_err.message.len() <= MAX_SNAPSHOT_ERROR);
    let sink_err = sink
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::NodeFailed { error, .. } => Some(error.message),
            _ => None,
        })
        .expect("NodeFailed on EventSink");
    assert_eq!(
        sink_err, full,
        "EventSink must see the full Failed message after persist"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn succeeded_fat_bytes_are_not_capped_by_last_error_bound() {
    let fat = Bytes::from(vec![7u8; MAX_SINK_ERROR + 4096]);
    struct Fat {
        payload: Bytes,
    }
    impl Executor for Fat {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("fat")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let payload = self.payload.clone();
            Box::pin(async move { NodeOutcome::Succeeded(payload) })
        }
    }
    let rt = Runtime::builder()
        .register(Fat {
            payload: fat.clone(),
        })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("fat", "fat")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let out = rt
        .inspect(&id)
        .await
        .unwrap()
        .node(&NodeId::new("fat"))
        .and_then(|n| n.output.clone())
        .expect("Succeeded output");
    assert_eq!(
        out.len(),
        fat.len(),
        "Succeeded Bytes stay uncapped (HTTP inspect 413 is adapter-only)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn custom_panic_huge_vec_is_failed_and_drop_reclaims() {
    let dropped = Arc::new(AtomicBool::new(false));
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct Boom {
        dropped: Arc<AtomicBool>,
    }
    impl Executor for Boom {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("boom")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let dropped = self.dropped.clone();
            Box::pin(async move {
                let _g = Guard(dropped);
                let _fat = vec![0u8; 1024 * 1024];
                panic!("huge vec");
            })
        }
    }
    let rt = Runtime::builder()
        .register(Boom {
            dropped: dropped.clone(),
        })
        .build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "boom")
                .build()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    assert!(
        dropped.load(Ordering::SeqCst),
        "CatchUnwind must drop the execute future (Vec + Guard), JoinSet abort not required"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn custom_cancel_mid_run_late_notify_is_not_succeeded() {
    let gate = Arc::new(Notify::new());
    let g = gate.clone();
    struct Slow {
        gate: Arc<Notify>,
    }
    impl Executor for Slow {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("slow")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            let gate = self.gate.clone();
            Box::pin(async move {
                gate.notified().await;
                NodeOutcome::Succeeded(Bytes::from_static(b"late"))
            })
        }
    }
    let rt = Runtime::builder().register(Slow { gate: g }).build();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = rt.inspect(&id).await {
                if matches!(
                    s.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(NodeState::Running { .. })
                ) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Running");
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    gate.notify_one();
    tokio::task::yield_now().await;
    let snap = rt.inspect(&id).await.unwrap();
    assert_eq!(snap.state, ExecutionState::Cancelled);
    assert!(!matches!(
        snap.node(&NodeId::new("slow")).map(|n| &n.state),
        Some(NodeState::Succeeded)
    ));
}

fn rss_bytes() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            return kb.saturating_mul(1024);
        }
    }
    0
}

fn median_ns(mut samples: Vec<u128>) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[tokio::test(flavor = "current_thread")]
async fn register_10k_catalog_sort_and_start_one_are_bounded() {
    const N: usize = 10_000;
    let rss0 = rss_bytes();
    let t_reg = Instant::now();
    let mut b = Runtime::builder();
    for i in 0..N {
        b = b.register_fn(format!("e{i}"), |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        });
    }
    let rt = b.build();
    let register_ms = t_reg.elapsed();
    let rss1 = rss_bytes();
    let t_cat = Instant::now();
    let ids = rt.executor_ids();
    let catalog_ms = t_cat.elapsed();
    assert_eq!(ids.len(), N + 1, "N custom + builtin wait");
    assert!(ids.windows(2).all(|w| w[0].as_str() <= w[1].as_str()));
    let t_start = Instant::now();
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("n", "e0")
                .build()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let start_ms = t_start.elapsed();
    eprintln!(
        "register_10k: register+build {:?} catalog {:?} start+wait {:?} rss {} -> {} (+{} KiB)",
        register_ms,
        catalog_ms,
        start_ms,
        rss0 / 1024,
        rss1 / 1024,
        rss1.saturating_sub(rss0) / 1024
    );
    assert!(
        catalog_ms < Duration::from_millis(500),
        "executor_ids sort of 10k must stay sub-500ms, got {catalog_ms:?}"
    );
    assert!(
        start_ms < Duration::from_millis(500),
        "start of one node must not scan the whole catalog in 500ms, got {start_ms:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn custom_type_diamond_vs_register_fn_is_same_order() {
    struct Http;
    impl Executor for Http {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("http")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) })
        }
    }
    struct Transform;
    impl Executor for Transform {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("transform")
        }
        fn execute<'a>(
            &'a self,
            ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(
                async move { NodeOutcome::Succeeded(Bytes::from(format!("t:{}", ctx.node_id))) },
            )
        }
    }
    struct Publish;
    impl Executor for Publish {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("publish")
        }
        fn execute<'a>(
            &'a self,
            ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async move {
                assert_eq!(ctx.inputs.len(), 2);
                NodeOutcome::Succeeded(Bytes::from_static(b"D"))
            })
        }
    }
    fn diamond() -> WorkflowDefinition {
        WorkflowDefinition::builder("diamond")
            .node("a", "http")
            .node("b", "transform")
            .node("c", "transform")
            .node("d", "publish")
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .build()
            .unwrap()
    }
    let mut typed = Vec::new();
    for _ in 0..7 {
        let rt = Runtime::builder()
            .concurrency(2)
            .register(Http)
            .register(Transform)
            .register(Publish)
            .build();
        let t0 = Instant::now();
        assert_eq!(
            within(rt.run(diamond())).await.unwrap(),
            ExecutionState::Succeeded
        );
        typed.push(t0.elapsed().as_nanos());
    }
    let mut clos = Vec::new();
    for _ in 0..7 {
        let rt = Runtime::builder()
            .concurrency(2)
            .register_fn("http", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"A"))
            })
            .register_fn("transform", |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(Bytes::from(format!("t:{}", ctx.node_id)))
            })
            .register_fn("publish", |ctx: ExecutionContext| async move {
                assert_eq!(ctx.inputs.len(), 2);
                NodeOutcome::Succeeded(Bytes::from_static(b"D"))
            })
            .build();
        let t0 = Instant::now();
        assert_eq!(
            within(rt.run(diamond())).await.unwrap(),
            ExecutionState::Succeeded
        );
        clos.push(t0.elapsed().as_nanos());
    }
    let t_med = median_ns(typed);
    let c_med = median_ns(clos);
    eprintln!(
        "diamond n=7 median: impl Executor {} ns vs register_fn {} ns",
        t_med, c_med
    );
    let hi = t_med.max(c_med);
    let lo = t_med.min(c_med).max(1);
    assert!(
        hi < lo.saturating_mul(10),
        "typed vs register_fn diamond should be the same order of magnitude: {t_med} vs {c_med}"
    );
}
