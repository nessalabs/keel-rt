//! Named regressions for every Phase 1 failure-catalog row that was MISSING.
//!
//! `cargo test --test catalog -- --test-threads=1`
//! See `docs/FAILURE_CATALOG.md`.

use bytes::Bytes;
use keel_rt::testing::{NetFault, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyCmd, ApplyError, DomainEvent, EventSink, Execution, ExecutionContext,
    ExecutionState, FunctionExecutor, Join, MemoryStore, NodeId, NodeOutcome, NodeState,
    OnFailure, Policy, PolicyDecision, Resume, ResumeToken, RetryPolicy, Runtime, StateStore,
    StoreError, Timestamp, WorkflowDefinition, DEFAULT_CANCEL_BOUND, SCHEMA_VERSION,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(5);

async fn within<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(BOUND, f)
        .await
        .expect("catalog test timed out")
}

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

// --- Definition ----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn all_done_source_with_no_preds_runs() {
    let run = within(
        WorkflowTest::new()
            .join("solo", Join::AllDone)
            .node("solo", ok("solo"))
            .run(),
    )
    .await;
    assert!(matches!(run.state("solo").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn fail_subtree_with_no_children_is_noop_on_descendants() {
    let run = within(
        WorkflowTest::new()
            .on_failure(OnFailure::FailSubtree)
            .node("leaf", ScriptedExecutor::new("leaf").fail("boom"))
            .run(),
    )
    .await;
    assert!(matches!(run.state("leaf").await, NodeState::Failed));
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Completed,
        "FailSubtree + 0 successors: live=0 and n_failed>0 is Completed, not Failed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn zero_predecessor_source_runs() {
    let run = within(WorkflowTest::new().node("src", ok("src")).run()).await;
    assert!(matches!(run.state("src").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn zero_successor_fail_execution_is_failed() {
    let run = within(
        WorkflowTest::new()
            .node("leaf", ScriptedExecutor::new("leaf").fail("boom"))
            .run(),
    )
    .await;
    assert!(matches!(run.state("leaf").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
}

// --- Start ---------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn start_after_runtime_dropped_execution_still_runs() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    drop(rt);
    assert_eq!(
        within(handle.wait()).await,
        ExecutionState::Succeeded,
        "dropping Runtime must not cancel in-flight executions"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn builder_defaults_without_store_policy_sink_clock() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ok")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("ok", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    assert_eq!(
        within(rt.run(def)).await.unwrap(),
        ExecutionState::Succeeded
    );
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_one_serializes_two_ready() {
    let entered = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut test = WorkflowTest::new().concurrency(1);
    for name in ["a", "b"] {
        let ent = entered.clone();
        let pk = peak.clone();
        test = test.executor(
            name,
            FunctionExecutor::new(name, move |_ctx: ExecutionContext| {
                let c = ent.fetch_add(1, Ordering::SeqCst) + 1;
                pk.fetch_max(c, Ordering::SeqCst);
                let ent = ent.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    ent.fetch_sub(1, Ordering::SeqCst);
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }),
        );
    }
    let run = within(test.run()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(
        peak.load(Ordering::SeqCst) <= 1,
        "concurrency 1 must not overlap execute, peak={}",
        peak.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_run_matches_start_then_wait() {
    let def = || {
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap()
    };
    let rt = Runtime::builder()
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let via_run = within(rt.run(def())).await.unwrap();
    let via_wait = within(rt.start(def()).unwrap().wait()).await;
    assert_eq!(via_run, ExecutionState::Succeeded);
    assert_eq!(via_wait, ExecutionState::Succeeded);
}

// --- Run / policy / payloads ---------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn waiting_executor_supplied_token_ignored_kernel_token_used() {
    let garbage = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("a"), 99);
    let g = garbage.clone();
    let run = within(
        WorkflowTest::new()
            .executor(
                "a",
                FunctionExecutor::new("a", move |_ctx: ExecutionContext| {
                    let token = g.clone();
                    async move { NodeOutcome::Waiting { token } }
                }),
            )
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    let kernel = run.resume_token("a").await;
    assert_ne!(
        kernel.nonce(),
        garbage.nonce(),
        "inspect must expose the kernel-issued token, not the executor's"
    );
    assert_eq!(kernel.attempt(), 1);
    let err = run
        .resume(
            garbage,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
        .unwrap_err();
    assert_eq!(err, ApplyError::TokenMismatch);
    run.resume(
        kernel,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn succeeded_empty_bytes_vs_fat_bytes() {
    let fat = Bytes::from(vec![7u8; 64 * 1024]);
    let run = within(
        WorkflowTest::new()
            .node(
                "empty",
                ScriptedExecutor::new("empty").succeed(Bytes::new()),
            )
            .node("fat", ScriptedExecutor::new("fat").succeed(fat.clone()))
            .node("join", ok("join"))
            .edge("empty", "join")
            .edge("fat", "join")
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.output("empty").await, Some(Bytes::new()));
    assert_eq!(
        run.output("fat").await.as_ref().map(|b| b.len()),
        Some(fat.len())
    );
    let inputs = run.inputs("join").await;
    assert_eq!(inputs.get(&NodeId::new("empty")), Some(&Bytes::new()));
    assert_eq!(inputs.get(&NodeId::new("fat")), Some(&fat));
}

#[tokio::test(flavor = "current_thread")]
async fn retry_is_at_least_once_two_execute_invocations() {
    let run = within(
        WorkflowTest::new()
            .node(
                "a",
                ScriptedExecutor::new("a")
                    .fail("once")
                    .succeed(Bytes::from_static(b"ok")),
            )
            .policy(RetryPolicy::new(3, Duration::ZERO))
            .run(),
    )
    .await;
    assert_eq!(
        run.scripted("a").attempts(),
        vec![1, 2],
        "retry is at-least-once: fail then succeed is two execute invocations"
    );
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

struct RejectFailed;
impl Policy for RejectFailed {
    fn decide(&self, outcome: &NodeOutcome, _attempt: u32) -> PolicyDecision {
        match outcome {
            NodeOutcome::Failed(_) | NodeOutcome::TimedOut => PolicyDecision::Reject,
            _ => PolicyDecision::Accept,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reject_policy_on_failed_fails_node() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").fail("boom"))
            .policy(RejectFailed)
            .run(),
    )
    .await;
    assert!(matches!(run.state("a").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    let snap = run.snapshot().await;
    let err = snap
        .node(&NodeId::new("a"))
        .and_then(|n| n.last_error.as_ref())
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(
        err.contains("policy rejected"),
        "Reject on Failed must fail the node with the reject error, got {err}"
    );
}

// --- Join / failure scope ------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn mixed_timed_out_failed_succeeded_fanin_all_done() {
    let run = within(
        WorkflowTest::new()
            .concurrency(3)
            .on_failure(OnFailure::FailSubtree)
            .join("reducer", Join::AllDone)
            .node("ok", ok("ok"))
            .node("fail", ScriptedExecutor::new("fail").fail("nope"))
            .node("to", ScriptedExecutor::new("to").fault(NetFault::Timeout))
            .node("reducer", ok("reducer"))
            .edge("ok", "reducer")
            .edge("fail", "reducer")
            .edge("to", "reducer")
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(matches!(run.state("ok").await, NodeState::Succeeded));
    assert!(matches!(run.state("fail").await, NodeState::Failed));
    assert!(matches!(run.state("to").await, NodeState::TimedOut));
    assert!(matches!(run.state("reducer").await, NodeState::Succeeded));
    let inputs = run.inputs("reducer").await;
    assert_eq!(inputs.len(), 1);
    assert!(inputs.contains_key(&NodeId::new("ok")));
}

#[tokio::test(flavor = "current_thread")]
async fn all_succeeded_reducer_with_failed_pred_terminates() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("ok", ok("ok"))
            .node("fail", ScriptedExecutor::new("fail").fail("nope"))
            .node("join", ok("join"))
            .edge("ok", "join")
            .edge("fail", "join")
            .run(),
    )
    .await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Failed,
        "AllSucceeded join must not hang after a failed pred"
    );
    assert!(matches!(run.state("join").await, NodeState::Cancelled));
    assert!(run.scripted("join").attempts().is_empty());
}

// --- HITL / wait / drop --------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn wait_does_not_return_while_waiting() {
    let def = WorkflowDefinition::builder("wf")
        .node("w", "w")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("w", |_ctx: ExecutionContext| async {
            NodeOutcome::Waiting {
                token: ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("w"), 1),
            }
        })
        .build();
    let handle = rt.start(def).expect("start");
    let wait = handle.wait();
    let raced = tokio::time::timeout(Duration::from_millis(150), wait).await;
    assert!(
        raced.is_err(),
        "wait() must not return on Waiting; got {:?}",
        raced.ok()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wait_stable_returns_on_waiting() {
    let def = WorkflowDefinition::builder("wf")
        .node("w", "w")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("w", |_ctx: ExecutionContext| async {
            NodeOutcome::Waiting {
                token: ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("w"), 1),
            }
        })
        .build();
    let handle = rt.start(def).expect("start");
    let stable = within(handle.wait_stable()).await;
    assert_eq!(stable, ExecutionState::Waiting);
    let snap = handle.inspect().await;
    assert_eq!(snap.state, ExecutionState::Waiting);
}

#[tokio::test(flavor = "current_thread")]
async fn drop_handle_while_waiting_cancels_and_releases_permit() {
    let mut run = within(
        WorkflowTest::new()
            .concurrency(1)
            .node("w", ScriptedExecutor::new("w").wait())
            .node("sib", ok("sib"))
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    assert!(matches!(run.state("sib").await, NodeState::Succeeded));
    run.drop_handle();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(snap) = run.stored_snapshot().await {
                if snap.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop while Waiting must cancel, not leak");
    let snap = run.stored_snapshot().await.expect("stored");
    assert!(matches!(
        snap.node(&NodeId::new("w")).map(|n| &n.state),
        Some(NodeState::Cancelled)
    ));
    assert!(matches!(
        snap.node(&NodeId::new("sib")).map(|n| &n.state),
        Some(NodeState::Succeeded)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn inspect_after_cancel_returns_cancelled_not_error() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").hang(false))
        .start()
        .await;
    tokio::time::timeout(BOUND, run.scripted("a").wait_until_hanging())
        .await
        .expect("hang");
    run.cancel().await;
    within(run.wait_stable()).await;
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Cancelled);
    assert_eq!(snap.schema_version, SCHEMA_VERSION);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_while_waiting_is_cancelled() {
    let run = within(
        WorkflowTest::new()
            .node("w", ScriptedExecutor::new("w").wait())
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    let token = run.resume_token("w").await;
    run.cancel().await;
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
    let err = run
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert_eq!(err, ApplyError::ResumeAfterCancel);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_already_terminal_is_noop() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    assert_eq!(
        within(handle.wait_stable()).await,
        ExecutionState::Succeeded
    );
    handle.cancel().await;
    let snap = handle.inspect().await;
    assert_eq!(snap.state, ExecutionState::Succeeded);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_after_failed_stays_failed() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").fail("boom"))
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    run.cancel().await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert!(matches!(run.state("a").await, NodeState::Failed));
}

#[tokio::test(flavor = "current_thread")]
async fn inspect_during_running_returns_live_snapshot() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").hang(false))
        .start()
        .await;
    tokio::time::timeout(BOUND, run.scripted("a").wait_until_hanging())
        .await
        .expect("hang");
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Running);
    assert!(matches!(
        snap.node(&NodeId::new("a")).map(|n| &n.state),
        Some(NodeState::Running { .. })
    ));
    run.cancel().await;
    within(run.wait_stable()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn live_snapshot_carries_schema_version_1() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let snap = handle.inspect().await;
    assert_eq!(snap.schema_version, SCHEMA_VERSION);
    assert_eq!(SCHEMA_VERSION, 1);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

// --- Clock ---------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn fake_clock_jump_over_retry_deadline_still_fires() {
    let test = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("once")
                .succeed(Bytes::from_static(b"ok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(100)));
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            match run.state("a").await {
                NodeState::Ready {
                    runnable_at: Some(_),
                } => break,
                NodeState::Waiting { .. } => panic!("retry delay must not be Waiting"),
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("parked at retry deadline");
    clock.advance(Duration::from_secs(10));
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn paused_clock_retry_does_not_busy_spin() {
    let test = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("once")
                .succeed(Bytes::from_static(b"ok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(5_000)));
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("a").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retry parked");
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        run.scripted("a").attempts(),
        vec![1],
        "paused FakeClock must not fire retry (no busy-spin)"
    );
    clock.advance(Duration::from_secs(5));
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
}

// --- Stale / concurrent / apply ------------------------------------------------

#[test]
fn second_finish_same_attempt_after_success_is_noop() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let now = Timestamp::from_millis(0);
    let p = AcceptPolicy;
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
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"first"))),
        },
        &p,
        now,
    )
    .unwrap();
    let rev = ex.revision();
    let effect = ex
        .apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"second"))),
            },
            &p,
            now,
        )
        .unwrap();
    assert!(!effect.changed);
    assert_eq!(ex.revision(), rev);
    assert_eq!(
        ex.snapshot().node(&NodeId::new("a")).unwrap().output,
        Some(Bytes::from_static(b"first"))
    );
}

// --- ADR 0001 / persist backpressure ------------------------------------------

struct GatePersist {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    gated: AtomicUsize,
    inner: MemoryStore,
}

#[async_trait::async_trait]
impl StateStore for GatePersist {
    async fn put(&self, snapshot: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }

    async fn get(
        &self,
        id: &keel_rt::ExecutionId,
    ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        if self.gated.fetch_add(1, Ordering::SeqCst) == 0 {
            let wait = self.release.notified();
            self.entered.notify_waiters();
            wait.await;
        }
        self.inner.persist(exec).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inspect_during_blocking_persist_completes_after_persist() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = GatePersist {
        entered: entered.clone(),
        release: release.clone(),
        gated: AtomicUsize::new(0),
        inner: MemoryStore::new(),
    };
    let def = WorkflowDefinition::builder("wf")
        .node("w", "w")
        .build()
        .unwrap();
    let parked = entered.notified();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("w", |_ctx: ExecutionContext| async {
            NodeOutcome::Waiting {
                token: ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("w"), 1),
            }
        })
        .build();
    let handle = rt.start(def).expect("start");
    tokio::time::timeout(BOUND, parked)
        .await
        .expect("persist must enter before inspect is sent");
    let inspect = handle.inspect();
    release.notify_waiters();
    let snap = tokio::time::timeout(BOUND, inspect)
        .await
        .expect("inspect must not deadlock behind a blocking persist (ADR 0001)");
    assert_ne!(snap.workflow_id.as_str(), "stopped");
    handle.cancel().await;
    within(handle.wait()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn persist_succeeds_before_execution_succeeded_is_emitted() {
    struct OrderStore {
        inner: MemoryStore,
        persisted_success: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl StateStore for OrderStore {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            self.inner.persist(exec).await?;
            if exec.state() == ExecutionState::Succeeded {
                self.persisted_success.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }
    let persisted_success = Arc::new(AtomicBool::new(false));
    let announced_before_persist = Arc::new(AtomicBool::new(false));
    let flag = persisted_success.clone();
    let announced = announced_before_persist.clone();
    let store = OrderStore {
        inner: MemoryStore::new(),
        persisted_success: persisted_success.clone(),
    };
    let inner = store.inner.clone();
    let sink = keel_rt::FnSink(move |e: &DomainEvent| {
        if matches!(e, DomainEvent::ExecutionSucceeded { .. })
            && !flag.load(Ordering::SeqCst)
        {
            announced.store(true, Ordering::SeqCst);
        }
    });
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store)
        .sink(sink)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.inspect().await.execution_id.clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert!(
        !announced_before_persist.load(Ordering::SeqCst),
        "ExecutionSucceeded must not be announced before the snapshot is durable"
    );
    assert_eq!(
        inner.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded
    );
}

#[tokio::test(flavor = "current_thread")]
async fn persist_panic_after_write_keeps_terminal_and_does_not_emit() {
    struct WriteThenPanic {
        inner: MemoryStore,
    }
    #[async_trait::async_trait]
    impl StateStore for WriteThenPanic {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            self.inner.persist(exec).await?;
            if exec.state() == ExecutionState::Succeeded {
                panic!("after durable write");
            }
            Ok(())
        }
    }
    let inner = MemoryStore::new();
    let seen_success = Arc::new(AtomicBool::new(false));
    let flag = seen_success.clone();
    let sink = keel_rt::FnSink(move |e: &DomainEvent| {
        if matches!(e, DomainEvent::ExecutionSucceeded { .. }) {
            flag.store(true, Ordering::SeqCst);
        }
    });
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(WriteThenPanic {
            inner: inner.clone(),
        })
        .sink(sink)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.inspect().await.execution_id.clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert!(
        !seen_success.load(Ordering::SeqCst),
        "must not announce a persist that panicked after the write"
    );
    assert_eq!(
        inner.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded,
        "terminal must remain in the store after persist-then-panic"
    );
}

// --- Display -------------------------------------------------------------------

#[test]
fn node_state_and_execution_state_display_covers_every_variant() {
    let token = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("n"), 1);
    let at = Timestamp::from_millis(42);
    let nodes = [
        (NodeState::Pending, "Pending"),
        (NodeState::Ready { runnable_at: None }, "Ready"),
        (
            NodeState::Ready {
                runnable_at: Some(at),
            },
            "Ready(42)",
        ),
        (NodeState::Running { attempt: 3 }, "Running(3)"),
        (NodeState::Waiting { token, attempt: 2 }, "Waiting(2)"),
        (NodeState::Succeeded, "Succeeded"),
        (NodeState::Failed, "Failed"),
        (NodeState::Cancelled, "Cancelled"),
        (NodeState::TimedOut, "TimedOut"),
    ];
    for (st, want) in nodes {
        assert_eq!(st.to_string(), want);
    }
    let execs = [
        (ExecutionState::Created, "Created"),
        (ExecutionState::Running, "Running"),
        (ExecutionState::Waiting, "Waiting"),
        (ExecutionState::Succeeded, "Succeeded"),
        (ExecutionState::Failed, "Failed"),
        (ExecutionState::Cancelled, "Cancelled"),
        (ExecutionState::Completed, "Completed"),
    ];
    for (st, want) in execs {
        assert_eq!(st.to_string(), want);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_is_the_builder_default() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.inspect().await.execution_id.clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded
    );
}

// --- Hunt: production paths the packs did not actually prove -------------------

#[tokio::test(flavor = "current_thread")]
async fn drop_runtime_while_running_and_waiting_keeps_execution() {
    let hanging = Arc::new(AtomicBool::new(false));
    let def_run = WorkflowDefinition::builder("run")
        .node("h", "h")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("h", {
            let hanging = hanging.clone();
            move |ctx: ExecutionContext| {
                let hanging = hanging.clone();
                async move {
                    hanging.store(true, Ordering::SeqCst);
                    loop {
                        if ctx.cancel.is_cancelled() {
                            return NodeOutcome::Failed(keel_rt::NodeError::new("cancelled"));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .build();
    let handle = rt.start(def_run).expect("start");
    drop(rt);
    tokio::time::timeout(BOUND, async {
        loop {
            if hanging.load(Ordering::SeqCst) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("execute started after Runtime drop");
    let snap = handle.inspect().await;
    assert_eq!(snap.state, ExecutionState::Running);
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);

    let def_wait = WorkflowDefinition::builder("wait")
        .node("w", "w")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("w", |_ctx: ExecutionContext| async {
            NodeOutcome::Waiting {
                token: ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("w"), 1),
            }
        })
        .build();
    let handle = rt.start(def_wait).expect("start");
    drop(rt);
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("w"))
        .and_then(|n| n.resume_token.clone())
        .expect("token");
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
async fn slow_execute_does_not_stall_inspect_or_cancel() {
    let in_execute = Arc::new(AtomicBool::new(false));
    let def = WorkflowDefinition::builder("wf")
        .node("slow", "slow")
        .node("idle", "idle")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .concurrency(1)
        .register_fn("slow", {
            let in_execute = in_execute.clone();
            move |ctx: ExecutionContext| {
                let in_execute = in_execute.clone();
                async move {
                    in_execute.store(true, Ordering::SeqCst);
                    loop {
                        if ctx.cancel.is_cancelled() {
                            return NodeOutcome::Failed(keel_rt::NodeError::new("cancelled"));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .register_fn("idle", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"idle"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if in_execute.load(Ordering::SeqCst) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow execute entered");
    let snap = handle.inspect().await;
    assert_eq!(
        snap.state,
        ExecutionState::Running,
        "inspect must return while execute is still in-flight (apply does not await execute)"
    );
    assert!(matches!(
        snap.node(&NodeId::new("slow")).map(|n| &n.state),
        Some(NodeState::Running { .. })
    ));
    let idle = snap.node(&NodeId::new("idle")).map(|n| n.state.clone());
    assert!(
        matches!(idle, Some(NodeState::Pending) | Some(NodeState::Ready { .. })),
        "conc 1: idle must not start while slow holds the permit, got {idle:?}"
    );
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn fail_execution_all_done_reducer_never_runs_but_terminates() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .join("reducer", Join::AllDone)
            .node("ok", ok("ok"))
            .node("fail", ScriptedExecutor::new("fail").fail("nope"))
            .node("reducer", ok("reducer"))
            .edge("ok", "reducer")
            .edge("fail", "reducer")
            .run(),
    )
    .await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Failed,
        "FailExecution + AllDone must fail-fast, not hang waiting for the reducer"
    );
    assert!(matches!(run.state("reducer").await, NodeState::Cancelled));
    assert!(
        run.scripted("reducer").attempts().is_empty(),
        "AllDone reducer must never start under fail-fast"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn clock_jump_over_several_staggered_retry_deadlines() {
    let test = WorkflowTest::new()
        .concurrency(2)
        .node(
            "fast",
            ScriptedExecutor::new("fast")
                .fail("fast")
                .succeed(Bytes::from_static(b"fok")),
        )
        .node(
            "slow",
            ScriptedExecutor::new("slow")
                .then(keel_rt::testing::ScriptedAction::Delay {
                    delay: Duration::from_millis(50),
                    then: Box::new(keel_rt::testing::ScriptedAction::Fail("slow".into())),
                })
                .succeed(Bytes::from_static(b"sok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(100)));
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("fast").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fast parked");
    clock.advance(Duration::from_millis(50));
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("slow").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow parked");
    assert!(
        matches!(
            run.state("fast").await,
            NodeState::Ready {
                runnable_at: Some(_)
            }
        ),
        "fast must still be parked when slow parks"
    );
    assert!(
        !matches!(run.state("fast").await, NodeState::Waiting { .. })
            && !matches!(run.state("slow").await, NodeState::Waiting { .. }),
        "retry must never be Waiting"
    );
    clock.advance(Duration::from_secs(10));
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("fast").attempts(), vec![1, 2]);
    assert_eq!(run.scripted("slow").attempts(), vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn store_error_on_terminal_write_keeps_in_memory_succeeded() {
    struct FailTerminal {
        inner: MemoryStore,
        terminal_fails: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StateStore for FailTerminal {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            if exec.state().is_terminal() {
                self.terminal_fails.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::Message("terminal persist".into()));
            }
            self.inner.persist(exec).await
        }
    }
    let terminal_fails = Arc::new(AtomicUsize::new(0));
    let store = FailTerminal {
        inner: MemoryStore::new(),
        terminal_fails: terminal_fails.clone(),
    };
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    assert_eq!(
        within(rt.run(def)).await.unwrap(),
        ExecutionState::Succeeded
    );
    assert!(
        terminal_fails.load(Ordering::SeqCst) >= 1,
        "terminal persist must have been attempted (and failed)"
    );
}

/// Transient persist `Err` of the terminal, then `wait` + Drop (Shutdown).
/// `last_persisted = revision` on persist fail used to skip retry; Shutdown
/// did not flush. The store stayed Running after `wait` returned Succeeded,
/// so a later resume re-invoked work the caller already observed as done.
#[tokio::test(flavor = "current_thread")]
async fn transient_terminal_persist_err_shutdown_flushes_succeeded() {
    struct FailFirstTerminal {
        inner: MemoryStore,
        terminal_attempts: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StateStore for FailFirstTerminal {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            if exec.state().is_terminal() {
                let n = self.terminal_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    return Err(StoreError::Message("busy terminal".into()));
                }
            }
            self.inner.persist(exec).await
        }
        async fn workflow_definition(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::WorkflowDefinition>, StoreError> {
            self.inner.workflow_definition(id).await
        }
    }
    let inner = MemoryStore::new();
    let terminal_attempts = Arc::new(AtomicUsize::new(0));
    let store = FailFirstTerminal {
        inner: inner.clone(),
        terminal_attempts: terminal_attempts.clone(),
    };
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.execution_id().clone();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    drop(rt);
    tokio::task::yield_now().await;
    assert_eq!(
        inner.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded,
        "clean shutdown must retry a transient terminal persist Err so the store matches wait()"
    );
    assert!(
        terminal_attempts.load(Ordering::SeqCst) >= 2,
        "Shutdown must retry the failed terminal persist, got {}",
        terminal_attempts.load(Ordering::SeqCst)
    );
}

/// Drop-cancel persist `Err` once, then Shutdown. File/store must be Cancelled,
/// not left Running so resume re-invokes work the handle cancelled.
#[tokio::test(flavor = "current_thread")]
async fn transient_cancel_persist_err_shutdown_flushes_cancelled() {
    struct FailFirstCancel {
        inner: MemoryStore,
        cancel_attempts: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StateStore for FailFirstCancel {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            if exec.state() == ExecutionState::Cancelled {
                let n = self.cancel_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    return Err(StoreError::Message("busy cancel".into()));
                }
            }
            self.inner.persist(exec).await
        }
        async fn workflow_definition(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::WorkflowDefinition>, StoreError> {
            self.inner.workflow_definition(id).await
        }
    }
    let inner = MemoryStore::new();
    let cancel_attempts = Arc::new(AtomicUsize::new(0));
    let store = FailFirstCancel {
        inner: inner.clone(),
        cancel_attempts: cancel_attempts.clone(),
    };
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store)
        .register(ScriptedExecutor::new("a").hang(false))
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.execution_id().clone();
    within(async {
        loop {
            if let Some(s) = inner.get(&id).await.unwrap() {
                if matches!(
                    s.node(&NodeId::new("a")).map(|n| &n.state),
                    Some(NodeState::Running { .. })
                ) {
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
            if let Some(s) = inner.get(&id).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        cancel_attempts.load(Ordering::SeqCst) >= 2,
        "Shutdown must retry the failed cancel persist, got {}",
        cancel_attempts.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fat_bytes_join_input_is_refcount_not_copy() {
    let fat = Bytes::from(vec![9u8; 64 * 1024]);
    let run = within(
        WorkflowTest::new()
            .node("fat", ScriptedExecutor::new("fat").succeed(fat.clone()))
            .node("join", ok("join"))
            .edge("fat", "join")
            .run(),
    )
    .await;
    let out = run.output("fat").await.expect("fat output");
    let input = run
        .inputs("join")
        .await
        .get(&NodeId::new("fat"))
        .cloned()
        .expect("join input");
    assert_eq!(out.len(), fat.len());
    assert_eq!(
        out.as_ptr(),
        input.as_ptr(),
        "join inputs must clone Bytes (refcount), not copy the buffer"
    );
}

#[test]
fn retry_due_twice_does_not_double_runnable() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let now = Timestamp::from_millis(0);
    let p = RetryPolicy::new(3, Duration::from_millis(10));
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::failed("once")),
        },
        &p,
        now,
    )
    .unwrap();
    let first = ex
        .apply(ApplyCmd::RetryDue { node_id: "a".into() }, &p, Timestamp::from_millis(10))
        .unwrap();
    assert!(first.changed);
    let rev = ex.revision();
    let second = ex
        .apply(ApplyCmd::RetryDue { node_id: "a".into() }, &p, Timestamp::from_millis(10))
        .unwrap();
    assert!(!second.changed, "second Timer for the same retry is a no-op");
    assert_eq!(ex.revision(), rev);
}

#[tokio::test(flavor = "current_thread")]
async fn retry_storm_under_concurrency_cap_never_waiting() {
    let n = 6usize;
    let mut test = WorkflowTest::new()
        .concurrency(2)
        .policy(RetryPolicy::new(2, Duration::from_millis(100)));
    for i in 0..n {
        let id = format!("n{i}");
        test = test.node(
            &id,
            ScriptedExecutor::new(id.as_str())
                .fail("once")
                .succeed(Bytes::from_static(b"ok")),
        );
    }
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            let running = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Running { .. }))
                .count();
            assert!(running <= 2, "retry storm Running {running} > cap 2");
            assert_ne!(snap.state, ExecutionState::Waiting, "retry is never Waiting");
            let parked = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Ready { runnable_at: Some(_) }))
                .count();
            if parked == n {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all nodes parked at retry");
    clock.advance(Duration::from_millis(100));
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    for i in 0..n {
        assert_eq!(run.scripted(&format!("n{i}")).attempts(), vec![1, 2]);
    }
}

struct PanicPolicy;
impl Policy for PanicPolicy {
    fn decide(&self, _o: &NodeOutcome, _a: u32) -> PolicyDecision {
        panic!("policy exploded");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn policy_panic_fail_fasts_even_under_fail_subtree() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .on_failure(OnFailure::FailSubtree)
            .policy(PanicPolicy)
            .node("a", ok("a"))
            .node("sib", ScriptedExecutor::new("sib").hang(false))
            .run(),
    )
    .await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Failed,
        "Policy::decide panic is fail-fast (process lives; graph does not use FailSubtree)"
    );
    let sib = run.state("sib").await;
    assert!(
        matches!(sib, NodeState::Cancelled | NodeState::Failed),
        "hanging sibling must not leak, got {sib:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_ignore_cancel_fanout_meets_bound() {
    let n = 8usize;
    let mut test = WorkflowTest::new()
        .concurrency(8)
        .cancel_bound(DEFAULT_CANCEL_BOUND);
    for i in 0..n {
        let id = format!("h{i}");
        test = test.node(&id, ScriptedExecutor::new(id.as_str()).hang(true));
    }
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            let running = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Running { .. }))
                .count();
            if running == n {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all ignore_cancel hangs Running");
    run.cancel().await;
    tokio::time::timeout(
        DEFAULT_CANCEL_BOUND + Duration::from_millis(100),
        run.wait_stable(),
    )
    .await
    .expect("cancel bound under fanout is a lie if this times out");
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_persists_failed_cancelled_completed_terminals() {
    let store = MemoryStore::new();
    let failed = within(
        WorkflowTest::new()
            .store(store.clone())
            .node("a", ScriptedExecutor::new("a").fail("boom"))
            .run(),
    )
    .await;
    assert_eq!(failed.execution_state().await, ExecutionState::Failed);
    let id = failed.snapshot().await.execution_id.clone();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Failed
    );

    let store = MemoryStore::new();
    let run = WorkflowTest::new()
        .store(store.clone())
        .node("h", ScriptedExecutor::new("h").hang(false))
        .start()
        .await;
    tokio::time::timeout(BOUND, run.scripted("h").wait_until_hanging())
        .await
        .expect("hang");
    run.cancel().await;
    within(run.wait_stable()).await;
    let id = run.snapshot().await.execution_id.clone();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Cancelled
    );

    let store = MemoryStore::new();
    let completed = within(
        WorkflowTest::new()
            .store(store.clone())
            .on_failure(OnFailure::FailSubtree)
            .node("a", ScriptedExecutor::new("a").fail("boom"))
            .run(),
    )
    .await;
    assert_eq!(completed.execution_state().await, ExecutionState::Completed);
    let id = completed.snapshot().await.execution_id.clone();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Completed
    );
}

/// `emit` is sync on apply. A blocking sink stalls inspect until it returns
/// (persist-class backpressure). The unbounded inbox must still accept the
/// Inspect send — that is not a lock-cycle. Proved on multi_thread so the
/// test task can run while apply is inside `emit`; on current_thread a
/// blocking emit freezes the runtime until it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eventsink_blocking_does_not_deadlock_inspect() {
    struct BlockFirstEmit {
        entered: Arc<AtomicBool>,
        in_block: Arc<AtomicBool>,
        pair: Arc<(Mutex<bool>, Condvar)>,
    }
    impl EventSink for BlockFirstEmit {
        fn emit(&self, _event: &DomainEvent) {
            if !self.entered.swap(true, Ordering::SeqCst) {
                self.in_block.store(true, Ordering::SeqCst);
                let (lock, cv) = &*self.pair;
                let mut g = lock.lock().expect("block-first emit");
                while !*g {
                    g = cv.wait(g).expect("block-first emit");
                }
                self.in_block.store(false, Ordering::SeqCst);
            }
        }
    }
    let entered = Arc::new(AtomicBool::new(false));
    let in_block = Arc::new(AtomicBool::new(false));
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .sink(BlockFirstEmit {
            entered: entered.clone(),
            in_block: in_block.clone(),
            pair: pair.clone(),
        })
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if entered.load(Ordering::SeqCst) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("EventSink::emit must enter");
    assert!(
        in_block.load(Ordering::SeqCst),
        "inspect is sent while emit still holds the apply task"
    );
    let inspect = handle.inspect();
    let release = async {
        let (lock, cv) = &*pair;
        *lock.lock().expect("release emit") = true;
        cv.notify_one();
    };
    let (snap, _) = tokio::time::timeout(BOUND, async { tokio::join!(inspect, release) })
        .await
        .expect("inspect send must not deadlock behind a blocking EventSink (ADR 0001)");
    assert_ne!(snap.workflow_id.as_str(), "stopped");
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

/// Executor panic is a node Failed. FailSubtree is honoured (unlike Policy
/// panic, which is fail-fast). conc 1: the panic path must release the
/// permit so the sibling can run.
#[tokio::test(flavor = "current_thread")]
async fn executor_panic_fail_subtree_releases_permit_sibling_runs() {
    let run = within(
        WorkflowTest::new()
            .concurrency(1)
            .on_failure(OnFailure::FailSubtree)
            .node("boom", ScriptedExecutor::new("boom").panic())
            .node("sib", ok("sib"))
            .run(),
    )
    .await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Completed,
        "executor panic under FailSubtree is graph-local, not fail-fast"
    );
    assert!(matches!(run.state("boom").await, NodeState::Failed));
    assert!(
        matches!(run.state("sib").await, NodeState::Succeeded),
        "permit must be released after executor panic; sibling must run"
    );
}

/// One Runtime, two sequential starts: executor panic must not leak JoinSet
/// / permits into the next execution.
#[tokio::test(flavor = "current_thread")]
async fn next_start_after_executor_panic_succeeds() {
    let rt = Runtime::builder()
        .concurrency(1)
        .register(ScriptedExecutor::new("boom").panic())
        .register_fn("ok", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let boom = WorkflowDefinition::builder("boom")
        .node("b", "boom")
        .build()
        .unwrap();
    assert_eq!(within(rt.run(boom)).await.unwrap(), ExecutionState::Failed);
    let ok_def = WorkflowDefinition::builder("ok")
        .node("a", "ok")
        .build()
        .unwrap();
    assert_eq!(
        within(rt.run(ok_def)).await.unwrap(),
        ExecutionState::Succeeded,
        "next start on the same Runtime must not hang after an executor panic"
    );
}

/// Hourglass + FailExecution: sources succeed, neck fails, sinks never start,
/// execution terminates Failed (AND-join default, fail-fast default).
#[tokio::test(flavor = "current_thread")]
async fn hourglass_neck_fail_cancels_sinks_and_terminates() {
    let n = 4usize;
    let mut test = WorkflowTest::new()
        .concurrency(8)
        .node("neck", ScriptedExecutor::new("neck").fail("neck"));
    for i in 0..n {
        let a = format!("a{i}");
        let b = format!("b{i}");
        test = test
            .node(&a, ok(&a))
            .node(&b, ok(&b))
            .edge(&a, "neck")
            .edge("neck", &b);
    }
    let run = within(test.run()).await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Failed,
        "hourglass neck fail must fail-fast, not hang on AND-join sinks"
    );
    for i in 0..n {
        assert!(matches!(run.state(&format!("a{i}")).await, NodeState::Succeeded));
        assert!(matches!(run.state(&format!("b{i}")).await, NodeState::Cancelled));
        assert!(
            run.scripted(&format!("b{i}")).attempts().is_empty(),
            "sink b{i} must never start"
        );
    }
}

// --- RAII / permit + task leak ------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn drop_handle_returns_permits_running_waiting_ready_timer() {
    // Running
    let store = MemoryStore::new();
    let mut run = WorkflowTest::new()
        .store(store.clone())
        .concurrency(2)
        .node("h", ScriptedExecutor::new("h").hang(false))
        .start()
        .await;
    tokio::time::timeout(BOUND, run.scripted("h").wait_until_hanging())
        .await
        .expect("hang");
    assert_eq!(run.snapshot().await.running_count(), 1);
    assert_eq!(run.snapshot().await.waiting_count(), 0);
    run.drop_handle();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(snap) = run.stored_snapshot().await {
                if snap.state == ExecutionState::Cancelled
                    && snap.running_count() == 0
                    && snap.waiting_count() == 0
                {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop handle Running: permits returned, no Running leftover");

    // Waiting
    let store = MemoryStore::new();
    let mut run = WorkflowTest::new()
        .store(store.clone())
        .node("w", ScriptedExecutor::new("w").wait())
        .start()
        .await;
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    assert_eq!(run.snapshot().await.running_count(), 0);
    assert_eq!(run.snapshot().await.waiting_count(), 1);
    run.drop_handle();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(snap) = run.stored_snapshot().await {
                if snap.state == ExecutionState::Cancelled && snap.waiting_count() == 0 {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop handle Waiting: waiting_count 0");

    // Ready { runnable_at } mid-timer
    let store = MemoryStore::new();
    let test = WorkflowTest::new()
        .store(store.clone())
        .policy(RetryPolicy::new(3, Duration::from_millis(100)))
        .node(
            "r",
            ScriptedExecutor::new("r")
                .fail("once")
                .succeed(Bytes::from_static(b"ok")),
        );
    let clock = test.fake_clock();
    let mut run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("r").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parked retry");
    assert_eq!(run.snapshot().await.running_count(), 0);
    run.drop_handle();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(snap) = run.stored_snapshot().await {
                if snap.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop mid-timer");
    clock.advance(Duration::from_secs(1));
    tokio::task::yield_now().await;
    let snap = run.stored_snapshot().await.expect("stored");
    assert_eq!(snap.state, ExecutionState::Cancelled);
    assert_eq!(snap.running_count(), 0);
    assert!(matches!(
        snap.node(&NodeId::new("r")).map(|n| &n.state),
        Some(NodeState::Cancelled)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn drop_runtime_then_drop_handle_does_not_leak() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("h", "h")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register(ScriptedExecutor::new("h").hang(false))
        .build();
    let handle = rt.start(def).expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if handle.inspect().await.running_count() == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running");
    let id = handle.inspect().await.execution_id.clone();
    drop(rt);
    assert_eq!(handle.inspect().await.running_count(), 1);
    drop(handle);
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(Some(snap)) = store.get(&id).await {
                if snap.state == ExecutionState::Cancelled && snap.running_count() == 0 {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last handle Drop owns JoinSet abort + permit return");
}

#[tokio::test(flavor = "current_thread")]
async fn panic_paths_release_permits() {
    let boom = within(
        WorkflowTest::new()
            .concurrency(1)
            .node("boom", ScriptedExecutor::new("boom").panic())
            .node("sib", ok("sib"))
            .run(),
    )
    .await;
    assert_eq!(boom.execution_state().await, ExecutionState::Failed);
    assert_eq!(boom.snapshot().await.running_count(), 0);
    assert_eq!(boom.snapshot().await.waiting_count(), 0);

    let policy = within(
        WorkflowTest::new()
            .concurrency(1)
            .policy(PanicPolicy)
            .node("a", ok("a"))
            .run(),
    )
    .await;
    assert_eq!(policy.execution_state().await, ExecutionState::Failed);
    assert_eq!(policy.snapshot().await.running_count(), 0);

    let sink_ok = within(
        WorkflowTest::new()
            .node("a", ok("a"))
            .run(),
    )
    .await;
    assert_eq!(sink_ok.snapshot().await.running_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn fifty_sequential_executions_do_not_leak_permits() {
    let rt = Runtime::builder()
        .concurrency(2)
        .register_fn("ok", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .register(ScriptedExecutor::new("wait").wait())
        .build();
    for i in 0..50 {
        let def = WorkflowDefinition::builder(format!("seq-{i}"))
            .node("a", "ok")
            .build()
            .unwrap();
        let handle = rt.start(def).expect("start");
        let snap = handle.inspect().await;
        assert!(snap.running_count() <= 2);
        assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    }
    let wait_def = WorkflowDefinition::builder("wait")
        .node("w", "wait")
        .build()
        .unwrap();
    let handle = rt.start(wait_def).expect("start");
    assert_eq!(within(handle.wait_stable()).await, ExecutionState::Waiting);
    let snap = handle.inspect().await;
    assert_eq!(snap.running_count(), 0, "Waiting must not hold a permit");
    assert_eq!(snap.waiting_count(), 1);
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    // Handle consumed by wait(); next start must not stall on a leaked permit.
    let def = WorkflowDefinition::builder("after")
        .node("a", "ok")
        .node("b", "ok")
        .build()
        .unwrap();
    let handle = rt.start(def).expect("start");
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_concurrent_get_during_persist_does_not_deadlock() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.inspect().await.execution_id.clone();
    let getter = async {
        for _ in 0..64 {
            let _ = store.get(&id).await;
            tokio::task::yield_now().await;
        }
    };
    let (state, _) = tokio::join!(handle.wait(), getter);
    assert_eq!(state, ExecutionState::Succeeded);
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().running_count(),
        0
    );
}
