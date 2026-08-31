//! Named regressions for every Phase 1 failure-catalog row that was MISSING.
//!
//! `cargo test --test catalog -- --test-threads=1`
//! See `docs/FAILURE_CATALOG.md`.

use bytes::Bytes;
use keel_rt::testing::{NetFault, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyCmd, ApplyError, Execution, ExecutionContext, ExecutionState,
    FunctionExecutor, Join, MemoryStore, NodeId, NodeOutcome, NodeState, OnFailure, Policy,
    PolicyDecision, Resume, ResumeToken, RetryPolicy, Runtime, StateStore, StoreError, Timestamp,
    WorkflowDefinition, DEFAULT_CANCEL_BOUND, SCHEMA_VERSION,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

struct BlockingPersist {
    delay: Duration,
    entered: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl StateStore for BlockingPersist {
    async fn put(&self, _snapshot: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
        Ok(())
    }

    async fn get(
        &self,
        _id: &keel_rt::ExecutionId,
    ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
        Ok(None)
    }

    async fn persist(&self, _exec: &Execution) -> Result<(), StoreError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inspect_during_blocking_persist_completes_after_persist() {
    let entered = Arc::new(AtomicUsize::new(0));
    let store = BlockingPersist {
        delay: Duration::from_millis(80),
        entered: entered.clone(),
    };
    let def = WorkflowDefinition::builder("wf")
        .node("h", "h")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store)
        .register_fn("h", |ctx: ExecutionContext| async move {
            loop {
                ctx.sleep(Duration::from_secs(60)).await;
            }
        })
        .build();
    let handle = rt.start(def).expect("start");
    let snap = tokio::time::timeout(BOUND, handle.inspect())
        .await
        .expect("inspect must not deadlock behind a blocking persist (ADR 0001)");
    assert!(
        entered.load(Ordering::SeqCst) >= 1,
        "persist must have been entered; inspect waited (backpressure), then completed"
    );
    assert_ne!(snap.workflow_id.as_str(), "stopped");
    handle.cancel().await;
    tokio::time::timeout(
        DEFAULT_CANCEL_BOUND + Duration::from_millis(200),
        handle.wait(),
    )
    .await
    .expect("cancel hang");
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
