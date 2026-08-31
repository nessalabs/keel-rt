//! Attack suite: concurrency, join/fail-fast, stale completions, timers,
//! cancel, panic isolation, definition edges, store/inspect, research smoke.

use bytes::Bytes;
use keel_rt::domain::policy::Policy;
use keel_rt::domain::state::{ApplyCmd, Execution};
use keel_rt::testing::{FailingStore, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyError, DomainEvent, ExecutionContext, ExecutionHandle, ExecutionState,
    FnSink, FunctionExecutor, MemoryStore, NodeId, NodeOutcome, NodeState, Resume, ResumeToken,
    RetryPolicy, Runtime, StateStore, WorkflowDefinition, DEFAULT_CANCEL_BOUND,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(5);

async fn within<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(BOUND, f)
        .await
        .expect("break test timed out")
}

fn hang(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).hang(false)
}

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

fn count_failed(events: &[DomainEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, DomainEvent::ExecutionFailed { .. }))
        .count()
}

fn running_count(snap: &keel_rt::ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}

// --- Concurrency / permits -------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn permit_cap_never_exceeded_during_burst() {
    let k = 3usize;
    let n = 8usize;
    let entered = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut test = WorkflowTest::new().concurrency(k);
    for i in 0..n {
        let id = format!("h{i}");
        let ent = entered.clone();
        let pk = peak.clone();
        test = test.executor(
            &id,
            FunctionExecutor::new(id.as_str(), move |_ctx: ExecutionContext| {
                let c = ent.fetch_add(1, Ordering::SeqCst) + 1;
                pk.fetch_max(c, Ordering::SeqCst);
                let ent = ent.clone();
                async move {
                    ent.fetch_sub(1, Ordering::SeqCst);
                    // stay Running long enough that a burst would overlap
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }),
        );
    }
    let run = test.start().await;
    let deadline = Instant::now() + Duration::from_millis(80);
    let mut observed_max = 0usize;
    while Instant::now() < deadline {
        let r = running_count(&run.snapshot().await);
        observed_max = observed_max.max(r);
        assert!(
            r <= k,
            "Running count {r} exceeded concurrency {k} during Ready→Running window"
        );
        tokio::task::yield_now().await;
    }
    within(run.wait_stable()).await;
    let p = peak.load(Ordering::SeqCst);
    assert!(p <= k, "dispatched/held peak {p} exceeded concurrency {k}");
    assert!(observed_max <= k);
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_zero_does_not_deadlock() {
    // RuntimeBuilder treats 0 as 1. Must not hang.
    let run = within(
        WorkflowTest::new()
            .concurrency(0)
            .node("a", ok("a"))
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_releases_permit_sibling_runs() {
    let run = within(
        WorkflowTest::new()
            .concurrency(1)
            .node("w", ScriptedExecutor::new("w").wait())
            .node("sib", ok("sib"))
            .run(),
    )
    .await;
    assert!(matches!(run.state("w").await, NodeState::Waiting { .. }));
    assert!(matches!(run.state("sib").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
}

#[tokio::test(flavor = "current_thread")]
async fn eight_waits_concurrency_2_then_resume_one() {
    let mut test = WorkflowTest::new().concurrency(2);
    for i in 0..8 {
        let id = format!("w{i}");
        test = test.node(&id, ScriptedExecutor::new(id.as_str()).wait());
    }
    let run = within(test.run()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    for i in 0..8 {
        assert!(
            matches!(run.state(&format!("w{i}")).await, NodeState::Waiting { .. }),
            "w{i} should be Waiting"
        );
    }
    let token = run.resume_token("w0").await;
    run.resume(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"w0"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    let snap = run.snapshot().await;
    let running = running_count(&snap);
    assert!(running <= 2, "resume Complete must not start 3 at once, running={running}");
    assert!(matches!(
        snap.node(&NodeId::new("w0")).unwrap().state,
        NodeState::Succeeded
    ));
}

// --- AND-join / fail-fast --------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn diamond_c_fails_d_cancelled_b_stays_succeeded() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("a", ok("a"))
            .node("b", ok("b"))
            .node("c", ScriptedExecutor::new("c").fail("c-boom"))
            .node("d", ok("d"))
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .run(),
    )
    .await;
    assert!(matches!(run.state("c").await, NodeState::Failed));
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(
        matches!(run.state("b").await, NodeState::Succeeded),
        "fail-fast must not rewrite terminals"
    );
    assert!(run.scripted("d").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert_eq!(count_failed(&run.events()), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn triple_join_only_b_fails() {
    let run = within(
        WorkflowTest::new()
            .concurrency(3)
            .node("a", ok("a"))
            .node("b", ScriptedExecutor::new("b").fail("b-boom"))
            .node("c", ok("c"))
            .node("d", ok("d"))
            .edge("a", "d")
            .edge("b", "d")
            .edge("c", "d")
            .run(),
    )
    .await;
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(run.scripted("d").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    // A/C cancelled only if they were not yet terminal.
    for name in ["a", "c"] {
        let st = run.state(name).await;
        assert!(
            matches!(st, NodeState::Succeeded | NodeState::Cancelled),
            "{name}={st:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn join_stays_pending_while_one_pred_waiting() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("w", ScriptedExecutor::new("w").wait())
            .node("s", ok("s"))
            .node("join", ok("join"))
            .edge("w", "join")
            .edge("s", "join")
            .run(),
    )
    .await;
    assert!(matches!(run.state("join").await, NodeState::Pending));
    assert!(run.scripted("join").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_complete_same_bytes_dependents_once() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").wait())
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    let out = NodeOutcome::Succeeded(Bytes::from_static(b"same"));
    run.resume(token.clone(), Resume::Complete(out.clone()))
        .await
        .unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("b").attempts(), vec![1]);
    run.resume(token, Resume::Complete(out))
        .await
        .expect("duplicate equivalent Complete is Ok");
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(run.scripted("b").attempts(), vec![1]);
}

#[tokio::test(flavor = "current_thread")]
async fn conflicting_complete_errors_second_payload_not_used() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").wait())
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    run.resume(
        token.clone(),
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"first"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    let err = run
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"second"))),
        )
        .await
        .unwrap_err();
    assert_eq!(err, ApplyError::ConflictingComplete);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"first"))
    );
}

// --- Stale completions / attempts ------------------------------------------

fn linear_exec() -> Execution {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    Execution::new(def)
}

#[test]
fn finish_wrong_attempt_ignored() {
    let mut ex = linear_exec();
    let now = keel_rt::Timestamp(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    let rev = ex.revision();
    let r = ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 99,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"stale"))),
        },
        &p,
        now,
    );
    assert!(r.is_ok(), "stale finish is ignored, not a crash");
    assert!(
        matches!(
            ex.snapshot().node(&NodeId::new("a")).unwrap().state,
            NodeState::Running { attempt: 1 }
        ),
        "wrong attempt must not finish the node"
    );
    assert_eq!(ex.revision(), rev, "stale finish must not bump revision");
}

#[test]
fn resume_wrong_node_wrong_execution_wrong_nonce() {
    let mut ex = linear_exec();
    let now = keel_rt::Timestamp(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    let good = ex
        .snapshot()
        .node(&NodeId::new("a"))
        .and_then(|n| n.resume_token.clone())
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Waiting {
                token: good.clone(),
            }),
        },
        &p,
        now,
    )
    .unwrap();

    let wrong_node = ResumeToken::issue(good.execution_id.clone(), NodeId::new("b"), 1);
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_node,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());

    let wrong_exec = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("a"), 1);
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_exec,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());

    let wrong_nonce = ResumeToken::issue(good.execution_id.clone(), NodeId::new("a"), 1);
    assert_ne!(wrong_nonce.nonce(), good.nonce());
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_nonce,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn resume_after_cancel_errors() {
    let run = within(WorkflowTest::new().node("a", ScriptedExecutor::new("a").wait()).run()).await;
    let token = run.resume_token("a").await;
    run.cancel().await;
    within(run.wait_stable()).await;
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
async fn resume_on_running_node_is_error() {
    let run = WorkflowTest::new()
        .node("a", hang("a"))
        .start()
        .await;
    within(run.scripted("a").wait_until_hanging()).await;
    let token = run.resume_token("a").await;
    let err = run
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::NotWaiting | ApplyError::Illegal(_)),
        "{err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reinvoke_then_complete_success_path() {
    let run = within(
        WorkflowTest::new()
            .node(
                "a",
                ScriptedExecutor::new("a")
                    .wait()
                    .wait()
                    .succeed(Bytes::from_static(b"unused")),
            )
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    run.resume(token, Resume::Reinvoke).await.unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 1]);
    let token2 = run.resume_token("a").await;
    run.resume(
        token2,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"done"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"done"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn retry_delay_zero_successor_sees_success_output_only() {
    let run = within(
        WorkflowTest::new()
            .node(
                "a",
                ScriptedExecutor::new("a")
                    .fail("once")
                    .succeed(Bytes::from_static(b"good")),
            )
            .node("b", ok("b"))
            .edge("a", "b")
            .policy(RetryPolicy::new(3, Duration::ZERO))
            .run(),
    )
    .await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert_eq!(run.scripted("b").attempts(), vec![1]);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"good"))
    );
}

// --- Timers / lost wake ----------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn two_nodes_same_retry_deadline_both_run() {
    let test = WorkflowTest::new()
        .concurrency(2)
        .node(
            "x",
            ScriptedExecutor::new("x")
                .fail("x")
                .succeed(Bytes::from_static(b"xok")),
        )
        .node(
            "y",
            ScriptedExecutor::new("y")
                .fail("y")
                .succeed(Bytes::from_static(b"yok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(100)));
    let clock = test.fake_clock();
    let run = test.start().await;
    within(async {
        loop {
            let xr = matches!(
                run.state("x").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            );
            let yr = matches!(
                run.state("y").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            );
            if xr && yr {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    clock.advance(Duration::from_millis(100));
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("x").attempts(), vec![1, 2]);
    assert_eq!(run.scripted("y").attempts(), vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_ready_with_future_deadline_does_not_start_later() {
    let test = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("x")
                .succeed(Bytes::from_static(b"late")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(500)));
    let clock = test.fake_clock();
    let run = test.start().await;
    within(async {
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
    .await;
    run.cancel().await;
    within(run.wait_stable()).await;
    assert!(matches!(run.state("a").await, NodeState::Cancelled));
    clock.advance(Duration::from_millis(500));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        matches!(run.state("a").await, NodeState::Cancelled),
        "advancing clock must not start a cancelled retry"
    );
    assert_eq!(run.scripted("a").attempts(), vec![1]);
}

#[test]
fn timer_after_succeeded_is_noop() {
    let mut ex = linear_exec();
    let now = keel_rt::Timestamp(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
        },
        &p,
        now,
    )
    .unwrap();
    let rev = ex.revision();
    let effect = ex
        .apply(ApplyCmd::RetryDue { node_id: "a".into() }, &p, now)
        .unwrap();
    assert!(!effect.changed);
    assert_eq!(ex.revision(), rev);
    assert!(matches!(
        ex.snapshot().node(&NodeId::new("a")).unwrap().state,
        NodeState::Succeeded
    ));
}

// --- Cancellation ----------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn drop_handle_cancels_unique_owner() {
    // ExecutionHandle is deliberately not Clone. Unique owner: Drop cancels.
    // Shared last-drop-wins was not added (would flap vs JoinSet semantics).
    let mut run = WorkflowTest::new().node("a", hang("a")).start().await;
    within(run.scripted("a").wait_until_hanging()).await;
    run.drop_handle();
    within(async {
        loop {
            if let Some(snap) = run.stored_snapshot().await {
                if snap.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let _ = std::any::type_name::<ExecutionHandle>();
}

#[tokio::test(flavor = "current_thread")]
async fn hang_ignore_cancel_ends_within_bound() {
    let run = WorkflowTest::new()
        .node("h", ScriptedExecutor::new("h").hang(true))
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .start()
        .await;
    within(run.scripted("h").wait_until_hanging()).await;
    let state = tokio::time::timeout(DEFAULT_CANCEL_BOUND + Duration::from_millis(200), async {
        run.cancel().await;
        run.wait_stable().await;
        run.execution_state().await
    })
    .await
    .expect("ignore_cancel must end within cancel bound");
    assert_eq!(state, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_running_pending_sibling_never_starts() {
    let run = WorkflowTest::new()
        .concurrency(1)
        .node("a", hang("a"))
        .node("b", ok("b"))
        .start()
        .await;
    within(run.scripted("a").wait_until_hanging()).await;
    run.cancel().await;
    within(run.wait_stable()).await;
    assert!(run.scripted("b").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn fail_fast_abort_hanging_sibling_one_failed_event_inspect_lives() {
    let run = WorkflowTest::new()
        .concurrency(2)
        .node("ok", ScriptedExecutor::new("ok").fail("boom"))
        .node("hang", hang("hang"))
        .start()
        .await;
    within(run.scripted("hang").wait_until_hanging()).await;
    // Release? ok fails immediately; hang may already be running.
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert_eq!(count_failed(&run.events()), 1);
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Failed);
}

// --- Panic isolation -------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn executor_panic_scheduler_survives() {
    let run = within(WorkflowTest::new().node("p", ScriptedExecutor::new("p").panic()).run()).await;
    assert!(matches!(run.state("p").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert!(run.snapshot().await.revision > 0);
}

struct PanicPolicy;
impl Policy for PanicPolicy {
    fn decide(&self, _o: &NodeOutcome, _a: u32) -> keel_rt::PolicyDecision {
        panic!("policy exploded");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn policy_decide_panic_does_not_kill_scheduler() {
    let run = within(
        WorkflowTest::new()
            .node("a", ok("a"))
            .policy(PanicPolicy)
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    let _ = run.snapshot().await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_sink_panic_kernel_survives_and_progresses() {
    let def = WorkflowDefinition::builder("sink")
        .node("a", "a")
        .build()
        .unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let sink = FnSink(move |_e: &DomainEvent| {
        let n = h.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            panic!("sink exploded");
        }
    });
    let rt = Runtime::builder()
        .sink(sink)
        .register(FunctionExecutor::new("a", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        }))
        .build();
    let handle = rt.start(def);
    let state = within(handle.wait()).await;
    assert_eq!(state, ExecutionState::Succeeded);
    assert!(hits.load(Ordering::SeqCst) >= 1);
}

// --- Definition / API edges ------------------------------------------------

#[test]
fn self_edge_rejected() {
    let err = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .edge("a", "a")
        .build()
        .unwrap_err();
    assert_eq!(err, keel_rt::DefinitionError::Cycle);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_edge_is_one_pred() {
    // Two A→B edges: AND-join still once. B runs after A succeeds (not twice).
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .node("b", "b")
        .edge("a", "b")
        .edge("a", "b")
        .build();
    assert!(def.is_ok(), "duplicate edge accepted or we treat as one pred");
    let run = within(
        WorkflowTest::new()
            .node("a", ok("a"))
            .node("b", ok("b"))
            .edge("a", "b")
            .edge("a", "b")
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("b").attempts(), vec![1]);
}

#[test]
fn two_disconnected_components_are_accepted() {
    // Frozen: fan-out of independent sources is legal. Weakly disconnected
    // components are the same shape — not the dangling-edge "disconnected" reject.
    let def = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .node("b", "e")
        .node("c", "e")
        .node("d", "e")
        .edge("a", "b")
        .edge("c", "d")
        .build();
    assert!(def.is_ok(), "two independent chains must be accepted");
}

#[test]
fn empty_node_id_does_not_panic() {
    let built = WorkflowDefinition::builder("wf").node("", "e").build();
    assert!(built.is_ok() || built.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn unregistered_executor_fails_node_no_hang() {
    let def = WorkflowDefinition::builder("wf")
        .node("ghost", "missing")
        .build()
        .unwrap();
    let rt = Runtime::builder().build();
    let handle = rt.start(def);
    let state = within(handle.wait()).await;
    assert_eq!(state, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn sequential_second_execution_does_not_mix_store() {
    let store = MemoryStore::new();
    let def = || {
        WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap()
    };
    let rt = Runtime::builder()
        .store(store.clone())
        .register(FunctionExecutor::new("a", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        }))
        .build();
    let h1 = rt.start(def());
    let s1 = within(h1.wait()).await;
    let h2 = rt.start(def());
    let id2 = {
        // inspect before wait
        let h2 = h2;
        let snap = h2.inspect().await;
        let id = snap.execution_id.clone();
        assert_eq!(within(h2.wait()).await, ExecutionState::Succeeded);
        id
    };
    assert_eq!(s1, ExecutionState::Succeeded);
    let stored = store.get(&id2).await.unwrap().expect("second snapshot");
    assert_eq!(stored.state, ExecutionState::Succeeded);
}

// --- Store / inspect -------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn failing_store_every_put_diamond_still_succeeds() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("a", ok("a"))
            .node("b", ok("b"))
            .node("c", ok("c"))
            .node("d", ok("d"))
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .store(FailingStore::fail_all())
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn inspect_two_hung_running() {
    let run = WorkflowTest::new()
        .concurrency(2)
        .node("b", hang("b"))
        .node("c", hang("c"))
        .start()
        .await;
    within(async {
        loop {
            if matches!(run.state("b").await, NodeState::Running { .. })
                && matches!(run.state("c").await, NodeState::Running { .. })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Running);
    assert!(matches!(
        snap.node(&NodeId::new("b")).unwrap().state,
        NodeState::Running { .. }
    ));
    assert!(matches!(
        snap.node(&NodeId::new("c")).unwrap().state,
        NodeState::Running { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn noop_store_diamond_succeeds() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("a", ok("a"))
            .node("b", ok("b"))
            .node("c", ok("c"))
            .node("d", ok("d"))
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .store(keel_rt::NoopStore)
            .run(),
    )
    .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

// --- Research diamond smoke ------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn research_diamond_50_times_event_order() {
    for i in 0..50 {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ev = events.clone();
        let sink = FnSink(move |e: &DomainEvent| ev.lock().unwrap().push(e.clone()));
        let def = WorkflowDefinition::builder("research")
            .node("research", "research")
            .node("summarizer", "summarizer")
            .node("critic", "critic")
            .node("writer", "writer")
            .edge("research", "summarizer")
            .edge("research", "critic")
            .edge("summarizer", "writer")
            .edge("critic", "writer")
            .build()
            .unwrap();
        let payload = move |name: &'static str| {
            FunctionExecutor::new(name, move |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(Bytes::from(format!("{name}-{}", ctx.attempt)))
            })
        };
        let rt = Runtime::builder()
            .concurrency(2)
            .sink(sink)
            .register(payload("research"))
            .register(payload("summarizer"))
            .register(payload("critic"))
            .register(FunctionExecutor::new("writer", |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(Bytes::from(format!("w-{}", ctx.inputs.len())))
            }))
            .build();
        let handle = rt.start(def);
        let snap = handle.inspect().await;
        let state = within(handle.wait()).await;
        assert_eq!(state, ExecutionState::Succeeded, "iter {i}");
        let evs = events.lock().unwrap().clone();
        let writer_start = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeStarted { node_id, .. } if node_id.as_str() == "writer")
        });
        let sum_ok = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeSucceeded { node_id } if node_id.as_str() == "summarizer")
        });
        let crit_ok = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeSucceeded { node_id } if node_id.as_str() == "critic")
        });
        let ws = writer_start.expect("writer started");
        assert!(sum_ok.unwrap() < ws, "writer started before summarizer succeeded");
        assert!(crit_ok.unwrap() < ws, "writer started before critic succeeded");
        let _ = snap;
    }
}

// --- Size -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn size_wide_fan_out_256() {
    let n = 256usize;
    let mut test = WorkflowTest::new()
        .concurrency(32)
        .node("src", ok("src"))
        .node("join", ok("join"));
    for i in 0..n {
        let id = format!("w{i}");
        test = test.node(&id, ok(&id)).edge("src", &id).edge(&id, "join");
    }
    let run = within(test.run()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("join").await.len(), n);
}

#[tokio::test(flavor = "current_thread")]
async fn size_deep_chain_128() {
    let n = 128usize;
    let mut test = WorkflowTest::new().node("n0", ok("n0"));
    for i in 1..n {
        let prev = format!("n{}", i - 1);
        let id = format!("n{i}");
        test = test.node(&id, ok(&id)).edge(&prev, &id);
    }
    let run = within(test.run()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs(&format!("n{}", n - 1)).await.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn size_cancel_under_load_64() {
    let mut test = WorkflowTest::new()
        .concurrency(16)
        .cancel_bound(DEFAULT_CANCEL_BOUND);
    for i in 0..64 {
        test = test.node(&format!("h{i}"), hang(&format!("h{i}")));
    }
    let run = test.start().await;
    within(async {
        loop {
            if running_count(&run.snapshot().await) >= 16 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    run.cancel().await;
    tokio::time::timeout(DEFAULT_CANCEL_BOUND + Duration::from_millis(200), run.wait_stable())
        .await
        .expect("cancel under load");
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}
