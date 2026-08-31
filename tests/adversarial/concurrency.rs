//! Burst cap, concurrency(0), Waiting releases permit, eight waits.

use super::common::{hang, ok, running_count, within};
use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    ExecutionContext, ExecutionState, FunctionExecutor, NodeId, NodeOutcome, NodeState,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    let run = within(WorkflowTest::new().concurrency(0).node("a", ok("a")).run()).await;
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
    let _ = hang("unused");
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
        keel_rt::Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"w0"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    let snap = run.snapshot().await;
    let running = running_count(&snap);
    assert!(
        running <= 2,
        "resume Complete must not start 3 at once, running={running}"
    );
    assert!(matches!(
        snap.node(&NodeId::new("w0")).unwrap().state,
        NodeState::Succeeded
    ));
}
