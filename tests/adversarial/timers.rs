//! Equal deadlines both fire, cancel future runnable_at, timer after Succeeded.

use super::common::{ok, within};
use bytes::Bytes;
use keel_rt::domain::state::{ApplyCmd, Execution};
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ExecutionState, NodeId, NodeOutcome, NodeState, RetryPolicy, WorkflowDefinition,
};
use std::time::Duration;

fn linear_exec() -> Execution {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    Execution::new(def)
}

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
    let _ = ok("unused");
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
