//! Equal and staggered retry deadlines, cancel future runnable_at, timer after Succeeded.

use super::common::{ok, within};
use bytes::Bytes;
use keel_rt::{ApplyCmd, Execution};
use keel_rt::testing::{ScriptedAction, ScriptedExecutor, WorkflowTest};
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

/// Staggered fail times → different `runnable_at`. The due timer must leave
/// the later deadline armed (`at > now` break) instead of firing it early.
#[tokio::test(flavor = "current_thread")]
async fn two_nodes_staggered_retry_deadlines_both_run() {
    for i in 0..16 {
        two_nodes_staggered_retry_deadlines_once(i).await;
    }
}

async fn two_nodes_staggered_retry_deadlines_once(iter: u32) {
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
                .then(ScriptedAction::Delay {
                    delay: Duration::from_millis(50),
                    then: Box::new(ScriptedAction::Fail("slow".into())),
                })
                .succeed(Bytes::from_static(b"sok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(100)));
    let clock = test.fake_clock();
    let run = test.start().await;
    within(async {
        loop {
            if matches!(
                run.state("fast").await,
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
    match run.state("fast").await {
        NodeState::Ready {
            runnable_at: Some(at),
        } => assert_eq!(at, keel_rt::Timestamp::from_millis(100)),
        other => panic!("iter {iter}: fast retry deadline must be t=100, got {other:?}"),
    }
    clock.advance(Duration::from_millis(50));
    within(async {
        loop {
            if matches!(
                run.state("slow").await,
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
    match run.state("fast").await {
        NodeState::Ready {
            runnable_at: Some(at),
        } => assert_eq!(at, keel_rt::Timestamp::from_millis(100)),
        other => panic!("iter {iter}: fast must still be waiting at t=100, got {other:?}"),
    }
    match run.state("slow").await {
        NodeState::Ready {
            runnable_at: Some(at),
        } => assert_eq!(at, keel_rt::Timestamp::from_millis(150)),
        other => panic!("iter {iter}: slow retry deadline must be t=150, got {other:?}"),
    }
    clock.advance(Duration::from_millis(50));
    within(async {
        loop {
            if matches!(run.state("fast").await, NodeState::Succeeded) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    match run.state("slow").await {
        NodeState::Ready {
            runnable_at: Some(at),
        } => assert_eq!(
            at,
            keel_rt::Timestamp::from_millis(150),
            "iter {iter}: slow's later deadline must not fire with fast's timer"
        ),
        other => panic!("iter {iter}: slow must still be Ready at t=150 after fast succeeds, got {other:?}"),
    }
    clock.advance(Duration::from_millis(50));
    within(run.wait_stable()).await;
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Succeeded,
        "iter {iter}"
    );
    assert_eq!(run.scripted("fast").attempts(), vec![1, 2], "iter {iter}");
    assert_eq!(run.scripted("slow").attempts(), vec![1, 2], "iter {iter}");
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
