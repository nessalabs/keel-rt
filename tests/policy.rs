use bytes::Bytes;
use std::time::Duration;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{AcceptPolicy, ExecutionState, NeverWaitPolicy, NodeState, RetryPolicy};

#[tokio::test(flavor = "current_thread")]
async fn retry_policy_max_3_fail_twice_then_succeed() {
    let run = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("1")
                .fail("2")
                .succeed(Bytes::from_static(b"ok")),
        )
        .policy(RetryPolicy::new(3, Duration::ZERO))
        .run()
        .await;

    assert_eq!(run.scripted("a").attempts(), vec![1, 2, 3]);
    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn retry_policy_max_2_always_fail() {
    let run = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a").fail("1").fail("2").fail("3"),
        )
        .policy(RetryPolicy::new(2, Duration::ZERO))
        .run()
        .await;

    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert!(matches!(run.state("a").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn retry_delay_is_ready_with_runnable_at_not_waiting() {
    // Frozen rule: retry delay is Ready { runnable_at }, NOT Waiting.
    let test = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("1")
                .fail("2")
                .succeed(Bytes::from_static(b"ok")),
        )
        .policy(RetryPolicy::new(3, Duration::from_millis(100)));
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(Duration::from_secs(2), async {
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
    .expect("node becomes Ready with runnable_at");

    assert!(
        !matches!(run.state("a").await, NodeState::Waiting { .. }),
        "retry delay is Ready, not Waiting"
    );
    assert_eq!(run.execution_state().await, ExecutionState::Running);

    clock.advance(Duration::from_millis(100));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match run.state("a").await {
                NodeState::Ready {
                    runnable_at: Some(_),
                } => break,
                NodeState::Failed => break,
                NodeState::Succeeded => break,
                NodeState::Running { attempt } if attempt >= 2 => break,
                NodeState::Waiting { .. } => panic!("retry delay must not be Waiting"),
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .ok();

    clock.advance(Duration::from_millis(100));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if run.scripted("a").attempts().len() >= 3
                || matches!(run.execution_state().await, ExecutionState::Succeeded)
            {
                break;
            }
            clock.advance(Duration::from_millis(50));
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("third attempt runs after advancing the fake clock");

    run.wait_stable().await;
    assert_eq!(run.scripted("a").attempts().len(), 3);
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn accept_policy_on_fail_no_retry() {
    let run = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("nope")
                .succeed(Bytes::from_static(b"unused")),
        )
        .policy(AcceptPolicy)
        .run()
        .await;

    assert_eq!(run.scripted("a").attempts(), vec![1]);
    assert!(matches!(run.state("a").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn never_wait_policy_rejects_waiting() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").wait())
        .policy(NeverWaitPolicy)
        .run()
        .await;

    assert!(matches!(run.state("a").await, NodeState::Failed));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
}
