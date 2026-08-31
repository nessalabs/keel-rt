use std::time::Duration;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{ExecutionHandle, ExecutionState, NodeState, DEFAULT_CANCEL_BOUND};

#[tokio::test(flavor = "current_thread")]
async fn cancel_mid_run_running_sees_token_pending_never_starts() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").hang(false))
        .node("b", ScriptedExecutor::new("b").succeed(bytes::Bytes::from_static(b"B")))
        .edge("a", "b")
        .concurrency(1)
        .start()
        .await;

    tokio::time::timeout(Duration::from_secs(2), run.scripted("a").wait_until_hanging())
        .await
        .expect("A should hang");
    assert!(matches!(run.state("a").await, NodeState::Running { .. }));
    assert!(matches!(run.state("b").await, NodeState::Pending));

    run.cancel().await;
    run.wait_stable().await;

    assert!(matches!(run.state("a").await, NodeState::Cancelled));
    assert!(matches!(run.state("b").await, NodeState::Cancelled));
    assert!(run.scripted("b").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn hang_ignore_cancel_ends_within_documented_bound() {
    // Documented bound: DEFAULT_CANCEL_BOUND (50ms). Scheduler marks Cancelled
    // and aborts leftover execute tasks. Test uses a timeout, not an infinite wait.
    let run = WorkflowTest::new()
        .node("h", ScriptedExecutor::new("h").hang(true))
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .start()
        .await;

    tokio::time::timeout(Duration::from_secs(2), run.scripted("h").wait_until_hanging())
        .await
        .expect("hang started");

    let wait = async {
        run.cancel().await;
        run.wait_stable().await;
        run.execution_state().await
    };

    let state = tokio::time::timeout(DEFAULT_CANCEL_BOUND + Duration::from_millis(200), wait)
        .await
        .expect("ignore_cancel hang must still end within cancel bound");
    assert_eq!(state, ExecutionState::Cancelled);
    assert!(matches!(run.state("h").await, NodeState::Cancelled));
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_execution_handle_cancels_graph_not_detach() {
    let mut run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").hang(false))
        .node("b", ScriptedExecutor::new("b").succeed(bytes::Bytes::from_static(b"B")))
        .edge("a", "b")
        .start()
        .await;

    tokio::time::timeout(Duration::from_secs(2), run.scripted("a").wait_until_hanging())
        .await
        .expect("A hanging");

    run.drop_handle();

    tokio::time::timeout(Duration::from_secs(2), async {
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
    .expect("drop cancels; graph does not keep running detached");
}

/// ExecutionHandle is deliberately not Clone. Unique owner: Drop cancels.
#[tokio::test(flavor = "current_thread")]
async fn drop_handle_cancels_unique_owner() {
    let mut run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").hang(false))
        .start()
        .await;
    tokio::time::timeout(Duration::from_secs(2), run.scripted("a").wait_until_hanging())
        .await
        .expect("A hanging");
    run.drop_handle();
    tokio::time::timeout(Duration::from_secs(2), async {
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
    .expect("unique owner drop cancels");
    let _ = std::any::type_name::<ExecutionHandle>();
}

#[tokio::test(flavor = "current_thread")]
async fn hang_ignore_cancel_ends_within_bound() {
    let run = WorkflowTest::new()
        .node("h", ScriptedExecutor::new("h").hang(true))
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .start()
        .await;
    tokio::time::timeout(Duration::from_secs(2), run.scripted("h").wait_until_hanging())
        .await
        .expect("hang started");
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
        .node("a", ScriptedExecutor::new("a").hang(false))
        .node(
            "b",
            ScriptedExecutor::new("b").succeed(bytes::Bytes::from_static(b"B")),
        )
        .start()
        .await;
    tokio::time::timeout(Duration::from_secs(2), run.scripted("a").wait_until_hanging())
        .await
        .expect("A hanging");
    run.cancel().await;
    run.wait_stable().await;
    assert!(run.scripted("b").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}
