use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{DomainEvent, ExecutionState, NodeState};

#[tokio::test(flavor = "current_thread")]
async fn b_fails_d_depends_on_b_cancelled_never_started() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"A")))
        .node("b", ScriptedExecutor::new("b").fail("b exploded"))
        .node("c", ScriptedExecutor::new("c").succeed(Bytes::from_static(b"C")))
        .node("d", ScriptedExecutor::new("d").succeed(Bytes::from_static(b"D")))
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .concurrency(2)
        .run()
        .await;

    assert!(matches!(run.state("b").await, NodeState::Failed));
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(run.scripted("d").attempts().is_empty(), "D must never start");
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    let failed_events = run
        .events()
        .iter()
        .filter(|e| matches!(e, DomainEvent::ExecutionFailed { .. }))
        .count();
    assert_eq!(
        failed_events, 1,
        "fail-fast emits one ExecutionFailed, not one per aborted sibling"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn executor_panic_node_failed_scheduler_alive_execution_failed() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"A")))
        .node("boom", ScriptedExecutor::new("boom").panic())
        .node("c", ScriptedExecutor::new("c").succeed(Bytes::from_static(b"C")))
        .edge("a", "boom")
        .edge("boom", "c")
        .run()
        .await;

    assert!(matches!(run.state("boom").await, NodeState::Failed));
    assert!(matches!(run.state("c").await, NodeState::Cancelled));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    // Scheduler stayed alive: we observed a terminal snapshot (not a hang).
    let snap = run.snapshot().await;
    assert!(snap.revision > 0);
}
