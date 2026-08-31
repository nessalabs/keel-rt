//! Inspect two Running, FailingStore, NoopStore.

use super::common::{hang, ok, within};
use keel_rt::testing::{FailingStore, WorkflowTest};
use keel_rt::{ExecutionState, NodeId, NodeState};

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
