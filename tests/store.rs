use bytes::Bytes;
use std::sync::Arc;
use workflow_kernel::testing::{FailingStore, ScriptedExecutor, WorkflowTest};
use workflow_kernel::{ExecutionState, MemoryStore, NodeState, NoopStore, StateStore};

fn diamond(store: impl StateStore + 'static) -> WorkflowTest {
    WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"A")))
        .node("b", ScriptedExecutor::new("b").succeed(Bytes::from_static(b"B")))
        .node("c", ScriptedExecutor::new("c").succeed(Bytes::from_static(b"C")))
        .node("d", ScriptedExecutor::new("d").succeed(Bytes::from_static(b"D")))
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .concurrency(2)
        .store(store)
}

#[tokio::test(flavor = "current_thread")]
async fn diamond_passes_with_noop_store() {
    let run = diamond(NoopStore).run().await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn diamond_passes_with_memory_store() {
    let run = diamond(MemoryStore::new()).run().await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_round_trip_waiting_has_token() {
    let store = Arc::new(MemoryStore::new());
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").wait())
        .store_arc(store.clone())
        .run()
        .await;

    assert!(matches!(run.state("a").await, NodeState::Waiting { .. }));
    let live = run.snapshot().await;
    let stored = store
        .get(&live.execution_id)
        .await
        .unwrap()
        .expect("MemoryStore must retain the snapshot");
    let node = stored.node(&workflow_kernel::NodeId::new("a")).unwrap();
    assert!(matches!(node.state, NodeState::Waiting { .. }));
    assert!(node.resume_token.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn failing_store_put_does_not_roll_back_in_memory() {
    let store = FailingStore::fail_on_nth_put(1);
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"ok")))
        .store(store)
        .run()
        .await;

    assert_eq!(
        run.execution_state().await,
        ExecutionState::Succeeded,
        "put error must not roll back in-memory apply"
    );
    assert!(matches!(run.state("a").await, NodeState::Succeeded));
}
