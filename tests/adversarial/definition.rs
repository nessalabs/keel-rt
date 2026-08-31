//! Self-edge, duplicate edge, empty NodeId, unregistered executor, sequential executions.

use super::common::{ok, within};
use bytes::Bytes;
use keel_rt::{
    ExecutionState, FunctionExecutor, MemoryStore, NodeOutcome, Runtime, StateStore,
    WorkflowDefinition,
};

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
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .node("b", "b")
        .edge("a", "b")
        .edge("a", "b")
        .build();
    assert!(def.is_ok(), "duplicate edge accepted or we treat as one pred");
    let run = within(
        keel_rt::testing::WorkflowTest::new()
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
