//! Policy panic, sink panic, executor panic (isolation).

use super::common::{ok, within};
use bytes::Bytes;
use keel_rt::Policy;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    Event, ExecutionState, FnSink, FunctionExecutor, NodeOutcome, NodeState, Runtime,
    WorkflowDefinition,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    let run = within(WorkflowTest::new().node("a", ok("a")).policy(PanicPolicy).run()).await;
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
    let sink = FnSink(move |_e: &Event| {
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
    let handle = rt.start(def).expect("start");
    let state = within(handle.wait()).await;
    assert_eq!(state, ExecutionState::Succeeded);
    assert!(hits.load(Ordering::SeqCst) >= 1);
}
