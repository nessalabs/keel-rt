//! Research diamond ×50 event order.

use super::common::within;
use bytes::Bytes;
use keel_rt::{
    DomainEvent, ExecutionContext, ExecutionState, FnSink, FunctionExecutor, NodeOutcome, Runtime,
    WorkflowDefinition,
};
use std::sync::Arc;

#[tokio::test(flavor = "current_thread")]
async fn research_diamond_50_times_event_order() {
    for i in 0..50 {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ev = events.clone();
        let sink = FnSink(move |e: &DomainEvent| ev.lock().unwrap().push(e.clone()));
        let def = WorkflowDefinition::builder("research")
            .node("research", "research")
            .node("summarizer", "summarizer")
            .node("critic", "critic")
            .node("writer", "writer")
            .edge("research", "summarizer")
            .edge("research", "critic")
            .edge("summarizer", "writer")
            .edge("critic", "writer")
            .build()
            .unwrap();
        let payload = move |name: &'static str| {
            FunctionExecutor::new(name, move |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(Bytes::from(format!("{name}-{}", ctx.attempt)))
            })
        };
        let rt = Runtime::builder()
            .concurrency(2)
            .sink(sink)
            .register(payload("research"))
            .register(payload("summarizer"))
            .register(payload("critic"))
            .register(FunctionExecutor::new("writer", |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(Bytes::from(format!("w-{}", ctx.inputs.len())))
            }))
            .build();
        let handle = rt.start(def).expect("start");
        let snap = handle.inspect().await;
        let state = within(handle.wait()).await;
        assert_eq!(state, ExecutionState::Succeeded, "iter {i}");
        let evs = events.lock().unwrap().clone();
        let writer_start = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeStarted { node_id, .. } if node_id.as_str() == "writer")
        });
        let sum_ok = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeSucceeded { node_id } if node_id.as_str() == "summarizer")
        });
        let crit_ok = evs.iter().position(|e| {
            matches!(e, DomainEvent::NodeSucceeded { node_id } if node_id.as_str() == "critic")
        });
        let ws = writer_start.expect("writer started");
        assert!(sum_ok.unwrap() < ws, "writer started before summarizer succeeded");
        assert!(crit_ok.unwrap() < ws, "writer started before critic succeeded");
        let _ = snap;
    }
}
