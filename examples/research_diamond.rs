//! North-star diamond: Research → (Summarizer ∥ Critic) → Writer.
//!
//! Consumer path: `register_fn` + `Runtime::run` (no handle to drop-cancel).

use bytes::Bytes;
use keel_rt::{
    DomainEvent, ExecutionContext, FnSink, NodeId, NodeOutcome, Runtime, WorkflowDefinition,
};
use std::process::ExitCode;

fn payload(ctx: &ExecutionContext, body: &str) -> Bytes {
    let mut line = format!("{}: {body}", ctx.node_id);
    if !ctx.inputs.is_empty() {
        line.push_str(" [from");
        let mut keys: Vec<_> = ctx.inputs.keys().map(NodeId::as_str).collect();
        keys.sort_unstable();
        for k in keys {
            line.push(' ');
            line.push_str(k);
        }
        line.push(']');
    }
    println!("  payload  {line}");
    Bytes::from(line)
}

#[tokio::main]
async fn main() -> ExitCode {
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
        .expect("diamond definition");

    let sink = FnSink(|event: &DomainEvent| {
        println!("event    {event}");
    });

    let runtime = Runtime::builder()
        .concurrency(2)
        .sink(sink)
        .register_fn("research", |ctx: ExecutionContext| async move {
            NodeOutcome::Succeeded(payload(&ctx, "gathered notes on keel-rt"))
        })
        .register_fn("summarizer", |ctx: ExecutionContext| async move {
            NodeOutcome::Succeeded(payload(&ctx, "one-page digest"))
        })
        .register_fn("critic", |ctx: ExecutionContext| async move {
            NodeOutcome::Succeeded(payload(&ctx, "risks and gaps"))
        })
        .register_fn("writer", |ctx: ExecutionContext| async move {
            NodeOutcome::Succeeded(payload(&ctx, "combined draft"))
        })
        .build();

    let state = runtime.run(def).await.expect("executors registered");
    println!("final    {state:?}");
    if state.is_successful_finish() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
