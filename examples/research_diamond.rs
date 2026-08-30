//! North-star diamond: Research → (Summarizer ∥ Critic) → Writer.
//!
//! Library-user surface only: `Runtime`, `WorkflowDefinition`, `FunctionExecutor`, `FnSink`.

use bytes::Bytes;
use keel_rt::{
    DomainEvent, ExecutionContext, ExecutionState, FnSink, FunctionExecutor, NodeId, NodeOutcome,
    Runtime, WorkflowDefinition,
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

fn exec(id: &'static str, body: &'static str) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync + 'static> {
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        let out = NodeOutcome::Succeeded(payload(&ctx, body));
        std::future::ready(out)
    })
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
        .register(exec("research", "gathered notes on keel-rt"))
        .register(exec("summarizer", "one-page digest"))
        .register(exec("critic", "risks and gaps"))
        .register(exec("writer", "combined draft"))
        .build();

    let handle = runtime.start(def);
    let state = handle.wait().await;
    println!("final    {state:?}");
    if state == ExecutionState::Succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
