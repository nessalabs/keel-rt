//! A → {B, C} → D. B fails; D must never start (Cancelled). Execution Failed.

use bytes::Bytes;
use keel_rt::{
    DomainEvent, ExecutionContext, ExecutionState, FnSink, FunctionExecutor, NodeId, NodeOutcome,
    NodeState, Runtime, WorkflowDefinition,
};
use std::process::ExitCode;

fn succeed(
    id: &'static str,
    body: &'static [u8],
) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync + 'static>
{
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        println!("  ran     {}", ctx.node_id);
        std::future::ready(NodeOutcome::Succeeded(Bytes::from_static(body)))
    })
}

fn fail(
    id: &'static str,
) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync + 'static>
{
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        println!("  ran     {} (fail)", ctx.node_id);
        std::future::ready(NodeOutcome::failed("B exploded"))
    })
}

fn must_not_run(
    id: &'static str,
) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync + 'static>
{
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        eprintln!("  ran     {} — D must never start", ctx.node_id);
        std::future::ready(NodeOutcome::Succeeded(Bytes::from_static(b"D")))
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let def = WorkflowDefinition::builder("fail-fast")
        .node("a", "a")
        .node("b", "b")
        .node("c", "c")
        .node("d", "d")
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .build()
        .expect("definition");

    let sink = FnSink(|event: &DomainEvent| println!("event    {event}"));

    let runtime = Runtime::builder()
        .concurrency(2)
        .sink(sink)
        .register(succeed("a", b"A"))
        .register(fail("b"))
        .register(succeed("c", b"C"))
        .register(must_not_run("d"))
        .build();

    let handle = runtime.start(def).expect("executors registered");
    handle.wait_stable().await;
    let snap = handle.inspect().await;

    for name in ["a", "b", "c", "d"] {
        let st = snap
            .node(&NodeId::new(name))
            .map(|n| n.state.clone())
            .unwrap_or(NodeState::Pending);
        println!("state    {name}={st:?}");
    }
    println!("final    execution={:?}", snap.state);

    let d = snap.node(&NodeId::new("d")).map(|n| &n.state);
    if matches!(d, Some(NodeState::Cancelled)) && snap.state == ExecutionState::Failed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
