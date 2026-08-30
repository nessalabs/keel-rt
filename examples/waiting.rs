//! One node yields Waiting; a sibling runs at concurrency=1; resume Complete
//! so the AND-join continues.

use bytes::Bytes;
use keel_rt::{
    DomainEvent, ExecutionContext, ExecutionState, FnSink, FunctionExecutor, NodeId, NodeOutcome,
    NodeState, Resume, Runtime, WorkflowDefinition,
};
use std::process::ExitCode;

fn wait_once(
    id: &'static str,
) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync + 'static>
{
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        println!("  ran     {} attempt={} → Waiting", ctx.node_id, ctx.attempt);
        std::future::ready(NodeOutcome::Waiting {
            token: ctx.resume_token,
        })
    })
}

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

#[tokio::main]
async fn main() -> ExitCode {
    let def = WorkflowDefinition::builder("waiting")
        .node("wait", "wait")
        .node("sibling", "sibling")
        .node("join", "join")
        .edge("wait", "join")
        .edge("sibling", "join")
        .build()
        .expect("definition");

    let sink = FnSink(|event: &DomainEvent| println!("event    {event}"));

    let runtime = Runtime::builder()
        .concurrency(1)
        .sink(sink)
        .register(wait_once("wait"))
        .register(succeed("sibling", b"sibling-out"))
        .register(succeed("join", b"joined"))
        .build();

    let handle = runtime.start(def);
    let stable = handle.wait_stable().await;
    println!("stable   {stable:?}");

    let snap = handle.inspect().await;
    let wait_state = snap.node(&NodeId::new("wait")).map(|n| n.state.clone());
    let sibling = snap.node(&NodeId::new("sibling")).map(|n| n.state.clone());
    println!("state    wait={wait_state:?} sibling={sibling:?}");

    if !matches!(wait_state, Some(NodeState::Waiting { .. }))
        || !matches!(sibling, Some(NodeState::Succeeded))
        || stable != ExecutionState::Waiting
    {
        eprintln!("expected wait=Waiting, sibling=Succeeded, execution=Waiting");
        return ExitCode::FAILURE;
    }

    let token = snap
        .node(&NodeId::new("wait"))
        .and_then(|n| n.resume_token.clone())
        .expect("resume token");
    handle
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"wait-out"))),
        )
        .await
        .expect("resume");

    let state = handle.wait().await;
    println!("final    {state:?}");
    if state == ExecutionState::Succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
