//! Two-process SDK loop in one binary.
//!
//! The engine owns executors; this client never registers them.
//!
//! **Engine** (this process): registers `research` and `write`. Builtin
//! `wait` needs no register. Then `serve_ephemeral` on `127.0.0.1`.
//!
//! **Client** (`KeelClient`): `GET /executors`, then sends only the
//! definition (`durable_bytes`). A node whose `executor_id` is missing
//! is **400** with that id (`client_start_unregistered_is_400_nothing_runs`).
//!
//! ```text
//! cargo run -p keel-rt-http --example sdk_loop
//! ```
//!
//! After approve, inspect `write` is [`InspectNodeState::Succeeded`] with
//! the join bytes. Waiting is still the only variant with a token.

use bytes::Bytes;
use keel_rt::{ExecutionContext, ExecutionState, NodeId, NodeOutcome, Runtime, WorkflowDefinition};
use keel_rt_http::{
    serve_ephemeral, CompleteSecret, InspectNodeState, InspectView, KeelClient, KeelClientError,
};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SECRET: &str = "sdk-loop-secret";
const BOUND: Duration = Duration::from_secs(5);

fn dag() -> WorkflowDefinition {
    WorkflowDefinition::builder("research-hold-write")
        .node("research", "research")
        .node("hold", "wait")
        .node("write", "write")
        .edge("research", "hold")
        .edge("hold", "write")
        .build()
        .expect("definition")
}

fn print_inspect(label: &str, view: &InspectView) {
    println!("{label}  id={} state={:?}", view.execution_id, view.state);
    for n in &view.nodes {
        let token = matches!(n.state, InspectNodeState::Waiting { .. });
        println!(
            "         node={} state={:?} wait_token={token}",
            n.id, n.state
        );
        if token != matches!(n.state, InspectNodeState::Waiting { .. }) {
            unreachable!("token flag follows Waiting only");
        }
    }
}

async fn inspect_until<F>(
    client: &KeelClient,
    id: &keel_rt::ExecutionId,
    mut ready: F,
) -> InspectView
where
    F: FnMut(&InspectView) -> bool,
{
    tokio::time::timeout(BOUND, async {
        loop {
            let view = client.inspect(id).await.expect("inspect");
            if ready(&view) {
                return view;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inspect timed out")
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let writes = Arc::new(AtomicU32::new(0));
    let last_write = Arc::new(Mutex::new(Bytes::new()));
    let w = writes.clone();
    let last = last_write.clone();

    // Engine process: executors live here. Client never sends them.
    let runtime = Arc::new(
        Runtime::builder()
            .register_fn("research", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"notes"))
            })
            .register_fn("write", move |ctx: ExecutionContext| {
                let w = w.clone();
                let last = last.clone();
                async move {
                    w.fetch_add(1, Ordering::SeqCst);
                    let out = ctx
                        .inputs
                        .get(&NodeId::new("hold"))
                        .cloned()
                        .unwrap_or_default();
                    *last.lock().expect("write bytes") = out.clone();
                    NodeOutcome::Succeeded(out)
                }
            })
            .build(),
    );

    let secret = CompleteSecret::new(SECRET).expect("secret");
    let (addr, server) = serve_ephemeral(runtime, secret.clone())
        .await
        .expect("bind 127.0.0.1");
    println!("engine   http://{addr}  (research + write registered; wait is builtin)");

    let client = KeelClient::new(format!("http://{addr}"), secret).expect("client");
    let catalog = match client.executors().await {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("catalog failed: {e}");
            server.abort();
            return ExitCode::FAILURE;
        }
    };
    println!(
        "catalog  {}",
        catalog
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if !catalog.iter().any(|id| id.as_str() == "research") {
        eprintln!("catalog missing research after engine register_fn");
        server.abort();
        return ExitCode::FAILURE;
    }

    let def = dag();
    println!(
        "client   start body is durable_bytes ({} B) — not a snapshot",
        def.durable_bytes().len()
    );

    // Unregistered executor: client can name it, engine never had it.
    match client
        .start(
            WorkflowDefinition::builder("missing")
                .node("x", "not-on-this-engine")
                .build()
                .unwrap(),
        )
        .await
    {
        Err(e @ KeelClientError::Unregistered { .. }) => {
            let text = e.to_string();
            if !text.contains("not-on-this-engine") {
                eprintln!("400 must name not-on-this-engine, got {text}");
                server.abort();
                return ExitCode::FAILURE;
            }
            println!("unreg    {text}");
        }
        other => {
            eprintln!("expected 400 unregistered with id, got {other:?}");
            server.abort();
            return ExitCode::FAILURE;
        }
    }

    // Run 1: start → inspect wait token → approve → write succeeds.
    let id = match client.start(def.clone()).await {
        Ok(id) => id,
        Err(e) => {
            eprintln!("start failed: {e}");
            server.abort();
            return ExitCode::FAILURE;
        }
    };
    println!("start    {id}");

    let parked = inspect_until(&client, &id, |v| {
        v.state == ExecutionState::Waiting
            && matches!(
                v.node(&NodeId::new("hold")).map(|n| &n.state),
                Some(InspectNodeState::Waiting { .. })
            )
    })
    .await;
    print_inspect("inspect", &parked);
    let parked_wire = serde_json::to_string(&parked).expect("json");
    println!("wire     {parked_wire}");
    match parked.node(&NodeId::new("research")).map(|n| &n.state) {
        Some(InspectNodeState::Succeeded { output }) if output.as_ref() == b"notes" => {}
        other => {
            eprintln!("research inspect must be Succeeded(notes), got {other:?}");
            server.abort();
            return ExitCode::FAILURE;
        }
    }
    let hold_json = serde_json::to_value(
        parked
            .node(&NodeId::new("hold"))
            .map(|n| &n.state)
            .expect("hold"),
    )
    .expect("hold json");
    if hold_json.get("output").is_some() {
        eprintln!("Waiting inspect JSON must not own output: {hold_json}");
        server.abort();
        return ExitCode::FAILURE;
    }
    for n in &parked.nodes {
        match &n.state {
            InspectNodeState::Waiting { .. } => {}
            _ => {
                if parked.resume_token(&n.id).is_some() {
                    eprintln!("token leaked on non-Waiting {}", n.id);
                    server.abort();
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    let token = parked
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token only on hold Waiting");
    println!(
        "token    exec={} node={} attempt={}",
        token.execution_id(),
        token.node_id(),
        token.attempt()
    );

    if let Err(e) = client.approve(token, Bytes::from_static(b"human-ok")).await {
        eprintln!("approve failed: {e}");
        server.abort();
        return ExitCode::FAILURE;
    }
    println!("approve  Decision::Complete(human-ok) via POST /approve");

    let done = inspect_until(&client, &id, |v| v.state == ExecutionState::Succeeded).await;
    print_inspect("done    ", &done);
    let done_wire = serde_json::to_string(&done).expect("done json");
    println!("inspect  {done_wire}");
    match done.node(&NodeId::new("write")).map(|n| &n.state) {
        Some(InspectNodeState::Succeeded { output }) if output.as_ref() == b"human-ok" => {}
        other => {
            eprintln!("write inspect must be Succeeded(human-ok), got {other:?}");
            server.abort();
            return ExitCode::FAILURE;
        }
    }
    let wrote = writes.load(Ordering::SeqCst);
    let payload = last_write.lock().expect("write").clone();
    if done.state != ExecutionState::Succeeded || wrote != 1 {
        eprintln!(
            "write did not finish: writes={wrote} state={:?}",
            done.state
        );
        server.abort();
        return ExitCode::FAILURE;
    }
    if payload.as_ref() != b"human-ok" {
        eprintln!("write join input was {:?}, expected human-ok", payload);
        server.abort();
        return ExitCode::FAILURE;
    }
    println!("write    Succeeded output human-ok (inspect + engine join)");

    // Run 2: start → cancel while waiting → later approve is 409.
    let id2 = client.start(def).await.expect("second start");
    println!("start    {id2}");
    let parked2 = inspect_until(&client, &id2, |v| {
        v.state == ExecutionState::Waiting
            && matches!(
                v.node(&NodeId::new("hold")).map(|n| &n.state),
                Some(InspectNodeState::Waiting { .. })
            )
    })
    .await;
    let token2 = parked2
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("token");
    client.cancel(&id2).await.expect("cancel one id");
    let cancelled = inspect_until(&client, &id2, |v| v.state == ExecutionState::Cancelled).await;
    print_inspect("cancel  ", &cancelled);
    match client
        .approve(token2, Bytes::from_static(b"too-late"))
        .await
    {
        Err(KeelClientError::Cancelled) => {
            println!("approve  after cancel → 409 (does not revive)")
        }
        other => {
            eprintln!("expected 409 Cancelled, got {other:?}");
            server.abort();
            return ExitCode::FAILURE;
        }
    }
    if writes.load(Ordering::SeqCst) != 1 {
        eprintln!("cancel must not run write a second time");
        server.abort();
        return ExitCode::FAILURE;
    }

    server.abort();
    println!("ok");
    ExitCode::SUCCESS
}
