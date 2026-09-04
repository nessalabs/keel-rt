//! Custom node types: implement [`Executor`], register on the engine, catalog,
//! start, approve, inspect. FakeClock. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionId, ExecutionState, Executor, ExecutorId, FakeClock, MemoryStore,
    NodeId, NodeOutcome, Resume, ResumeToken, Runtime, RuntimeBuilder, StateStore,
    WorkflowDefinition, WAIT_ID,
};
use keel_rt_http::{
    serve_ephemeral, CompleteSecret, InspectNodeState, InspectView, KeelClient, KeelClientError,
    MAX_BODY,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "test-custom-nodes-secret";

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
}

fn client_at(addr: std::net::SocketAddr) -> KeelClient {
    KeelClient::new(format!("http://{addr}"), secret()).unwrap()
}

fn builder() -> RuntimeBuilder {
    Runtime::builder().clock(Arc::new(FakeClock::new()))
}

async fn bind(rt: Arc<Runtime>) -> (KeelClient, tokio::task::JoinHandle<std::io::Result<()>>) {
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    (client_at(addr), server)
}

async fn inspect_until(
    client: &KeelClient,
    id: &ExecutionId,
    mut ready: impl FnMut(&InspectView) -> bool,
) -> InspectView {
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(id).await {
                if ready(&v) {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inspect")
}

fn dag() -> WorkflowDefinition {
    WorkflowDefinition::builder("research-hold-write")
        .node("research", "research")
        .node("hold", "wait")
        .node("write", "write")
        .edge("research", "hold")
        .edge("hold", "write")
        .build()
        .unwrap()
}

fn one_node(id: &str, executor: &str) -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node(id, executor)
        .build()
        .unwrap()
}

struct Research;

impl Executor for Research {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("research")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"notes")) })
    }
}

struct Write;

impl Executor for Write {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("write")
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async move {
            let out = ctx
                .inputs
                .get(&NodeId::new("hold"))
                .cloned()
                .unwrap_or_default();
            NodeOutcome::Succeeded(out)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn custom_executor_types_catalog_start_approve_inspect() {
    let rt = Arc::new(builder().register(Research).register(Write).build());
    let (client, server) = bind(rt).await;
    let catalog = client.executors().await.expect("catalog");
    let names: Vec<&str> = catalog.iter().map(|id| id.as_str()).collect();
    assert!(names.contains(&"research"), "{names:?}");
    assert!(names.contains(&"write"), "{names:?}");
    assert!(names.contains(&WAIT_ID), "{names:?}");
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "{names:?}");

    let id = client.start(dag()).await.expect("start");
    let parked = inspect_until(&client, &id, |v| {
        v.state == ExecutionState::Waiting
            && matches!(
                v.node(&NodeId::new("hold")).map(|n| &n.state),
                Some(InspectNodeState::Waiting { .. })
            )
    })
    .await;
    assert_eq!(
        parked.node(&NodeId::new("research")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"notes"),
        })
    );
    let token = parked
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token");
    client
        .approve(token, Bytes::from_static(b"human-ok"))
        .await
        .expect("approve");
    let done = inspect_until(&client, &id, |v| v.state == ExecutionState::Succeeded).await;
    assert_eq!(
        done.node(&NodeId::new("write")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"human-ok"),
        })
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_subset_registered_start_is_400_names_missing_only_nothing_runs() {
    let rt = Arc::new(builder().register(Research).build());
    let (client, server) = bind(rt).await;
    let catalog = client.executors().await.expect("catalog");
    assert!(catalog.iter().any(|id| id.as_str() == "research"));
    assert!(!catalog.iter().any(|id| id.as_str() == "write"));
    let err = client.start(dag()).await.unwrap_err();
    match &err {
        KeelClientError::Unregistered { executors } => {
            let names: Vec<&str> = executors.iter().map(|e| e.as_str()).collect();
            assert_eq!(names, vec!["write"], "{names:?}");
        }
        other => panic!("expected Unregistered write-only, got {other:?}"),
    }
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_adapter_failed_inspect_is_failed_not_succeeded() {
    let rt = Arc::new(
        builder()
            .register_fn("boom", |_ctx: ExecutionContext| async {
                NodeOutcome::failed("boom")
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client.start(one_node("x", "boom")).await.expect("start");
    let done = inspect_until(&client, &id, |v| v.state.is_terminal()).await;
    assert_eq!(done.state, ExecutionState::Failed);
    assert!(matches!(
        done.node(&NodeId::new("x")).map(|n| &n.state),
        Some(InspectNodeState::Failed)
    ));
    let json = serde_json::to_value(&done).unwrap();
    let node = json["nodes"].as_array().unwrap().iter().next().unwrap();
    assert!(
        node["state"].get("output").is_none(),
        "Failed must not own output: {node}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_adapter_panic_inspect_is_failed() {
    let rt = Arc::new(
        builder()
            .register_fn("panics", |_ctx: ExecutionContext| async {
                panic!("adapter exploded")
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client.start(one_node("x", "panics")).await.expect("start");
    let done = inspect_until(&client, &id, |v| v.state.is_terminal()).await;
    assert_eq!(done.state, ExecutionState::Failed);
    assert!(matches!(
        done.node(&NodeId::new("x")).map(|n| &n.state),
        Some(InspectNodeState::Failed)
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_cancel_mid_running_is_cancelled() {
    let gate = Arc::new(Notify::new());
    let g = gate.clone();
    let rt = Arc::new(
        builder()
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let g = g.clone();
                async move {
                    g.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"late"))
                }
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client.start(one_node("slow", "slow")).await.expect("start");
    inspect_until(&client, &id, |v| {
        matches!(
            v.node(&NodeId::new("slow")).map(|n| &n.state),
            Some(InspectNodeState::Running { .. })
        )
    })
    .await;
    client.cancel(&id).await.expect("cancel");
    inspect_until(&client, &id, |v| v.state == ExecutionState::Cancelled).await;
    gate.notify_one();
    let after = client.inspect(&id).await.expect("after");
    assert_eq!(after.state, ExecutionState::Cancelled);
    assert!(!matches!(
        after.node(&NodeId::new("slow")).map(|n| &n.state),
        Some(InspectNodeState::Succeeded { .. })
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_drop_http_server_while_running_cancels() {
    let store = MemoryStore::new();
    let gate = Arc::new(Notify::new());
    let g = gate.clone();
    let rt = Arc::new(
        builder()
            .store(store.clone())
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let g = g.clone();
                async move {
                    g.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"late"))
                }
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client.start(one_node("slow", "slow")).await.expect("start");
    inspect_until(&client, &id, |v| {
        matches!(
            v.node(&NodeId::new("slow")).map(|n| &n.state),
            Some(InspectNodeState::Running { .. })
        )
    })
    .await;
    drop(client);
    server.abort();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server drop Drop-cancels a still-running custom node");
    gate.notify_one();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_empty_bytes_output_inspect_succeeded() {
    let rt = Arc::new(
        builder()
            .register_fn("empty", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::new())
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client
        .start(one_node("empty", "empty"))
        .await
        .expect("start");
    let done = inspect_until(&client, &id, |v| v.state == ExecutionState::Succeeded).await;
    assert_eq!(
        done.node(&NodeId::new("empty")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::new(),
        })
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_double_register_last_wins_catalog_and_run() {
    let rt = Arc::new(
        builder()
            .register_fn("tool", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"first"))
            })
            .register_fn("tool", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"second"))
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let catalog = client.executors().await.expect("catalog");
    assert_eq!(
        catalog.iter().filter(|id| id.as_str() == "tool").count(),
        1,
        "{catalog:?}"
    );
    let id = client.start(one_node("n", "tool")).await.expect("start");
    let done = inspect_until(&client, &id, |v| v.state == ExecutionState::Succeeded).await;
    assert_eq!(
        done.node(&NodeId::new("n")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"second"),
        })
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_empty_register_id_is_not_in_catalog() {
    let rt = Arc::new(
        builder()
            .register_fn("", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"nope"))
            })
            .register(Research)
            .build(),
    );
    let (client, server) = bind(rt).await;
    let catalog = client.executors().await.expect("catalog");
    assert!(
        !catalog.iter().any(|id| id.as_str().is_empty()),
        "catalog must not list empty id: {catalog:?}"
    );
    assert!(catalog.iter().any(|id| id.as_str() == "research"));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_id_case_mismatch_start_is_400_names_the_id() {
    let rt = Arc::new(builder().register(Research).build());
    let (client, server) = bind(rt).await;
    let err = client.start(one_node("n", "Research")).await.unwrap_err();
    match &err {
        KeelClientError::Unregistered { executors } => {
            let names: Vec<&str> = executors.iter().map(|e| e.as_str()).collect();
            assert_eq!(names, vec!["Research"], "{names:?}");
        }
        other => panic!("expected Unregistered Research, got {other:?}"),
    }
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_waiting_forged_token_complete_is_404_kernel_unblocks() {
    let rt = Arc::new(
        builder()
            .register_fn("hold", |_ctx: ExecutionContext| async {
                NodeOutcome::Waiting {
                    token: ResumeToken::issue(ExecutionId::new(), NodeId::new("hold"), 99),
                }
            })
            .build(),
    );
    let (client, server) = bind(rt).await;
    let id = client.start(one_node("hold", "hold")).await.expect("start");
    let parked = inspect_until(&client, &id, |v| v.state == ExecutionState::Waiting).await;
    let kernel = parked
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("kernel token");
    let forged = ResumeToken::issue(id.clone(), NodeId::new("hold"), 1);
    match client
        .complete(
            forged,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
    {
        Err(KeelClientError::UnknownToken) => {}
        other => panic!("forged complete must 404 UnknownToken, got {other:?}"),
    }
    client
        .approve(kernel, Bytes::from_static(b"ok"))
        .await
        .expect("kernel token");
    inspect_until(&client, &id, |v| v.state == ExecutionState::Succeeded).await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_failed_huge_last_error_inspect_is_200_omits_error() {
    let huge = "x".repeat(2 * 1024 * 1024);
    let h = huge.clone();
    let rt = Arc::new(
        builder()
            .register_fn("boom", move |_ctx: ExecutionContext| {
                let h = h.clone();
                async move { NodeOutcome::failed(h) }
            })
            .build(),
    );
    let (client, server) = bind(rt.clone()).await;
    let id = client.start(one_node("x", "boom")).await.expect("start");
    let done = inspect_until(&client, &id, |v| v.state.is_terminal()).await;
    assert_eq!(done.state, ExecutionState::Failed);
    let json = serde_json::to_vec(&done).unwrap();
    assert!(
        json.len() < MAX_BODY,
        "Failed inspect must omit last_error so a huge adapter error is not a 413: {}",
        json.len()
    );
    let v = serde_json::to_value(&done).unwrap();
    let node = v["nodes"].as_array().unwrap().iter().next().unwrap();
    assert!(node.get("last_error").is_none(), "{node}");
    assert!(node["state"].get("output").is_none(), "{node}");
    let snap = rt.inspect(&id).await.expect("kernel inspect");
    let msg = snap
        .node(&NodeId::new("x"))
        .and_then(|n| n.last_error.clone())
        .expect("kernel snapshot keeps last_error");
    assert_eq!(msg.message.len(), huge.len());
    server.abort();
}
