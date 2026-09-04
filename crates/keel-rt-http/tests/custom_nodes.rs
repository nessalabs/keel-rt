//! Custom node types for agent authors: implement [`Executor`], register
//! on the engine, catalog, start, approve, inspect. Not a second runtime.
//! FakeClock. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, Executor, ExecutorId, FakeClock, MemoryStore, NodeId,
    NodeOutcome, Runtime, StateStore, WorkflowDefinition, WAIT_ID,
};
use keel_rt_http::{
    serve_ephemeral, CompleteSecret, InspectNodeState, KeelClient, KeelClientError,
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

struct EmptyOut;

impl Executor for EmptyOut {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("empty")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::new()) })
    }
}

struct Boom;

impl Executor for Boom {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("boom")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::failed("boom") })
    }
}

struct Panics;

impl Executor for Panics {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("panics")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { panic!("adapter exploded") })
    }
}

struct Hang {
    gate: Arc<Notify>,
}

impl Executor for Hang {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("slow")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        let gate = self.gate.clone();
        Box::pin(async move {
            gate.notified().await;
            NodeOutcome::Succeeded(Bytes::from_static(b"late"))
        })
    }
}

struct First;

impl Executor for First {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("tool")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"first")) })
    }
}

struct Last;

impl Executor for Last {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("tool")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"second")) })
    }
}

struct Nameless;

impl Executor for Nameless {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("")
    }

    fn execute<'a>(
        &'a self,
        _ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"nope")) })
    }
}

/// ≥2 user adapters + wait → catalog → approve → downstream bytes → inspect.
#[tokio::test(flavor = "current_thread")]
async fn custom_executor_types_catalog_start_approve_inspect() {
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Research)
            .register(Write)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let catalog = client.executors().await.expect("catalog");
    let names: Vec<&str> = catalog.iter().map(|id| id.as_str()).collect();
    assert!(names.contains(&"research"), "{names:?}");
    assert!(names.contains(&"write"), "{names:?}");
    assert!(names.contains(&WAIT_ID), "{names:?}");
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "catalog is sorted: {names:?}");

    let id = client.start(dag()).await.expect("start");
    let parked = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state == ExecutionState::Waiting {
                if let Some(InspectNodeState::Waiting { .. }) =
                    v.node(&NodeId::new("hold")).map(|n| &n.state)
                {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("park");
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
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state == ExecutionState::Succeeded {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded");
    assert_eq!(
        done.node(&NodeId::new("write")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"human-ok"),
        })
    );
    server.abort();
}

/// Subset registered: research + wait, write missing. 400 names only write.
#[tokio::test(flavor = "current_thread")]
async fn custom_subset_registered_start_is_400_names_missing_only_nothing_runs() {
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Research)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
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
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Boom)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "boom")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state.is_terminal() {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal");
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
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Panics)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("x", "panics")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state.is_terminal() {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal");
    assert_eq!(done.state, ExecutionState::Failed);
    assert!(matches!(
        done.node(&NodeId::new("x")).map(|n| &n.state),
        Some(InspectNodeState::Failed)
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_cancel_mid_custom_running_is_cancelled() {
    let gate = Arc::new(Notify::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Hang { gate: gate.clone() })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if matches!(
                    v.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                ) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running");
    client.cancel(&id).await.expect("cancel");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state == ExecutionState::Cancelled {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Cancelled");
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
async fn client_drop_http_server_while_custom_running_cancels() {
    let store = MemoryStore::new();
    let gate = Arc::new(Notify::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .store(store.clone())
            .register(Hang { gate: gate.clone() })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if matches!(
                    v.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                ) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running");
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
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(EmptyOut)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("empty", "empty")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state == ExecutionState::Succeeded {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded");
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
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(First)
            .register(Last)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let catalog = client.executors().await.expect("catalog");
    assert_eq!(
        catalog.iter().filter(|id| id.as_str() == "tool").count(),
        1,
        "{catalog:?}"
    );
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("n", "tool")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state == ExecutionState::Succeeded {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded");
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
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Nameless)
            .register(Research)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let catalog = client.executors().await.expect("catalog");
    assert!(
        !catalog.iter().any(|id| id.as_str().is_empty()),
        "catalog must not list empty id: {catalog:?}"
    );
    assert!(catalog.iter().any(|id| id.as_str() == "research"));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn custom_catalog_without_secret_is_401() {
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Research)
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = KeelClient::new(
        format!("http://{addr}"),
        CompleteSecret::new("wrong").unwrap(),
    )
    .unwrap()
    .executors()
    .await
    .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    server.abort();
}
