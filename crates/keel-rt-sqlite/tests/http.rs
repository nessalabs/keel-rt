//! HTTP start + inspect + complete against a real sqlite file.
//! Lives here so `keel-rt-http` src never names SqliteStore.

use bytes::Bytes;
use keel_rt::testing::FakeClock;
use keel_rt::{
    ExecutionContext, ExecutionId, ExecutionState, Executor, ExecutorId, NodeId, NodeOutcome,
    Resume, Runtime, StateStore, WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteSecret, InspectNodeState, KeelClient, KeelClientError,
};
use keel_rt_sqlite::SqliteStore;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "test-sqlite-http-secret";

fn tmp() -> PathBuf {
    let p = std::env::temp_dir().join(format!("keel-rt-sqlite-http-{}.db", ExecutionId::new()));
    let _ = std::fs::remove_file(&p);
    p
}

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
}

/// Two HTTP servers, one sqlite file: start + inspect + complete.
/// A new id is not a steal. Completing the other Runtime's token is
/// ClaimedElsewhere.
#[tokio::test(flavor = "current_thread")]
async fn http_sqlite_start_inspect_complete_two_runtimes_new_id_is_not_steal() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let clock = Arc::new(FakeClock::new());
    let a = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store.clone())
            .build(),
    );
    let b = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let (addr_a, sa) = serve_ephemeral(a, secret()).await.unwrap();
    let (addr_b, sb) = serve_ephemeral(b, secret()).await.unwrap();
    let ca = KeelClient::new(format!("http://{addr_a}"), secret()).unwrap();
    let cb = KeelClient::new(format!("http://{addr_b}"), secret()).unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .build()
        .unwrap();
    let id_a = ca.start(def.clone()).await.expect("start a");
    let id_b = cb.start(def).await.expect("start b");
    assert_ne!(id_a, id_b, "new id is a new execution, not a steal of a");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = ca.inspect(&id_a).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inspect a token");
    let err = cb
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"steal"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::ClaimedElsewhere),
        "B complete of A's token is ClaimedElsewhere, not a steal: {err:?}"
    );
    ca.complete(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
    )
    .await
    .expect("owner still completes");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id_a).await.unwrap() {
                if s.state == ExecutionState::Succeeded {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner persist Succeeded");
    sa.abort();
    sb.abort();
    let _ = std::fs::remove_file(&path);
}

/// HTTP start + cancel on one sqlite file. Second Runtime complete/cancel
/// is ClaimedElsewhere. New id is not a steal.
#[tokio::test(flavor = "current_thread")]
async fn http_sqlite_start_cancel_second_runtime_is_claimed_elsewhere() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let clock = Arc::new(FakeClock::new());
    let a = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store.clone())
            .build(),
    );
    let b = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let (addr_a, sa) = serve_ephemeral(a, secret()).await.unwrap();
    let (addr_b, sb) = serve_ephemeral(b, secret()).await.unwrap();
    let ca = KeelClient::new(format!("http://{addr_a}"), secret()).unwrap();
    let cb = KeelClient::new(format!("http://{addr_b}"), secret()).unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .build()
        .unwrap();
    let id_a = ca.start(def.clone()).await.expect("start a");
    let id_b = cb.start(def).await.expect("start b");
    assert_ne!(id_a, id_b, "new id is a new execution, not a steal of a");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = ca.inspect(&id_a).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inspect a token");
    let err = cb.cancel(&id_a).await.unwrap_err();
    assert!(
        matches!(err, KeelClientError::ClaimedElsewhere),
        "B cancel of A's id is ClaimedElsewhere, not inject: {err:?}"
    );
    let err = cb
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"steal"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::ClaimedElsewhere),
        "B complete of A's token is ClaimedElsewhere: {err:?}"
    );
    ca.cancel(&id_a).await.expect("owner cancels");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id_a).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner persist Cancelled");
    assert_eq!(
        cb.inspect(&id_b).await.expect("b").state,
        ExecutionState::Waiting,
        "cancel of a must not cancel b"
    );
    sa.abort();
    sb.abort();
    let _ = std::fs::remove_file(&path);
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

fn custom_dag() -> WorkflowDefinition {
    WorkflowDefinition::builder("research-hold-write")
        .node("research", "research")
        .node("hold", "wait")
        .node("write", "write")
        .edge("research", "hold")
        .edge("hold", "write")
        .build()
        .unwrap()
}

/// Custom adapters persist Bytes on sqlite. Inspect on a Runtime that did
/// not register `write` still reads Succeeded. Start without `write` is
/// named 400. New id is not a steal.
#[tokio::test(flavor = "current_thread")]
async fn http_sqlite_custom_executor_types_inspect_succeeded_bytes() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let clock = Arc::new(FakeClock::new());
    let a = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store.clone())
            .register(Research)
            .register(Write)
            .build(),
    );
    let b = Arc::new(
        Runtime::builder()
            .clock(clock)
            .store(store.clone())
            .register(Research)
            .build(),
    );
    let (addr_a, sa) = serve_ephemeral(a, secret()).await.unwrap();
    let (addr_b, sb) = serve_ephemeral(b, secret()).await.unwrap();
    let ca = KeelClient::new(format!("http://{addr_a}"), secret()).unwrap();
    let cb = KeelClient::new(format!("http://{addr_b}"), secret()).unwrap();
    let err = cb.start(custom_dag()).await.unwrap_err();
    match &err {
        KeelClientError::Unregistered { executors } => {
            assert_eq!(
                executors.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
                vec!["write"],
                "{executors:?}"
            );
        }
        other => panic!("B without write must 400, got {other:?}"),
    }
    let id = ca.start(custom_dag()).await.expect("start a");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = ca.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    ca.approve(token, Bytes::from_static(b"human-ok"))
        .await
        .expect("approve");
    tokio::time::timeout(BOUND, async {
        loop {
            let v = ca.inspect(&id).await.expect("a");
            if v.state == ExecutionState::Succeeded {
                assert_eq!(
                    v.node(&NodeId::new("write")).map(|n| &n.state),
                    Some(&InspectNodeState::Succeeded {
                        output: Bytes::from_static(b"human-ok"),
                    })
                );
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a succeeded");
    let from_b = cb.inspect(&id).await.expect("b inspect is store-read");
    assert_eq!(from_b.state, ExecutionState::Succeeded);
    assert_eq!(
        from_b.node(&NodeId::new("write")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"human-ok"),
        })
    );
    let id_b = cb
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("new wait on B");
    assert_ne!(id, id_b, "new id is not a steal");
    sa.abort();
    sb.abort();
    let _ = std::fs::remove_file(&path);
}
