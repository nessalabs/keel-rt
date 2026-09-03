//! Public CompleteClient against the existing POST /complete server.
//! Same protocol as `complete.rs`. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, FakeClock, MemoryStore, NodeId, NodeOutcome, Resume, Runtime,
    StateStore, WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteClient, CompleteClientError, CompleteSecret, Decision,
    MAX_COMPLETE_BODY,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "test-complete-secret";

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
}

fn wait_then_next() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .node("next", "next")
        .edge("hold", "next")
        .build()
        .unwrap()
}

fn runtime_with_next() -> Arc<Runtime> {
    let clock = Arc::new(FakeClock::new());
    Arc::new(
        Runtime::builder()
            .clock(clock)
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build(),
    )
}

async fn park_wait(rt: &Runtime) -> (keel_rt::ExecutionHandle, keel_rt::ResumeToken) {
    let handle = rt.start(wait_then_next()).unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .expect("park"),
        ExecutionState::Waiting
    );
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    (handle, token)
}

fn client_at(addr: std::net::SocketAddr) -> CompleteClient {
    CompleteClient::new(format!("http://{addr}"), secret()).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn client_complete_unblocks_wait_node() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    tokio::time::timeout(
        BOUND,
        client_at(addr).complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        ),
    )
    .await
    .expect("http")
    .expect("complete");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_decision_complete_unblocks_wait_node() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    tokio::time::timeout(
        BOUND,
        client_at(addr).complete(token, Decision::Complete(Bytes::from_static(b"gate"))),
    )
    .await
    .expect("http")
    .expect("decide");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_without_secret_is_401() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let client = CompleteClient::without_secret(format!("http://{addr}")).unwrap();
    let err = client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CompleteClientError::Unauthorized), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_wrong_secret_is_401() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = CompleteClient::new(
        format!("http://{addr}"),
        CompleteSecret::new("wrong-secret").unwrap(),
    )
    .unwrap();
    let err = client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CompleteClientError::Unauthorized), "{err:?}");
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_unknown_token_is_unknown() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let err = client_at(addr)
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CompleteClientError::UnknownToken), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_after_drop_handle_is_409_does_not_revive() {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    tokio::time::timeout(BOUND, handle.wait_stable())
        .await
        .unwrap();
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    drop(handle);
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop cancelled");
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let err = client_at(addr)
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CompleteClientError::Cancelled), "{err:?}");
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Cancelled);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_duplicate_complete_is_noop() {
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(Runtime::builder().clock(clock).build());
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    tokio::time::timeout(BOUND, handle.wait_stable())
        .await
        .unwrap();
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let resume = Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"once")));
    client
        .complete(token.clone(), resume.clone())
        .await
        .unwrap();
    client.complete(token, resume).await.unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_oversized_body_is_413_does_not_complete() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let huge = Bytes::from(vec![1u8; MAX_COMPLETE_BODY]);
    let err = client_at(addr)
        .complete(token, Resume::Complete(NodeOutcome::Succeeded(huge)))
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompleteClientError::PayloadTooLarge),
        "{err:?}"
    );
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn two_client_completes_one_token_downstream_runs_once() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock)
            .register_fn("next", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"next")) }
            })
            .build(),
    );
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let resume = Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate")));
    let (a, b) = tokio::join!(
        client.complete(token.clone(), resume.clone()),
        client.complete(token, resume)
    );
    assert!(a.is_ok() || b.is_ok(), "a={a:?} b={b:?}");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no double downstream");
    server.abort();
}
