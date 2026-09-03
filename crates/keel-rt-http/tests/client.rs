//! Public KeelClient against the existing POST /complete server.
//! Same protocol as `complete.rs`. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionId, ExecutionState, FakeClock, Join, MemoryStore, NodeId,
    NodeOutcome, NodeState, OnFailure, Resume, ResumeToken, Runtime, StateStore,
    WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteBody, CompleteSecret, Decision, InspectNodeState, KeelClient,
    KeelClientError, StartBody, CLAIMED_ELSEWHERE, HANG_BOUND, MAX_BODY, SECRET_HEADER,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

fn client_at(addr: std::net::SocketAddr) -> KeelClient {
    KeelClient::new(format!("http://{addr}"), secret()).unwrap()
}

fn runtime_echo_hold() -> Arc<Runtime> {
    let clock = Arc::new(FakeClock::new());
    Arc::new(
        Runtime::builder()
            .clock(clock)
            .register_fn("next", |ctx: ExecutionContext| async move {
                NodeOutcome::Succeeded(
                    ctx.inputs
                        .get(&NodeId::new("hold"))
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .build(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_then_complete_unblocks_wait() {
    let rt = runtime_echo_hold();
    let handle = rt.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .expect("park"),
        ExecutionState::Waiting
    );
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let view = tokio::time::timeout(BOUND, client.inspect(&id))
        .await
        .expect("inspect")
        .expect("view");
    assert_eq!(view.execution_id, id);
    assert_eq!(view.state, ExecutionState::Waiting);
    let hold = view.node(&NodeId::new("hold")).expect("hold");
    assert!(matches!(hold.state, InspectNodeState::Waiting { .. }));
    let token = view
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("token");
    let wire = serde_json::to_string(&view).unwrap();
    let token_json = serde_json::to_string(&token).unwrap();
    assert_eq!(
        wire.matches(token_json.as_str()).count(),
        1,
        "parked wait InspectView JSON has exactly one token: {wire}"
    );
    assert!(!wire.contains("resume_token"), "{wire}");
    client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .expect("complete");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    let done = rt.inspect(&id).await.expect("downstream snapshot");
    assert_eq!(
        done.node(&NodeId::new("hold"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"gate"))
    );
    assert_eq!(
        done.node(&NodeId::new("next"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"gate"))
    );
    let after = client.inspect(&id).await.expect("after");
    assert_eq!(after.state, ExecutionState::Succeeded);
    assert!(after.resume_token(&NodeId::new("hold")).is_none());
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_while_running_has_no_token_then_wait_sees_token() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock)
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"go"))
                }
            })
            .build(),
    );
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .node("hold", "wait")
                .edge("slow", "hold")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let running = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if v.state == ExecutionState::Running
                    && matches!(
                        v.node(&NodeId::new("slow")).map(|n| &n.state),
                        Some(InspectNodeState::Running { .. })
                    )
                {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running");
    let hold = running.node(&NodeId::new("hold")).expect("hold");
    assert!(
        matches!(hold.state, InspectNodeState::Pending),
        "wait node is not parked yet: {:?}",
        hold.state
    );
    assert!(
        running.resume_token(&NodeId::new("hold")).is_none(),
        "no wait token while predecessor is Running"
    );
    assert!(
        running.resume_token(&NodeId::new("slow")).is_none(),
        "InspectView must not expose a Running-node token"
    );
    go.notify_one();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .expect("park"),
        ExecutionState::Waiting
    );
    let waiting = client.inspect(&id).await.expect("waiting");
    assert_eq!(waiting.state, ExecutionState::Waiting);
    assert!(waiting.resume_token(&NodeId::new("hold")).is_some());
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_wrong_secret_is_401() {
    let rt = runtime_with_next();
    let handle = rt.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    tokio::time::timeout(BOUND, handle.wait_stable())
        .await
        .unwrap();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let err = KeelClient::new(
        format!("http://{addr}"),
        CompleteSecret::new("wrong-secret").unwrap(),
    )
    .unwrap()
    .inspect(&id)
    .await
    .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    assert!(
        !err.to_string().to_ascii_lowercase().contains("complete"),
        "inspect 401 must not say complete: {err}"
    );
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_without_secret_is_401() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = KeelClient::without_secret(format!("http://{addr}"))
        .unwrap()
        .inspect(&ExecutionId::parse("exec-missing").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    assert!(
        !err.to_string().to_ascii_lowercase().contains("complete"),
        "inspect 401 must not say complete: {err}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_401_error_text_does_not_say_complete() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = KeelClient::without_secret(format!("http://{addr}"))
        .unwrap()
        .inspect(&ExecutionId::parse("exec-missing").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    let text = err.to_string();
    assert!(
        !text.to_ascii_lowercase().contains("complete"),
        "inspect 401 Display must be verb-neutral: {text}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_unknown_id_is_404() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = client_at(addr)
        .inspect(&ExecutionId::parse("exec-missing").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::UnknownExecution), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_after_drop_handle_is_cancelled_complete_409() {
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
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let token = client
        .inspect(&id)
        .await
        .expect("token from client, not handle")
        .resume_token(&NodeId::new("hold"))
        .cloned()
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
    let view = client.inspect(&id).await.expect("inspect cancelled");
    assert_eq!(view.state, ExecutionState::Cancelled);
    assert!(view.resume_token(&NodeId::new("hold")).is_none());
    let err = client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Cancelled), "{err:?}");
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Cancelled, "409 must not revive");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn two_clients_inspect_same_token() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let id = handle.execution_id().clone();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let a = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let b = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let (va, vb) = tokio::join!(a.inspect(&id), b.inspect(&id));
    let va = va.expect("a");
    let vb = vb.expect("b");
    assert_eq!(va.resume_token(&NodeId::new("hold")), Some(&token));
    assert_eq!(vb.resume_token(&NodeId::new("hold")), Some(&token));
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn two_runtimes_inspect_does_not_steal_lease() {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let owner = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store.clone())
            .build(),
    );
    let other = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let handle = owner
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
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                if s.state == ExecutionState::Waiting {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("persisted");
    let (addr, server) = serve_ephemeral(other.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let view = client.inspect(&id).await.expect("other inspect");
    assert_eq!(view.state, ExecutionState::Waiting);
    let token = view
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token");
    let wire = serde_json::to_string(&view).unwrap();
    let token_json = serde_json::to_string(&token).unwrap();
    assert_eq!(
        wire.matches(token_json.as_str()).count(),
        1,
        "inspect JSON must carry the wait token once: {wire}"
    );
    assert!(!wire.contains("resume_token"), "{wire}");
    let steal = CompleteBody {
        token: token.clone(),
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"steal"))),
    };
    let steal_json = serde_json::to_vec(&steal).unwrap();
    let req = format!(
        "POST /complete HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{SECRET_HEADER}: {SECRET}\r\n\r\n",
        steal_json.len()
    );
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    raw.write_all(req.as_bytes()).await.unwrap();
    raw.write_all(&steal_json).await.unwrap();
    let mut buf = Vec::new();
    raw.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let status_line = text.lines().next().unwrap_or("");
    assert!(
        status_line.contains(&CLAIMED_ELSEWHERE.to_string()),
        "ClaimedElsewhere is {CLAIMED_ELSEWHERE} Locked, not 400/409: {status_line}"
    );
    assert!(
        !status_line.contains(" 400 ") && !status_line.contains("400"),
        "must not collide with malformed body: {status_line}"
    );
    assert!(
        !status_line.contains("409"),
        "must not be Cancelled 409: {status_line}"
    );
    assert!(
        text.contains("claimed_elsewhere"),
        "body must name claimed_elsewhere, not Cancelled: {text}"
    );
    assert!(
        !text.to_ascii_lowercase().contains("cancelled"),
        "body must be distinguishable from Cancelled: {text}"
    );
    let err = client
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"steal"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::ClaimedElsewhere),
        "other Runtime complete is ClaimedElsewhere, not 404/401/409: {err:?}"
    );
    assert!(
        !matches!(
            err,
            KeelClientError::BadRequest
                | KeelClientError::Cancelled
                | KeelClientError::UnknownToken
                | KeelClientError::Unauthorized
        ),
        "must not collapse onto 400/401/404/409: {err:?}"
    );
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    owner
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .expect("owner still completes");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_complete_of_running_handle_token_does_not_unblock_wait() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"go"))
                }
            })
            .build(),
    );
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .node("hold", "wait")
                .edge("slow", "hold")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = handle.inspect().await;
            if matches!(
                snap.node(&NodeId::new("slow")).map(|n| &n.state),
                Some(NodeState::Running { .. })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running");
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let view = client.inspect(&id).await.expect("view");
    assert!(view.resume_token(&NodeId::new("slow")).is_none());
    assert!(view.resume_token(&NodeId::new("hold")).is_none());
    let running_tok = handle
        .inspect()
        .await
        .node(&NodeId::new("slow"))
        .and_then(|n| n.resume_token.clone())
        .expect("handle still has Running token");
    let err = client
        .complete(
            running_tok,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::BadRequest | KeelClientError::UnknownToken
        ),
        "Running token complete must not park-unblock: {err:?}"
    );
    assert_eq!(
        client.inspect(&id).await.expect("still").state,
        ExecutionState::Running
    );
    go.notify_one();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .unwrap(),
        ExecutionState::Waiting
    );
    assert!(client
        .inspect(&id)
        .await
        .unwrap()
        .resume_token(&NodeId::new("hold"))
        .is_some());
    handle.cancel().await;
    server.abort();
}

/// 0.1%: execution is Running because a sibling is Running, but a wait
/// node is already parked. Completing the wait token must not complete
/// the Running node; completing the Running handle token must not
/// unblock the wait.
#[tokio::test(flavor = "current_thread")]
async fn client_inspect_wait_sibling_while_running_completes_only_wait() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"go"))
                }
            })
            .build(),
    );
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let view = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let slow_run = matches!(
                    v.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                );
                let hold_wait = matches!(
                    v.node(&NodeId::new("hold")).map(|n| &n.state),
                    Some(InspectNodeState::Waiting { .. })
                );
                if v.state == ExecutionState::Running && slow_run && hold_wait {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed Running + Waiting");
    assert_eq!(view.state, ExecutionState::Running);
    assert!(
        view.resume_token(&NodeId::new("slow")).is_none(),
        "InspectView must not expose the Running sibling token"
    );
    let wait_tok = view
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token while sibling is Running");
    let wire = serde_json::to_string(&view).unwrap();
    assert_eq!(
        wire.matches(serde_json::to_string(&wait_tok).unwrap().as_str())
            .count(),
        1,
        "wait token once: {wire}"
    );
    match &view.node(&NodeId::new("hold")).unwrap().state {
        InspectNodeState::Waiting { token, .. } => assert_eq!(token, &wait_tok),
        other => panic!("expected Waiting, got {other:?}"),
    }
    let running_tok = handle
        .inspect()
        .await
        .node(&NodeId::new("slow"))
        .and_then(|n| n.resume_token.clone())
        .expect("handle still has Running token");
    assert!(
        !wire.contains(&serde_json::to_string(&running_tok).unwrap()),
        "Running sibling token must not appear on inspect JSON: {wire}"
    );
    let err = client
        .complete(
            running_tok,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::BadRequest | KeelClientError::UnknownToken
        ),
        "Running token must not complete the wait sibling: {err:?}"
    );
    let still = client.inspect(&id).await.expect("still mixed");
    assert!(matches!(
        still.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Waiting { .. })
    ));
    assert!(matches!(
        still.node(&NodeId::new("slow")).map(|n| &n.state),
        Some(InspectNodeState::Running { .. })
    ));
    client
        .complete(
            wait_tok,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .expect("complete wait sibling");
    tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("poll");
            if matches!(
                v.node(&NodeId::new("hold")).map(|n| &n.state),
                Some(InspectNodeState::Succeeded)
            ) {
                assert!(
                    matches!(
                        v.node(&NodeId::new("slow")).map(|n| &n.state),
                        Some(InspectNodeState::Running { .. })
                    ),
                    "completing the wait must not finish the Running sibling"
                );
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hold succeeded, slow still running");
    go.notify_one();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    server.abort();
}

/// 0.1%: inspect JSON for a mixed Running+Waiting execution has the wait
/// token only. Completing a token issued for the Running node (not on the
/// inspect wire) leaves the wait parked.
#[tokio::test(flavor = "current_thread")]
async fn client_complete_issued_token_for_running_node_leaves_wait_parked() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"go"))
                }
            })
            .build(),
    );
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .unwrap();
    let id = handle.execution_id().clone();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let view = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let slow_run = matches!(
                    v.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                );
                let hold_wait = matches!(
                    v.node(&NodeId::new("hold")).map(|n| &n.state),
                    Some(InspectNodeState::Waiting { .. })
                );
                if v.state == ExecutionState::Running && slow_run && hold_wait {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed Running + Waiting");
    let wait_tok = view
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token");
    let json = serde_json::to_string(&view).unwrap();
    assert_eq!(
        json.matches(serde_json::to_string(&wait_tok).unwrap().as_str())
            .count(),
        1,
        "{json}"
    );
    let running_tok = handle
        .inspect()
        .await
        .node(&NodeId::new("slow"))
        .and_then(|n| n.resume_token.clone())
        .expect("Running snapshot token");
    assert!(
        !json.contains(&serde_json::to_string(&running_tok).unwrap()),
        "inspect JSON must not carry the Running node's resume token: {json}"
    );
    let issued = ResumeToken::issue(id.clone(), NodeId::new("slow"), 1);
    assert_ne!(issued, wait_tok, "issued token is not the wait token");
    let err = client
        .complete(
            issued,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::BadRequest | KeelClientError::UnknownToken
        ),
        "issued Running-node token must not unblock wait: {err:?}"
    );
    let still = client.inspect(&id).await.expect("still mixed");
    assert!(matches!(
        still.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Waiting { token, .. }) if token == &wait_tok
    ));
    assert!(matches!(
        still.node(&NodeId::new("slow")).map(|n| &n.state),
        Some(InspectNodeState::Running { .. })
    ));
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn client_hung_inspect_is_hung_not_forever() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _hold = tokio::spawn(async move {
        let (_s, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let id = ExecutionId::parse("exec-hung").unwrap();
    let mut fut = std::pin::pin!(client.inspect(&id));
    for _ in 0..64 {
        tokio::select! {
            biased;
            r = fut.as_mut() => panic!("hung inspect finished before bound: {r:?}"),
            _ = tokio::task::yield_now() => {}
        }
    }
    tokio::time::advance(HANG_BOUND + Duration::from_millis(1)).await;
    let err = fut.await.unwrap_err();
    assert!(matches!(err, KeelClientError::Hung), "{err:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_does_not_follow_redirect_off_loopback() {
    let trap = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let trap_addr = trap.local_addr().unwrap();
    let hits = Arc::new(AtomicU32::new(0));
    let c = hits.clone();
    let trap_task = tokio::spawn(async move {
        if let Ok((s, _)) = trap.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            drop(s);
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = s.read(&mut buf).await;
        write_http(
            &mut s,
            302,
            &format!("Location: http://{trap_addr}/inspect/exec-x\r\n"),
        )
        .await;
    });
    let err = client_at(addr)
        .inspect(&ExecutionId::parse("exec-x").unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::Unexpected(302)),
        "must not follow inspect redirect: {err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "SSRF: followed to 0.0.0.0");
    server.abort();
    trap_task.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_wire_sends_both_secret_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        write_http(&mut s, 404, "").await;
        buf.truncate(n);
        buf
    });
    let err = client_at(addr)
        .inspect(&ExecutionId::parse("exec-wire").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::UnknownExecution), "{err:?}");
    let raw = String::from_utf8(server.await.unwrap()).unwrap();
    let headers = raw.split("\r\n\r\n").next().expect("http");
    assert!(
        headers.starts_with("GET /inspect/exec-wire "),
        "path/protocol: {headers}"
    );
    let secret_hdr = format!("{SECRET_HEADER}: {SECRET}");
    assert!(
        headers.to_ascii_lowercase().contains(&secret_hdr),
        "missing {SECRET_HEADER}: {headers}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {SECRET}")),
        "missing Authorization: Bearer: {headers}"
    );
    assert!(
        !headers.contains('?') && !raw.contains(&format!("?{SECRET_HEADER}")),
        "secret must not be a query string: {raw}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_succeeds_against_bearer_only_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        let raw = String::from_utf8_lossy(&buf[..n]);
        let lower = raw.to_ascii_lowercase();
        let bearer_ok = lower.contains(&format!("authorization: bearer {SECRET}"));
        write_http(&mut s, if bearer_ok { 404 } else { 401 }, "").await;
        bearer_ok
    });
    let err = client_at(addr)
        .inspect(&ExecutionId::parse("exec-wire").unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::UnknownExecution),
        "Bearer-only inspect server must accept KeelClient: {err:?}"
    );
    assert!(
        server.await.unwrap(),
        "inspect request lacked Authorization: Bearer"
    );
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
    let client = KeelClient::without_secret(format!("http://{addr}")).unwrap();
    let err = client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_wrong_secret_is_401() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = KeelClient::new(
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
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
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
    assert!(matches!(err, KeelClientError::UnknownToken), "{err:?}");
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
    assert!(matches!(err, KeelClientError::Cancelled), "{err:?}");
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
    let huge = Bytes::from(vec![1u8; MAX_BODY]);
    let err = client_at(addr)
        .complete(token, Resume::Complete(NodeOutcome::Succeeded(huge)))
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::PayloadTooLarge), "{err:?}");
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

#[tokio::test(flavor = "current_thread")]
async fn two_keel_clients_one_token_downstream_runs_once() {
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
    let a = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let b = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let resume = Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate")));
    let (ra, rb) = tokio::join!(
        a.complete(
            token.clone(),
            Decision::Complete(Bytes::from_static(b"gate"))
        ),
        b.complete(token, resume)
    );
    assert!(ra.is_ok() || rb.is_ok(), "a={ra:?} b={rb:?}");
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no double downstream");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_complete_succeeds_against_bearer_only_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        let raw = String::from_utf8_lossy(&buf[..n]);
        let lower = raw.to_ascii_lowercase();
        let bearer_ok = lower.contains(&format!("authorization: bearer {SECRET}"));
        write_http(&mut s, if bearer_ok { 200 } else { 401 }, "").await;
        bearer_ok
    });
    client_at(addr)
        .complete(dummy_token(), Resume::Reinvoke)
        .await
        .expect("KeelClient::complete must satisfy a Bearer-only server");
    assert!(
        server.await.unwrap(),
        "request lacked Authorization: Bearer"
    );
}

fn dummy_token() -> keel_rt::ResumeToken {
    keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-wire").unwrap(),
        NodeId::new("hold"),
        1,
    )
}

async fn write_http(s: &mut tokio::net::TcpStream, status: u16, extra: &str) {
    let reason = match status {
        200 => "OK",
        302 => "Found",
        _ => "X",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n{extra}\r\n"
    );
    s.write_all(resp.as_bytes()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn client_wire_is_complete_body_and_secret_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        write_http(&mut s, 200, "").await;
        buf.truncate(n);
        buf
    });
    let token = dummy_token();
    client_at(addr)
        .complete(
            token.clone(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"g"))),
        )
        .await
        .unwrap();
    let raw = String::from_utf8(server.await.unwrap()).unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").expect("http");
    assert!(
        headers.starts_with("POST /complete "),
        "path/protocol: {headers}"
    );
    let secret_hdr = format!("{SECRET_HEADER}: {SECRET}");
    assert!(
        headers.to_ascii_lowercase().contains(&secret_hdr),
        "missing {SECRET_HEADER}: {headers}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {SECRET}")),
        "missing Authorization: Bearer: {headers}"
    );
    assert!(
        !headers.contains('?') && !raw.contains(&format!("?{SECRET_HEADER}")),
        "secret must not be a query string: {raw}"
    );
    let parsed: CompleteBody = serde_json::from_str(body.trim_end_matches('\0')).unwrap();
    assert_eq!(parsed.token, token);
    assert!(matches!(
        parsed.resume,
        Resume::Complete(NodeOutcome::Succeeded(ref b)) if b.as_ref() == b"g"
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn client_does_not_follow_redirect_off_loopback() {
    let trap = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let trap_addr = trap.local_addr().unwrap();
    assert!(
        trap_addr.ip().is_unspecified(),
        "trap must be unspecified: {trap_addr}"
    );
    let hits = Arc::new(AtomicU32::new(0));
    let c = hits.clone();
    let trap_task = tokio::spawn(async move {
        if let Ok((s, _)) = trap.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            drop(s);
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = s.read(&mut buf).await;
        write_http(
            &mut s,
            302,
            &format!("Location: http://{trap_addr}/complete\r\n"),
        )
        .await;
    });
    let err = client_at(addr)
        .complete(dummy_token(), Resume::Reinvoke)
        .await
        .unwrap_err();
    assert!(
        matches!(err, KeelClientError::Unexpected(302)),
        "must not follow redirect: {err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "SSRF: followed to 0.0.0.0");
    server.abort();
    trap_task.abort();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn client_hung_server_is_hung_not_forever() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _hold = tokio::spawn(async move {
        let (_s, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let mut fut = std::pin::pin!(client.complete(dummy_token(), Resume::Reinvoke));
    for _ in 0..64 {
        tokio::select! {
            biased;
            r = fut.as_mut() => panic!("hung server finished before bound: {r:?}"),
            _ = tokio::task::yield_now() => {}
        }
    }
    tokio::time::advance(HANG_BOUND + Duration::from_millis(1)).await;
    let err = fut.await.unwrap_err();
    assert!(matches!(err, KeelClientError::Hung), "{err:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn client_drop_server_mid_post_is_transport_token_untouched() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        let _ = s.read(&mut buf).await;
        drop(s);
    });
    let err = client_at(addr)
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Transport(_)), "{err:?}");
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_drop_inflight_does_not_complete() {
    let rt = runtime_with_next();
    let (handle, token) = park_wait(&rt).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(tokio::sync::Notify::new());
    let go = accepted.clone();
    let hold = tokio::spawn(async move {
        let (_s, _) = listener.accept().await.unwrap();
        go.notify_one();
        std::future::pending::<()>().await;
    });
    let client = client_at(addr);
    let inflight = tokio::spawn(async move {
        client
            .hang_bound(Duration::from_secs(30))
            .complete(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"nope"))),
            )
            .await
    });
    tokio::time::timeout(BOUND, accepted.notified())
        .await
        .expect("accepted");
    inflight.abort();
    let _ = inflight.await;
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    let (real, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    client_at(real)
        .complete(
            handle
                .inspect()
                .await
                .node(&NodeId::new("hold"))
                .unwrap()
                .resume_token
                .clone()
                .unwrap(),
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    hold.abort();
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_decision_fail_fails_execution() {
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
        .unwrap();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    client_at(addr)
        .complete(token, Decision::Fail)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Failed
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_inspect_complete_unblocks_wait() {
    let rt = runtime_echo_hold();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let id = tokio::time::timeout(BOUND, client.start(wait_then_next()))
        .await
        .expect("start bound")
        .expect("start");
    let view = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if v.state == ExecutionState::Waiting
                    && v.resume_token(&NodeId::new("hold")).is_some()
                {
                    return v;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("park via inspect");
    assert_eq!(view.execution_id, id);
    let token = view
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token");
    client
        .complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .expect("complete");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("inspect done");
            if v.state.is_terminal() {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal");
    assert_eq!(done.state, ExecutionState::Succeeded);
    let snap = rt.inspect(&id).await.expect("server snapshot");
    assert_eq!(
        snap.node(&NodeId::new("next"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"gate"))
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_then_inspect_is_waiting_not_cancelled() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let view = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if v.state == ExecutionState::Waiting {
                    return v;
                }
                assert_ne!(
                    v.state,
                    ExecutionState::Cancelled,
                    "HTTP start must hold the handle; Drop-cancel is not start"
                );
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiting");
    assert!(view.resume_token(&NodeId::new("hold")).is_some());
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_two_starts_are_distinct_ids() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let a = client.start(wait_then_next()).await.expect("a");
    let b = client.start(wait_then_next()).await.expect("b");
    assert_ne!(a, b, "Runtime::start is a new execution each call");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_without_secret_is_401() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = KeelClient::without_secret(format!("http://{addr}"))
        .unwrap()
        .start(wait_then_next())
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    assert!(
        !err.to_string().to_ascii_lowercase().contains("complete"),
        "start 401 must reuse verb-neutral Display: {err}"
    );
    assert!(
        !err.to_string().contains("start rejected"),
        "do not add a start-only Display: {err}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_wrong_secret_is_401() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = KeelClient::new(
        format!("http://{addr}"),
        CompleteSecret::new("wrong-secret").unwrap(),
    )
    .unwrap()
    .start(wait_then_next())
    .await
    .unwrap_err();
    assert!(matches!(err, KeelClientError::Unauthorized), "{err:?}");
    assert!(
        !err.to_string().to_ascii_lowercase().contains("complete"),
        "start 401 must reuse verb-neutral Display: {err}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_unregistered_is_400_nothing_runs() {
    let rt = Arc::new(Runtime::builder().clock(Arc::new(FakeClock::new())).build());
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("work", "missing-exec")
        .build()
        .unwrap();
    let err = client_at(addr).start(def).await.unwrap_err();
    assert!(matches!(err, KeelClientError::BadRequest), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_empty_definition_is_400() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = client_at(addr)
        .start(StartBody {
            id: keel_rt::WorkflowId::new("wf"),
            on_failure: OnFailure::FailExecution,
            nodes: vec![],
            edges: vec![],
        })
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::BadRequest), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_oversized_body_is_413() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let mut nodes = Vec::new();
    let pad = "n".repeat(64);
    while nodes.len() * 80 < MAX_BODY {
        nodes.push(keel_rt_http::StartNode {
            id: NodeId::new(format!("{pad}-{}", nodes.len())),
            executor_id: keel_rt::ExecutorId::new("wait"),
            join: Join::AllSucceeded,
        });
    }
    let err = client_at(addr)
        .start(StartBody {
            id: keel_rt::WorkflowId::new("huge"),
            on_failure: OnFailure::FailExecution,
            nodes,
            edges: vec![],
        })
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::PayloadTooLarge), "{err:?}");
    server.abort();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn client_hung_start_is_hung_not_forever() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _hold = tokio::spawn(async move {
        let (_s, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let mut fut = std::pin::pin!(client.start(wait_then_next()));
    for _ in 0..64 {
        tokio::select! {
            biased;
            r = fut.as_mut() => panic!("hung start finished before bound: {r:?}"),
            _ = tokio::task::yield_now() => {}
        }
    }
    tokio::time::advance(HANG_BOUND + Duration::from_millis(1)).await;
    let err = fut.await.unwrap_err();
    assert!(matches!(err, KeelClientError::Hung), "{err:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_does_not_follow_redirect_off_loopback() {
    let trap = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let trap_addr = trap.local_addr().unwrap();
    let hits = Arc::new(AtomicU32::new(0));
    let c = hits.clone();
    let trap_task = tokio::spawn(async move {
        if let Ok((s, _)) = trap.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            drop(s);
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = s.read(&mut buf).await;
        write_http(
            &mut s,
            302,
            &format!("Location: http://{trap_addr}/start\r\n"),
        )
        .await;
    });
    let err = client_at(addr).start(wait_then_next()).await.unwrap_err();
    assert!(
        matches!(err, KeelClientError::Unexpected(302)),
        "must not follow start redirect: {err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "SSRF: followed to 0.0.0.0");
    server.abort();
    trap_task.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_wire_sends_both_secret_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        write_http(&mut s, 400, "").await;
        buf.truncate(n);
        buf
    });
    let err = client_at(addr).start(wait_then_next()).await.unwrap_err();
    assert!(matches!(err, KeelClientError::BadRequest), "{err:?}");
    let raw = String::from_utf8(server.await.unwrap()).unwrap();
    let headers = raw.split("\r\n\r\n").next().expect("http");
    assert!(
        headers.starts_with("POST /start "),
        "path/protocol: {headers}"
    );
    let secret_hdr = format!("{SECRET_HEADER}: {SECRET}");
    assert!(
        headers.to_ascii_lowercase().contains(&secret_hdr),
        "missing {SECRET_HEADER}: {headers}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {SECRET}")),
        "missing Authorization: Bearer: {headers}"
    );
    assert!(
        !headers.contains('?') && !raw.contains(&format!("?{SECRET_HEADER}")),
        "secret must not be a query string: {raw}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_succeeds_against_bearer_only_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let expected = ExecutionId::parse("exec-started").unwrap();
    let reply = expected.clone();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        let raw = String::from_utf8_lossy(&buf[..n]);
        let headers = raw.split("\r\n\r\n").next().unwrap();
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {SECRET}")),
            "{headers}"
        );
        let body = serde_json::to_vec(&keel_rt_http::StartView {
            execution_id: reply,
        })
        .unwrap();
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(&body).await.unwrap();
    });
    let id = client_at(addr)
        .start(wait_then_next())
        .await
        .expect("start");
    assert_eq!(id, expected);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_inspect_approve_unblocks_wait() {
    let rt = runtime_echo_hold();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wait token");
    client
        .approve(token, Bytes::from_static(b"gate"))
        .await
        .expect("approve");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("inspect");
            if v.state.is_terminal() {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal");
    assert_eq!(done.state, ExecutionState::Succeeded);
    assert!(matches!(
        done.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Succeeded)
    ));
    let snap = rt.inspect(&id).await.expect("downstream");
    assert_eq!(
        snap.node(&NodeId::new("next"))
            .and_then(|n| n.output.clone()),
        Some(Bytes::from_static(b"gate"))
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_inspect_reject_fails_execution() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wait token");
    client.reject(token).await.expect("reject");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("inspect");
            if v.state.is_terminal() {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal");
    assert_eq!(
        done.state,
        ExecutionState::Failed,
        "Decision::Fail is NodeOutcome::failed(\"failed\"); cite client_decision_fail_fails_execution"
    );
    assert!(matches!(
        done.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Failed)
    ));
    let snap = rt.inspect(&id).await.expect("kernel snapshot");
    let err = snap
        .node(&NodeId::new("hold"))
        .and_then(|n| n.last_error.clone())
        .expect("failed reason");
    assert_eq!(
        err.to_string(),
        "failed",
        "reject is Decision::Fail → NodeOutcome::failed(\"failed\"), not a new contract"
    );
    server.abort();
}

/// Killer: HTTP StartBody that omitted on_failure / join silently became
/// fail-fast + AllSucceeded. FailSubtree + AllDone must survive the wire.
#[tokio::test(flavor = "current_thread")]
async fn client_start_fail_subtree_keeps_running_sibling() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"sib"))
                }
            })
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"join"))
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let def = WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("hold", "wait")
        .node("sib", "slow")
        .node("join", "next")
        .join("join", Join::AllDone)
        .edge("hold", "join")
        .edge("sib", "join")
        .build()
        .unwrap();
    assert_eq!(def.on_failure(), OnFailure::FailSubtree);
    assert_eq!(def.join_of(&NodeId::new("join")), Some(Join::AllDone));
    let via_from = StartBody::from(&def);
    assert_eq!(
        via_from.on_failure,
        OnFailure::FailSubtree,
        "From<&WorkflowDefinition> must not strip on_failure"
    );
    assert_eq!(
        via_from
            .nodes
            .iter()
            .find(|n| n.id == NodeId::new("join"))
            .map(|n| n.join),
        Some(Join::AllDone),
        "From<&WorkflowDefinition> must not strip join"
    );
    let id = client.start(&def).await.expect("start via From");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let hold = v.resume_token(&NodeId::new("hold")).cloned();
                let sib_run = matches!(
                    v.node(&NodeId::new("sib")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                );
                if let (Some(tok), true) = (hold, sib_run) {
                    return tok;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hold Waiting + sib Running");
    client.reject(token).await.expect("reject hold");
    let after = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("inspect");
            if matches!(
                v.node(&NodeId::new("hold")).map(|n| &n.state),
                Some(InspectNodeState::Failed)
            ) {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hold Failed");
    assert!(
        matches!(
            after.node(&NodeId::new("sib")).map(|n| &n.state),
            Some(InspectNodeState::Running { .. })
        ),
        "FailSubtree must survive HTTP start; fail-fast would cancel sib: {:?}",
        after.node(&NodeId::new("sib")).map(|n| &n.state)
    );
    go.notify_waiters();
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("inspect");
            if matches!(
                v.node(&NodeId::new("join")).map(|n| &n.state),
                Some(InspectNodeState::Succeeded)
            ) {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("AllDone join must run after failed pred");
    assert_eq!(
        done.state,
        ExecutionState::Completed,
        "FailSubtree mixed terminals are Completed, not Failed"
    );
    server.abort();
}

/// FailSubtree reject must not reap the handle: complete returns while
/// the sibling is still Running, and later server-drop still cancels it.
#[tokio::test(flavor = "current_thread")]
async fn client_fail_subtree_reject_returns_and_server_drop_cancels_sibling() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let store = MemoryStore::new();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .store(store.clone())
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"sib"))
                }
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let def = WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("hold", "wait")
        .node("sib", "slow")
        .build()
        .unwrap();
    let id = client.start(&def).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let hold = v.resume_token(&NodeId::new("hold")).cloned();
                let sib_run = matches!(
                    v.node(&NodeId::new("sib")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                );
                if let (Some(tok), true) = (hold, sib_run) {
                    return tok;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed");
    tokio::time::timeout(BOUND, client.reject(token))
        .await
        .expect("reject must not hang on the running sibling")
        .expect("reject");
    let after = client.inspect(&id).await.expect("after reject");
    assert_eq!(after.state, ExecutionState::Running);
    assert!(
        matches!(
            after.node(&NodeId::new("sib")).map(|n| &n.state),
            Some(InspectNodeState::Running { .. })
        ),
        "FailSubtree sibling must still be Running: {:?}",
        after.node(&NodeId::new("sib")).map(|n| &n.state)
    );
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
    .expect("held handle Drop-cancels the still-running sibling");
}

#[tokio::test(flavor = "current_thread")]
async fn client_start_reinvoke_old_token_does_not_approve() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let old = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    client
        .complete(old.clone(), Resume::Reinvoke)
        .await
        .expect("reinvoke");
    let fresh = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    if t != &old {
                        return t.clone();
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new token");
    let err = client
        .approve(old, Bytes::from_static(b"stale"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::UnknownToken | KeelClientError::BadRequest
        ),
        "old token after Reinvoke must not approve: {err:?}"
    );
    let still = client.inspect(&id).await.expect("still");
    assert_eq!(still.state, ExecutionState::Waiting);
    client
        .approve(fresh, Bytes::from_static(b"ok"))
        .await
        .expect("fresh");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_approve_then_reject_does_not_fail_succeeded() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    client
        .approve(token.clone(), Bytes::from_static(b"gate"))
        .await
        .expect("approve");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state.is_terminal() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("done");
    let second = client.reject(token).await;
    assert!(
        second.is_ok()
            || matches!(
                second,
                Err(KeelClientError::UnknownToken
                    | KeelClientError::Cancelled
                    | KeelClientError::BadRequest)
            ),
        "second decision must not invent a new error: {second:?}"
    );
    let snap = client.inspect(&id).await.expect("after");
    assert_eq!(snap.state, ExecutionState::Succeeded);
    assert!(matches!(
        snap.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Succeeded)
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_reject_then_approve_does_not_revive() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    client.reject(token.clone()).await.expect("reject");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state == ExecutionState::Failed {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed");
    let second = client.approve(token, Bytes::from_static(b"late")).await;
    assert!(
        second.is_ok()
            || matches!(
                second,
                Err(KeelClientError::UnknownToken
                    | KeelClientError::Cancelled
                    | KeelClientError::BadRequest)
            ),
        "{second:?}"
    );
    let snap = client.inspect(&id).await.expect("after");
    assert_eq!(snap.state, ExecutionState::Failed);
    assert!(matches!(
        snap.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Failed)
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn two_approves_one_token_downstream_runs_once() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("next", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"next")) }
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let a = client_at(addr);
    let b = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let id = a.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = a.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    let (ra, rb) = tokio::join!(
        a.approve(token.clone(), Bytes::from_static(b"gate")),
        b.approve(token, Bytes::from_static(b"gate"))
    );
    assert!(ra.is_ok() || rb.is_ok(), "a={ra:?} b={rb:?}");
    tokio::time::timeout(BOUND, async {
        loop {
            if a.inspect(&id).await.expect("i").state.is_terminal() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("done");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no double downstream");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_approve_after_http_start_server_drop_is_409() {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
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
    .expect("drop cancelled");
    let (addr2, server2) = serve_ephemeral(rt, secret()).await.unwrap();
    let err = client_at(addr2)
        .approve(token, Bytes::from_static(b"late"))
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Cancelled), "{err:?}");
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Cancelled
    );
    server2.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_approve_after_fail_fast_other_node_is_409() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let def = WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .node("boom", "wait")
        .build()
        .unwrap();
    let id = client.start(def).await.expect("start");
    let (hold_tok, boom_tok) = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let hold = v.resume_token(&NodeId::new("hold")).cloned();
                let boom = v.resume_token(&NodeId::new("boom")).cloned();
                if let (Some(h), Some(b)) = (hold, boom) {
                    return (h, b);
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both waiting");
    client.reject(boom_tok).await.expect("fail-fast boom");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state == ExecutionState::Failed {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed");
    let err = client
        .approve(hold_tok, Bytes::from_static(b"nope"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::Cancelled | KeelClientError::UnknownToken
        ),
        "approve must not succeed a fail-fast-cancelled wait: {err:?}"
    );
    let snap = client.inspect(&id).await.expect("after");
    assert_eq!(snap.state, ExecutionState::Failed);
    assert!(!matches!(
        snap.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Succeeded)
    ));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_duplicate_approve_is_noop() {
    let rt = runtime_with_next();
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    client
        .approve(token.clone(), Bytes::from_static(b"gate"))
        .await
        .expect("first");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state == ExecutionState::Succeeded {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded + reap");
    client
        .approve(token, Bytes::from_static(b"gate"))
        .await
        .expect("duplicate after terminal handle drop is 200 noop");
    assert_eq!(
        client.inspect(&id).await.expect("end").state,
        ExecutionState::Succeeded
    );
    server.abort();
}

/// After inspect sees Succeeded the handle is dropped. Same-token approve
/// must stay 200 noop (not Apply/400). Live-park drop stays 409.
#[tokio::test(flavor = "current_thread")]
async fn client_approve_after_terminal_handle_drop_is_200_noop() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("next", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"next")) }
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    client
        .approve(token.clone(), Bytes::from_static(b"gate"))
        .await
        .expect("first");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state == ExecutionState::Succeeded {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reaped");
    client
        .approve(token, Bytes::from_static(b"gate"))
        .await
        .expect("200 after terminal handle drop");
    assert_eq!(
        client.inspect(&id).await.expect("end").state,
        ExecutionState::Succeeded
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no second downstream");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn client_inspect_during_approve_does_not_double_apply() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("next", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"next")) }
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client.start(wait_then_next()).await.expect("start");
    let token = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                    return t.clone();
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token");
    let inspect_client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let (view, approved) = tokio::join!(
        inspect_client.inspect(&id),
        client.approve(token, Bytes::from_static(b"gate"))
    );
    view.expect("inspect during approve");
    approved.expect("approve");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id).await.expect("i").state.is_terminal() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("done");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no double-apply");
    server.abort();
}

/// Dropping the HTTP server drops App → remaining handles Drop-cancel live parks.
#[tokio::test(flavor = "current_thread")]
async fn client_drop_http_server_after_start_cancels_wait() {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(Runtime::builder().clock(clock).store(store.clone()).build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if v.state == ExecutionState::Waiting {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parked");
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
    .expect("server drop must Drop-cancel the held handle");
}

/// N instant HTTP starts, no inspect of those ids, then one more start
/// reaps terminals. Abort cancels only the live park; N stay Succeeded.
#[tokio::test(flavor = "current_thread")]
async fn client_n_instant_http_starts_are_reaped_on_next_start() {
    const N: usize = 8;
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock)
            .store(store.clone())
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let def = WorkflowDefinition::builder("wf")
        .node("next", "next")
        .build()
        .unwrap();
    let mut ids = Vec::new();
    for _ in 0..N {
        ids.push(client.start(def.clone()).await.expect("start"));
    }
    tokio::time::timeout(BOUND, async {
        loop {
            let mut n = 0;
            for id in &ids {
                if let Some(s) = store.get(id).await.unwrap() {
                    if s.state == ExecutionState::Succeeded {
                        n += 1;
                    }
                }
            }
            if n == N {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("N succeeded with no inspect of those ids");
    let park = client
        .start(
            WorkflowDefinition::builder("park")
                .node("hold", "wait")
                .build()
                .unwrap(),
        )
        .await
        .expect("next start reaps terminals");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&park).await {
                if v.state == ExecutionState::Waiting {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("park");
    drop(client);
    server.abort();
    tokio::time::timeout(BOUND, async {
        loop {
            for id in &ids {
                if let Some(s) = store.get(id).await.unwrap() {
                    assert_ne!(
                        s.state,
                        ExecutionState::Cancelled,
                        "reaped terminal must not Drop-cancel"
                    );
                    assert_eq!(s.state, ExecutionState::Succeeded);
                }
            }
            if let Some(s) = store.get(&park).await.unwrap() {
                if s.state == ExecutionState::Cancelled {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("park Cancelled; N still Succeeded");
}

/// After inspect sees Succeeded, reap consumes the handle. Aborting the
/// server must not Cancel a run that already finished.
#[tokio::test(flavor = "current_thread")]
async fn client_start_terminal_survives_server_drop() {
    let store = MemoryStore::new();
    let clock = Arc::new(FakeClock::new());
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock)
            .store(store.clone())
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let id = client
        .start(
            WorkflowDefinition::builder("wf")
                .node("next", "next")
                .build()
                .unwrap(),
        )
        .await
        .expect("start");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                if v.state == ExecutionState::Succeeded {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded");
    drop(client);
    server.abort();
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id).await.unwrap() {
                assert_ne!(
                    s.state,
                    ExecutionState::Cancelled,
                    "reaped terminal must not Drop-cancel"
                );
                if s.state == ExecutionState::Succeeded {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Succeeded must survive server drop");
}

/// Two HTTP servers, one MemoryStore: a new start id is not a steal.
/// Completing the other Runtime's token is ClaimedElsewhere.
#[tokio::test(flavor = "current_thread")]
async fn http_start_second_runtime_new_id_is_not_steal() {
    let store = MemoryStore::new();
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
    let ca = client_at(addr_a);
    let cb = client_at(addr_b);
    let def = WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .build()
        .unwrap();
    let id_a = ca.start(def.clone()).await.expect("start a");
    let id_b = cb.start(def).await.expect("start b");
    assert_ne!(id_a, id_b, "new id is a new execution, not a steal of a");
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(s) = store.get(&id_a).await.unwrap() {
                if s.state == ExecutionState::Waiting {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a persisted");
    let token = ca
        .inspect(&id_a)
        .await
        .expect("inspect a")
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("wait token");
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
    sa.abort();
    sb.abort();
}

/// 0.1%: approve of a token issued for a Running sibling must not unblock
/// a parked wait (same class as complete of a non-wait token).
#[tokio::test(flavor = "current_thread")]
async fn client_approve_issued_token_for_running_node_leaves_wait_parked() {
    let go = Arc::new(tokio::sync::Notify::new());
    let gate = go.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register_fn("slow", move |_ctx: ExecutionContext| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"go"))
                }
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = client_at(addr);
    let def = WorkflowDefinition::builder("wf")
        .node("slow", "slow")
        .node("hold", "wait")
        .build()
        .unwrap();
    let id = client.start(def).await.expect("start");
    let (wait_tok, issued) = tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(v) = client.inspect(&id).await {
                let hold = v.resume_token(&NodeId::new("hold")).cloned();
                let slow_run = matches!(
                    v.node(&NodeId::new("slow")).map(|n| &n.state),
                    Some(InspectNodeState::Running { .. })
                );
                if let (Some(wait), true) = (hold, slow_run) {
                    return (wait, ResumeToken::issue(id.clone(), NodeId::new("slow"), 1));
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed");
    let json = serde_json::to_string(&client.inspect(&id).await.unwrap()).unwrap();
    assert_eq!(
        json.matches(serde_json::to_string(&wait_tok).unwrap().as_str())
            .count(),
        1,
        "{json}"
    );
    assert!(
        !json.contains(&serde_json::to_string(&issued).unwrap()),
        "issued Running token must not be on inspect JSON: {json}"
    );
    let err = client
        .approve(issued, Bytes::from_static(b"nope"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            KeelClientError::BadRequest | KeelClientError::UnknownToken
        ),
        "approve of a non-wait token must not unblock: {err:?}"
    );
    let still = client.inspect(&id).await.expect("still");
    assert!(matches!(
        still.node(&NodeId::new("hold")).map(|n| &n.state),
        Some(InspectNodeState::Waiting { token, .. }) if token == &wait_tok
    ));
    server.abort();
}
