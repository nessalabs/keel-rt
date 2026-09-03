//! Public KeelClient against the existing POST /complete server.
//! Same protocol as `complete.rs`. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionId, ExecutionState, FakeClock, MemoryStore, NodeId, NodeOutcome,
    NodeState, Resume, ResumeToken, Runtime, StateStore, WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteBody, CompleteSecret, Decision, InspectNodeState, KeelClient,
    KeelClientError, CLAIMED_ELSEWHERE, HANG_BOUND, MAX_COMPLETE_BODY, SECRET_HEADER,
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
    let huge = Bytes::from(vec![1u8; MAX_COMPLETE_BODY]);
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
