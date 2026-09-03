//! Public KeelClient against the existing POST /complete server.
//! Same protocol as `complete.rs`. No wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, FakeClock, MemoryStore, NodeId, NodeOutcome, Resume, Runtime,
    StateStore, WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteBody, CompleteSecret, Decision, KeelClient, KeelClientError,
    COMPLETE_HANG_BOUND, COMPLETE_SECRET_HEADER, MAX_COMPLETE_BODY,
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
    let secret_hdr = format!("{COMPLETE_SECRET_HEADER}: {SECRET}");
    assert!(
        headers.to_ascii_lowercase().contains(&secret_hdr),
        "missing {COMPLETE_SECRET_HEADER}: {headers}"
    );
    assert!(
        !headers.to_ascii_lowercase().contains("authorization:"),
        "client sends X-Keel-Complete, not Bearer: {headers}"
    );
    assert!(
        !headers.contains('?') && !raw.contains(&format!("?{COMPLETE_SECRET_HEADER}")),
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
    tokio::time::advance(COMPLETE_HANG_BOUND + Duration::from_millis(1)).await;
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
