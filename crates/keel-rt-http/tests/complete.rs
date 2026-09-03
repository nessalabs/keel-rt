//! Bound localhost: POST /complete → Runtime::complete.
//! Shared secret required. Default bind is 127.0.0.1.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, FakeClock, MemoryStore, NodeId, NodeOutcome, Resume, Runtime,
    StateStore, WorkflowDefinition,
};
use keel_rt_http::{
    serve_ephemeral, CompleteBody, CompleteSecret, CLAIMED_ELSEWHERE, MAX_COMPLETE_BODY,
    SECRET_HEADER,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "test-complete-secret";

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
}

async fn post_raw(addr: std::net::SocketAddr, extra_headers: &str, body: &[u8]) -> u16 {
    post_raw_full(addr, extra_headers, body).await.0
}

async fn post_raw_full(
    addr: std::net::SocketAddr,
    extra_headers: &str,
    body: &[u8],
) -> (u16, String) {
    let req = format!(
        "POST /complete HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
        body.len()
    );
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status");
    (status, text)
}

async fn post_complete_auth(
    addr: std::net::SocketAddr,
    body: &CompleteBody,
    auth: Option<&str>,
) -> u16 {
    let json = serde_json::to_vec(body).unwrap();
    let extra = match auth {
        Some(s) => format!("{SECRET_HEADER}: {s}\r\n"),
        None => String::new(),
    };
    post_raw(addr, &extra, &json).await
}

async fn post_complete(addr: std::net::SocketAddr, body: &CompleteBody) -> u16 {
    post_complete_auth(addr, body, Some(SECRET)).await
}

fn wait_then_next() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .node("next", "next")
        .edge("hold", "next")
        .build()
        .unwrap()
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

#[tokio::test(flavor = "current_thread")]
async fn serve_ephemeral_binds_loopback_not_unspecified() {
    let rt = Arc::new(Runtime::builder().build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    assert!(addr.ip().is_loopback(), "{addr}");
    assert!(!addr.ip().is_unspecified(), "{addr}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_without_secret_is_401() {
    let rt = Arc::new(Runtime::builder().build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let status = post_complete_auth(
        addr,
        &CompleteBody {
            token,
            resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        },
        None,
    )
    .await;
    assert_eq!(status, 401);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_wrong_secret_is_401() {
    let rt = Arc::new(
        Runtime::builder()
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build(),
    );
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let status = post_complete_auth(
        addr,
        &CompleteBody {
            token,
            resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        },
        Some("wrong-secret"),
    )
    .await;
    assert_eq!(status, 401);
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_query_secret_is_still_401() {
    let rt = Arc::new(Runtime::builder().build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let json = serde_json::to_vec(&CompleteBody {
        token,
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
    })
    .unwrap();
    let req = format!(
        "POST /complete?{SECRET_HEADER}={SECRET} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        json.len()
    );
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(&json).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let status = String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap();
    assert_eq!(status, 401, "secret on the query string is not accepted");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_complete_unblocks_wait_node() {
    let rt = Arc::new(
        Runtime::builder()
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build(),
    );
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let status = tokio::time::timeout(
        BOUND,
        post_complete(
            addr,
            &CompleteBody {
                token,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
            },
        ),
    )
    .await
    .expect("http");
    assert_eq!(status, 200);
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait())
            .await
            .expect("wait"),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_unknown_token_is_404() {
    let rt = Arc::new(Runtime::builder().build());
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-missing").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let status = post_complete(
        addr,
        &CompleteBody {
            token,
            resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        },
    )
    .await;
    assert_eq!(status, 404);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_after_cancel_is_409_does_not_revive() {
    let rt = Arc::new(Runtime::builder().build());
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
    handle.cancel().await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let status = post_complete(
        addr,
        &CompleteBody {
            token,
            resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"late"))),
        },
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(handle.inspect().await.state, ExecutionState::Cancelled);
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Cancelled
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_claimed_elsewhere_is_423_locked_not_409() {
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
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let (addr, server) = serve_ephemeral(other, secret()).await.unwrap();
    let body = CompleteBody {
        token,
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"steal"))),
    };
    let json = serde_json::to_vec(&body).unwrap();
    let extra = format!("{SECRET_HEADER}: {SECRET}\r\n");
    let (status, text) = post_raw_full(addr, &extra, &json).await;
    assert_eq!(status, CLAIMED_ELSEWHERE, "{text}");
    assert_eq!(status, 423);
    assert_ne!(status, 400);
    assert_ne!(status, 401);
    assert_ne!(status, 404);
    assert_ne!(status, 409);
    assert!(
        text.contains("claimed_elsewhere"),
        "body must name claimed_elsewhere: {text}"
    );
    assert!(
        !text.to_ascii_lowercase().contains("cancelled"),
        "body must be distinguishable from Cancelled: {text}"
    );
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    server.abort();
    handle.cancel().await;
}

#[tokio::test(flavor = "current_thread")]
async fn post_duplicate_complete_is_200_noop() {
    let rt = Arc::new(Runtime::builder().build());
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
    let body = CompleteBody {
        token,
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"once"))),
    };
    assert_eq!(post_complete(addr, &body).await, 200);
    assert_eq!(post_complete(addr, &body).await, 200);
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_oversized_body_is_413_does_not_complete() {
    let rt = Arc::new(
        Runtime::builder()
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build(),
    );
    let (handle, _token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let huge = vec![b'x'; MAX_COMPLETE_BODY + 1];
    let extra = format!("{SECRET_HEADER}: {SECRET}\r\n");
    let status = post_raw(addr, &extra, &huge).await;
    assert!(status == 413 || status == 400, "got {status}");
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    handle.cancel().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn two_http_completes_one_token_downstream_runs_once() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Arc::new(
        Runtime::builder()
            .register_fn("next", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"next")) }
            })
            .build(),
    );
    let (handle, token) = park_wait(&rt).await;
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let body = CompleteBody {
        token,
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
    };
    let (a, b) = tokio::join!(post_complete(addr, &body), post_complete(addr, &body));
    assert!(a == 200 || b == 200, "a={a} b={b}");
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
async fn post_reinvoke_then_stale_complete_is_404() {
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = Arc::new(
        Runtime::builder()
            .register_fn("wait", move |ctx: ExecutionContext| {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if n < 3 {
                        NodeOutcome::Waiting {
                            token: ctx.resume_token,
                        }
                    } else {
                        NodeOutcome::Succeeded(Bytes::from_static(b"done"))
                    }
                }
            })
            .build(),
    );
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
    let old = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("token");
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    assert_eq!(
        post_complete(
            addr,
            &CompleteBody {
                token: old.clone(),
                resume: Resume::Reinvoke,
            },
        )
        .await,
        200
    );
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = handle.inspect().await;
            if matches!(
                snap.node(&NodeId::new("hold")).unwrap().state,
                keel_rt::NodeState::Waiting { .. }
            ) && snap
                .node(&NodeId::new("hold"))
                .unwrap()
                .resume_token
                .as_ref()
                != Some(&old)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reinvoke parks with a new token");
    assert_eq!(
        post_complete(
            addr,
            &CompleteBody {
                token: old,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"stale"))),
            },
        )
        .await,
        404
    );
    assert_eq!(handle.inspect().await.state, ExecutionState::Waiting);
    let fresh = handle
        .inspect()
        .await
        .node(&NodeId::new("hold"))
        .unwrap()
        .resume_token
        .clone()
        .expect("fresh");
    assert_eq!(
        post_complete(
            addr,
            &CompleteBody {
                token: fresh,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
            },
        )
        .await,
        200
    );
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
        ExecutionState::Succeeded
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn http_complete_n_parked_waits_reports_ms() {
    const N: usize = 32;
    let rt = Arc::new(Runtime::builder().build());
    let mut handles = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..N {
        let h = rt
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(BOUND, h.wait_stable()).await.unwrap();
        let t = h
            .inspect()
            .await
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .unwrap();
        tokens.push(t);
        handles.push(h);
    }
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let started = Instant::now();
    for token in tokens {
        let status = post_complete(
            addr,
            &CompleteBody {
                token,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"g"))),
            },
        )
        .await;
        assert_eq!(status, 200);
    }
    for h in handles {
        assert_eq!(
            tokio::time::timeout(BOUND, h.wait()).await.unwrap(),
            ExecutionState::Succeeded
        );
    }
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    eprintln!("http_complete_n_parked n={N} elapsed_ms={ms:.3}");
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn post_bearer_secret_is_200() {
    let rt = Arc::new(Runtime::builder().build());
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
    let json = serde_json::to_vec(&CompleteBody {
        token,
        resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
    })
    .unwrap();
    let status = post_raw(addr, &format!("Authorization: Bearer {SECRET}\r\n"), &json).await;
    assert_eq!(status, 200);
    tokio::time::timeout(BOUND, handle.wait()).await.unwrap();
    server.abort();
}
