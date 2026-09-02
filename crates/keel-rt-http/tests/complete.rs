//! Bound localhost: POST /complete → Runtime::complete.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, NodeId, NodeOutcome, Resume, Runtime, WorkflowDefinition,
};
use keel_rt_http::{serve_ephemeral, CompleteBody};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BOUND: Duration = Duration::from_secs(5);

async fn post_complete(addr: std::net::SocketAddr, body: &CompleteBody) -> u16 {
    let json = serde_json::to_vec(body).unwrap();
    let req = format!(
        "POST /complete HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        json.len()
    );
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(&json).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status");
    status
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
    let handle = rt
        .start(
            WorkflowDefinition::builder("wf")
                .node("hold", "wait")
                .node("next", "next")
                .edge("hold", "next")
                .build()
                .unwrap(),
        )
        .unwrap();
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

    let (addr, server) = serve_ephemeral(rt.clone()).await.unwrap();
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
    let (addr, server) = serve_ephemeral(rt).await.unwrap();
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
