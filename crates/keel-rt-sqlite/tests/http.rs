//! HTTP start + inspect + complete against a real sqlite file.
//! Lives here so `keel-rt-http` src never names SqliteStore.

use bytes::Bytes;
use keel_rt::testing::FakeClock;
use keel_rt::{
    ExecutionId, ExecutionState, NodeId, NodeOutcome, Resume, Runtime, StateStore,
    WorkflowDefinition,
};
use keel_rt_http::{serve_ephemeral, CompleteSecret, KeelClient, KeelClientError};
use keel_rt_sqlite::SqliteStore;
use std::path::PathBuf;
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
