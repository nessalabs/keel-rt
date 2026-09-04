//! CI lock for `examples/sdk_loop.rs`. Same public types, FakeClock, no wall sleep.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, Executor, ExecutorId, FakeClock, NodeId, NodeOutcome,
    Runtime, WorkflowDefinition, WAIT_ID,
};
use keel_rt_http::{
    serve_ephemeral, CompleteSecret, InspectNodeState, KeelClient, KeelClientError,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "sdk-loop-secret";

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
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

struct Write {
    hits: Arc<AtomicU32>,
    last: Arc<Mutex<Bytes>>,
}

impl Executor for Write {
    fn id(&self) -> ExecutorId {
        ExecutorId::new("write")
    }

    fn execute<'a>(
        &'a self,
        ctx: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
        let hits = self.hits.clone();
        let last = self.last.clone();
        Box::pin(async move {
            hits.fetch_add(1, Ordering::SeqCst);
            let out = ctx
                .inputs
                .get(&NodeId::new("hold"))
                .cloned()
                .unwrap_or_default();
            *last.lock().expect("write") = out.clone();
            NodeOutcome::Succeeded(out)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_loop_approve_then_cancel_is_409() {
    let writes = Arc::new(AtomicU32::new(0));
    let last = Arc::new(Mutex::new(Bytes::new()));
    let rt = Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .register(Research)
            .register(Write {
                hits: writes.clone(),
                last: last.clone(),
            })
            .build(),
    );
    let (addr, server) = serve_ephemeral(rt, secret()).await.unwrap();
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();

    let catalog = client.executors().await.expect("catalog");
    assert!(
        catalog.iter().any(|id| id.as_str() == "research"),
        "{catalog:?}"
    );
    assert!(
        catalog.iter().any(|id| id.as_str() == WAIT_ID),
        "{catalog:?}"
    );

    let err = client
        .start(
            WorkflowDefinition::builder("missing")
                .node("x", "not-on-this-engine")
                .build()
                .unwrap(),
        )
        .await
        .unwrap_err();
    match &err {
        KeelClientError::Unregistered { executors } => {
            assert!(
                executors
                    .iter()
                    .any(|id| id.as_str() == "not-on-this-engine"),
                "{executors:?}"
            );
        }
        other => panic!("expected Unregistered with id, got {other:?}"),
    }
    assert!(err.to_string().contains("not-on-this-engine"), "{err}");

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
    assert!(parked.resume_token(&NodeId::new("research")).is_none());
    assert!(parked.resume_token(&NodeId::new("write")).is_none());
    let token = parked
        .resume_token(&NodeId::new("hold"))
        .cloned()
        .expect("token only on Waiting");
    let hold_json =
        serde_json::to_value(&parked.node(&NodeId::new("hold")).unwrap().state).unwrap();
    assert!(
        hold_json.get("output").is_none(),
        "Waiting must not own output: {hold_json}"
    );

    client
        .approve(token, Bytes::from_static(b"human-ok"))
        .await
        .expect("approve");
    let done = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id).await.expect("i");
            if v.state == ExecutionState::Succeeded {
                assert_eq!(
                    v.node(&NodeId::new("write")).map(|n| &n.state),
                    Some(&InspectNodeState::Succeeded {
                        output: Bytes::from_static(b"human-ok"),
                    })
                );
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("succeeded");
    let wire = serde_json::to_value(&done).unwrap();
    let write = wire["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == "write")
        .unwrap();
    assert!(
        write["state"]["output"].is_string(),
        "write Succeeded output is one base64 field: {write}"
    );
    assert_eq!(
        done.node(&NodeId::new("write")).map(|n| &n.state),
        Some(&InspectNodeState::Succeeded {
            output: Bytes::from_static(b"human-ok"),
        })
    );
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(last.lock().unwrap().as_ref(), b"human-ok");

    let id2 = client.start(dag()).await.expect("second");
    let token2 = tokio::time::timeout(BOUND, async {
        loop {
            let v = client.inspect(&id2).await.expect("i");
            if let Some(t) = v.resume_token(&NodeId::new("hold")) {
                return t.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("token2");
    client.cancel(&id2).await.expect("cancel");
    tokio::time::timeout(BOUND, async {
        loop {
            if client.inspect(&id2).await.expect("i").state == ExecutionState::Cancelled {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Cancelled");
    let err = client
        .approve(token2, Bytes::from_static(b"too-late"))
        .await
        .unwrap_err();
    assert!(matches!(err, KeelClientError::Cancelled), "{err:?}");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    server.abort();
}
