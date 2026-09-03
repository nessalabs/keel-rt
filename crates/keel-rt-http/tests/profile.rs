//! Release inspect+complete numbers.
//! `cargo test -p keel-rt-http --release --test profile -- --nocapture --test-threads=1`

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, FakeClock, MemoryStore, NodeId, NodeOutcome, NodeState,
    Resume, Runtime, WorkflowDefinition,
};
use keel_rt_http::{serve_ephemeral, CompleteSecret, InspectNodeState, InspectView, KeelClient};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(5);
const SECRET: &str = "test-complete-secret";
const MEDIAN_ITERS: usize = 7;
const N_10K: usize = 10_000;

fn secret() -> CompleteSecret {
    CompleteSecret::new(SECRET).unwrap()
}

fn rss_bytes() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            return kb.saturating_mul(1024);
        }
    }
    0
}

fn thread_count() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("Threads:") {
            return rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
        }
    }
    0
}

fn median_dur(mut xs: Vec<Duration>) -> Duration {
    xs.sort();
    xs[xs.len() / 2]
}

fn ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

fn format_bytes(n: u64) -> String {
    if n >= 1024 * 1024 {
        format!("{:.1} MiB", n as f64 / (1024.0 * 1024.0))
    } else if n >= 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn wait_then_next() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("hold", "wait")
        .node("next", "next")
        .edge("hold", "next")
        .build()
        .unwrap()
}

fn runtime() -> Arc<Runtime> {
    Arc::new(
        Runtime::builder()
            .clock(Arc::new(FakeClock::new()))
            .store(MemoryStore::new())
            .register_fn("next", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn profile_inspect_complete_release() {
    let rt = runtime();
    let handle = rt.start(wait_then_next()).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .unwrap(),
        ExecutionState::Waiting
    );
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();

    let view = client.inspect(&id).await.unwrap();
    let view_json = serde_json::to_vec(&view).unwrap();
    let snap = handle.inspect().await;
    let snap_json = serde_json::to_vec(&snap).unwrap();
    let wait_only: Vec<_> = view
        .nodes
        .iter()
        .filter(|n| matches!(n.state, InspectNodeState::Waiting { .. }))
        .collect();
    eprintln!(
        "profile json InspectView={}B snapshot={}B wait_nodes={}",
        view_json.len(),
        snap_json.len(),
        wait_only.len(),
    );
    eprintln!(
        "profile json InspectView body={}",
        String::from_utf8_lossy(&view_json)
    );

    let mut one = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let v = client.inspect(&id).await.unwrap();
        assert_eq!(v.state, ExecutionState::Waiting);
        one.push(t.elapsed());
    }

    let threads0 = thread_count();
    let rss0 = rss_bytes();
    let t10k = Instant::now();
    for _ in 0..N_10K {
        let v = client.inspect(&id).await.unwrap();
        assert_eq!(v.state, ExecutionState::Waiting);
    }
    let elapsed_10k = t10k.elapsed();
    let rss1 = rss_bytes();
    let threads1 = thread_count();

    let mut rt_inspect = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let _ = rt.inspect(&id).await.unwrap();
        rt_inspect.push(t.elapsed());
    }
    let t_rt_10k = Instant::now();
    for _ in 0..N_10K {
        let _ = rt.inspect(&id).await.unwrap();
    }
    let rt_10k = t_rt_10k.elapsed();

    let mut handle_inspect = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let _ = handle.inspect().await;
        handle_inspect.push(t.elapsed());
    }
    let t_h_10k = Instant::now();
    for _ in 0..N_10K {
        let _ = handle.inspect().await;
    }
    let handle_10k = t_h_10k.elapsed();

    assert!(
        threads1 <= threads0,
        "hang-bound must not add a thread per inspect: {threads0}→{threads1}"
    );
    eprintln!(
        "profile inspect client n=1 median={} n={N_10K} total={} per={} rss_before={} rss_after={} d_rss={} threads {}→{}",
        ms(median_dur(one)),
        ms(elapsed_10k),
        ms(elapsed_10k / N_10K as u32),
        format_bytes(rss0),
        format_bytes(rss1),
        format_bytes(rss1.saturating_sub(rss0)),
        threads0,
        threads1
    );
    eprintln!(
        "profile inspect kernel Runtime::inspect n=1 median={} n={N_10K} total={} per={} handle.inspect n=1 median={} n={N_10K} total={} per={}",
        ms(median_dur(rt_inspect)),
        ms(rt_10k),
        ms(rt_10k / N_10K as u32),
        ms(median_dur(handle_inspect)),
        ms(handle_10k),
        ms(handle_10k / N_10K as u32),
    );

    handle.cancel().await;
    server.abort();

    let mut http_rt = Vec::new();
    let mut inproc_rt = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let rt = runtime();
        let handle = rt.start(wait_then_next()).unwrap();
        let id = handle.execution_id().clone();
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .unwrap();
        let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
        let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
        let t = Instant::now();
        let view = client.inspect(&id).await.unwrap();
        let token = view.resume_token(&NodeId::new("hold")).cloned().unwrap();
        client
            .complete(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
            ExecutionState::Succeeded
        );
        http_rt.push(t.elapsed());
        server.abort();
    }
    for _ in 0..MEDIAN_ITERS {
        let rt = runtime();
        let handle = rt.start(wait_then_next()).unwrap();
        tokio::time::timeout(BOUND, handle.wait_stable())
            .await
            .unwrap();
        let t = Instant::now();
        let token = handle
            .inspect()
            .await
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .unwrap();
        rt.complete(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::time::timeout(BOUND, handle.wait()).await.unwrap(),
            ExecutionState::Succeeded
        );
        inproc_rt.push(t.elapsed());
    }
    eprintln!(
        "profile roundtrip inspect+complete HTTP median={} in-process handle.inspect+Runtime::complete median={} (n={MEDIAN_ITERS})",
        ms(median_dur(http_rt)),
        ms(median_dur(inproc_rt)),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn profile_start_approve_release() {
    let rt = runtime();
    let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
    let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
    let body = keel_rt_http::StartBody::from(&wait_then_next());
    eprintln!(
        "profile json StartBody={}B",
        serde_json::to_vec(&body).unwrap().len()
    );

    let mut http_start = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let id = client.start(wait_then_next()).await.unwrap();
        http_start.push(t.elapsed());
        let _ = id;
    }
    let mut inproc_start = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let handle = rt.start(wait_then_next()).unwrap();
        inproc_start.push(t.elapsed());
        handle.cancel().await;
    }
    eprintln!(
        "profile start HTTP median={} in-process Runtime::start median={} (n={MEDIAN_ITERS})",
        ms(median_dur(http_start)),
        ms(median_dur(inproc_start)),
    );

    let mut round = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let t = Instant::now();
        let id = client.start(wait_then_next()).await.unwrap();
        let token = loop {
            if let Ok(v) = client.inspect(&id).await {
                if let Some(tok) = v.resume_token(&NodeId::new("hold")) {
                    break tok.clone();
                }
            }
            tokio::task::yield_now().await;
        };
        client
            .approve(token, Bytes::from_static(b"gate"))
            .await
            .unwrap();
        loop {
            let v = client.inspect(&id).await.unwrap();
            if v.state.is_terminal() {
                assert_eq!(v.state, ExecutionState::Succeeded);
                break;
            }
            tokio::task::yield_now().await;
        }
        round.push(t.elapsed());
    }
    eprintln!(
        "profile start+inspect+approve HTTP median={} (n={MEDIAN_ITERS})",
        ms(median_dur(round)),
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn profile_cancel_release() {
    let mut http_cancel = Vec::new();
    let mut inproc_cancel = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        let rt = runtime();
        let (addr, server) = serve_ephemeral(rt.clone(), secret()).await.unwrap();
        let client = KeelClient::new(format!("http://{addr}"), secret()).unwrap();
        let id = client
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        tokio::time::timeout(BOUND, async {
            loop {
                if client.inspect(&id).await.unwrap().state == ExecutionState::Waiting {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let t = Instant::now();
        client.cancel(&id).await.unwrap();
        http_cancel.push(t.elapsed());
        server.abort();
    }
    for _ in 0..MEDIAN_ITERS {
        let rt = runtime();
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
        let t = Instant::now();
        rt.cancel(&id).await.unwrap();
        inproc_cancel.push(t.elapsed());
    }
    eprintln!(
        "profile cancel HTTP median={} in-process Runtime::cancel median={} (n={MEDIAN_ITERS})",
        ms(median_dur(http_cancel)),
        ms(median_dur(inproc_cancel)),
    );
}

#[test]
fn inspect_view_json_is_not_full_snapshot() {
    let token = keel_rt::ResumeToken::issue(
        keel_rt::ExecutionId::parse("exec-1").unwrap(),
        NodeId::new("hold"),
        1,
    );
    let mut snap = keel_rt::ExecutionSnapshot {
        schema_version: keel_rt::SCHEMA_VERSION,
        revision: 1,
        execution_id: keel_rt::ExecutionId::parse("exec-1").unwrap(),
        workflow_id: keel_rt::WorkflowId::new("wf"),
        state: ExecutionState::Waiting,
        nodes: Default::default(),
        node_order: vec![NodeId::new("hold")],
        definition_hash: Default::default(),
    };
    snap.nodes.insert(
        NodeId::new("hold"),
        keel_rt::NodeSnapshot {
            state: NodeState::Waiting {
                token: token.clone(),
                attempt: 1,
            },
            output: Some(Bytes::from(vec![0u8; 64 * 1024])),
            attempt: 1,
            resume_token: Some(token),
            last_error: None,
        },
    );
    let view = InspectView::from_snapshot(&snap);
    let view_n = serde_json::to_vec(&view).unwrap().len();
    let snap_n = serde_json::to_vec(&snap).unwrap().len();
    eprintln!("profile json_lock InspectView={view_n}B snapshot={snap_n}B");
    assert!(
        view_n < snap_n,
        "InspectView must omit fat snapshot fields: view={view_n} snap={snap_n}"
    );
    assert!(
        view_n < 4096,
        "wait-node InspectView JSON should stay small without outputs: {view_n}"
    );
}
