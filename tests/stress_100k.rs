//! 10k–100k scale pack. Shared FunctionExecutor, silent sink (no event log).
//!
//! Debug first; timeout 60s so a hang is a failure.
//!
//! ```text
//! cargo test --test stress_100k -- --nocapture --test-threads=1
//! cargo test --release --test stress_100k -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    ExecutionContext, ExecutionState, Executor, FunctionExecutor, NodeId, NodeOutcome,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 60s is enough for chain/diamonds; wide_100k AND-join is ~57s in debug
/// on this machine, so the bound is 90s to avoid a load flake.
const BOUND: Duration = Duration::from_secs(90);

fn instant_ok() -> Arc<dyn Executor> {
    Arc::new(FunctionExecutor::new("ok", |_ctx: ExecutionContext| async {
        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
    }))
}

fn succeed(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(id.as_bytes().to_vec()))
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

fn profile_name() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// 1 source → `fan` independents → 1 join.
fn wide_graph(fan: usize, concurrency: usize) -> WorkflowTest {
    let ok = instant_ok();
    let mut test = WorkflowTest::with_graph_capacity(fan + 2, fan * 2)
        .silent()
        .concurrency(concurrency)
        .node("src", succeed("src"))
        .node("join", succeed("join"));
    for i in 0..fan {
        let id = format!("w{i}");
        test = test
            .node_arc(&id, ok.clone())
            .edge("src", &id)
            .edge(&id, "join");
    }
    test
}

fn chain_graph(n: usize) -> WorkflowTest {
    let ok = instant_ok();
    let mut test = WorkflowTest::with_graph_capacity(n, n.saturating_sub(1))
        .silent()
        .concurrency(8)
        .node("n0", succeed("n0"));
    for i in 1..n {
        let prev = format!("n{}", i - 1);
        let id = format!("n{i}");
        if i + 1 == n {
            test = test.node(&id, succeed(&id)).edge(&prev, &id);
        } else {
            test = test.node_arc(&id, ok.clone()).edge(&prev, &id);
        }
    }
    test
}

/// `diamonds` × Research→{Sum,Crit}→Writer, chained through the writer.
fn diamond_graph(diamonds: usize) -> WorkflowTest {
    let ok = instant_ok();
    let nodes = diamonds * 4;
    let mut test = WorkflowTest::with_graph_capacity(nodes, diamonds * 5)
        .silent()
        .concurrency(64);
    for i in 0..diamonds {
        let r = format!("r{i}");
        let s = format!("s{i}");
        let c = format!("c{i}");
        let w = format!("w{i}");
        let last = i + 1 == diamonds;
        test = test
            .node_arc(&r, ok.clone())
            .node_arc(&s, ok.clone())
            .node_arc(&c, ok.clone());
        if last {
            test = test.node(&w, succeed(&w));
        } else {
            test = test.node_arc(&w, ok.clone());
        }
        test = test
            .edge(&r, &s)
            .edge(&r, &c)
            .edge(&s, &w)
            .edge(&c, &w);
        if i > 0 {
            test = test.edge(&format!("w{}", i - 1), &r);
        }
    }
    test
}

async fn run_scale(label: &str, nodes: usize, test: WorkflowTest) -> Duration {
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .unwrap_or_else(|_| panic!("{label} timed out after {BOUND:?} (nodes={nodes})"));
    let elapsed = started.elapsed();
    assert_eq!(
        run.execution_state().await,
        ExecutionState::Succeeded,
        "{label}"
    );
    eprintln!(
        "stress_100k {label} nodes={nodes} elapsed={} profile={}",
        format_ms(elapsed),
        profile_name()
    );
    elapsed
}

#[tokio::test(flavor = "current_thread")]
async fn wide_100k() {
    let fan = 99_998usize;
    let run_test = wide_graph(fan, 64);
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, run_test.run())
        .await
        .expect("wide_100k timed out");
    let elapsed = started.elapsed();
    eprintln!(
        "stress_100k wide_100k nodes={} elapsed={} profile={}",
        fan + 2,
        format_ms(elapsed),
        profile_name()
    );
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(
        run.inputs("join").await.len(),
        fan,
        "join must see every predecessor (AND-join tax)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn chain_10k() {
    let n = 10_000usize;
    let elapsed = run_scale("chain_10k", n, chain_graph(n)).await;
    let _ = elapsed;
}

#[tokio::test(flavor = "current_thread")]
async fn chain_25k() {
    let n = 25_000usize;
    run_scale("chain_25k", n, chain_graph(n)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn chain_100k() {
    let n = 100_000usize;
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, chain_graph(n).run())
        .await
        .expect("chain_100k timed out — largest linear N that fits is documented in benches/BASELINE.md");
    let elapsed = started.elapsed();
    eprintln!(
        "stress_100k chain_100k nodes={n} elapsed={} profile={}",
        format_ms(elapsed),
        profile_name()
    );
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let last = format!("n{}", n - 1);
    let inputs = run.inputs(&last).await;
    assert_eq!(inputs.len(), 1);
    assert!(inputs.contains_key(&NodeId::new(format!("n{}", n - 2))));
}

#[tokio::test(flavor = "current_thread")]
async fn diamonds_100k() {
    let diamonds = 25_000usize;
    let nodes = diamonds * 4;
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, diamond_graph(diamonds).run())
        .await
        .expect("diamonds_100k timed out");
    let elapsed = started.elapsed();
    eprintln!(
        "stress_100k diamonds_100k diamonds={diamonds} nodes={nodes} elapsed={} profile={}",
        format_ms(elapsed),
        profile_name()
    );
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("w24999").await.len(), 2);
}
