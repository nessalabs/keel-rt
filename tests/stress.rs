//! Size pack + kernel microbenches.
//!
//! ScriptedExecutor succeeds immediately (zero user work) except
//! cancel-under-load, which hangs until cancel.
//!
//! Kernel medians are printed by `kernel_median_benches` (current_thread,
//! `--test-threads=1`). See `benches/BASELINE.md`.
//!
//! This-run medians (debug, n=7):
//!   wide: 38.608ms → 4.581ms (−88.1%)
//!   chain: 10.386ms → 1.870ms (−82.0%)
//!   diamond_10k: >30s → 148.668ms (−99.5%+)
//!   apply_only: 900.569ms → 14.479ms (−98.4%)
//!
//! ```text
//! cargo test --test stress -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use keel_rt::{AcceptPolicy, ApplyCmd, Execution};
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    ExecutionContext, ExecutionState, FunctionExecutor, NodeId, NodeOutcome, NodeState,
    Timestamp, WorkflowDefinition, DEFAULT_CANCEL_BOUND,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(30);
const MEDIAN_ITERS: usize = 7;

fn succeed(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(id.as_bytes().to_vec()))
}

fn median_dur(mut xs: Vec<Duration>) -> Duration {
    xs.sort();
    xs[xs.len() / 2]
}

fn wide_test(n: usize) -> WorkflowTest {
    let mut test = WorkflowTest::new()
        .concurrency(32)
        .node("src", succeed("src"))
        .node("join", succeed("join"));
    for i in 0..n {
        let id = format!("w{i}");
        test = test
            .node(&id, succeed(&id))
            .edge("src", &id)
            .edge(&id, "join");
    }
    test
}

fn chain_test(n: usize) -> WorkflowTest {
    let mut test = WorkflowTest::new().concurrency(8).node("n0", succeed("n0"));
    for i in 1..n {
        let prev = format!("n{}", i - 1);
        let id = format!("n{i}");
        test = test.node(&id, succeed(&id)).edge(&prev, &id);
    }
    test
}

/// 2500 sequential Research→{Sum,Crit}→Writer diamonds (10_000 nodes).
fn diamond_10k_test() -> WorkflowTest {
    let diamonds = 2500usize;
    let mut test = WorkflowTest::new().concurrency(32);
    for i in 0..diamonds {
        let r = format!("r{i}");
        let s = format!("s{i}");
        let c = format!("c{i}");
        let w = format!("w{i}");
        test = test
            .node(&r, succeed(&r))
            .node(&s, succeed(&s))
            .node(&c, succeed(&c))
            .node(&w, succeed(&w))
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

fn diamond_def(diamonds: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("diamond-apply");
    for i in 0..diamonds {
        let r = format!("r{i}");
        let s = format!("s{i}");
        let c = format!("c{i}");
        let w = format!("w{i}");
        b = b
            .node(r.as_str(), "ok")
            .node(s.as_str(), "ok")
            .node(c.as_str(), "ok")
            .node(w.as_str(), "ok")
            .edge(r.as_str(), s.as_str())
            .edge(r.as_str(), c.as_str())
            .edge(s.as_str(), w.as_str())
            .edge(c.as_str(), w.as_str());
        if i > 0 {
            b = b.edge(format!("w{}", i - 1).as_str(), r.as_str());
        }
    }
    b.build().expect("diamond definition")
}

fn apply_only_drive(def: WorkflowDefinition) {
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    let mut effect = ex.apply(ApplyCmd::Start, &p, now).unwrap();
    let mut ready: VecDeque<_> = effect.newly_runnable.into();
    while let Some(id) = ready.pop_front() {
        effect = ex
            .apply(ApplyCmd::StartNode { node_id: id.clone() }, &p, now)
            .unwrap();
        ready.extend(effect.newly_runnable);
        effect = ex
            .apply(
                ApplyCmd::FinishNode {
                    node_id: id,
                    attempt: 1,
                    outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
                },
                &p,
                now,
            )
            .unwrap();
        ready.extend(effect.newly_runnable);
    }
    assert_eq!(ex.state(), ExecutionState::Succeeded);
}

async fn time_run(test: WorkflowTest) -> Duration {
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("kernel bench timed out");
    let elapsed = started.elapsed();
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    elapsed
}

#[tokio::test(flavor = "current_thread")]
async fn wide_fan_out_256() {
    let started = Instant::now();
    let n = 256usize;
    let run = tokio::time::timeout(BOUND, wide_test(n).run())
        .await
        .expect("wide fan-out timed out");
    let elapsed = started.elapsed();
    eprintln!("stress wide_fan_out nodes={} elapsed={elapsed:?}", n + 2);

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("join").await.len(), n, "join sees every fan-out output");
}

#[tokio::test(flavor = "current_thread")]
async fn deep_chain_128() {
    let started = Instant::now();
    let n = 128usize;
    let run = tokio::time::timeout(BOUND, chain_test(n).run())
        .await
        .expect("deep chain timed out");
    let elapsed = started.elapsed();
    eprintln!("stress deep_chain nodes={n} elapsed={elapsed:?}");

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let last = format!("n{}", n - 1);
    let inputs = run.inputs(&last).await;
    assert_eq!(inputs.len(), 1, "last node sees only its predecessor");
    assert!(inputs.contains_key(&NodeId::new(format!("n{}", n - 2))));
}

#[tokio::test(flavor = "current_thread")]
async fn diamond_10k() {
    let started = Instant::now();
    let run = tokio::time::timeout(BOUND, diamond_10k_test().run())
        .await
        .expect("diamond_10k timed out");
    let elapsed = started.elapsed();
    eprintln!("stress diamond_10k nodes=10000 elapsed={elapsed:?}");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("w2499").await.len(), 2);
}

#[test]
fn apply_only() {
    let def = diamond_def(2500);
    let started = Instant::now();
    apply_only_drive(def);
    let elapsed = started.elapsed();
    eprintln!("stress apply_only diamonds=2500 nodes=10000 elapsed={elapsed:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_median_benches() {
    let mut wide = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        wide.push(time_run(wide_test(256)).await);
    }
    let mut chain = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        chain.push(time_run(chain_test(128)).await);
    }
    let mut diamond = Vec::new();
    for _ in 0..MEDIAN_ITERS {
        diamond.push(time_run(diamond_10k_test()).await);
    }
    let mut apply = Vec::new();
    let apply_def = diamond_def(2500);
    for _ in 0..MEDIAN_ITERS {
        let def = apply_def.clone();
        let t = Instant::now();
        apply_only_drive(def);
        apply.push(t.elapsed());
    }
    eprintln!(
        "kernel_median wide_fan_out_256={} deep_chain_128={} diamond_10k={} apply_only={} (n={MEDIAN_ITERS})",
        format_ms(median_dur(wide)),
        format_ms(median_dur(chain)),
        format_ms(median_dur(diamond)),
        format_ms(median_dur(apply)),
    );
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_under_load_64() {
    let started = Instant::now();
    let n = 64usize;
    let mut test = WorkflowTest::new()
        .concurrency(16)
        .cancel_bound(DEFAULT_CANCEL_BOUND);
    for i in 0..n {
        let id = format!("h{i}");
        test = test.node(&id, ScriptedExecutor::new(id.as_str()).hang(false));
    }

    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            let running = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Running { .. }))
                .count();
            if running >= 16 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("never reached 16 running hangs");

    let cancel_at = Instant::now();
    run.cancel().await;
    tokio::time::timeout(DEFAULT_CANCEL_BOUND + Duration::from_millis(200), run.wait_stable())
        .await
        .expect("cancel under load did not finish within cancel bound");
    let cancel_elapsed = cancel_at.elapsed();
    let elapsed = started.elapsed();
    eprintln!(
        "stress cancel_under_load nodes={n} cancel_elapsed={cancel_elapsed:?} elapsed={elapsed:?}"
    );

    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_1_wide_64() {
    let started = Instant::now();
    let n = 64usize;
    let current = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let mut test = WorkflowTest::new().concurrency(1);
    for i in 0..n {
        let id = format!("n{i}");
        let cur = current.clone();
        let mx = max.clone();
        let exec_id = id.clone();
        test = test.executor(
            &id,
            FunctionExecutor::new(exec_id.as_str(), move |_ctx: ExecutionContext| {
                let c = cur.fetch_add(1, Ordering::SeqCst) + 1;
                mx.fetch_max(c, Ordering::SeqCst);
                let cur = cur.clone();
                async move {
                    cur.fetch_sub(1, Ordering::SeqCst);
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }),
        );
    }

    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("concurrency=1 wide timed out");
    let elapsed = started.elapsed();
    let peak = max.load(Ordering::SeqCst);
    eprintln!("stress concurrency_1_wide nodes={n} peak_running={peak} elapsed={elapsed:?}");

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(peak <= 1, "concurrency=1 must never run two nodes, peak={peak}");
}
