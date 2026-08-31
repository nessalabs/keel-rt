//! Hot-path benches using the **system** allocator.
//!
//! `keel-rt` has no `jemalloc` feature and never sets `#[global_allocator]`.
//! The jemalloc comparison binary lives in `benches/jemalloc_compare/`
//! (unpublished; not part of this crate).
//!
//! ```text
//! cargo run --release --example kernel_benches
//! cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml
//! cargo run --release --example kernel_benches -- --multi-thread
//! ```
//!
//! Tokio default here is **current_thread** (Phase 1). `--multi-thread` is a
//! one-off experiment and does not change the library scheduler.

use bytes::Bytes;
use keel_rt::{
    AcceptPolicy, ApplyCmd, Execution, ExecutionContext, ExecutionState, NodeId, NodeOutcome,
    Runtime, Timestamp, WorkflowDefinition,
};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const MEDIAN_ITERS: usize = 7;
const BOUND: Duration = Duration::from_secs(30);

fn allocator_name() -> String {
    std::env::var("KEEL_BENCH_ALLOC")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "sys".into())
}

fn instant(_ctx: ExecutionContext) -> std::future::Ready<NodeOutcome> {
    std::future::ready(NodeOutcome::Succeeded(Bytes::from_static(b"ok")))
}

fn wide_def(n: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("wide").node("src", "ok").node("join", "ok");
    for i in 0..n {
        let id = format!("w{i}");
        b = b.node(id.as_str(), "ok").edge("src", id.as_str()).edge(id.as_str(), "join");
    }
    b.build().expect("wide")
}

fn chain_def(n: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("chain").node("n0", "ok");
    for i in 1..n {
        let prev = format!("n{}", i - 1);
        let id = format!("n{i}");
        b = b.node(id.as_str(), "ok").edge(prev.as_str(), id.as_str());
    }
    b.build().expect("chain")
}

fn diamond_def(diamonds: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("diamond");
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
    b.build().expect("diamond")
}

fn runtime(concurrency: usize) -> Runtime {
    Runtime::builder()
        .concurrency(concurrency)
        .register_fn("ok", instant)
        .build()
}

fn apply_only_drive(def: WorkflowDefinition) {
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    let mut effect = ex.apply(ApplyCmd::Start, &p, now).unwrap();
    let mut ready: VecDeque<NodeId> = VecDeque::new();
    ready.extend(effect.newly_runnable_ids(&ex));
    while let Some(id) = ready.pop_front() {
        effect = ex
            .apply(ApplyCmd::StartNode { node_id: id.clone() }, &p, now)
            .unwrap();
        ready.extend(effect.newly_runnable_ids(&ex));
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
        ready.extend(effect.newly_runnable_ids(&ex));
    }
    assert_eq!(ex.state(), ExecutionState::Succeeded);
}

fn median_dur(mut xs: Vec<Duration>) -> Duration {
    xs.sort();
    xs[xs.len() / 2]
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

async fn time_run(def: WorkflowDefinition, concurrency: usize) -> Duration {
    let rt = runtime(concurrency);
    let started = Instant::now();
    let state = tokio::time::timeout(BOUND, rt.run(def))
        .await
        .expect("bench timed out")
        .expect("start");
    assert_eq!(state, ExecutionState::Succeeded);
    started.elapsed()
}

fn median_n<F>(n: usize, mut f: F) -> Duration
where
    F: FnMut() -> Duration,
{
    let mut xs = Vec::with_capacity(n);
    for _ in 0..n {
        xs.push(f());
    }
    median_dur(xs)
}

async fn median_async<F, Fut>(n: usize, mut f: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Duration>,
{
    let mut xs = Vec::with_capacity(n);
    for _ in 0..n {
        xs.push(f().await);
    }
    median_dur(xs)
}

fn run_current_thread() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio");
    rt.block_on(async {
        let wide = median_async(MEDIAN_ITERS, || time_run(wide_def(256), 32)).await;
        let chain = median_async(MEDIAN_ITERS, || time_run(chain_def(128), 8)).await;
        let diamond = median_async(MEDIAN_ITERS, || time_run(diamond_def(2500), 32)).await;
        let apply = median_n(MEDIAN_ITERS, || {
            let t = Instant::now();
            apply_only_drive(diamond_def(2500));
            t.elapsed()
        });
        println!(
            "current_thread allocator={} wide_256={} chain_128={} diamond_10k={} apply_only={} (n={MEDIAN_ITERS})",
            allocator_name(),
            format_ms(wide),
            format_ms(chain),
            format_ms(diamond),
            format_ms(apply),
        );
        if !cfg!(debug_assertions) {
            let t = Instant::now();
            let st = time_run(wide_def(100_000), 32).await;
            println!(
                "current_thread allocator={} wide_100k={} (release, n=1)",
                allocator_name(),
                format_ms(st)
            );
            let _ = t;
        }
    });
}

fn run_multi_thread() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio mt");
    rt.block_on(async {
        let wide = median_async(MEDIAN_ITERS, || time_run(wide_def(256), 32)).await;
        let chain = median_async(MEDIAN_ITERS, || time_run(chain_def(128), 8)).await;
        println!(
            "multi_thread(4) allocator={} wide_256={} chain_128={} (experiment; scheduler is still one apply task per execution)",
            allocator_name(),
            format_ms(wide),
            format_ms(chain),
        );
    });
}

pub fn main() {
    let mt = std::env::args().any(|a| a == "--multi-thread");
    println!("keel-rt kernel_benches allocator={}", allocator_name());
    if mt {
        run_multi_thread();
    } else {
        run_current_thread();
    }
}
