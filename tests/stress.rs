//! Kernel stress: ScriptedExecutor succeeds immediately (zero user work)
//! except cancel-under-load, which hangs until cancel.

use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    ExecutionContext, ExecutionState, FunctionExecutor, NodeId, NodeOutcome, NodeState,
    DEFAULT_CANCEL_BOUND,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(5);

fn succeed(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(id.as_bytes().to_vec()))
}

#[tokio::test(flavor = "current_thread")]
async fn wide_fan_out_256() {
    let started = Instant::now();
    let n = 256usize;
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

    let run = tokio::time::timeout(BOUND, test.run())
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
    let mut test = WorkflowTest::new().concurrency(8).node("n0", succeed("n0"));
    for i in 1..n {
        let prev = format!("n{}", i - 1);
        let id = format!("n{i}");
        test = test.node(&id, succeed(&id)).edge(&prev, &id);
    }

    let run = tokio::time::timeout(BOUND, test.run())
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
