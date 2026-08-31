//! Network / resilience matrix. Faults are [`NetFault`] + FakeClock — no
//! sockets in the kernel. Fail-fast stays **execution-wide**.
//!
//! | fault | scheduler | this node | same-exec siblings | other executions |
//! |---|---|---|---|---|
//! | Timeout + Accept | alive | TimedOut | Cancelled (non-terminal) | n/a |
//! | Timeout + Retry | alive | Ready then Succeeded | may run (permit released) | n/a |
//! | Timeout in one of 100 execs | alive | that exec Failed | n/a | other 99 Succeeded |
//!
//! ```text
//! cargo test --test resilience -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use keel_rt::testing::{
    FailingStore, FaultySink, FlakyThen, NetFault, ScriptedExecutor, WorkflowTest,
};
use keel_rt::{
    AcceptPolicy, DomainEvent, ExecutionState, FunctionExecutor, MemoryStore, NodeId, NodeOutcome,
    NodeState, RetryPolicy, Runtime, StateStore, WorkflowDefinition,
};
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(30);
const DELAY: Duration = Duration::from_millis(20);

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

fn profile_name() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

fn diamond_def(i: usize) -> WorkflowDefinition {
    WorkflowDefinition::builder(format!("research-{i}"))
        .node("research", "research")
        .node("summarizer", "ok")
        .node("critic", "ok")
        .node("writer", "ok")
        .edge("research", "summarizer")
        .edge("research", "critic")
        .edge("summarizer", "writer")
        .edge("critic", "writer")
        .build()
        .expect("diamond")
}

fn running_count(snap: &keel_rt::ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}

/// Timeout + AcceptPolicy: node TimedOut, execution fail-fasts. Scheduler lives.
#[tokio::test(flavor = "current_thread")]
async fn timeout_accept_fail_fasts_execution() {
    let started = Instant::now();
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(1)
            .policy(AcceptPolicy)
            .node("t", ScriptedExecutor::new("t").fault(NetFault::Timeout))
            .node("sib", ok("sib"))
            .node("join", ok("join"))
            .edge("t", "join")
            .edge("sib", "join")
            .run(),
    )
    .await
    .expect("timeout_accept_fail_fasts_execution timed out");

    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert!(matches!(run.state("t").await, NodeState::TimedOut));
    assert!(
        matches!(run.state("sib").await, NodeState::Cancelled),
        "fail-fast is execution-wide: non-terminal siblings are Cancelled"
    );
    assert!(matches!(run.state("join").await, NodeState::Cancelled));
    assert!(run.scripted("sib").attempts().is_empty());
    assert!(run.scripted("join").attempts().is_empty());
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Failed);
    assert!(snap.revision > 0, "inspect still works after fail-fast");
    eprintln!(
        "resilience timeout_accept_fail_fasts_execution node=TimedOut siblings=Cancelled exec=Failed elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Timeout + Retry: permit released (Ready {{ runnable_at }}), sibling may run.
/// After clock, retry succeeds; join sees success bytes. Execution stays Running.
#[tokio::test(flavor = "current_thread")]
async fn timeout_retry_releases_permit_then_succeeds() {
    let started = Instant::now();
    let test = WorkflowTest::new()
        .concurrency(1)
        .policy(RetryPolicy::new(3, DELAY))
        .node(
            "a",
            ScriptedExecutor::new("a").fault(NetFault::TimeoutThenSucceed),
        )
        .node("b", ok("b"))
        .node("join", ok("join"))
        .edge("a", "join")
        .edge("b", "join");
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("a").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a never parked Ready {{ runnable_at }} after Timeout");

    // Permit released: with concurrency=1, b can run while a is not Running.
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("b").await, NodeState::Succeeded) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sibling b never ran after Timeout-to-retry released the permit");
    assert!(
        !matches!(run.state("a").await, NodeState::Running { .. }),
        "a must not hold Running while retry-delayed"
    );
    assert!(matches!(run.state("join").await, NodeState::Pending));
    assert_eq!(run.execution_state().await, ExecutionState::Running);

    clock.advance(DELAY);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("timeout retry never finished");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert_eq!(
        run.inputs("join").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"ok"))
    );
    eprintln!(
        "resilience timeout_retry_releases_permit node=Ready-then-Succeeded sibling=ran exec=Succeeded elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Delay on one pred: AND-join does not start early. FakeClock.
#[tokio::test(flavor = "current_thread")]
async fn delay_and_join_waits_for_slow_pred() {
    let started = Instant::now();
    let test = WorkflowTest::new()
        .concurrency(2)
        .node("fast", ok("fast"))
        .node(
            "slow",
            ScriptedExecutor::new("slow").fault(NetFault::Delay(DELAY)),
        )
        .node("join", ok("join"))
        .edge("fast", "join")
        .edge("slow", "join");
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("fast").await, NodeState::Succeeded)
                && matches!(run.state("slow").await, NodeState::Running { .. })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fast never succeeded / slow never Running");
    assert!(matches!(run.state("join").await, NodeState::Pending));
    assert!(run.scripted("join").attempts().is_empty());

    clock.advance(DELAY);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("delay join timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("join").await.len(), 2);
    eprintln!(
        "resilience delay_and_join_waits_for_slow_pred elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Reset (Failed) + Retry then success — same permit-release contract as Timeout.
#[tokio::test(flavor = "current_thread")]
async fn reset_retry_then_success() {
    let started = Instant::now();
    let test = WorkflowTest::new()
        .concurrency(1)
        .policy(RetryPolicy::new(3, DELAY))
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fault(NetFault::Reset)
                .succeed(Bytes::from_static(b"after-reset")),
        )
        .node("b", ok("b"))
        .node("join", ok("join"))
        .edge("a", "join")
        .edge("b", "join");
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("a").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) && matches!(run.state("b").await, NodeState::Succeeded)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reset-retry never parked / sibling never ran");
    clock.advance(DELAY);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("reset retry timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(
        run.inputs("join").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"after-reset"))
    );
    eprintln!(
        "resilience reset_retry_then_success elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// One Timeout among 100 sequential diamonds: that exec Failed, the other 99
/// Succeeded. MemoryStore ids do not mix. This is how a crawl stays resilient.
#[tokio::test(flavor = "current_thread")]
async fn timeout_isolates_one_of_100_executions() {
    let n = 100usize;
    let started = Instant::now();
    let store = MemoryStore::new();
    let launched = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let child = {
        let launched = launched.clone();
        FunctionExecutor::new("research", move |_ctx| {
            let i = launched.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if i == 0 {
                    NodeOutcome::TimedOut
                } else {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }
        })
    };
    let ok_ex = FunctionExecutor::new("ok", |_ctx| async {
        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
    });
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(2)
        .policy(AcceptPolicy)
        .register(ok_ex)
        .register(child)
        .build();

    let mut failed = 0usize;
    let mut succeeded = 0usize;
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let h = rt.start(diamond_def(i));
        let id = h.inspect().await.execution_id.clone();
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("diamond {i} timed out"));
        if i == 0 {
            assert_eq!(state, ExecutionState::Failed);
            failed += 1;
        } else {
            assert_eq!(state, ExecutionState::Succeeded);
            succeeded += 1;
        }
        ids.push(id);
    }
    assert_eq!(failed, 1);
    assert_eq!(succeeded, n - 1);
    let mut sorted = ids.clone();
    sorted.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    sorted.dedup();
    assert_eq!(sorted.len(), n, "MemoryStore must not mix ExecutionIds");

    let first = store
        .get(&ids[0])
        .await
        .expect("store get")
        .expect("first snapshot kept");
    assert_eq!(first.state, ExecutionState::Failed);
    assert!(
        matches!(
            first.node(&NodeId::new("research")).map(|n| &n.state),
            Some(NodeState::TimedOut)
        ),
        "later diamonds must not overwrite the failed execution"
    );
    for id in ids.iter().skip(1) {
        let snap = store
            .get(id)
            .await
            .expect("store get")
            .unwrap_or_else(|| panic!("missing snapshot {id}"));
        assert_eq!(snap.state, ExecutionState::Succeeded);
        assert!(matches!(
            snap.node(&NodeId::new("research")).map(|n| &n.state),
            Some(NodeState::Succeeded)
        ));
    }
    eprintln!(
        "resilience timeout_isolates_one_of_100_executions failed=1 succeeded=99 store_isolated=yes elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
    eprintln!(
        "| fault | scheduler | this node | same-exec siblings | other executions |\n\
         | Timeout+Accept | alive | TimedOut | Cancelled | n/a |\n\
         | Timeout+Retry | alive | Ready then Succeeded | may run | n/a |\n\
         | Timeout in 1 of 100 execs | alive | that exec Failed | n/a | other 99 Succeeded |"
    );
}

/// FaultySink panic: scheduler lives, execution still progresses.
#[tokio::test(flavor = "current_thread")]
async fn faulty_sink_panic_scheduler_lives() {
    let started = Instant::now();
    let sink = FaultySink::panic_on_nth(1);
    let def = WorkflowDefinition::builder("sink")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .sink(sink)
        .register(FunctionExecutor::new("a", |_ctx| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        }))
        .build();
    let state = tokio::time::timeout(BOUND, rt.start(def).wait())
        .await
        .expect("faulty sink hang");
    assert_eq!(state, ExecutionState::Succeeded);
    eprintln!(
        "resilience faulty_sink_panic_scheduler_lives exec=Succeeded elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// FailingStore every put: in-memory apply still progresses.
#[tokio::test(flavor = "current_thread")]
async fn failing_store_in_memory_progresses() {
    let started = Instant::now();
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .store(FailingStore::fail_all())
            .node("a", ok("a"))
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await
    .expect("failing store timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    eprintln!(
        "resilience failing_store_in_memory_progresses elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Mixed faults in one graph: no Accept-Failed. Peak Running ≤ 8.
#[tokio::test(flavor = "current_thread")]
async fn mixed_faults_retry_then_succeed() {
    let n = 32usize;
    let started = Instant::now();
    let mut test = WorkflowTest::new()
        .concurrency(8)
        .policy(RetryPolicy::new(3, DELAY));
    for i in 0..n {
        let id = format!("n{i}");
        let exec = match i % 20 {
            0 | 1 | 2 | 3 | 4 => {
                // 25%: Delay
                ScriptedExecutor::new(id.as_str()).fault(NetFault::Delay(DELAY))
            }
            5 | 6 => {
                // 10%: Timeout then succeed
                ScriptedExecutor::new(id.as_str()).fault(NetFault::TimeoutThenSucceed)
            }
            7 => {
                // 5%: Reset then succeed
                ScriptedExecutor::new(id.as_str()).fault(NetFault::Flaky {
                    fail_times: 1,
                    then: FlakyThen::Succeed(Bytes::from_static(b"ok")),
                })
            }
            _ => ok(&id),
        };
        test = test.node(&id, exec);
    }
    let clock = test.fake_clock();
    let run = test.start().await;
    let mut peak = 0usize;
    tokio::time::timeout(BOUND, async {
        loop {
            let r = running_count(&run.snapshot().await);
            peak = peak.max(r);
            assert!(r <= 8, "Running {r} > concurrency 8");
            if run.execution_state().await == ExecutionState::Succeeded {
                break;
            }
            clock.advance(DELAY);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed faults timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(peak <= 8);
    eprintln!(
        "resilience mixed_faults_retry_then_succeed N=32 mix=25%Delay+10%TimeoutRetry+5%ResetRetry peak={peak} elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Socket-timeout analog: Running until FakeClock hits the script, then TimedOut.
#[tokio::test(flavor = "current_thread")]
async fn timeout_after_clock_while_running() {
    let started = Instant::now();
    let test = WorkflowTest::new()
        .policy(AcceptPolicy)
        .node("t", ScriptedExecutor::new("t").timeout_after(DELAY));
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("t").await, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout_after never Running");

    clock.advance(DELAY);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("timeout_after never TimedOut");
    assert!(matches!(run.state("t").await, NodeState::TimedOut));
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    let evs = run.events();
    assert!(evs
        .iter()
        .any(|e| matches!(e, DomainEvent::NodeTimedOut { node_id } if node_id.as_str() == "t")));
    eprintln!(
        "resilience timeout_after_clock_while_running node=TimedOut exec=Failed elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Delay is aborted when the handle is cancelled (CancellationToken first).
#[tokio::test(flavor = "current_thread")]
async fn delay_aborts_on_cancel() {
    let started = Instant::now();
    let test = WorkflowTest::new().node(
        "d",
        ScriptedExecutor::new("d").fault(NetFault::Delay(Duration::from_secs(10))),
    );
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("d").await, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delay never Running");
    run.cancel().await;
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("cancel during Delay hung");
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
    eprintln!(
        "resilience delay_aborts_on_cancel exec=Cancelled elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

// requires failure scopes — siblings must NOT survive Timeout in the same DAG
// under Phase 1. Do not change fail-fast to make this pass.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires failure scopes"]
async fn siblings_survive_timeout_same_dag() {
    let run = WorkflowTest::new()
        .policy(AcceptPolicy)
        .node("t", ScriptedExecutor::new("t").fault(NetFault::Timeout))
        .node("sib", ok("sib"))
        .run()
        .await;
    assert!(
        matches!(run.state("sib").await, NodeState::Succeeded),
        "would need per-node failure scopes; today sib is Cancelled"
    );
}
