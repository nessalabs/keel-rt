//! Uneven work: stragglers, mixed fan-in, hourglass, fat payloads, FIFO vs
//! timer, skewed retry. FakeClock so we measure the kernel, not `sleep`.
//!
//! ```text
//! cargo test --test stress_uneven -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    Event, ExecutionContext, ExecutionState, FunctionExecutor, NodeId, NodeOutcome,
    NodeState, RetryPolicy, DEFAULT_CANCEL_BOUND,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(30);

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

fn running_count(snap: &keel_rt::ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}

/// 1 source → 64 children (63 instant, 1 FakeClock Delay) → join.
/// Join must not start until the straggler succeeds.
#[tokio::test(flavor = "current_thread")]
async fn straggler_join() {
    let started = Instant::now();
    let delay = Duration::from_millis(50);
    let mut test = WorkflowTest::new()
        .concurrency(64)
        .node("src", ok("src"))
        .node("join", ok("join"));
    for i in 0..63 {
        let id = format!("c{i}");
        test = test.node(&id, ok(&id)).edge("src", &id).edge(&id, "join");
    }
    test = test
        .node(
            "slow",
            ScriptedExecutor::new("slow").delay_succeed(delay, Bytes::from_static(b"slow-out")),
        )
        .edge("src", "slow")
        .edge("slow", "join");
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            let mut done = 0usize;
            for i in 0..63 {
                if matches!(run.state(&format!("c{i}")).await, NodeState::Succeeded) {
                    done += 1;
                }
            }
            if done == 63 && matches!(run.state("slow").await, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("straggler_join: 63 instant children never succeeded");

    assert!(
        matches!(run.state("join").await, NodeState::Pending),
        "join must stay Pending while the straggler is still Running"
    );
    assert!(run.scripted("join").attempts().is_empty());
    let evs = run.events();
    assert!(
        !evs.iter().any(|e| {
            matches!(e, Event::NodeStarted { node_id, .. } if node_id.as_str() == "join")
        }),
        "join must not have started before the straggler"
    );

    clock.advance(delay);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("straggler_join: did not finish after clock advance");

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let inputs = run.inputs("join").await;
    assert_eq!(inputs.len(), 64, "join sees all 64 children");
    assert_eq!(
        inputs.get(&NodeId::new("slow")),
        Some(&Bytes::from_static(b"slow-out"))
    );
    let evs = run.events();
    let slow_ok = evs.iter().position(|e| {
        matches!(e, Event::NodeSucceeded { node_id, .. } if node_id.as_str() == "slow")
    });
    let join_start = evs.iter().position(|e| {
        matches!(e, Event::NodeStarted { node_id, .. } if node_id.as_str() == "join")
    });
    assert!(
        slow_ok.unwrap() < join_start.unwrap(),
        "join started before straggler succeeded"
    );
    eprintln!(
        "stress_uneven straggler_join nodes=66 elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// In-degree 1, 2, 8, 64. High-degree join only after all preds. No extra executes.
#[tokio::test(flavor = "current_thread")]
async fn mixed_fanin() {
    let started = Instant::now();
    let mut test = WorkflowTest::new()
        .concurrency(32)
        .node("src", ok("src"))
        .node("deg1", ok("deg1"))
        .edge("src", "deg1")
        .node("j2", ok("j2"))
        .node("j8", ok("j8"))
        .node("j64", ok("j64"));
    for i in 0..2 {
        let id = format!("p2_{i}");
        test = test.node(&id, ok(&id)).edge("src", &id).edge(&id, "j2");
    }
    for i in 0..8 {
        let id = format!("p8_{i}");
        test = test.node(&id, ok(&id)).edge("src", &id).edge(&id, "j8");
    }
    for i in 0..64 {
        let id = format!("p64_{i}");
        test = test.node(&id, ok(&id)).edge("src", &id).edge(&id, "j64");
    }
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("mixed_fanin timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("deg1").attempts(), vec![1]);
    assert_eq!(run.scripted("j2").attempts(), vec![1]);
    assert_eq!(run.scripted("j8").attempts(), vec![1]);
    assert_eq!(run.scripted("j64").attempts(), vec![1]);
    assert_eq!(run.inputs("deg1").await.len(), 1);
    assert_eq!(run.inputs("j2").await.len(), 2);
    assert_eq!(run.inputs("j8").await.len(), 8);
    assert_eq!(run.inputs("j64").await.len(), 64);
    eprintln!(
        "stress_uneven mixed_fanin nodes=1+1+2+8+64+3 elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// 256 sources → Delay bottleneck → 256 sinks. Second wave stays Pending
/// until the bottleneck succeeds.
#[tokio::test(flavor = "current_thread")]
async fn hourglass() {
    let started = Instant::now();
    let delay = Duration::from_millis(40);
    let n = 256usize;
    let mut test = WorkflowTest::new().concurrency(32).node(
        "neck",
        ScriptedExecutor::new("neck").delay_succeed(delay, Bytes::from_static(b"neck")),
    );
    for i in 0..n {
        let a = format!("a{i}");
        let b = format!("b{i}");
        test = test
            .node(&a, ok(&a))
            .node(&b, ok(&b))
            .edge(&a, "neck")
            .edge("neck", &b);
    }
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("neck").await, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hourglass: bottleneck never Running");

    for i in 0..n {
        assert!(
            matches!(run.state(&format!("b{i}")).await, NodeState::Pending),
            "second wave b{i} started before bottleneck succeeded"
        );
        assert!(run.scripted(&format!("b{i}")).attempts().is_empty());
    }

    clock.advance(delay);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("hourglass timed out after advance");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("b0").attempts(), vec![1]);
    eprintln!(
        "stress_uneven hourglass nodes={} elapsed={} profile=debug",
        n * 2 + 1,
        format_ms(started.elapsed())
    );
}

/// Mix 1-byte and 64 KiB payloads. Join sees the fat `Bytes` by NodeId
/// (refcount clone, not a 64 KiB kernel memcpy).
#[tokio::test(flavor = "current_thread")]
async fn uneven_payloads() {
    let started = Instant::now();
    let fat = Bytes::from(vec![0xAB; 64 * 1024]);
    let fat_b = fat.clone();
    let fat_d = fat.clone();
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(4)
            .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"x")))
            .node("b", ScriptedExecutor::new("b").succeed(fat_b))
            .node("c", ScriptedExecutor::new("c").succeed(Bytes::from_static(b"y")))
            .node("d", ScriptedExecutor::new("d").succeed(fat_d))
            .node("join", ok("join"))
            .edge("a", "join")
            .edge("b", "join")
            .edge("c", "join")
            .edge("d", "join")
            .run(),
    )
    .await
    .expect("uneven_payloads timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let inputs = run.inputs("join").await;
    assert_eq!(inputs.len(), 4);
    assert_eq!(inputs.get(&NodeId::new("a")).map(|b| b.len()), Some(1));
    let got = inputs.get(&NodeId::new("b")).expect("fat b");
    assert_eq!(got.len(), 64 * 1024);
    assert_eq!(got, &fat);
    // Same backing allocation: Bytes clone is a refcount bump.
    assert_eq!(got.as_ptr(), fat.as_ptr());
    eprintln!(
        "stress_uneven uneven_payloads nodes=5 fat=64KiB elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// 32 sources, concurrency 4, FakeClock Delay cycling {0,1,5,50}ms.
/// Peak Running ≤ 4. Delayed nodes are Running (executor sleep), not a
/// FIFO skip. Ready-now FIFO is checked with concurrency=1. Ready
/// { runnable_at } (retry timer) is not Running until due.
#[tokio::test(flavor = "current_thread")]
async fn uneven_delays_fifo() {
    let started = Instant::now();
    let delays = [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(50),
    ];

    // Part A — Delay holds a permit (node is Running while sleeping).
    let peak = Arc::new(AtomicUsize::new(0));
    let mut test = WorkflowTest::new().concurrency(4);
    for i in 0..32 {
        let id = format!("d{i}");
        let delay = delays[i % 4];
        test = test.node(
            &id,
            ScriptedExecutor::new(id.as_str()).delay_succeed(delay, Bytes::from_static(b"ok")),
        );
    }
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            let r = running_count(&run.snapshot().await);
            peak.fetch_max(r, Ordering::SeqCst);
            assert!(r <= 4, "Running {r} exceeded concurrency 4");
            if run.execution_state().await == ExecutionState::Succeeded {
                break;
            }
            clock.advance(Duration::from_millis(50));
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("uneven_delays_fifo part A timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(peak.load(Ordering::SeqCst) <= 4);

    // Part B — retry delay is Ready { runnable_at }, not Running.
    // Four instant siblings drain; four fail-then-succeed wait on the timer.
    let mut test = WorkflowTest::new()
        .concurrency(4)
        .policy(RetryPolicy::new(3, Duration::from_millis(50)));
    for i in 0..4 {
        test = test.node(&format!("fast{i}"), ok(&format!("fast{i}")));
    }
    for i in 0..4 {
        let id = format!("late{i}");
        test = test.node(
            &id,
            ScriptedExecutor::new(id.as_str())
                .fail("once")
                .succeed(Bytes::from_static(b"ok")),
        );
    }
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            let mut fast_ok = 0;
            let mut late_ready = 0;
            for i in 0..4 {
                if matches!(run.state(&format!("fast{i}")).await, NodeState::Succeeded) {
                    fast_ok += 1;
                }
                if matches!(
                    run.state(&format!("late{i}")).await,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                ) {
                    late_ready += 1;
                }
            }
            if fast_ok == 4 && late_ready == 4 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retry-delayed nodes never reached Ready {{ runnable_at }}");
    for i in 0..4 {
        assert!(
            !matches!(run.state(&format!("late{i}")).await, NodeState::Running { .. }),
            "late{i} must not be Running until the retry deadline"
        );
    }
    clock.advance(Duration::from_millis(50));
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("uneven_delays_fifo part B timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);

    // Part C — FIFO among Ready-now: concurrency=1, first enqueued source runs first.
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut test = WorkflowTest::new().concurrency(1);
    for i in 0..8 {
        let id = format!("f{i}");
        let ord = order.clone();
        let name = id.clone();
        test = test.executor(
            &id,
            FunctionExecutor::new(id.as_str(), move |_ctx: ExecutionContext| {
                let mut g = ord.lock().expect("order");
                g.push(name.clone());
                async move { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            }),
        );
    }
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("fifo Ready-now timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let started_order = order.lock().expect("order").clone();
    assert_eq!(
        started_order.first().map(String::as_str),
        Some("f0"),
        "FIFO: first enqueued Ready-now source must run first, got {started_order:?}"
    );

    eprintln!(
        "stress_uneven uneven_delays_fifo elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// One node retries twice with delay; siblings succeed; join waits; after
/// clock advance the retried node succeeds and join runs once.
#[tokio::test(flavor = "current_thread")]
async fn skewed_retry() {
    let started = Instant::now();
    let delay = Duration::from_millis(25);
    let test = WorkflowTest::new()
        .concurrency(4)
        .policy(RetryPolicy::new(3, delay))
        .node("a", ok("a"))
        .node(
            "flaky",
            ScriptedExecutor::new("flaky")
                .fail("e1")
                .fail("e2")
                .succeed(Bytes::from_static(b"flaky-ok")),
        )
        .node("c", ok("c"))
        .node("join", ok("join"))
        .edge("a", "join")
        .edge("flaky", "join")
        .edge("c", "join");
    let clock = test.fake_clock();
    let run = test.start().await;

    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("flaky").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) && matches!(run.state("a").await, NodeState::Succeeded)
                && matches!(run.state("c").await, NodeState::Succeeded)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("skewed_retry: siblings never succeeded / flaky never parked");

    assert!(matches!(run.state("join").await, NodeState::Pending));
    assert!(run.scripted("join").attempts().is_empty());

    // Two retry delays (fail, fail, then succeed).
    clock.advance(delay);
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                run.state("flaky").await,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ) || matches!(run.state("flaky").await, NodeState::Succeeded)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("skewed_retry: second fail never parked");
    clock.advance(delay);

    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("skewed_retry timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.scripted("flaky").attempts(), vec![1, 2, 3]);
    assert_eq!(run.scripted("join").attempts(), vec![1]);
    assert_eq!(
        run.inputs("join").await.get(&NodeId::new("flaky")),
        Some(&Bytes::from_static(b"flaky-ok"))
    );
    eprintln!(
        "stress_uneven skewed_retry nodes=4 elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// Cancel 1k hanging fan-out, concurrency 32: Cancelled, within cancel bound.
#[tokio::test(flavor = "current_thread")]
async fn cancel_hanging_fanout_1k() {
    let started = Instant::now();
    let n = 1000usize;
    let mut test = WorkflowTest::new()
        .concurrency(32)
        .cancel_bound(DEFAULT_CANCEL_BOUND);
    for i in 0..n {
        let id = format!("h{i}");
        test = test.node(&id, ScriptedExecutor::new(id.as_str()).hang(false));
    }
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if running_count(&run.snapshot().await) >= 32 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancel_hanging_fanout_1k: never reached 32 Running");

    run.cancel().await;
    tokio::time::timeout(
        DEFAULT_CANCEL_BOUND + Duration::from_millis(200),
        run.wait_stable(),
    )
    .await
    .expect("cancel 1k fan-out did not finish within cancel bound");
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
    eprintln!(
        "stress_uneven cancel_hanging_fanout_1k nodes={n} elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}

/// concurrency=1 on 256 mixed-delay nodes: peak Running == 1.
#[tokio::test(flavor = "current_thread")]
async fn concurrency_1_mixed_delay_256() {
    let started = Instant::now();
    let n = 256usize;
    let delays = [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(20),
    ];
    let peak = Arc::new(AtomicUsize::new(0));
    let mut test = WorkflowTest::new().concurrency(1);
    for i in 0..n {
        let id = format!("m{i}");
        let delay = delays[i % 4];
        test = test.node(
            &id,
            ScriptedExecutor::new(id.as_str()).delay_succeed(delay, Bytes::from_static(b"ok")),
        );
    }
    let clock = test.fake_clock();
    let run = test.start().await;

    // Drive the FakeClock: with concurrency=1 a Delay node holds the only
    // permit until the clock advances.
    tokio::time::timeout(BOUND, async {
        loop {
            let r = running_count(&run.snapshot().await);
            peak.fetch_max(r, Ordering::SeqCst);
            assert!(r <= 1, "concurrency=1 but Running={r}");
            if run.execution_state().await == ExecutionState::Succeeded {
                break;
            }
            clock.advance(Duration::from_millis(20));
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("concurrency_1_mixed_delay_256 timed out");

    let p = peak.load(Ordering::SeqCst);
    assert_eq!(p, 1, "expected to observe a Running node, peak={p}");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    eprintln!(
        "stress_uneven concurrency_1_mixed_delay_256 nodes={n} peak={p} elapsed={} profile=debug",
        format_ms(started.elapsed())
    );
}
