//! Production-shaped in-process workloads. Still ScriptedExecutor /
//! FunctionExecutor — no YAML, CLI, HTTP, or Agent types in the kernel.
//!
//! Phase 1 fail-fast is **execution-wide**. A crawl or farm that must survive
//! a failed branch is N executions, not one DAG with Accept-Failed.
//!
//! ```text
//! cargo test --test workloads -- --nocapture --test-threads=1
//! cargo test --release --test workloads -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, Event, ExecutionContext, ExecutionState, Executor, FunctionExecutor,
    MemoryStore, NodeId, NodeOutcome, NodeState, Recover, Resume, RetryPolicy, Runtime, StateStore,
    WorkflowDefinition,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_millis(10);

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

fn median_dur(mut xs: Vec<Duration>) -> Duration {
    xs.sort();
    xs[xs.len() / 2]
}

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

fn instant(id: &str) -> Arc<dyn Executor> {
    Arc::new(FunctionExecutor::new(id, |_ctx: ExecutionContext| async {
        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
    }))
}

fn running_count(snap: &keel_rt::ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}

fn diamond_def() -> WorkflowDefinition {
    WorkflowDefinition::builder("research")
        .node("research", "research")
        .node("summarizer", "summarizer")
        .node("critic", "critic")
        .node("writer", "writer")
        .edge("research", "summarizer")
        .edge("research", "critic")
        .edge("summarizer", "writer")
        .edge("critic", "writer")
        .build()
        .expect("diamond")
}

fn payload_exec(id: &'static str) -> FunctionExecutor<impl Fn(ExecutionContext) -> std::future::Ready<NodeOutcome> + Send + Sync> {
    FunctionExecutor::new(id, move |ctx: ExecutionContext| {
        let n = 16 + (ctx.node_id.as_str().len() * 13) % 241;
        std::future::ready(NodeOutcome::Succeeded(Bytes::from(vec![0x11; n])))
    })
}

/// Frozen rule, named explicitly: Accept-Failed fail-fasts the **whole**
/// execution. Terminals stay Succeeded; everyone else is Cancelled.
#[tokio::test(flavor = "current_thread")]
async fn fail_fast_is_execution_wide() {
    let started = Instant::now();
    // concurrency=1 so A finishes before B fails — A stays Succeeded.
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(1)
            .policy(AcceptPolicy)
            .node("src", ok("src"))
            .node("a", ok("a"))
            .node("b", ScriptedExecutor::new("b").fail("page-404"))
            .node("c", ok("c"))
            .node("join", ok("join"))
            .edge("src", "a")
            .edge("src", "b")
            .edge("src", "c")
            .edge("a", "join")
            .edge("b", "join")
            .edge("c", "join")
            .run(),
    )
    .await
    .expect("fail_fast_is_execution_wide timed out");

    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert!(matches!(run.state("b").await, NodeState::Failed));
    assert!(
        matches!(run.state("c").await, NodeState::Cancelled),
        "non-terminal sibling is Cancelled — fail-fast is not a subgraph pocket"
    );
    assert!(matches!(run.state("join").await, NodeState::Cancelled));
    assert!(run.scripted("join").attempts().is_empty());
    assert!(run.scripted("c").attempts().is_empty());
    let n_failed = run
        .events()
        .iter()
        .filter(|e| matches!(e, Event::ExecutionFailed { .. }))
        .count();
    assert_eq!(n_failed, 1);
    eprintln!(
        "workload fail_fast_is_execution_wide N=5 mix=1-fail-Accept elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Production crawl: one failed page must not kill the site. That is N
/// executions, not one giant DAG with Accept-Failed.
#[tokio::test(flavor = "current_thread")]
async fn crawl_as_many_executions() {
    let started = Instant::now();
    let n = 500usize;
    let n_fail = 10usize;
    let store = MemoryStore::new();
    let launched = Arc::new(AtomicUsize::new(0));
    let child = {
        let launched = launched.clone();
        FunctionExecutor::new("child", move |_ctx: ExecutionContext| {
            let i = launched.fetch_add(1, Ordering::SeqCst);
            async move {
                if i < 10 {
                    NodeOutcome::failed("page-404")
                } else {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }
        })
    };
    let ok_ex = FunctionExecutor::new("ok", |_ctx: ExecutionContext| async {
        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
    });
    let def = || {
        WorkflowDefinition::builder("crawl-branch")
            .node("seed", "ok")
            .node("child", "child")
            .node("d1", "ok")
            .node("d2", "ok")
            .node("d3", "ok")
            .edge("seed", "child")
            .edge("child", "d1")
            .edge("d1", "d2")
            .edge("d2", "d3")
            .build()
            .expect("crawl branch")
    };
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(2)
        .policy(AcceptPolicy)
        .register(ok_ex)
        .register(child)
        .build();

    let mut ids = Vec::with_capacity(n);
    let mut failed = 0usize;
    let mut succeeded = 0usize;
    for i in 0..n {
        let h = rt.start(def()).expect("start");
        let id = h.inspect().await.execution_id.clone();
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("crawl exec {i} timed out"));
        if i < n_fail {
            assert_eq!(state, ExecutionState::Failed, "exec {i} should fail");
            let snap = store.get(&id).await.unwrap().expect("stored fail");
            assert!(matches!(
                snap.node(&NodeId::new("child")).unwrap().state,
                NodeState::Failed
            ));
            for d in ["d1", "d2", "d3"] {
                assert!(
                    matches!(snap.node(&NodeId::new(d)).unwrap().state, NodeState::Cancelled),
                    "{d} must be Cancelled never-started on a failed branch"
                );
            }
            failed += 1;
        } else {
            assert_eq!(state, ExecutionState::Succeeded, "exec {i} should succeed");
            succeeded += 1;
        }
        ids.push(id);
    }
    assert_eq!(failed, n_fail);
    assert_eq!(succeeded, n - n_fail);
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    ids.dedup();
    assert_eq!(ids.len(), n, "MemoryStore must not clobber ExecutionIds");
    let elapsed = started.elapsed();
    let per_s = n as f64 / elapsed.as_secs_f64();
    eprintln!(
        "workload crawl_as_many_executions N={n} mix=10-fail/490-succeed execs/s={per_s:.1} elapsed={} profile={}",
        format_ms(elapsed),
        profile_name()
    );
}

/// N independent Research→{Sum∥Crit}→Writer diamonds: 5% research retry,
/// 2% critic HITL Waiting. Peak Running ≤ 32.
#[tokio::test(flavor = "current_thread")]
async fn agent_farm_1000() {
    let n = 1000usize;
    let started = Instant::now();
    let delay = RETRY_DELAY;
    let ok_ex = instant("ok");
    let mut test = WorkflowTest::with_graph_capacity(n * 4, n * 4)
        .silent()
        .concurrency(32)
        .policy(RetryPolicy::new(3, delay));

    let mut retry_idx = Vec::new();
    let mut wait_idx = Vec::new();
    for i in 0..n {
        let r = format!("r{i}");
        let s = format!("s{i}");
        let c = format!("c{i}");
        let w = format!("w{i}");
        // Disjoint: a retrying Research never shares a diamond with a Waiting
        // Critic (otherwise the critic cannot enter Waiting until the clock
        // advances, and the park condition deadlocks).
        let retry = i % 20 == 0;
        let wait = i % 50 == 1;
        if retry {
            retry_idx.push(i);
            test = test.node(
                &r,
                ScriptedExecutor::new(r.as_str())
                    .fail("research-flaky")
                    .succeed(Bytes::from_static(b"research-ok")),
            );
        } else {
            test = test.node_arc(&r, ok_ex.clone());
        }
        test = test.node_arc(&s, ok_ex.clone());
        if wait {
            wait_idx.push(i);
            test = test.node(&c, ScriptedExecutor::new(c.as_str()).wait());
        } else {
            test = test.node_arc(&c, ok_ex.clone());
        }
        test = test.node(&w, ok(&w));
        test = test
            .edge(&r, &s)
            .edge(&r, &c)
            .edge(&s, &w)
            .edge(&c, &w);
    }

    let clock = test.fake_clock();
    let run = test.start().await;
    let mut peak = 0usize;

    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            let r = running_count(&snap);
            peak = peak.max(r);
            assert!(r <= 32, "agent_farm peak Running {r} > 32");
            let waits_ready = wait_idx.iter().all(|&i| {
                matches!(
                    snap.node(&NodeId::new(format!("c{i}"))).map(|n| &n.state),
                    Some(NodeState::Waiting { .. })
                )
            });
            let retries_parked_or_done = retry_idx.iter().all(|&i| {
                matches!(
                    snap.node(&NodeId::new(format!("r{i}"))).map(|n| &n.state),
                    Some(
                        NodeState::Succeeded
                            | NodeState::Ready {
                                runnable_at: Some(_)
                            }
                    )
                )
            });
            if waits_ready && retries_parked_or_done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent_farm: retries/HITL never parked");

    for &i in &wait_idx {
        assert!(
            run.scripted(&format!("w{i}")).attempts().is_empty(),
            "writer w{i} ran while critic still Waiting"
        );
        assert!(matches!(run.state(&format!("w{i}")).await, NodeState::Pending));
    }

    clock.advance(delay);
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            peak = peak.max(running_count(&snap));
            assert!(peak <= 32);
            if retry_idx.iter().all(|&i| {
                matches!(
                    snap.node(&NodeId::new(format!("r{i}"))).map(|n| &n.state),
                    Some(NodeState::Succeeded)
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent_farm: retried research never succeeded");

    for &i in &retry_idx {
        if !wait_idx.contains(&i) {
            tokio::time::timeout(BOUND, async {
                loop {
                    if matches!(run.state(&format!("w{i}")).await, NodeState::Succeeded) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("retried diamond {i} writer never ran"));
            assert_eq!(run.inputs(&format!("w{i}")).await.len(), 2);
        }
    }

    for &i in &wait_idx {
        let token = run.resume_token(&format!("c{i}")).await;
        run.resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"critic-hitl"))),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("agent_farm did not reach Succeeded after HITL resume");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(peak <= 32, "peak Running {peak}");

    for i in 0..n {
        let w = format!("w{i}");
        assert_eq!(run.scripted(&w).attempts(), vec![1], "writer {w}");
        let inputs = run.inputs(&w).await;
        assert_eq!(inputs.len(), 2, "writer {w} must see summarizer+critic");
        assert!(inputs.contains_key(&NodeId::new(format!("s{i}"))));
        assert!(inputs.contains_key(&NodeId::new(format!("c{i}"))));
    }
    eprintln!(
        "workload agent_farm_1000 N={n} diamonds nodes={} mix=5%retry+2%wait peak={peak} elapsed={} profile={}",
        n * 4,
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// 10_000 maps → 100 partials (100-way AND) → 1 final. Final sees 100 inputs.
#[tokio::test(flavor = "current_thread")]
async fn map_reduce_tree() {
    let n_maps = 10_000usize;
    let n_parts = 100usize;
    let per = n_maps / n_parts;
    let started = Instant::now();
    let map_ex: Arc<dyn Executor> = Arc::new(payload_exec("map"));
    let part_ex: Arc<dyn Executor> = Arc::new(payload_exec("part"));
    let mut test = WorkflowTest::with_graph_capacity(n_maps + n_parts + 1, n_maps + n_parts)
        .silent()
        .concurrency(32)
        .node("fin", ok("fin"));
    for p in 0..n_parts {
        let pid = format!("p{p}");
        test = test.node_arc(&pid, part_ex.clone()).edge(&pid, "fin");
        for k in 0..per {
            let mid = format!("m{}", p * per + k);
            test = test.node_arc(&mid, map_ex.clone()).edge(&mid, &pid);
        }
    }
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("map_reduce_tree timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let inputs = run.inputs("fin").await;
    assert_eq!(inputs.len(), n_parts, "final must AND-join the 100 partials");
    for p in 0..n_parts {
        assert!(
            inputs.contains_key(&NodeId::new(format!("p{p}"))),
            "missing partial p{p}"
        );
    }
    assert!(
        !inputs.keys().any(|k| k.as_str().starts_with('m')),
        "maps must not leak into the final reducer"
    );
    eprintln!(
        "workload map_reduce_tree N={} maps mix=100x100→1 elapsed={} profile={}",
        n_maps,
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// 200 Waiting nodes (permits free so all 200 enter Waiting), resume in
/// shuffled batches of 20. Peak Running ≤ 16. Duplicate Complete is Ok.
#[tokio::test(flavor = "current_thread")]
async fn hitl_drain() {
    let n = 200usize;
    let started = Instant::now();
    let mut test = WorkflowTest::with_graph_capacity(n * 2, n)
        .concurrency(16);
    for i in 0..n {
        let w = format!("w{i}");
        let d = format!("d{i}");
        test = test
            .node(&w, ScriptedExecutor::new(w.as_str()).wait())
            .node(&d, ok(&d))
            .edge(&w, &d);
    }
    let run = test.start().await;
    let mut peak = 0usize;
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            peak = peak.max(running_count(&snap));
            assert!(peak <= 16, "HITL entry peak {peak}");
            let waiting = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Waiting { .. }))
                .count();
            if waiting == n {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hitl_drain: not all 200 entered Waiting (permits stuck?)");
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);

    let mut order: Vec<usize> = (0..n).collect();
    let mut seed = 0xC0FFEE_u64;
    for i in 0..n {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        let j = (seed as usize) % n;
        order.swap(i, j);
    }

    let mut first_complete: Option<(keel_rt::ResumeToken, Bytes)> = None;
    for (batch_i, chunk) in order.chunks(20).enumerate() {
        for &i in chunk {
            let token = run.resume_token(&format!("w{i}")).await;
            let payload = Bytes::from(format!("hitl-{i}"));
            if first_complete.is_none() {
                first_complete = Some((token.clone(), payload.clone()));
            }
            run.resume(
                token,
                Resume::Complete(NodeOutcome::Succeeded(payload)),
            )
            .await
            .expect("resume Complete");
        }
        tokio::time::timeout(BOUND, async {
            loop {
                let snap = run.snapshot().await;
                peak = peak.max(running_count(&snap));
                assert!(peak <= 16, "HITL drain peak {peak}");
                let done = chunk.iter().all(|&i| {
                    matches!(
                        snap.node(&NodeId::new(format!("d{i}"))).map(|n| &n.state),
                        Some(NodeState::Succeeded)
                    )
                });
                if done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("hitl_drain batch {batch_i} dependents never ran"));
    }

    let (dup, payload) = first_complete.expect("token");
    run.resume(
        dup,
        Resume::Complete(NodeOutcome::Succeeded(payload)),
    )
    .await
    .expect("duplicate equivalent Complete is Ok noop");

    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("hitl_drain timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(run.inputs("d0").await.len(), 1);
    eprintln!(
        "workload hitl_drain N={n} mix=Waiting+shuffled-resume-20 peak={peak} elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Shared graph: retry-then-succeed only. Permanent Accept-Failed is a
/// separate isolated-execution test (`many_executions_1pct_fail`).
#[tokio::test(flavor = "current_thread")]
async fn flaky_io_retry_storm() {
    let n = 1000usize;
    let n_once = 300usize;
    let n_twice = 50usize;
    let started = Instant::now();
    let delay = RETRY_DELAY;
    let ok_ex = instant("ok");
    let mut test = WorkflowTest::with_graph_capacity(n + 1, n)
        .silent()
        .concurrency(32)
        .policy(RetryPolicy::new(3, delay))
        .node("join", ok("join"));
    for i in 0..n {
        let id = format!("n{i}");
        test = test.edge(&id, "join");
        if i < n_once {
            test = test.node(
                &id,
                ScriptedExecutor::new(id.as_str())
                    .fail("io-1")
                    .succeed(Bytes::from_static(b"after-1")),
            );
        } else if i < n_once + n_twice {
            test = test.node(
                &id,
                ScriptedExecutor::new(id.as_str())
                    .fail("io-1")
                    .fail("io-2")
                    .succeed(Bytes::from_static(b"after-2")),
            );
        } else {
            test = test.node_arc(&id, ok_ex.clone());
        }
    }
    let clock = test.fake_clock();
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if run.execution_state().await == ExecutionState::Succeeded {
                break;
            }
            clock.advance(delay);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("flaky_io_retry_storm timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);

    for i in 0..n_once {
        let a = run.scripted(&format!("n{i}")).attempts();
        assert_eq!(a, vec![1, 2], "fail-once n{i} attempts={a:?}");
        assert!(a.iter().all(|&x| x <= 3));
    }
    for i in n_once..(n_once + n_twice) {
        let a = run.scripted(&format!("n{i}")).attempts();
        assert_eq!(a, vec![1, 2, 3], "fail-twice n{i} attempts={a:?}");
    }
    let inputs = run.inputs("join").await;
    assert_eq!(inputs.len(), n);
    assert_eq!(
        inputs.get(&NodeId::new("n0")),
        Some(&Bytes::from_static(b"after-1")),
        "join must see the successful output, not the failed attempt"
    );
    assert_eq!(
        inputs.get(&NodeId::new(format!("n{n_once}"))),
        Some(&Bytes::from_static(b"after-2"))
    );
    eprintln!(
        "workload flaky_io_retry_storm N={n} mix=30%fail1+5%fail2+65%ok elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// 1% permanent fail cannot live in the shared graph (global fail-fast).
/// Isolated one-node executions: 2 Failed, 198 Succeeded.
#[tokio::test(flavor = "current_thread")]
async fn many_executions_1pct_fail() {
    let n = 200usize;
    let n_fail = 2usize; // 1%
    let started = Instant::now();
    let store = MemoryStore::new();
    let mut failed = 0usize;
    for i in 0..n {
        let mut test = WorkflowTest::new()
            .store(store.clone())
            .policy(RetryPolicy::new(3, Duration::ZERO))
            .concurrency(1);
        test = if i < n_fail {
            test.node(
                "n",
                ScriptedExecutor::new("n")
                    .fail("e1")
                    .fail("e2")
                    .fail("e3"),
            )
        } else {
            test.node("n", ok("n"))
        };
        let run = tokio::time::timeout(BOUND, test.run())
            .await
            .unwrap_or_else(|_| panic!("1pct exec {i} timed out"));
        if i < n_fail {
            assert_eq!(run.execution_state().await, ExecutionState::Failed);
            assert_eq!(run.scripted("n").attempts(), vec![1, 2, 3]);
            failed += 1;
        } else {
            assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
        }
    }
    assert_eq!(failed, n_fail);
    eprintln!(
        "workload many_executions_1pct_fail N={n} mix=1%Accept-Failed-isolated elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Wave A (2000) → Waiting gate → wave B (2000). B does not start until the
/// gate completes. Peak Running ≤ 64 both waves.
#[tokio::test(flavor = "current_thread")]
async fn burst_idle_burst() {
    let wave = 2000usize;
    let started = Instant::now();
    let peak_a = Arc::new(AtomicUsize::new(0));
    let cur_a = Arc::new(AtomicUsize::new(0));
    let b_starts = Arc::new(AtomicUsize::new(0));
    let a_ex: Arc<dyn Executor> = {
        let peak = peak_a.clone();
        let cur = cur_a.clone();
        Arc::new(FunctionExecutor::new("a", move |_ctx: ExecutionContext| {
            let c = cur.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(c, Ordering::SeqCst);
            let cur = cur.clone();
            async move {
                cur.fetch_sub(1, Ordering::SeqCst);
                NodeOutcome::Succeeded(Bytes::from_static(b"a"))
            }
        }))
    };
    let b_ex: Arc<dyn Executor> = {
        let b_starts = b_starts.clone();
        Arc::new(FunctionExecutor::new("b", move |_ctx: ExecutionContext| {
            b_starts.fetch_add(1, Ordering::SeqCst);
            async move { NodeOutcome::Succeeded(Bytes::from_static(b"b")) }
        }))
    };
    let mut test = WorkflowTest::with_graph_capacity(wave * 2 + 1, wave * 2)
        .silent()
        .concurrency(64)
        .node("gate", ScriptedExecutor::new("gate").wait());
    for i in 0..wave {
        let a = format!("a{i}");
        let b = format!("b{i}");
        test = test
            .node_arc(&a, a_ex.clone())
            .node_arc(&b, b_ex.clone())
            .edge(&a, "gate")
            .edge("gate", &b);
    }
    let run = test.start().await;
    let mut peak_snap = 0usize;
    tokio::time::timeout(BOUND, async {
        loop {
            let snap = run.snapshot().await;
            let r = running_count(&snap);
            peak_snap = peak_snap.max(r);
            assert!(r <= 64, "wave A Running {r} > 64");
            if matches!(
                snap.node(&NodeId::new("gate")).map(|n| &n.state),
                Some(NodeState::Waiting { .. })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("burst_idle_burst: gate never Waiting");
    assert_eq!(
        b_starts.load(Ordering::SeqCst),
        0,
        "wave B started before the gate completed"
    );
    let pa = peak_a.load(Ordering::SeqCst).max(peak_snap);
    assert!(pa <= 64, "wave A peak {pa}");
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);

    let token = run.resume_token("gate").await;
    run.resume(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
    )
    .await
    .unwrap();
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("burst_idle_burst wave B timed out");
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(b_starts.load(Ordering::SeqCst), wave);
    eprintln!(
        "workload burst_idle_burst N={} mix=2000-wait-gate-2000 peak_a={pa} elapsed={} profile={}",
        wave * 2 + 1,
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// 1_000 sequential 4-node diamonds on one MemoryStore. Snapshots do not clobber.
#[tokio::test(flavor = "current_thread")]
async fn many_executions_1000_sequential() {
    let n = 1000usize;
    let started = Instant::now();
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(2)
        .register(payload_exec("research"))
        .register(payload_exec("summarizer"))
        .register(payload_exec("critic"))
        .register(payload_exec("writer"))
        .build();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let h = rt.start(diamond_def()).expect("start");
        let id = h.inspect().await.execution_id.clone();
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("diamond exec {i} timed out"));
        assert_eq!(state, ExecutionState::Succeeded);
        ids.push(id);
    }
    let elapsed = started.elapsed();
    for id in &ids {
        let snap = store.get(id).await.unwrap().expect("stored diamond");
        assert_eq!(snap.state, ExecutionState::Succeeded);
        assert_eq!(snap.execution_id.as_str(), id.as_str());
    }
    let mut uniq = ids.clone();
    uniq.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    uniq.dedup();
    assert_eq!(uniq.len(), n);
    let per_s = n as f64 / elapsed.as_secs_f64();
    eprintln!(
        "workload many_executions_1000_sequential N={n} diamonds execs/s={per_s:.1} elapsed={} profile={}",
        format_ms(elapsed),
        profile_name()
    );
}

/// Phase 1: one Runtime may spawn several scheduler loops on current_thread.
#[tokio::test(flavor = "current_thread")]
async fn many_executions_8_concurrent() {
    let n = 8usize;
    let started = Instant::now();
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(2)
        .register(payload_exec("research"))
        .register(payload_exec("summarizer"))
        .register(payload_exec("critic"))
        .register(payload_exec("writer"))
        .build();
    let handles: Vec<_> = (0..n)
        .map(|_| rt.start(diamond_def()).expect("start"))
        .collect();
    let mut ids = Vec::new();
    for h in &handles {
        ids.push(h.inspect().await.execution_id.clone());
    }
    for (i, h) in handles.into_iter().enumerate() {
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("concurrent diamond {i} timed out"));
        assert_eq!(state, ExecutionState::Succeeded);
    }
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut uniq = ids.clone();
    uniq.dedup();
    assert_eq!(uniq.len(), n, "concurrent executions must not share ids");
    for id in &ids {
        assert_eq!(
            store.get(id).await.unwrap().expect("snap").state,
            ExecutionState::Succeeded
        );
    }
    eprintln!(
        "workload many_executions_8_concurrent N={n} mix=one-Runtime-many-schedulers elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

/// Fail-fast diamond × N: RetryFailed re-invokes the Failed node and cancelled
/// successors; Succeeded research is not re-run.
#[tokio::test(flavor = "current_thread")]
async fn retry_failed_fail_fast_diamond_times_n() {
    const N: usize = 64;
    let started = Instant::now();
    let store = MemoryStore::new();
    let def = diamond_def();
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(4)
        .register_fn("research", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"A"))
        })
        .register_fn("summarizer", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"S"))
        })
        .register_fn("critic", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("boom")
        })
        .register_fn("writer", |_ctx: ExecutionContext| async {
            panic!("writer must not run on fail-fast")
        })
        .build();
    let mut ids = Vec::with_capacity(N);
    for i in 0..N {
        let h = rt.start(def.clone()).expect("start");
        let id = h.execution_id().clone();
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("fail-fast diamond {i} timed out"));
        assert_eq!(state, ExecutionState::Failed);
        ids.push(id);
    }
    let research = Arc::new(AtomicUsize::new(0));
    let critic = Arc::new(AtomicUsize::new(0));
    let writer = Arc::new(AtomicUsize::new(0));
    let (rc, cc, wc) = (research.clone(), critic.clone(), writer.clone());
    let rt = Runtime::builder()
        .store(store)
        .concurrency(4)
        .register_fn("research", move |_ctx: ExecutionContext| {
            rc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) }
        })
        .register_fn("summarizer", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"S"))
        })
        .register_fn("critic", move |_ctx: ExecutionContext| {
            cc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"C")) }
        })
        .register_fn("writer", move |_ctx: ExecutionContext| {
            wc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"W")) }
        })
        .build();
    for (i, id) in ids.iter().enumerate() {
        let h = tokio::time::timeout(BOUND, rt.resume_with(id, Recover::RetryFailed))
            .await
            .unwrap_or_else(|_| panic!("RetryFailed {i} timed out"))
            .unwrap();
        let state = tokio::time::timeout(BOUND, h.wait())
            .await
            .unwrap_or_else(|_| panic!("RetryFailed wait {i} timed out"));
        assert_eq!(state, ExecutionState::Succeeded);
    }
    assert_eq!(research.load(Ordering::SeqCst), 0, "Succeeded A stays");
    assert_eq!(critic.load(Ordering::SeqCst), N, "Failed critic retried");
    assert_eq!(writer.load(Ordering::SeqCst), N, "Cancelled writer runs");
    eprintln!(
        "workload retry_failed_fail_fast_diamond_times_n N={N} elapsed={} profile={}",
        format_ms(started.elapsed()),
        profile_name()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn workload_median_report() {
    // Cheap workloads only — farm/map-reduce/burst are single-shot above.
    let mut ff = Vec::new();
    for _ in 0..3 {
        let t = Instant::now();
        let run = WorkflowTest::new()
            .concurrency(1)
            .node("src", ok("src"))
            .node("a", ok("a"))
            .node("b", ScriptedExecutor::new("b").fail("x"))
            .edge("src", "a")
            .edge("src", "b")
            .run()
            .await;
        assert_eq!(run.execution_state().await, ExecutionState::Failed);
        ff.push(t.elapsed());
    }
    eprintln!(
        "workload_median fail_fast_is_execution_wide={} (n=3) profile={}",
        format_ms(median_dur(ff)),
        profile_name()
    );
}
