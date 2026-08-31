//! `OnFailure::FailSubtree` + `Join::AllDone` are **opt-in**.
//! Library default remains `OnFailure::FailExecution` (this file never flips it).
//!
//! `cargo test --test scenarios -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{NetFault, ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, DomainEvent, ExecutionState, FunctionExecutor, Join, MemoryStore, NodeId,
    NodeOutcome, NodeState, OnFailure, RetryPolicy, Runtime, StateStore, WorkflowDefinition,
};
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(30);
const DELAY: Duration = Duration::from_millis(10);

fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

fn running_count(snap: &keel_rt::ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}

/// 1. Diamond A→{B,C}→D, C fails, FailSubtree, AllSucceeded.
#[tokio::test(flavor = "current_thread")]
async fn diamond_fail_subtree_all_succeeded_completes() {
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(2)
            .on_failure(OnFailure::FailSubtree)
            .node("a", ok("a"))
            .node("b", ok("b"))
            .node("c", ScriptedExecutor::new("c").fail("c-boom"))
            .node("d", ok("d"))
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .run(),
    )
    .await
    .expect("diamond_fail_subtree timed out");

    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert!(matches!(run.state("c").await, NodeState::Failed));
    assert!(
        matches!(run.state("b").await, NodeState::Succeeded),
        "sibling B must not be Cancelled under FailSubtree"
    );
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(run.scripted("d").attempts().is_empty(), "D must never start");
    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    let evs = run.events();
    let n_failed = evs
        .iter()
        .filter(|e| matches!(e, DomainEvent::NodeFailed { .. }))
        .count();
    assert_eq!(n_failed, 1);
    assert!(!evs
        .iter()
        .any(|e| matches!(e, DomainEvent::ExecutionFailed { .. })));
    assert!(evs
        .iter()
        .any(|e| matches!(e, DomainEvent::ExecutionCompleted { .. })));
}

/// 2. Same diamond, default FailExecution — lock the old contract.
#[tokio::test(flavor = "current_thread")]
async fn diamond_fail_execution_still_fail_fasts() {
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(2)
            .node("a", ok("a"))
            .node("b", ok("b"))
            .node("c", ScriptedExecutor::new("c").fail("c-boom"))
            .node("d", ok("d"))
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .run(),
    )
    .await
    .expect("diamond_fail_execution timed out");

    assert!(matches!(run.state("c").await, NodeState::Failed));
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(run.scripted("d").attempts().is_empty());
    let b = run.state("b").await;
    assert!(
        matches!(b, NodeState::Succeeded | NodeState::Cancelled),
        "B Cancelled if not already terminal; got {b:?}"
    );
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert_eq!(
        run.events()
            .iter()
            .filter(|e| matches!(e, DomainEvent::ExecutionFailed { .. }))
            .count(),
        1
    );
}

/// 3. Fan-in 4 → reducer AllDone, FailSubtree, one child fails: reducer runs.
#[tokio::test(flavor = "current_thread")]
async fn fanin_all_done_reducer_runs() {
    let mut test = WorkflowTest::new()
        .concurrency(4)
        .on_failure(OnFailure::FailSubtree)
        .join("reducer", Join::AllDone)
        .node("reducer", ok("reducer"));
    for i in 0..4 {
        let id = format!("c{i}");
        let exec = if i == 1 {
            ScriptedExecutor::new(id.as_str()).fail("child-fail")
        } else {
            ok(&id)
        };
        test = test.node(&id, exec).edge(&id, "reducer");
    }
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("fanin_all_done timed out");

    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(matches!(run.state("c1").await, NodeState::Failed));
    assert!(matches!(run.state("reducer").await, NodeState::Succeeded));
    let inputs = run.inputs("reducer").await;
    assert_eq!(inputs.len(), 3);
    assert!(!inputs.contains_key(&NodeId::new("c1")));
    assert!(inputs.contains_key(&NodeId::new("c0")));
}

/// 4. Fan-in 4 → reducer AllSucceeded, FailSubtree: reducer Cancelled.
#[tokio::test(flavor = "current_thread")]
async fn fanin_all_succeeded_reducer_cancelled() {
    let mut test = WorkflowTest::new()
        .concurrency(4)
        .on_failure(OnFailure::FailSubtree)
        .node("reducer", ok("reducer"));
    for i in 0..4 {
        let id = format!("c{i}");
        let exec = if i == 1 {
            ScriptedExecutor::new(id.as_str()).fail("child-fail")
        } else {
            ok(&id)
        };
        test = test.node(&id, exec).edge(&id, "reducer");
    }
    let run = tokio::time::timeout(BOUND, test.run())
        .await
        .expect("fanin_all_succeeded timed out");

    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(matches!(run.state("reducer").await, NodeState::Cancelled));
    assert!(run.scripted("reducer").attempts().is_empty());
    assert!(matches!(run.state("c0").await, NodeState::Succeeded));
    assert!(matches!(run.state("c1").await, NodeState::Failed));
}

/// 5. Triple nested: fail mid; only its downstream cancelled; uncle Writer runs.
#[tokio::test(flavor = "current_thread")]
async fn nested_fail_subtree_uncle_writer_runs() {
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(4)
            .on_failure(OnFailure::FailSubtree)
            .node("seed", ok("seed"))
            .node("mid", ScriptedExecutor::new("mid").fail("mid-fail"))
            .node("down", ok("down"))
            .node("uncle", ok("uncle"))
            .node("writer", ok("writer"))
            .edge("seed", "mid")
            .edge("mid", "down")
            .edge("seed", "uncle")
            .edge("uncle", "writer")
            .run(),
    )
    .await
    .expect("nested_fail_subtree timed out");

    assert!(matches!(run.state("seed").await, NodeState::Succeeded));
    assert!(matches!(run.state("mid").await, NodeState::Failed));
    assert!(matches!(run.state("down").await, NodeState::Cancelled));
    assert!(run.scripted("down").attempts().is_empty());
    assert!(matches!(run.state("uncle").await, NodeState::Succeeded));
    assert!(matches!(run.state("writer").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Completed);
}

/// 6. TimedOut + FailSubtree: descendants cancelled, siblings live.
#[tokio::test(flavor = "current_thread")]
async fn timeout_fail_subtree_siblings_live() {
    let run = tokio::time::timeout(
        BOUND,
        WorkflowTest::new()
            .concurrency(2)
            .policy(AcceptPolicy)
            .on_failure(OnFailure::FailSubtree)
            .node("a", ok("a"))
            .node(
                "t",
                ScriptedExecutor::new("t").fault(NetFault::Timeout),
            )
            .node("sib", ok("sib"))
            .node("join", ok("join"))
            .edge("a", "t")
            .edge("a", "sib")
            .edge("t", "join")
            .edge("sib", "join")
            .run(),
    )
    .await
    .expect("timeout_fail_subtree timed out");

    assert!(matches!(run.state("t").await, NodeState::TimedOut));
    assert!(matches!(run.state("sib").await, NodeState::Succeeded));
    assert!(matches!(run.state("join").await, NodeState::Cancelled));
    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(!run
        .events()
        .iter()
        .any(|e| matches!(e, DomainEvent::ExecutionFailed { .. })));
}

/// 7. Retry then FailSubtree: after max, subtree — not FailExecution.
#[tokio::test(flavor = "current_thread")]
async fn retry_then_fail_subtree() {
    let test = WorkflowTest::new()
        .concurrency(2)
        .on_failure(OnFailure::FailSubtree)
        .policy(RetryPolicy::new(2, DELAY))
        .node(
            "a",
            ScriptedExecutor::new("a")
                .fail("again")
                .fail("again"),
        )
        .node("sib", ok("sib"));
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
    .expect("retry never parked");
    clock.advance(DELAY);
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("retry_then_fail_subtree timed out");

    assert!(matches!(run.state("a").await, NodeState::Failed));
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert!(matches!(run.state("sib").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(!run
        .events()
        .iter()
        .any(|e| matches!(e, DomainEvent::ExecutionFailed { .. })));
}

/// 8. User cancel still cancels the whole graph under FailSubtree.
#[tokio::test(flavor = "current_thread")]
async fn user_cancel_overrides_fail_subtree() {
    let test = WorkflowTest::new()
        .on_failure(OnFailure::FailSubtree)
        .node("a", ScriptedExecutor::new("a").hang(false))
        .node("b", ok("b"));
    let run = test.start().await;
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(run.state("a").await, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hang never Running");
    run.cancel().await;
    tokio::time::timeout(BOUND, run.wait_stable())
        .await
        .expect("cancel hung");
    assert_eq!(run.execution_state().await, ExecutionState::Cancelled);
    assert!(matches!(run.state("a").await, NodeState::Cancelled));
    let b = run.state("b").await;
    assert!(
        matches!(b, NodeState::Cancelled | NodeState::Succeeded),
        "cancel is graph-wide; got {b:?}"
    );
}

/// 9. 1 seed → 50 children → reducer AllDone, 5 fail/timeout, FailSubtree.
#[tokio::test(flavor = "current_thread")]
async fn farm_50_fail_subtree_all_done() {
    let n = 50usize;
    let mut test = WorkflowTest::new()
        .concurrency(16)
        .on_failure(OnFailure::FailSubtree)
        .join("reducer", Join::AllDone)
        .node("seed", ok("seed"))
        .node("reducer", ok("reducer"));
    for i in 0..n {
        let id = format!("c{i}");
        let exec = match i % 20 {
            0 => ScriptedExecutor::new(id.as_str()).fault(NetFault::Timeout),
            10 => ScriptedExecutor::new(id.as_str()).fail("child"),
            _ => ok(&id),
        };
        test = test.node(&id, exec).edge("seed", &id).edge(&id, "reducer");
    }
    let run = test.start().await;
    let mut peak = 0usize;
    tokio::time::timeout(BOUND, async {
        loop {
            let r = running_count(&run.snapshot().await);
            peak = peak.max(r);
            assert!(r <= 16, "Running {r} > concurrency 16");
            if run.execution_state().await.is_terminal() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("farm_50 timed out");

    assert_eq!(run.execution_state().await, ExecutionState::Completed);
    assert!(peak <= 16);
    assert!(matches!(run.state("reducer").await, NodeState::Succeeded));
    let inputs = run.inputs("reducer").await;
    assert_eq!(inputs.len(), 45);
    for i in 0..n {
        if i % 20 == 0 || i % 20 == 10 {
            assert!(
                !inputs.contains_key(&NodeId::new(format!("c{i}"))),
                "failed/timed-out c{i} must not appear in reducer inputs"
            );
        }
    }
    assert_eq!(run.scripted("reducer").attempts().len(), 1);
}

/// 10. Sequential other Runtime executions stay isolated.
#[tokio::test(flavor = "current_thread")]
async fn sequential_executions_isolated() {
    let store = MemoryStore::new();
    let fail = FunctionExecutor::new("fail", |_ctx| async {
        NodeOutcome::failed("boom")
    });
    let ok_ex = FunctionExecutor::new("ok", |_ctx| async {
        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
    });
    let rt = Runtime::builder()
        .store(store.clone())
        .concurrency(2)
        .register(ok_ex)
        .register(fail)
        .build();

    let scoped = WorkflowDefinition::builder("scoped")
        .on_failure(OnFailure::FailSubtree)
        .node("a", "ok")
        .node("b", "fail")
        .node("c", "ok")
        .node("d", "ok")
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .build()
        .unwrap();
    let h1 = rt.start(scoped);
    let id1 = h1.inspect().await.execution_id.clone();
    let s1 = tokio::time::timeout(BOUND, h1.wait())
        .await
        .expect("scoped exec timed out");
    assert_eq!(s1, ExecutionState::Completed);

    let happy = WorkflowDefinition::builder("happy")
        .node("a", "ok")
        .node("b", "ok")
        .node("c", "ok")
        .node("d", "ok")
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .build()
        .unwrap();
    let h2 = rt.start(happy);
    let id2 = h2.inspect().await.execution_id.clone();
    let s2 = tokio::time::timeout(BOUND, h2.wait())
        .await
        .expect("happy exec timed out");
    assert_eq!(s2, ExecutionState::Succeeded);

    let snap1 = store.get(&id1).await.unwrap().unwrap();
    let snap2 = store.get(&id2).await.unwrap().unwrap();
    assert_eq!(snap1.state, ExecutionState::Completed);
    assert_eq!(snap2.state, ExecutionState::Succeeded);
    assert!(matches!(
        snap1.node(&NodeId::new("b")).map(|n| &n.state),
        Some(NodeState::Failed)
    ));
    assert!(matches!(
        snap2.node(&NodeId::new("b")).map(|n| &n.state),
        Some(NodeState::Succeeded)
    ));
    assert_ne!(id1.as_str(), id2.as_str());
}
