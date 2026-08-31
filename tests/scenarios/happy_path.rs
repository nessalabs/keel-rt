use bytes::Bytes;
use std::time::Duration;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{ExecutionState, NodeId, NodeState};

#[tokio::test(flavor = "current_thread")]
async fn linear_a_b_c_strict_order_c_sees_b_output() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"from-a")))
        .node("b", ScriptedExecutor::new("b").succeed(Bytes::from_static(b"from-b")))
        .node("c", ScriptedExecutor::new("c").succeed(Bytes::from_static(b"from-c")))
        .edge("a", "b")
        .edge("b", "c")
        .concurrency(1)
        .run()
        .await;

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert!(matches!(run.state("b").await, NodeState::Succeeded));
    assert!(matches!(run.state("c").await, NodeState::Succeeded));
    assert_eq!(run.output("c").await, Some(Bytes::from_static(b"from-c")));

    let c_in = run.inputs("c").await;
    assert_eq!(
        c_in.get(&NodeId::new("b")),
        Some(&Bytes::from_static(b"from-b"))
    );
    assert!(
        !c_in.contains_key(&NodeId::new("a")),
        "dependents see predecessors only, not all ancestors"
    );

    let b_attempts = run.scripted("b").attempts();
    let a_attempts = run.scripted("a").attempts();
    let c_attempts = run.scripted("c").attempts();
    assert_eq!(a_attempts, vec![1]);
    assert_eq!(b_attempts, vec![1]);
    assert_eq!(c_attempts, vec![1]);
}

#[tokio::test(flavor = "current_thread")]
async fn diamond_b_and_c_overlap_d_waits_for_both() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"A")))
        .node("b", ScriptedExecutor::new("b").hang(false))
        .node("c", ScriptedExecutor::new("c").hang(false))
        .node("d", ScriptedExecutor::new("d").succeed(Bytes::from_static(b"D")))
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .concurrency(2)
        .start()
        .await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let b = run.state("b").await;
            let c = run.state("c").await;
            if matches!(b, NodeState::Running { .. }) && matches!(c, NodeState::Running { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("B and C should both be Running (overlapping) with concurrency>=2");

    run.release_hang("b").await;
    run.release_hang("c").await;
    run.wait_stable().await;

    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    let d_in = run.inputs("d").await;
    assert_eq!(d_in.get(&NodeId::new("b")), Some(&Bytes::from_static(b"released")));
    assert_eq!(d_in.get(&NodeId::new("c")), Some(&Bytes::from_static(b"released")));
}

#[tokio::test(flavor = "current_thread")]
async fn fan_out_concurrency_1_never_more_than_one_running() {
    let run = WorkflowTest::new()
        .node("n0", ScriptedExecutor::new("n0").hang(false))
        .node("n1", ScriptedExecutor::new("n1").hang(false))
        .node("n2", ScriptedExecutor::new("n2").hang(false))
        .node("n3", ScriptedExecutor::new("n3").hang(false))
        .concurrency(1)
        .start()
        .await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap = run.snapshot().await;
            let running = snap
                .nodes
                .values()
                .filter(|n| matches!(n.state, NodeState::Running { .. }))
                .count();
            if running == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exactly one node Running");

    let snap = run.snapshot().await;
    let running: Vec<_> = snap
        .nodes
        .iter()
        .filter(|(_, n)| matches!(n.state, NodeState::Running { .. }))
        .map(|(id, _)| id.as_str().to_string())
        .collect();
    assert_eq!(running.len(), 1, "concurrency=1 must never run two nodes");

    // Release the running hang; another may start; never two at once.
    run.scripted(&running[0]).release();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let snap = run.snapshot().await;
    let running_count = snap
        .nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count();
    assert!(running_count <= 1);

    for name in ["n0", "n1", "n2", "n3"] {
        run.scripted(name).release();
    }
    run.wait_stable().await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}
