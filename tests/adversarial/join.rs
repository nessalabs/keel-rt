//! Diamond/triple fail-fast, waiting pred, conflicting/duplicate Complete.

use super::common::{count_failed, hang, ok, within};
use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{ApplyError, ExecutionState, NodeId, NodeOutcome, NodeState};

#[tokio::test(flavor = "current_thread")]
async fn diamond_c_fails_d_cancelled_b_stays_succeeded() {
    let run = within(
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
    .await;
    assert!(matches!(run.state("c").await, NodeState::Failed));
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(
        matches!(run.state("b").await, NodeState::Succeeded),
        "fail-fast must not rewrite terminals"
    );
    assert!(run.scripted("d").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert_eq!(count_failed(&run.events()), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn triple_join_only_b_fails() {
    let run = within(
        WorkflowTest::new()
            .concurrency(3)
            .node("a", ok("a"))
            .node("b", ScriptedExecutor::new("b").fail("b-boom"))
            .node("c", ok("c"))
            .node("d", ok("d"))
            .edge("a", "d")
            .edge("b", "d")
            .edge("c", "d")
            .run(),
    )
    .await;
    assert!(matches!(run.state("d").await, NodeState::Cancelled));
    assert!(run.scripted("d").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    for name in ["a", "c"] {
        let st = run.state(name).await;
        assert!(
            matches!(st, NodeState::Succeeded | NodeState::Cancelled),
            "{name}={st:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn join_stays_pending_while_one_pred_waiting() {
    let run = within(
        WorkflowTest::new()
            .concurrency(2)
            .node("w", ScriptedExecutor::new("w").wait())
            .node("s", ok("s"))
            .node("join", ok("join"))
            .edge("w", "join")
            .edge("s", "join")
            .run(),
    )
    .await;
    assert!(matches!(run.state("join").await, NodeState::Pending));
    assert!(run.scripted("join").attempts().is_empty());
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_complete_same_bytes_dependents_once() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").wait())
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    let out = NodeOutcome::Succeeded(Bytes::from_static(b"same"));
    run.resume(token.clone(), keel_rt::Resume::Complete(out.clone()))
        .await
        .unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("b").attempts(), vec![1]);
    run.resume(token, keel_rt::Resume::Complete(out))
        .await
        .expect("duplicate equivalent Complete is Ok");
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    assert_eq!(run.scripted("b").attempts(), vec![1]);
}

#[tokio::test(flavor = "current_thread")]
async fn conflicting_complete_errors_second_payload_not_used() {
    let run = within(
        WorkflowTest::new()
            .node("a", ScriptedExecutor::new("a").wait())
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    run.resume(
        token.clone(),
        keel_rt::Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"first"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    let err = run
        .resume(
            token,
            keel_rt::Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"second"))),
        )
        .await
        .unwrap_err();
    assert_eq!(err, ApplyError::ConflictingComplete);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"first"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fail_fast_abort_hanging_sibling_one_failed_event_inspect_lives() {
    let run = WorkflowTest::new()
        .concurrency(2)
        .node("ok", ScriptedExecutor::new("ok").fail("boom"))
        .node("hang", hang("hang"))
        .start()
        .await;
    within(run.scripted("hang").wait_until_hanging()).await;
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Failed);
    assert_eq!(count_failed(&run.events()), 1);
    let snap = run.snapshot().await;
    assert_eq!(snap.state, ExecutionState::Failed);
}
