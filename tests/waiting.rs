use bytes::Bytes;
use std::time::Duration;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{ApplyError, ExecutionState, NodeOutcome, NodeState, Resume};

#[tokio::test(flavor = "current_thread")]
async fn waiting_releases_permit_other_ready_node_runs() {
    let run = WorkflowTest::new()
        .node("w", ScriptedExecutor::new("w").wait())
        .node("x", ScriptedExecutor::new("x").succeed(Bytes::from_static(b"X")))
        .concurrency(1)
        .run()
        .await;

    assert!(matches!(run.state("w").await, NodeState::Waiting { .. }));
    assert!(matches!(run.state("x").await, NodeState::Succeeded));
    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_complete_succeeded_dependents_run_with_output() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").wait())
        .node("b", ScriptedExecutor::new("b").succeed(Bytes::from_static(b"B")))
        .edge("a", "b")
        .run()
        .await;

    assert_eq!(run.execution_state().await, ExecutionState::Waiting);
    let token = run.resume_token("a").await;
    run.resume(
        token,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"A-out"))),
    )
    .await
    .unwrap();
    run.wait_stable().await;

    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert!(matches!(run.state("b").await, NodeState::Succeeded));
    assert_eq!(
        run.inputs("b").await.get(&keel_rt::NodeId::new("a")),
        Some(&Bytes::from_static(b"A-out"))
    );
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_reinvoke_same_attempt() {
    let run = WorkflowTest::new()
        .node(
            "a",
            ScriptedExecutor::new("a")
                .wait()
                .succeed(Bytes::from_static(b"second")),
        )
        .run()
        .await;

    assert_eq!(run.scripted("a").attempts(), vec![1]);
    let token = run.resume_token("a").await;
    assert_eq!(token.attempt, 1);
    run.resume(token, Resume::Reinvoke).await.unwrap();
    run.wait_stable().await;

    // Same attempt, second invocation.
    assert_eq!(run.scripted("a").attempts(), vec![1, 1]);
    assert!(matches!(run.state("a").await, NodeState::Succeeded));
    assert_eq!(run.output("a").await, Some(Bytes::from_static(b"second")));
}

#[tokio::test(flavor = "current_thread")]
async fn resume_after_cancel_is_error() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").wait())
        .run()
        .await;

    let token = run.resume_token("a").await;
    run.cancel().await;
    run.wait_stable().await;
    let err = run
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert_eq!(err, ApplyError::ResumeAfterCancel);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_complete_same_success_ok_dependents_not_run_twice() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").wait())
        .node("b", ScriptedExecutor::new("b").succeed(Bytes::from_static(b"B")))
        .edge("a", "b")
        .run()
        .await;

    let token = run.resume_token("a").await;
    let outcome = NodeOutcome::Succeeded(Bytes::from_static(b"same"));
    run.resume(token.clone(), Resume::Complete(outcome.clone()))
        .await
        .unwrap();
    run.wait_stable().await;
    assert_eq!(run.scripted("b").attempts(), vec![1]);

    run.resume(token, Resume::Complete(outcome))
        .await
        .expect("duplicate equivalent Complete is Ok noop");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        run.scripted("b").attempts(),
        vec![1],
        "dependents must not run twice"
    );
}
