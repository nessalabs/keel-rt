//! Wrong attempt, wrong token, resume-on-Running, Reinvoke, retry delay 0.

use super::common::{hang, ok, within};
use bytes::Bytes;
use keel_rt::domain::state::{ApplyCmd, Execution};
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::{
    AcceptPolicy, ApplyError, ExecutionState, NodeId, NodeOutcome, NodeState, Resume, ResumeToken,
    RetryPolicy, WorkflowDefinition,
};
use std::time::Duration;

fn linear_exec() -> Execution {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    Execution::new(def)
}

#[test]
fn finish_wrong_attempt_ignored() {
    let mut ex = linear_exec();
    let now = keel_rt::Timestamp(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    let rev = ex.revision();
    let r = ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 99,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"stale"))),
        },
        &p,
        now,
    );
    assert!(r.is_ok(), "stale finish is ignored, not a crash");
    assert!(
        matches!(
            ex.snapshot().node(&NodeId::new("a")).unwrap().state,
            NodeState::Running { attempt: 1 }
        ),
        "wrong attempt must not finish the node"
    );
    assert_eq!(ex.revision(), rev, "stale finish must not bump revision");
}

#[test]
fn resume_wrong_node_wrong_execution_wrong_nonce() {
    let mut ex = linear_exec();
    let now = keel_rt::Timestamp(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
        .unwrap();
    let good = ex
        .snapshot()
        .node(&NodeId::new("a"))
        .and_then(|n| n.resume_token.clone())
        .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Waiting {
                token: good.clone(),
            }),
        },
        &p,
        now,
    )
    .unwrap();

    let wrong_node = ResumeToken::issue(good.execution_id.clone(), NodeId::new("b"), 1);
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_node,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());

    let wrong_exec = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("a"), 1);
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_exec,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());

    let wrong_nonce = ResumeToken::issue(good.execution_id.clone(), NodeId::new("a"), 1);
    assert_ne!(wrong_nonce.nonce(), good.nonce());
    assert!(ex
        .apply(
            ApplyCmd::Resume {
                token: wrong_nonce,
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
            },
            &p,
            now,
        )
        .is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn resume_after_cancel_errors() {
    let run = within(WorkflowTest::new().node("a", ScriptedExecutor::new("a").wait()).run()).await;
    let token = run.resume_token("a").await;
    run.cancel().await;
    within(run.wait_stable()).await;
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
async fn resume_on_running_node_is_error() {
    let run = WorkflowTest::new().node("a", hang("a")).start().await;
    within(run.scripted("a").wait_until_hanging()).await;
    let token = run.resume_token("a").await;
    let err = run
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::NotWaiting | ApplyError::Illegal(_)),
        "{err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reinvoke_then_complete_success_path() {
    let run = within(
        WorkflowTest::new()
            .node(
                "a",
                ScriptedExecutor::new("a")
                    .wait()
                    .wait()
                    .succeed(Bytes::from_static(b"unused")),
            )
            .node("b", ok("b"))
            .edge("a", "b")
            .run(),
    )
    .await;
    let token = run.resume_token("a").await;
    run.resume(token, Resume::Reinvoke).await.unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 1]);
    let token2 = run.resume_token("a").await;
    run.resume(
        token2,
        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"done"))),
    )
    .await
    .unwrap();
    within(run.wait_stable()).await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"done"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn retry_delay_zero_successor_sees_success_output_only() {
    let run = within(
        WorkflowTest::new()
            .node(
                "a",
                ScriptedExecutor::new("a")
                    .fail("once")
                    .succeed(Bytes::from_static(b"good")),
            )
            .node("b", ok("b"))
            .edge("a", "b")
            .policy(RetryPolicy::new(3, Duration::ZERO))
            .run(),
    )
    .await;
    assert_eq!(run.scripted("a").attempts(), vec![1, 2]);
    assert_eq!(run.scripted("b").attempts(), vec![1]);
    assert_eq!(
        run.inputs("b").await.get(&NodeId::new("a")),
        Some(&Bytes::from_static(b"good"))
    );
}
