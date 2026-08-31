//! Public-API consumer pack (no `WorkflowTest` / FakeClock required).
//!
//! `cargo test --test consumer -- --test-threads=1`

use bytes::Bytes;
use keel_rt::{
    AcceptPolicy, ApplyCmd, ApplyError, Clock, DomainEvent, Execution, ExecutionContext,
    ExecutionState, FnSink, Join, MemoryStore, NeverWaitPolicy, NodeId, NodeOutcome, NoopStore,
    OnFailure, Policy, PolicyDecision, ResumeToken, Runtime, StartError, StateStore, Timestamp,
    WorkflowDefinition,
};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn register_fn_tiny_diamond() {
    let def = WorkflowDefinition::builder("diamond")
        .node("a", "http")
        .node("b", "transform")
        .node("c", "transform")
        .node("d", "publish")
        .edge("a", "b")
        .edge("a", "c")
        .edge("b", "d")
        .edge("c", "d")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .concurrency(2)
        .register_fn("http", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"A"))
        })
        .register_fn("transform", |ctx: ExecutionContext| async move {
            NodeOutcome::Succeeded(Bytes::from(format!("t:{}", ctx.node_id)))
        })
        .register_fn("publish", |ctx: ExecutionContext| async move {
            assert_eq!(ctx.inputs.len(), 2);
            NodeOutcome::Succeeded(Bytes::from_static(b"D"))
        })
        .build();
    let state = rt.run(def).await.expect("start");
    assert_eq!(state, ExecutionState::Succeeded);
    assert!(state.is_successful_finish());
}

#[test]
fn builder_accepts_owned_string() {
    let def = WorkflowDefinition::builder(format!("burst-{}", 3))
        .node(NodeId::new(format!("n{}", 1)), "e")
        .build()
        .unwrap();
    assert_eq!(def.id().as_str(), "burst-3");
    assert!(def.node(&NodeId::new("n1")).is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn start_unknown_executor_errors_and_nothing_runs() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("ghost", "missing")
        .node("also", "gone")
        .build()
        .unwrap();
    let rt = Runtime::builder().store(store.clone()).build();
    let err = match rt.start(def) {
        Ok(_) => panic!("must not start"),
        Err(e) => e,
    };
    match &err {
        StartError::UnregisteredExecutors(ids) => {
            let names: Vec<&str> = ids.0.iter().map(|e| e.as_str()).collect();
            assert!(names.contains(&"missing"), "{names:?}");
            assert!(names.contains(&"gone"), "{names:?}");
        }
    }
    assert!(
        err.to_string().contains("missing") && err.to_string().contains("gone"),
        "{err}"
    );
    // Nothing persisted — start failed before spawn.
    // Store may be empty (no execution id to look up).
    let _ = store;
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_iter_nodes_matches_definition_order() {
    let mut b = WorkflowDefinition::builder("fan");
    for i in 0..20 {
        b = b.node(format!("page-{i:02}"), "ok");
    }
    let def = b.build().unwrap();
    let want: Vec<String> = def
        .nodes()
        .iter()
        .map(|n| n.id.as_str().to_string())
        .collect();

    let rt = Runtime::builder()
        .concurrency(8)
        .register_fn("ok", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    handle.wait_stable().await;
    let snap = handle.inspect().await;
    let got: Vec<String> = snap
        .iter_nodes()
        .map(|(id, _)| id.as_str().to_string())
        .collect();
    assert_eq!(got, want);
    assert_eq!(got.len(), 20);
    assert_eq!(got[0], "page-00");
    assert_eq!(got[19], "page-19");
    let rendered = snap.to_string();
    assert!(rendered.contains("page-00"));
    assert!(rendered.contains("page-19"));
    let _ = handle.wait().await;
}

#[tokio::test(flavor = "current_thread")]
async fn ctx_sleep_uses_public_clock() {
    let def = WorkflowDefinition::builder("sleep")
        .node("n", "n")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("n", |ctx: ExecutionContext| async move {
            // Public Clock + ExecutionContext::sleep — not test-util.
            let _now = ctx.clock.now();
            ctx.sleep(Duration::ZERO).await;
            Clock::sleep(&*ctx.clock, Duration::ZERO).await;
            NodeOutcome::Succeeded(Bytes::from_static(b"slept"))
        })
        .build();
    let state = rt.run(def).await.expect("start");
    assert_eq!(state, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn completed_is_successful_finish_failed_is_not() {
    let scoped = WorkflowDefinition::builder("scoped")
        .on_failure(OnFailure::FailSubtree)
        .join("j", Join::AllDone)
        .node("ok", "ok")
        .node("bad", "bad")
        .node("j", "j")
        .edge("ok", "j")
        .edge("bad", "j")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("ok", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .register_fn("bad", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("nope")
        })
        .register_fn("j", |ctx: ExecutionContext| async move {
            assert_eq!(ctx.inputs.len(), 1);
            NodeOutcome::Succeeded(Bytes::from_static(b"joined"))
        })
        .build();
    let state = rt.run(scoped).await.expect("start");
    assert_eq!(state, ExecutionState::Completed);
    assert!(state.is_successful_finish());

    let boom = WorkflowDefinition::builder("boom")
        .node("x", "x")
        .build()
        .unwrap();
    let fail_rt = Runtime::builder()
        .register_fn("x", |_ctx: ExecutionContext| async {
            NodeOutcome::failed("x")
        })
        .build();
    let failed = fail_rt.run(boom).await.expect("start");
    assert_eq!(failed, ExecutionState::Failed);
    assert!(!failed.is_successful_finish());
}

#[tokio::test(flavor = "current_thread")]
async fn noop_store_put_get_are_empty() {
    let store = NoopStore;
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let handle = rt.start(def).expect("start");
    let id = handle.inspect().await.execution_id.clone();
    handle.wait().await;
    assert!(store.get(&id).await.unwrap().is_none());
    let dummy = keel_rt::ExecutionSnapshot {
        schema_version: keel_rt::SCHEMA_VERSION,
        revision: 1,
        execution_id: id.clone(),
        workflow_id: keel_rt::WorkflowId::new("wf"),
        state: ExecutionState::Succeeded,
        nodes: Default::default(),
        node_order: Vec::new(),
    };
    store.put(&dummy).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_none(), "NoopStore never retains");
}

#[test]
fn definition_walk_and_unknown_id_is_empty() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .node("b", "e")
        .edge("a", "b")
        .build()
        .unwrap();
    assert_eq!(def.edges().len(), 1);
    assert_eq!(def.sources(), vec![NodeId::new("a")]);
    assert_eq!(def.predecessors(&NodeId::new("b")), vec![NodeId::new("a")]);
    assert_eq!(def.successors(&NodeId::new("a")), vec![NodeId::new("b")]);
    assert!(def.predecessors(&NodeId::new("ghost")).is_empty());
    assert!(def.successors(&NodeId::new("ghost")).is_empty());
}

#[test]
fn node_outcome_helpers_and_display() {
    let ok = NodeOutcome::succeeded(Bytes::from_static(b"xy"));
    let fail = NodeOutcome::failed("boom");
    let token = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("n"), 1);
    let wait = NodeOutcome::Waiting { token: token.clone() };
    assert!(ok.is_success());
    assert!(!fail.is_success());
    assert!(ok.equivalent(&NodeOutcome::succeeded(Bytes::from_static(b"xy"))));
    assert!(fail.equivalent(&NodeOutcome::failed("boom")));
    assert!(NodeOutcome::TimedOut.equivalent(&NodeOutcome::TimedOut));
    assert!(wait.equivalent(&NodeOutcome::Waiting { token }));
    assert!(!ok.equivalent(&fail));
    assert_eq!(ok.to_string(), "Succeeded(2 bytes)");
    assert!(fail.to_string().contains("boom"));
    assert!(wait.to_string().contains("Waiting(n)"));
    assert_eq!(NodeOutcome::TimedOut.to_string(), "TimedOut");
}

#[test]
fn never_wait_accepts_success_rejects_waiting() {
    let p = NeverWaitPolicy;
    let token = ResumeToken::issue(keel_rt::ExecutionId::new(), NodeId::new("n"), 1);
    assert_eq!(
        p.decide(&NodeOutcome::succeeded(Bytes::new()), 1),
        PolicyDecision::Accept
    );
    assert_eq!(
        p.decide(&NodeOutcome::Waiting { token }, 1),
        PolicyDecision::Reject
    );
}

#[test]
fn timestamp_round_trip_and_display() {
    let t = Timestamp::from_millis(1500);
    assert_eq!(t.as_millis(), 1500);
    assert_eq!(t.to_string(), "1500");
}

#[tokio::test(flavor = "current_thread")]
async fn fn_sink_display_names_start_and_success() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .sink(FnSink(move |e: &DomainEvent| {
            log.lock().unwrap().push(e.to_string());
        }))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    assert!(rt.run(def).await.unwrap().is_successful_finish());
    let lines = seen.lock().unwrap().clone();
    assert!(
        lines.iter().any(|s| s.starts_with("execution started")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|s| s.contains("node a succeeded")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|s| s.starts_with("execution succeeded")),
        "{lines:?}"
    );
}

#[test]
fn apply_start_twice_is_illegal_unknown_retry_is_noop() {
    let def = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let now = Timestamp::from_millis(0);
    let p = AcceptPolicy;
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    let err = ex.apply(ApplyCmd::Start, &p, now).unwrap_err();
    assert!(matches!(err, ApplyError::Illegal(_)));
    assert!(ex.executor_id(&NodeId::new("a")).is_some());
    assert!(ex.executor_id(&NodeId::new("ghost")).is_none());
    assert!(ex.inputs_for(&NodeId::new("ghost")).is_empty());
    let late = ex
        .apply(ApplyCmd::RetryDue { node_id: "ghost".into() }, &p, now)
        .unwrap();
    assert!(!late.changed);
    let idle = ex.apply(ApplyCmd::ForceCancelRunning, &p, now).unwrap();
    assert!(!idle.changed);
}
