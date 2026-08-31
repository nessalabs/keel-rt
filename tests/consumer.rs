//! Public-API consumer pack (no `WorkflowTest` / FakeClock required).
//!
//! `cargo test --test consumer -- --test-threads=1`

use bytes::Bytes;
use keel_rt::{
    AcceptPolicy, ApplyCmd, ApplyError, Clock, DomainEvent, Execution, ExecutionContext,
    ExecutionState, FnSink, Join, MemoryStore, NeverWaitPolicy, NodeId, NodeOutcome, NodeState,
    NoopStore, OnFailure, Policy, PolicyDecision, ResumeToken, Runtime, StartError, StateStore,
    Timestamp, WorkflowDefinition, DEFAULT_CANCEL_BOUND,
};
use std::sync::Arc;
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

#[test]
fn domain_event_display_covers_every_variant() {
    let execution_id = keel_rt::ExecutionId::new();
    let node_id = NodeId::new("n");
    let token = ResumeToken::issue(execution_id.clone(), node_id.clone(), 2);
    let at = Timestamp::from_millis(9);
    let cases = [
        (
            DomainEvent::ExecutionStarted {
                execution_id: execution_id.clone(),
            },
            "execution started",
        ),
        (
            DomainEvent::ExecutionSucceeded {
                execution_id: execution_id.clone(),
            },
            "execution succeeded",
        ),
        (
            DomainEvent::ExecutionFailed {
                execution_id: execution_id.clone(),
            },
            "execution failed",
        ),
        (
            DomainEvent::ExecutionCancelled {
                execution_id: execution_id.clone(),
            },
            "execution cancelled",
        ),
        (
            DomainEvent::ExecutionWaiting {
                execution_id: execution_id.clone(),
            },
            "execution waiting",
        ),
        (
            DomainEvent::ExecutionCompleted {
                execution_id: execution_id.clone(),
            },
            "execution completed",
        ),
        (
            DomainEvent::NodeReady {
                node_id: node_id.clone(),
                runnable_at: None,
            },
            "node n ready",
        ),
        (
            DomainEvent::NodeReady {
                node_id: node_id.clone(),
                runnable_at: Some(at),
            },
            "node n ready at",
        ),
        (
            DomainEvent::NodeStarted {
                node_id: node_id.clone(),
                attempt: 3,
            },
            "started attempt=3",
        ),
        (
            DomainEvent::NodeSucceeded {
                node_id: node_id.clone(),
            },
            "node n succeeded",
        ),
        (
            DomainEvent::NodeFailed {
                node_id: node_id.clone(),
                error: keel_rt::NodeError::new("boom"),
            },
            "failed: boom",
        ),
        (
            DomainEvent::NodeCancelled {
                node_id: node_id.clone(),
            },
            "node n cancelled",
        ),
        (
            DomainEvent::NodeWaiting {
                node_id: node_id.clone(),
                token: token.clone(),
            },
            "node n waiting",
        ),
        (
            DomainEvent::NodeTimedOut {
                node_id: node_id.clone(),
            },
            "node n timed out",
        ),
    ];
    for (ev, needle) in cases {
        let s = ev.to_string();
        assert!(s.contains(needle), "{s} should contain {needle}");
    }
}

#[test]
fn ids_display_default_from_string_and_serde_round_trip() {
    let wf = keel_rt::WorkflowId::from(String::from("wf"));
    assert_eq!(wf.to_string(), "wf");
    let exec = keel_rt::ExecutorId::from(String::from("http"));
    assert_eq!(exec.to_string(), "http");
    let eid = keel_rt::ExecutionId::default();
    assert!(eid.as_str().starts_with("exec-"));
    assert_eq!(eid.to_string(), eid.as_str());

    let id = NodeId::new("page-7");
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"page-7\"");
    let back: NodeId = serde_json::from_str(&json).unwrap();
    assert_eq!(back, id);

    let snap = keel_rt::ExecutionSnapshot {
        schema_version: keel_rt::SCHEMA_VERSION,
        revision: 2,
        execution_id: eid.clone(),
        workflow_id: wf,
        state: ExecutionState::Succeeded,
        nodes: Default::default(),
        node_order: vec![id.clone()],
    };
    let sjson = serde_json::to_string(&snap).unwrap();
    let restored: keel_rt::ExecutionSnapshot = serde_json::from_str(&sjson).unwrap();
    assert_eq!(restored.execution_id, eid);
    assert_eq!(restored.node_order, vec![id]);
}

#[tokio::test(flavor = "current_thread")]
async fn policy_store_sink_arc_and_box_adapters_run() {
    use std::sync::Arc;
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let store: Arc<dyn keel_rt::StateStore> = Arc::new(MemoryStore::new());
    let policy: Arc<dyn Policy> = Arc::new(AcceptPolicy);
    let boxed: Box<dyn Policy> = Box::new(AcceptPolicy);
    assert_eq!(
        policy.decide(&NodeOutcome::succeeded(Bytes::new()), 1),
        PolicyDecision::Accept
    );
    assert_eq!(
        boxed.decide(&NodeOutcome::succeeded(Bytes::new()), 1),
        PolicyDecision::Accept
    );
    let def_store = WorkflowDefinition::builder("persist")
        .node("n", "n")
        .build()
        .unwrap();
    let exec = Execution::new(def_store);
    store.persist(&exec).await.unwrap();
    assert!(store.get(exec.id()).await.unwrap().is_some());
    store.put(&exec.snapshot()).await.unwrap();
    let seen = Arc::new(std::sync::Mutex::new(0u32));
    let c = seen.clone();
    let sink: Arc<dyn keel_rt::EventSink> = Arc::new(FnSink(move |_e: &DomainEvent| {
        *c.lock().unwrap() += 1;
    }));
    let rt = Runtime::builder()
        .store(store)
        .policy(policy)
        .sink_arc(sink)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    assert_eq!(rt.run(def).await.unwrap(), ExecutionState::Succeeded);
    assert!(*seen.lock().unwrap() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn persist_panic_does_not_kill_execution() {
    struct PanicStore;
    #[async_trait::async_trait]
    impl keel_rt::StateStore for PanicStore {
        async fn put(
            &self,
            _snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), keel_rt::StoreError> {
            panic!("put must not be the persist path for this test");
        }
        async fn get(
            &self,
            _id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, keel_rt::StoreError> {
            Ok(None)
        }
        async fn persist(
            &self,
            _exec: &keel_rt::Execution,
        ) -> Result<(), keel_rt::StoreError> {
            panic!("persist boom");
        }
    }

    // Catch persist panics like EventSink panics. Repeat: dispatch vs persist
    // interleaving must not leak a non-Succeeded wait.
    for i in 0..32 {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap();
        let rt = Runtime::builder()
            .store(PanicStore)
            .register_fn("a", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let state = tokio::time::timeout(Duration::from_secs(2), rt.run(def))
            .await
            .unwrap_or_else(|_| panic!("iter {i}: run hung after persist panic"))
            .expect("start");
        assert_eq!(
            state,
            ExecutionState::Succeeded,
            "iter {i}: persist panic must keep in-memory progress, got {state:?}"
        );
    }
}

struct PanicClock;

#[async_trait::async_trait]
impl Clock for PanicClock {
    fn now(&self) -> Timestamp {
        panic!("clock now");
    }
    async fn sleep(&self, _duration: Duration) {
        panic!("clock sleep");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_clock_inspect_is_stopped_and_wait_is_cancelled() {
    // Scheduler death with a live handle: inspect is the stopped snapshot,
    // wait/wait_stable are Cancelled — not whatever last watch value happened
    // to be. Repeat to catch Created vs Running vs Cancelled flakes.
    for i in 0..32 {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap();
        let rt = Runtime::builder()
            .clock(Arc::new(PanicClock))
            .register_fn("a", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let handle = rt.start(def).expect("start");
        let stable = tokio::time::timeout(Duration::from_secs(2), handle.wait_stable())
            .await
            .unwrap_or_else(|_| panic!("iter {i}: wait_stable hung after clock panic"));
        assert_eq!(
            stable,
            ExecutionState::Cancelled,
            "iter {i}: wait_stable after scheduler death, got {stable:?}"
        );
        let snap = handle.inspect().await;
        assert_eq!(
            snap.workflow_id.as_str(),
            "stopped",
            "iter {i}: inspect must not return a live snapshot after scheduler death"
        );
        assert_eq!(snap.state, ExecutionState::Cancelled);
        assert!(snap.nodes.is_empty(), "iter {i}");
        let state = tokio::time::timeout(Duration::from_secs(2), handle.wait())
            .await
            .unwrap_or_else(|_| panic!("iter {i}: wait hung after clock panic"));
        assert_eq!(
            state,
            ExecutionState::Cancelled,
            "iter {i}: wait after scheduler death, got {state:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn executor_panic_string_and_unknown_payload_fail_the_node() {
    let owned = WorkflowDefinition::builder("owned")
        .node("p", "p")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("p", |_ctx: ExecutionContext| async {
            std::panic::panic_any(String::from("owned-panic"));
        })
        .build();
    assert_eq!(rt.run(owned).await.unwrap(), ExecutionState::Failed);

    let num = WorkflowDefinition::builder("num")
        .node("p", "p")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .register_fn("p", |_ctx: ExecutionContext| async {
            std::panic::panic_any(42u32);
        })
        .build();
    let handle = rt.start(num).expect("start");
    assert_eq!(handle.wait().await, ExecutionState::Failed);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_twice_then_bound_still_cancels_hang() {
    let def = WorkflowDefinition::builder("hang")
        .node("h", "h")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .cancel_bound(DEFAULT_CANCEL_BOUND)
        .register_fn("h", |ctx: ExecutionContext| async move {
            loop {
                ctx.sleep(Duration::from_secs(60)).await;
            }
        })
        .build();
    let handle = rt.start(def).expect("start");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap = handle.inspect().await;
            if matches!(
                snap.node(&NodeId::new("h")).map(|n| &n.state),
                Some(NodeState::Running { .. })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hang node must be Running before cancel");
    handle.cancel().await;
    handle.cancel().await;
    tokio::time::sleep(DEFAULT_CANCEL_BOUND + Duration::from_millis(80)).await;
    let snap = handle.inspect().await;
    assert_eq!(snap.state, ExecutionState::Cancelled);
    let state = tokio::time::timeout(
        DEFAULT_CANCEL_BOUND + Duration::from_millis(200),
        handle.wait(),
    )
    .await
    .expect("cancel bound must finish hang");
    assert_eq!(state, ExecutionState::Cancelled);
}
