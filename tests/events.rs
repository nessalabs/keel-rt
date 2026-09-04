//! Phase 3 public Event + EventSink. FakeClock stamps `at`.
//!
//! `cargo test --test events -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{FailingStore, FakeClock};
use keel_rt::{
    Event, EventSink, ExecutionContext, ExecutionState, FnSink, MemoryStore, NodeError, NodeId,
    NodeOutcome, Resume, ResumeToken, Runtime, SinkError, StateStore, Timestamp,
    WorkflowDefinition, SCHEMA_VERSION,
};
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "current_thread")]
async fn events_carry_ids_clock_time_and_schema() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp::from_millis(42));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .clock(clock)
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let def = WorkflowDefinition::builder("wf-events")
        .node("a", "a")
        .build()
        .unwrap();
    assert_eq!(rt.run(def).await.unwrap(), ExecutionState::Succeeded);
    let events = seen.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ExecutionStarted { .. })),
        "{events:?}"
    );
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::NodeStarted { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::NodeSucceeded { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ExecutionSucceeded { .. })));
    for e in &events {
        assert_eq!(e.at().as_millis(), 42);
        assert_eq!(e.schema_version(), SCHEMA_VERSION);
        assert_eq!(e.workflow_id().as_str(), "wf-events");
        assert!(e.execution_id().as_str().starts_with("exec-"));
        match e {
            Event::ExecutionStarted { .. }
            | Event::ExecutionSucceeded { .. }
            | Event::ExecutionFailed { .. }
            | Event::ExecutionCompleted { .. }
            | Event::ExecutionCancelled { .. } => {
                assert!(e.node_id().is_none());
                assert!(e.attempt().is_none());
            }
            _ => {
                assert_eq!(e.node_id().map(|n| n.as_str()), Some("a"));
                assert_eq!(e.attempt(), Some(1));
            }
        }
    }
    assert!(
        !events
            .iter()
            .any(|e| format!("{e:?}").contains("NodeReady")),
        "no NodeReady"
    );
}

/// Compile lock: adding `Event::NodeReady` (or any new variant) fails this match.
fn public_event_is_known(event: &Event) {
    match event {
        Event::ExecutionStarted { .. }
        | Event::ExecutionSucceeded { .. }
        | Event::ExecutionFailed { .. }
        | Event::ExecutionCompleted { .. }
        | Event::ExecutionCancelled { .. }
        | Event::NodeStarted { .. }
        | Event::NodeSucceeded { .. }
        | Event::NodeFailed { .. }
        | Event::NodeAttemptFailed { .. }
        | Event::NodeTimedOut { .. }
        | Event::NodeCancelled { .. }
        | Event::NodeWaiting { .. } => {}
    }
}

#[test]
fn public_event_variants_exclude_node_ready() {
    let execution_id = keel_rt::ExecutionId::new();
    let workflow_id = keel_rt::WorkflowId::new("wf");
    let node_id = NodeId::new("n");
    let at = Timestamp::from_millis(1);
    let sv = SCHEMA_VERSION;
    let token = ResumeToken::issue(execution_id.clone(), node_id.clone(), 1);
    let all = [
        Event::ExecutionStarted {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            at,
            schema_version: sv,
        },
        Event::ExecutionSucceeded {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            at,
            schema_version: sv,
        },
        Event::ExecutionFailed {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            at,
            schema_version: sv,
        },
        Event::ExecutionCompleted {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            at,
            schema_version: sv,
        },
        Event::ExecutionCancelled {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            at,
            schema_version: sv,
        },
        Event::NodeStarted {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
        },
        Event::NodeSucceeded {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
        },
        Event::NodeFailed {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
            error: NodeError::new("e"),
        },
        Event::NodeAttemptFailed {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
            error: NodeError::new("e"),
        },
        Event::NodeTimedOut {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
        },
        Event::NodeCancelled {
            execution_id: execution_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
        },
        Event::NodeWaiting {
            execution_id,
            workflow_id,
            node_id: node_id.clone(),
            attempt: 1,
            at,
            schema_version: sv,
            token,
        },
    ];
    assert_eq!(all.len(), 12);
    for e in &all {
        public_event_is_known(e);
        assert!(
            !format!("{e:?}").contains("NodeReady"),
            "public Event must not have NodeReady"
        );
        match e {
            Event::ExecutionStarted { .. }
            | Event::ExecutionSucceeded { .. }
            | Event::ExecutionFailed { .. }
            | Event::ExecutionCompleted { .. }
            | Event::ExecutionCancelled { .. } => {
                assert!(e.node_id().is_none(), "{e:?}");
                assert!(e.attempt().is_none(), "{e:?}");
            }
            _ => {
                assert_eq!(e.node_id(), Some(&node_id), "{e:?}");
                assert_eq!(e.attempt(), Some(1), "{e:?}");
            }
        }
    }
}

#[test]
fn emit_swallows_try_emit_err() {
    struct ErrSink;
    impl EventSink for ErrSink {
        fn try_emit(&self, _event: &Event) -> Result<(), SinkError> {
            Err(SinkError::Message("announce failed".into()))
        }
    }
    let execution_id = keel_rt::ExecutionId::new();
    let ev = Event::ExecutionStarted {
        execution_id: execution_id.clone(),
        workflow_id: keel_rt::WorkflowId::new("wf"),
        at: Timestamp::from_millis(1),
        schema_version: SCHEMA_VERSION,
    };
    EventSink::emit(&ErrSink, &ev);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_waiting_does_not_repersist_same_revision() {
    use keel_rt::testing::SequenceStore;
    let store = SequenceStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let handle = rt.start(def).unwrap();
    handle.wait_stable().await;
    let id = handle.execution_id().clone();
    let puts = store.puts().len();
    std::mem::forget(handle);
    drop(rt);

    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            panic!("Waiting must not re-run execute")
        })
        .build();
    let handle = rt.resume(&id).await.unwrap();
    handle.wait_stable().await;
    assert_eq!(
        store.puts().len(),
        puts,
        "Waiting resume must not persist the same revision again"
    );
    handle.cancel().await;
    let _ = handle.wait().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sink_err_does_not_unpersist_or_fail_execution() {
    struct ErrSink;
    impl EventSink for ErrSink {
        fn try_emit(&self, _event: &Event) -> Result<(), SinkError> {
            Err(SinkError::Message("announce failed".into()))
        }
    }
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .sink(ErrSink)
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(snap.state, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn store_put_err_does_not_call_sink_emit() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .store(FailingStore::fail_all())
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    assert!(
        seen.lock().unwrap().is_empty(),
        "persist Err must not announce (persist-before-announce)"
    );
}

/// Persist `Err` used to `mem::take` pending events and drop them, so a later
/// persist `Ok` announced only the last apply. Snapshot was Succeeded with no
/// `ExecutionStarted` / `NodeStarted` on the sink (hunt: missing event for a
/// persisted transition).
#[tokio::test(flavor = "current_thread")]
async fn persist_err_then_ok_emits_events_for_the_durable_snapshot() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .store(FailingStore::fail_on_nth_put(1))
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    let events = seen.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ExecutionStarted { .. })),
        "durable Succeeded must still announce ExecutionStarted after a transient persist Err: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::NodeStarted { .. })),
        "durable Succeeded must still announce NodeStarted: {events:?}"
    );
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::NodeSucceeded { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ExecutionSucceeded { .. })));
    for e in &events {
        assert_eq!(e.execution_id(), &id, "must not invent a new execution");
    }
}

/// Transient terminal persist `Err` then Shutdown persist `Ok` used to emit
/// nothing: `persist_then_emit` dropped events, Shutdown retried with empty
/// pending. Snapshot matched `wait()`; sink never saw ExecutionSucceeded.
#[tokio::test(flavor = "current_thread")]
async fn transient_terminal_persist_err_shutdown_still_emits_execution_succeeded() {
    use keel_rt::{Execution, StoreError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailFirstTerminal {
        inner: MemoryStore,
        n: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StateStore for FailFirstTerminal {
        async fn put(&self, snapshot: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            if exec.state().is_terminal() {
                if self.n.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(StoreError::Message("busy terminal".into()));
                }
            }
            self.inner.persist(exec).await
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let inner = MemoryStore::new();
    let rt = Runtime::builder()
        .store(FailFirstTerminal {
            inner: inner.clone(),
            n: Arc::new(AtomicUsize::new(0)),
        })
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    drop(rt);
    tokio::task::yield_now().await;
    assert_eq!(
        inner.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Succeeded
    );
    let events = seen.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ExecutionSucceeded { .. })),
        "Shutdown persist Ok must announce ExecutionSucceeded: {events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_persist_err_then_shutdown_emits_execution_cancelled() {
    use keel_rt::testing::ScriptedExecutor;
    use keel_rt::{Execution, StoreError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailFirstCancel {
        inner: MemoryStore,
        n: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StateStore for FailFirstCancel {
        async fn put(&self, snapshot: &keel_rt::ExecutionSnapshot) -> Result<(), StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
            if exec.state() == ExecutionState::Cancelled {
                if self.n.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(StoreError::Message("busy cancel".into()));
                }
            }
            self.inner.persist(exec).await
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let inner = MemoryStore::new();
    let rt = Runtime::builder()
        .store(FailFirstCancel {
            inner: inner.clone(),
            n: Arc::new(AtomicUsize::new(0)),
        })
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register(ScriptedExecutor::new("a").hang(false))
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    handle.cancel().await;
    assert_eq!(handle.wait().await, ExecutionState::Cancelled);
    drop(rt);
    tokio::task::yield_now().await;
    assert_eq!(
        inner.get(&id).await.unwrap().unwrap().state,
        ExecutionState::Cancelled
    );
    let events = seen.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ExecutionCancelled { .. })),
        "Shutdown persist Ok must announce ExecutionCancelled: {events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resume_waiting_does_not_reemit_node_waiting_or_new_execution() {
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let rt = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let handle = rt.start(def).unwrap();
    handle.wait_stable().await;
    let snap = handle.inspect().await;
    let id = snap.execution_id.clone();
    let token = snap
        .node(&NodeId::new("a"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    std::mem::forget(handle);
    drop(rt);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .store(store)
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |_ctx: ExecutionContext| async {
            panic!("Waiting must not re-run execute")
        })
        .build();
    let handle = rt.resume(&id).await.unwrap();
    handle.wait_stable().await;
    let after = seen.lock().unwrap().clone();
    assert!(
        !after.iter().any(|e| matches!(e, Event::NodeWaiting { .. })),
        "resume of Waiting must not re-emit NodeWaiting: {after:?}"
    );
    for e in &after {
        assert_eq!(e.execution_id(), &id);
    }
    handle
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
        )
        .await
        .unwrap();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
}

#[tokio::test(flavor = "current_thread")]
async fn resume_reinvoke_emits_node_started_again() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution};

    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "ea")
        .node("b", "eb")
        .edge("a", "b")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = keel_rt::Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "a".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"A"))),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "b".into(),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();

    let b_starts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let seen_ids = Arc::new(Mutex::new(Vec::new()));
    let c = b_starts.clone();
    let ids = seen_ids.clone();
    let expect_id = id.clone();
    let rt = Runtime::builder()
        .store(store)
        .sink(FnSink(move |e: &Event| {
            if let Event::NodeStarted {
                node_id,
                execution_id,
                attempt,
                ..
            } = e
            {
                if node_id.as_str() == "b" {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    ids.lock().unwrap().push((execution_id.clone(), *attempt));
                }
            }
        }))
        .register_fn("ea", |_ctx: ExecutionContext| async {
            panic!("a must not re-run")
        })
        .register_fn("eb", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"B"))
        })
        .build();
    let handle = rt.resume(&id).await.unwrap();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    assert_eq!(
        b_starts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Running-at-crash re-invoke emits NodeStarted (duplicate vs first run)"
    );
    let got = seen_ids.lock().unwrap().clone();
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].0, expect_id,
        "duplicate NodeStarted must not invent a new execution"
    );
    assert_eq!(
        got[0].1, 2,
        "dispatch after restore increments attempt (at-least-once)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_emits_node_waiting_not_execution_waiting() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register_fn("a", |ctx: ExecutionContext| async move {
            NodeOutcome::Waiting {
                token: ctx.resume_token,
            }
        })
        .build();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    handle.wait_stable().await;
    let token = handle
        .inspect()
        .await
        .node(&NodeId::new("a"))
        .unwrap()
        .resume_token
        .clone()
        .unwrap();
    handle
        .resume(
            token,
            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
        )
        .await
        .unwrap();
    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    let events = seen.lock().unwrap().clone();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::NodeWaiting { .. })));
    assert!(!events
        .iter()
        .any(|e| format!("{e:?}").contains("ExecutionWaiting")));
}
