//! Phase 5: snapshot deadline T. Public API + FakeClock.
//!
//! T is `NodeState::Ready { runnable_at: Some(Timestamp) }`. Policy
//! (`RetryPolicy` delay, `timeout_after`) still chooses how long; the kernel
//! only parks until Instant T. Waiting stays HITL. No EventLog, no NodeReady,
//! no `Recover::RetryFailed`.
//!
//! `cargo test --test timers -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Clock, Event, Execution, ExecutionContext, ExecutionState, FnSink,
    MemoryStore, NodeId, NodeOutcome, NodeState, RetryPolicy, Runtime, StateStore, Timestamp,
    WorkflowDefinition,
};
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);
const DELAY: Duration = Duration::from_millis(50);

async fn within<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(BOUND, f)
        .await
        .expect("timers test timed out")
}

fn def_a() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap()
}

/// Persist a retry-delay snapshot: attempt 1 TimedOut, Ready { T = now+delay }.
async fn persist_backoff(
    store: &MemoryStore,
    delay: Duration,
) -> (keel_rt::ExecutionId, Timestamp) {
    let mut ex = Execution::new(def_a());
    let p = RetryPolicy::new(3, delay);
    let now = Timestamp(0);
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
            outcome: Ok(NodeOutcome::TimedOut),
        },
        &p,
        now,
    )
    .unwrap();
    let t = match &ex.snapshot().node(&NodeId::new("a")).unwrap().state {
        NodeState::Ready {
            runnable_at: Some(at),
        } => *at,
        other => panic!("timeout/retry must park Ready {{ T }}, got {other:?}"),
    };
    assert!(
        !matches!(
            ex.snapshot().node(&NodeId::new("a")).unwrap().state,
            NodeState::Waiting { .. }
        ),
        "Waiting is HITL; timers must not reuse it"
    );
    store.persist(&ex).await.unwrap();
    (ex.id().clone(), t)
}

/// Start, arm timeout, crash before fire, resume, advance FakeClock → TimedOut.
///
/// In-flight `timeout_after` keeps the node Running (executor Delay). Crash
/// restores Running as Ready and re-invokes; the new invoke re-arms Delay on
/// FakeClock. Kernel snapshot T is the retry park (`Ready { runnable_at }`).
#[tokio::test(flavor = "current_thread")]
async fn start_arm_timeout_crash_before_fire_resume_advance_is_timed_out() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let mut ex = Execution::new(def_a());
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "a".into(),
        },
        &p,
        now,
    )
    .unwrap();
    assert!(matches!(
        ex.snapshot().node(&NodeId::new("a")).unwrap().state,
        NodeState::Running { .. }
    ));
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();

    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .register(ScriptedExecutor::new("a").timeout_after(DELAY))
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if matches!(
                handle
                    .inspect()
                    .await
                    .node(&NodeId::new("a"))
                    .map(|n| &n.state),
                Some(NodeState::Running { .. })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
    assert_eq!(
        store
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("a"))
            .unwrap()
            .state,
        NodeState::TimedOut
    );
}

/// Timeout + Retry: crash while parked on T, resume, advance → retry then succeed.
#[tokio::test(flavor = "current_thread")]
async fn start_timeout_retry_crash_during_backoff_resume_advance_succeeds() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    assert_eq!(t, Timestamp::from_millis(50));
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(
        snap.node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready {
            runnable_at: Some(t)
        },
        "T must sit on the snapshot so crash-resume sees it"
    );

    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .policy(RetryPolicy::new(3, DELAY))
        .register_fn("a", move |_ctx: ExecutionContext| {
            f.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0, "must wait for T");
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

/// Crash after timeout persisted (Ready { T }) but before retry dispatch.
/// Resume must not double-run and must not lose T.
#[tokio::test(flavor = "current_thread")]
async fn crash_after_timeout_persisted_before_dispatch_does_not_double_run() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .policy(RetryPolicy::new(3, DELAY))
        .register_fn("a", move |_ctx: ExecutionContext| {
            f.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    let snap = store.get(&id).await.unwrap().unwrap();
    assert_eq!(
        snap.node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready {
            runnable_at: Some(t)
        },
        "deadline must survive resume before T"
    );
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1, "retry dispatch once");
}

/// Same attempt policy after crash: max_attempts is total executes, no extras.
#[tokio::test(flavor = "current_thread")]
async fn backoff_retry_survives_crash_same_attempt_policy() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let (id, _) = persist_backoff(&store, DELAY).await;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let log = attempts.clone();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .policy(RetryPolicy::new(2, DELAY))
        .register_fn("a", move |ctx: ExecutionContext| {
            log.lock().unwrap().push(ctx.attempt);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![2],
        "attempt 1 already ran; resume must not invent attempt 3 or re-do 1"
    );
}

/// Cancel while parked on T: Cancelled, sleeper dropped, advance does not start.
#[tokio::test(flavor = "current_thread")]
async fn cancel_during_parked_deadline_is_cancelled_sleeper_dropped() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let exec = ScriptedExecutor::new("a")
        .timeout()
        .succeed(Bytes::from_static(b"late"));
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .policy(RetryPolicy::new(3, DELAY))
        .register(exec.clone())
        .build();
    let handle = rt.start(def_a()).unwrap();
    within(async {
        loop {
            if matches!(
                handle
                    .inspect()
                    .await
                    .node(&NodeId::new("a"))
                    .map(|n| &n.state),
                Some(NodeState::Ready {
                    runnable_at: Some(_)
                })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        clock.live_sleeps() >= 1,
        "park must hold a FakeClock sleeper for T"
    );
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    within(async {
        loop {
            if clock.live_sleeps() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    clock.advance(DELAY);
    tokio::task::yield_now().await;
    // handle already consumed by wait; clock advance must not resurrect
    assert_eq!(exec.attempts(), vec![1]);
}

/// Drop handle during parked deadline still cancels the sleeper.
#[tokio::test(flavor = "current_thread")]
async fn drop_handle_during_parked_deadline_cancels_sleeper() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .policy(RetryPolicy::new(3, DELAY))
        .register(ScriptedExecutor::new("a").timeout())
        .build();
    let handle = rt.start(def_a()).unwrap();
    let id = handle.execution_id().clone();
    within(async {
        loop {
            if let Some(snap) = store.get(&id).await.unwrap() {
                if matches!(
                    snap.node(&NodeId::new("a")).map(|n| &n.state),
                    Some(NodeState::Ready {
                        runnable_at: Some(_)
                    })
                ) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    drop(handle);
    within(async {
        loop {
            if let Some(snap) = store.get(&id).await.unwrap() {
                if snap.state == ExecutionState::Cancelled && clock.live_sleeps() == 0 {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

/// Hunt: T already due on resume must dispatch once, not twice (Timer + Restore).
#[tokio::test(flavor = "current_thread")]
async fn persisted_deadline_already_due_on_resume_runs_once_not_twice() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp::from_millis(50));
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    assert_eq!(t, Timestamp::from_millis(50));
    assert!(t <= clock.now(), "T is due at restore");

    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock)
        .policy(RetryPolicy::new(3, DELAY))
        .sink(FnSink(move |e: &Event| {
            if matches!(e, Event::NodeStarted { .. }) {
                log.lock().unwrap().push(e.clone());
            }
            assert!(
                !matches!(e, Event::NodeWaiting { .. }),
                "timer due must not look like HITL Waiting"
            );
        }))
        .register_fn("a", move |_ctx: ExecutionContext| {
            f.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "due T on resume must not double-dispatch"
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "one NodeStarted, not two");
}

/// TimedOut persisted (Accept): crash before any further dispatch; resume does
/// not re-run. Timeout is not lost (node stays TimedOut).
#[tokio::test(flavor = "current_thread")]
async fn crash_after_accept_timeout_persisted_resume_stays_timed_out() {
    let store = MemoryStore::new();
    let mut ex = Execution::new(def_a());
    let p = AcceptPolicy;
    let now = Timestamp(0);
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
            outcome: Ok(NodeOutcome::TimedOut),
        },
        &p,
        now,
    )
    .unwrap();
    assert_eq!(
        ex.snapshot().node(&NodeId::new("a")).unwrap().state,
        NodeState::TimedOut
    );
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .register_fn("a", |_ctx: ExecutionContext| async {
                panic!("TimedOut must not re-run")
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);
}

/// Retry delay never emits NodeReady; Waiting is not used for T.
#[tokio::test(flavor = "current_thread")]
async fn timer_path_does_not_emit_node_ready_or_reuse_waiting() {
    let clock = Arc::new(FakeClock::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .clock(clock.clone())
        .policy(RetryPolicy::new(2, DELAY))
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register(
            ScriptedExecutor::new("a")
                .timeout()
                .succeed(Bytes::from_static(b"ok")),
        )
        .build();
    let handle = rt.start(def_a()).unwrap();
    within(async {
        loop {
            if matches!(
                handle
                    .inspect()
                    .await
                    .node(&NodeId::new("a"))
                    .map(|n| &n.state),
                Some(NodeState::Ready {
                    runnable_at: Some(_)
                })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let parked = handle.inspect().await;
    assert!(matches!(
        parked.node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready {
            runnable_at: Some(_)
        }
    ));
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    let events = seen.lock().unwrap().clone();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::NodeWaiting { .. })),
        "timer must not emit NodeWaiting: {events:?}"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, Event::NodeStarted { .. }))
            .count()
            >= 2,
        "retry dispatch after T: {events:?}"
    );
}

/// MemoryStore hot path without timers still succeeds immediately (no park).
#[tokio::test(flavor = "current_thread")]
async fn memory_store_hot_path_without_timers_does_not_park() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .register_fn("a", |_ctx: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    assert_eq!(rt.run(def_a()).await.unwrap(), ExecutionState::Succeeded);
    assert_eq!(clock.live_sleeps(), 0);
    assert_eq!(
        clock.now(),
        Timestamp(0),
        "no FakeClock advance on hot path"
    );
}
