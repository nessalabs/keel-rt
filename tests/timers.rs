//! Phase 5: snapshot deadline T. Public API + FakeClock.
//!
//! T is `NodeState::Ready { runnable_at: Some(Timestamp) }`. Policy
//! (`RetryPolicy` delay, `timeout_after`) still chooses how long; the kernel
//! only parks until Instant T. Waiting stays HITL. No EventLog, no NodeReady,
//! no `Recover::RetryFailed`.
//!
//! `cargo test --test timers -- --test-threads=1`

use bytes::Bytes;
use keel_rt::testing::{FailingStore, FakeClock, ScriptedExecutor, SequenceStore};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Clock, Event, Execution, ExecutionContext, ExecutionState, FnSink,
    MemoryStore, NodeId, NodeOutcome, NodeState, OnFailure, RetryPolicy, Runtime, StateStore,
    Timestamp, WorkflowDefinition,
};
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    store: &impl StateStore,
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

/// Killer: T already due + Cancel in inbox — Cancel must win (park try_recv).
#[tokio::test(flavor = "current_thread")]
async fn cancel_when_deadline_already_due_does_not_dispatch() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    clock.set(t);
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", move |_ctx: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    handle.cancel().await;
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    assert_eq!(
        fired.load(Ordering::SeqCst),
        0,
        "queued Cancel must beat due Timer"
    );
}

/// 256 parked Ready {{ T }}, one advance, each node fires once.
#[tokio::test(flavor = "current_thread")]
async fn wide_256_parked_advance_once_each_fires_once() {
    let n = 256usize;
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let mut b = WorkflowDefinition::builder("wide-t");
    for i in 0..n {
        b = b.node(format!("w{i}"), "e");
    }
    let def = b.build().unwrap();
    let p = RetryPolicy::new(2, DELAY);
    let now = Timestamp(0);
    let mut ex = Execution::new(def);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    for i in 0..n {
        let id = format!("w{i}");
        ex.apply(
            ApplyCmd::StartNode {
                node_id: id.clone().into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: id.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
    }
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let parked = store
        .get(&id)
        .await
        .unwrap()
        .unwrap()
        .nodes
        .values()
        .filter(|n| {
            matches!(
                n.state,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            )
        })
        .count();
    assert_eq!(parked, n, "every worker must carry T");

    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(32)
            .policy(p)
            .register_fn("e", move |_ctx: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), n as u32, "each node once");
}

/// Mixed: half dispatchable now, half parked. Resume must not fire parked early.
#[tokio::test(flavor = "current_thread")]
async fn wide_mixed_immediate_and_parked_resume() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("mix")
        .node("now", "now")
        .node("later", "later")
        .build()
        .unwrap();
    let p = RetryPolicy::new(3, DELAY);
    let now = Timestamp(0);
    let mut ex = Execution::new(def);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "later".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "later".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::TimedOut),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let now_hits = Arc::new(AtomicU32::new(0));
    let later_hits = Arc::new(AtomicU32::new(0));
    let nh = now_hits.clone();
    let lh = later_hits.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(2)
            .policy(p)
            .register_fn("now", move |_c: ExecutionContext| {
                nh.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"n")) }
            })
            .register_fn("later", move |_c: ExecutionContext| {
                lh.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"l")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if now_hits.load(Ordering::SeqCst) >= 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(later_hits.load(Ordering::SeqCst), 0, "parked must wait");
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(later_hits.load(Ordering::SeqCst), 1);
}

/// T == now is due. T in the past on restore is due. T far future does not wall-sleep.
#[tokio::test(flavor = "current_thread")]
async fn t_equals_now_and_past_due_far_future_does_not_wall_sleep() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    clock.set(t);
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1);

    let store = MemoryStore::new();
    let (id, _) = persist_backoff(&store, DELAY).await;
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp::from_millis(u64::MAX));
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);

    let store = MemoryStore::new();
    let mut ex = Execution::new(def_a());
    let p = RetryPolicy::new(3, Duration::from_secs(86400 * 365));
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
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let clock = Arc::new(FakeClock::new());
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let wall = Instant::now();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(p)
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    assert!(
        wall.elapsed() < Duration::from_millis(200),
        "far-future T must not real-sleep, elapsed={:?}",
        wall.elapsed()
    );
    handle.cancel().await;
    within(handle.wait()).await;
}

/// Overflow Instant: saturating T stays parked; FakeClock does not wall-sleep.
#[tokio::test(flavor = "current_thread")]
async fn overflow_t_saturates_and_does_not_real_sleep() {
    let store = MemoryStore::new();
    let mut ex = Execution::new(def_a());
    let p = RetryPolicy::new(3, Duration::from_millis(1));
    let now = Timestamp(u64::MAX);
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
    match &ex.snapshot().node(&NodeId::new("a")).unwrap().state {
        NodeState::Ready {
            runnable_at: Some(at),
        } => assert_eq!(*at, Timestamp(u64::MAX), "saturating_add must not wrap"),
        other => panic!("{other:?}"),
    }
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(u64::MAX));
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(p)
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

/// Resume of a parked graph must not persist (last_persisted seeded from snapshot).
#[tokio::test(flavor = "current_thread")]
async fn resume_parked_does_not_open_extra_persist() {
    let store = SequenceStore::new();
    let (id, _) = persist_backoff(&store, DELAY).await;
    let before = store.puts().len();
    assert!(before >= 1);
    let clock = Arc::new(FakeClock::new());
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock)
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("must stay parked")
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        store.puts().len(),
        before,
        "no-op park restore must not persist (last_persisted was 0 if this grows)"
    );
    handle.cancel().await;
    within(handle.wait()).await;
}

/// NodeTimedOut is announced only after persist Ok.
#[tokio::test(flavor = "current_thread")]
async fn node_timed_out_emitted_only_after_persist_ok() {
    let store = FailingStore::fail_all();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let rt = Runtime::builder()
        .store(store)
        .sink(FnSink(move |e: &Event| log.lock().unwrap().push(e.clone())))
        .register(ScriptedExecutor::new("a").timeout())
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
                Some(NodeState::TimedOut)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        !seen
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, Event::NodeTimedOut { .. })),
        "must not announce NodeTimedOut before persist Ok"
    );
    handle.cancel().await;
    let _ = within(handle.wait()).await;
}

/// Deep chain: first node TimedOut. Fail-fast cancels the rest. FailSubtree
/// cancels descendants only; execution Completes.
#[tokio::test(flavor = "current_thread")]
async fn deep_chain_timeout_fail_fast_vs_fail_subtree() {
    let chain = |on_failure: OnFailure| {
        WorkflowDefinition::builder("chain")
            .on_failure(on_failure)
            .node("a", "t")
            .node("b", "ok")
            .node("c", "ok")
            .edge("a", "b")
            .edge("b", "c")
            .build()
            .unwrap()
    };
    let rt = Runtime::builder()
        .register(ScriptedExecutor::new("t").timeout())
        .register_fn("ok", |_c: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let h = rt.start(chain(OnFailure::FailExecution)).unwrap();
    assert_eq!(within(h.wait()).await, ExecutionState::Failed);

    let rt = Runtime::builder()
        .register(ScriptedExecutor::new("t").timeout())
        .register_fn("ok", |_c: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let h = rt.start(chain(OnFailure::FailSubtree)).unwrap();
    assert_eq!(within(h.wait()).await, ExecutionState::Completed);
}

/// Sibling timeout_after (Running) + sibling retry park. Crash. Resume keeps T
/// on the parked node; the Running side re-invokes. Independent — fail-fast
/// Accept on the timeout sibling would cancel the park.
#[tokio::test(flavor = "current_thread")]
async fn retry_backoff_and_timeout_sibling_crash_between() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("both")
        .node("park", "park")
        .node("run", "run")
        .build()
        .unwrap();
    let p = RetryPolicy::new(3, DELAY);
    let now = Timestamp(0);
    let mut ex = Execution::new(def);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "park".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "park".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::TimedOut),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "run".into(),
        },
        &p,
        now,
    )
    .unwrap();
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let t = match store
        .get(&id)
        .await
        .unwrap()
        .unwrap()
        .node(&NodeId::new("park"))
    {
        Some(n) => match &n.state {
            NodeState::Ready {
                runnable_at: Some(at),
            } => *at,
            other => panic!("{other:?}"),
        },
        None => panic!("park"),
    };
    let park_hits = Arc::new(AtomicU32::new(0));
    let run_hits = Arc::new(AtomicU32::new(0));
    let ph = park_hits.clone();
    let rh = run_hits.clone();
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .concurrency(2)
            .policy(p)
            .register_fn("park", move |_c: ExecutionContext| {
                ph.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"p")) }
            })
            .register_fn("run", move |_c: ExecutionContext| {
                rh.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"r")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if run_hits.load(Ordering::SeqCst) >= 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(park_hits.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("park"))
            .unwrap()
            .state,
        NodeState::Ready {
            runnable_at: Some(t)
        }
    );
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(park_hits.load(Ordering::SeqCst), 1);
}

/// Two sleepers: Running timeout_after + parked sibling. Drop drops both.
#[tokio::test(flavor = "current_thread")]
async fn two_sleepers_drop_handle_releases_both() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .concurrency(2)
        .policy(RetryPolicy::new(3, Duration::from_secs(60)))
        .register(ScriptedExecutor::new("run").timeout_after(Duration::from_secs(60)))
        .register(ScriptedExecutor::new("park").timeout())
        .build();
    let def = WorkflowDefinition::builder("sleepers")
        .node("run", "run")
        .node("park", "park")
        .build()
        .unwrap();
    let handle = rt.start(def).unwrap();
    let id = handle.execution_id().clone();
    within(async {
        loop {
            if let Some(snap) = store.get(&id).await.unwrap() {
                let run = snap.node(&NodeId::new("run")).map(|n| &n.state);
                let park = snap.node(&NodeId::new("park")).map(|n| &n.state);
                if matches!(run, Some(NodeState::Running { .. }))
                    && matches!(
                        park,
                        Some(NodeState::Ready {
                            runnable_at: Some(_)
                        })
                    )
                {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        clock.live_sleeps() >= 2,
        "executor Delay + park sleeper, got {}",
        clock.live_sleeps()
    );
    drop(handle);
    within(async {
        loop {
            if clock.live_sleeps() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

/// Advance FakeClock past T while persist is in flight; fire once after persist Ok.
#[tokio::test(flavor = "current_thread")]
async fn advance_past_t_while_persist_in_flight_fires_once() {
    struct HoldPersist {
        inner: MemoryStore,
        go: tokio::sync::Notify,
        holding: AtomicU32,
    }
    #[async_trait::async_trait]
    impl StateStore for HoldPersist {
        async fn put(
            &self,
            snapshot: &keel_rt::ExecutionSnapshot,
        ) -> Result<(), keel_rt::StoreError> {
            self.inner.put(snapshot).await
        }
        async fn get(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<keel_rt::ExecutionSnapshot>, keel_rt::StoreError> {
            self.inner.get(id).await
        }
        async fn persist(&self, exec: &Execution) -> Result<(), keel_rt::StoreError> {
            self.persist_with_events(exec, &[]).await
        }
        async fn persist_with_events(
            &self,
            exec: &Execution,
            events: &[Event],
        ) -> Result<(), keel_rt::StoreError> {
            if self.holding.fetch_add(1, Ordering::SeqCst) == 1 {
                self.go.notified().await;
            }
            self.inner.persist_with_events(exec, events).await
        }
        async fn workflow_definition(
            &self,
            id: &keel_rt::ExecutionId,
        ) -> Result<Option<WorkflowDefinition>, keel_rt::StoreError> {
            self.inner.workflow_definition(id).await
        }
    }

    let clock = Arc::new(FakeClock::new());
    let store = Arc::new(HoldPersist {
        inner: MemoryStore::new(),
        go: tokio::sync::Notify::new(),
        holding: AtomicU32::new(0),
    });
    let dyn_store: Arc<dyn StateStore> = store.clone();
    let rt = Runtime::builder()
        .store_arc(dyn_store)
        .clock(clock.clone())
        .policy(RetryPolicy::new(2, DELAY))
        .register(
            ScriptedExecutor::new("a")
                .timeout()
                .succeed(Bytes::from_static(b"ok")),
        )
        .build();
    let handle = rt.start(def_a()).unwrap();
    within(async {
        loop {
            if store.holding.load(Ordering::SeqCst) >= 2 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    clock.advance(DELAY);
    store.go.notify_waiters();
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

/// Two Runtimes, one MemoryStore, parked T: still unfenced (both may resume).
#[tokio::test(flavor = "current_thread")]
async fn two_runtimes_parked_deadline_are_not_fenced() {
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    let clock = Arc::new(FakeClock::new());
    clock.set(t);
    let a = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .policy(RetryPolicy::new(3, DELAY))
        .register_fn("a", |_c: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let b = Runtime::builder()
        .store(store)
        .clock(clock)
        .policy(RetryPolicy::new(3, DELAY))
        .register_fn("a", |_c: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        })
        .build();
    let ha = within(a.resume(&id)).await.unwrap();
    let hb = within(b.resume(&id)).await;
    assert!(
        hb.is_ok(),
        "AlreadyActive is per Runtime; two runtimes stay unfenced"
    );
    let _ = within(ha.wait()).await;
    if let Ok(h) = hb {
        let _ = within(h.wait()).await;
    }
}

async fn persist_retry_at(
    store: &impl StateStore,
    node: &str,
    delay: Duration,
    now: Timestamp,
) -> (keel_rt::ExecutionId, Timestamp) {
    let mut ex = Execution::new(
        WorkflowDefinition::builder("wf")
            .node(node, node)
            .build()
            .unwrap(),
    );
    let p = RetryPolicy::new(3, delay);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: node.into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: node.into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::TimedOut),
        },
        &p,
        now,
    )
    .unwrap();
    let t = match &ex.snapshot().node(&NodeId::new(node)).unwrap().state {
        NodeState::Ready {
            runnable_at: Some(at),
        } => *at,
        other => panic!("{other:?}"),
    };
    store.persist(&ex).await.unwrap();
    (ex.id().clone(), t)
}

/// `Duration::from_secs(1<<61)` used to wrap `as_millis() as u64` to 0 (T==now).
#[tokio::test(flavor = "current_thread")]
async fn huge_duration_backoff_does_not_fire_as_due_now() {
    let store = MemoryStore::new();
    let (id, t) = persist_retry_at(&store, "a", Duration::from_secs(1 << 61), Timestamp(100)).await;
    assert_eq!(t, Timestamp(u64::MAX));
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(100));
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(RetryPolicy::new(3, Duration::from_secs(1 << 61)))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fired.load(Ordering::SeqCst), 0, "wrapped T==now would fire");
    handle.cancel().await;
    within(handle.wait()).await;
}

/// Zero delay is Ready now (no park). Duration::MAX saturates and does not wall-sleep.
#[tokio::test(flavor = "current_thread")]
async fn zero_delay_retries_now_max_delay_does_not_wall_sleep() {
    let clock = Arc::new(FakeClock::new());
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = Runtime::builder()
        .clock(clock.clone())
        .policy(RetryPolicy::new(2, Duration::ZERO))
        .register_fn("a", move |_c: ExecutionContext| {
            let n = f.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    NodeOutcome::TimedOut
                } else {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            }
        })
        .build();
    assert_eq!(rt.run(def_a()).await.unwrap(), ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 2);
    assert_eq!(clock.live_sleeps(), 0);

    let store = MemoryStore::new();
    let (id, t) = persist_retry_at(&store, "a", Duration::MAX, Timestamp(0)).await;
    assert_eq!(t, Timestamp(u64::MAX));
    let clock = Arc::new(FakeClock::new());
    let wall = Instant::now();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(RetryPolicy::new(3, Duration::MAX))
            .register_fn("a", |_c: ExecutionContext| async { panic!("parked") })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(wall.elapsed() < Duration::from_millis(200));
    handle.cancel().await;
    within(handle.wait()).await;
}

/// Two nodes same T fire once each. Clock jump far past T still once. Clock
/// going backwards does not fire early.
#[tokio::test(flavor = "current_thread")]
async fn same_t_huge_jump_and_clock_backwards() {
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let def = WorkflowDefinition::builder("two")
        .node("a", "e")
        .node("b", "e")
        .build()
        .unwrap();
    let p = RetryPolicy::new(2, DELAY);
    let now = Timestamp(0);
    let mut ex = Execution::new(def);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    for id in ["a", "b"] {
        ex.apply(
            ApplyCmd::StartNode {
                node_id: id.into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: id.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
    }
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(2)
            .policy(p)
            .register_fn("e", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    clock.set(Timestamp(0));
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fired.load(Ordering::SeqCst), 0, "backwards/stay must not fire");
    clock.advance(Duration::from_secs(365 * 86400));
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 2);
}

/// 255 due, one still in the future. Resume fires 255. One tick later, the last.
#[tokio::test(flavor = "current_thread")]
async fn resume_255_due_one_future_then_one_tick() {
    let n = 256usize;
    let store = MemoryStore::new();
    let mut b = WorkflowDefinition::builder("mix-t");
    for i in 0..n {
        b = b.node(format!("w{i}"), "e");
    }
    let mut ex = Execution::new(b.build().unwrap());
    let p = RetryPolicy::new(2, Duration::from_millis(10));
    ex.apply(ApplyCmd::Start, &p, Timestamp(0)).unwrap();
    for i in 0..n {
        let id = format!("w{i}");
        let now = if i + 1 == n {
            Timestamp(1)
        } else {
            Timestamp(0)
        };
        ex.apply(
            ApplyCmd::StartNode {
                node_id: id.clone().into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: id.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
    }
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(10));
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(32)
            .policy(p)
            .register_fn("e", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if fired.load(Ordering::SeqCst) >= 255 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(fired.load(Ordering::SeqCst), 255, "future sibling must wait");
    clock.advance(Duration::from_millis(1));
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 256);
}

/// Persist Ok of Ready {{ T }}, never spawn a park task, resume later.
#[tokio::test(flavor = "current_thread")]
async fn persist_ok_without_park_task_resume_still_waits_for_t() {
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    let clock = Arc::new(FakeClock::new());
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    assert_eq!(t, Timestamp::from_millis(50));
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

/// Fire persist Err: store keeps Ready {{ T }}. Clock then goes backwards —
/// resume must wait (T still in the future on disk).
#[tokio::test(flavor = "current_thread")]
async fn persist_err_on_fire_leaves_t_on_store_clock_backwards_waits() {
    let fail = Arc::new(FailingStore::fail_all());
    let (id, t) = persist_backoff(fail.inner(), DELAY).await;
    assert_eq!(
        fail.inner()
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("a"))
            .unwrap()
            .state,
        NodeState::Ready {
            runnable_at: Some(t)
        }
    );
    let clock = Arc::new(FakeClock::new());
    clock.set(t);
    let dyn_store: Arc<dyn StateStore> = fail.clone();
    let handle = within(
        Runtime::builder()
            .store_arc(dyn_store)
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
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
                Some(NodeState::Running { .. }) | Some(NodeState::Succeeded)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let disk = fail.inner().get(&id).await.unwrap().unwrap();
    assert_eq!(
        disk.node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready {
            runnable_at: Some(t)
        },
        "fire persist Err must leave T on disk"
    );
    handle.cancel().await;
    let _ = within(handle.wait()).await;

    clock.set(Timestamp(0));
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(fail.inner().clone())
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fired.load(Ordering::SeqCst), 0, "T still in the future");
    clock.set(t);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

/// Drop Runtime (keep handle) while 256 parks are armed — parks stay, one advance fires once.
#[tokio::test(flavor = "current_thread")]
async fn drop_runtime_while_256_parks_armed_does_not_cancel() {
    let n = 256usize;
    let clock = Arc::new(FakeClock::new());
    let store = MemoryStore::new();
    let mut b = WorkflowDefinition::builder("drop-rt");
    for i in 0..n {
        b = b.node(format!("w{i}"), "e");
    }
    let mut ex = Execution::new(b.build().unwrap());
    let p = RetryPolicy::new(2, DELAY);
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    for i in 0..n {
        let id = format!("w{i}");
        ex.apply(ApplyCmd::StartNode { node_id: id.clone().into() }, &p, now).unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: id.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
    }
    store.persist(&ex).await.unwrap();
    let id = ex.id().clone();
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = Runtime::builder()
        .store(store)
        .clock(clock.clone())
        .concurrency(32)
        .policy(p)
        .register_fn("e", move |_c: ExecutionContext| {
            f.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
        })
        .build();
    let handle = within(rt.resume(&id)).await.unwrap();
    drop(rt);
    tokio::task::yield_now().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), n as u32);
}

/// Stale Complete while parked: NotWaiting. Then T fires once.
#[tokio::test(flavor = "current_thread")]
async fn stale_complete_while_parked_is_not_waiting_then_t_fires_once() {
    let store = MemoryStore::new();
    let (id, _) = persist_backoff(&store, DELAY).await;
    let clock = Arc::new(FakeClock::new());
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    let stale = keel_rt::ResumeToken::issue(id.clone(), NodeId::new("a"), 1);
    let err = handle
        .resume(stale, keel_rt::Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))))
        .await;
    assert!(err.is_err(), "Ready {{ T }} is not Waiting: {err:?}");
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

/// Stale snapshot put while parked must not clobber T.
#[tokio::test(flavor = "current_thread")]
async fn stale_put_while_park_in_flight_does_not_drop_t() {
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    let mut stale = store.get(&id).await.unwrap().unwrap();
    stale.revision = 0;
    stale.nodes.get_mut(&NodeId::new("a")).unwrap().state = NodeState::Ready { runnable_at: None };
    assert!(store.put(&stale).await.is_err());
    let clock = Arc::new(FakeClock::new());
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready { runnable_at: Some(t) }
    );
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Succeeded);
}

/// Policy panic on TimedOut fail-fasts. Sink panic on NodeTimedOut does not un-persist.
#[tokio::test(flavor = "current_thread")]
async fn policy_panic_on_timeout_and_sink_panic_on_node_timed_out() {
    struct Boom;
    impl keel_rt::Policy for Boom {
        fn decide(&self, o: &NodeOutcome, _: u32) -> keel_rt::PolicyDecision {
            if matches!(o, NodeOutcome::TimedOut) {
                panic!("policy timeout");
            }
            keel_rt::PolicyDecision::Accept
        }
    }
    let rt = Runtime::builder()
        .policy(Boom)
        .register(ScriptedExecutor::new("a").timeout())
        .build();
    assert_eq!(rt.run(def_a()).await.unwrap(), ExecutionState::Failed);

    let store = MemoryStore::new();
    let rt = Runtime::builder()
        .store(store.clone())
        .sink(FnSink(|e: &Event| {
            if matches!(e, Event::NodeTimedOut { .. }) {
                panic!("sink NodeTimedOut");
            }
        }))
        .register(ScriptedExecutor::new("a").timeout())
        .build();
    let handle = rt.start(def_a()).unwrap();
    let id = handle.execution_id().clone();
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
        NodeState::TimedOut,
        "sink panic must not un-persist NodeTimedOut"
    );
}

struct PanicSleep;
#[async_trait::async_trait]
impl Clock for PanicSleep {
    fn now(&self) -> Timestamp {
        Timestamp(0)
    }
    async fn sleep(&self, _: Duration) {
        panic!("clock sleep");
    }
}

/// Clock::sleep panic while parked: wait is Cancelled; T stays on disk.
#[tokio::test(flavor = "current_thread")]
async fn clock_sleep_panic_while_parked_keeps_t_on_store() {
    let store = MemoryStore::new();
    let (id, t) = persist_backoff(&store, DELAY).await;
    let handle = within(
        Runtime::builder()
            .store(store.clone())
            .clock(Arc::new(PanicSleep))
            .policy(RetryPolicy::new(3, DELAY))
            .register_fn("a", |_c: ExecutionContext| async { panic!("must not run") })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Cancelled);
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().node(&NodeId::new("a")).unwrap().state,
        NodeState::Ready { runnable_at: Some(t) }
    );
}

/// Fail-fast vs FailSubtree with a parked sibling; crash after park persist.
#[tokio::test(flavor = "current_thread")]
async fn crash_between_park_and_timeout_fail_fast_vs_fail_subtree() {
    async fn parked_with_pending_boom(on: OnFailure) -> (MemoryStore, keel_rt::ExecutionId) {
        let store = MemoryStore::new();
        let def = WorkflowDefinition::builder("mix")
            .on_failure(on)
            .node("park", "park")
            .node("boom", "boom")
            .node("child", "child")
            .edge("boom", "child")
            .build()
            .unwrap();
        let p = RetryPolicy::new(3, DELAY);
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "park".into() }, &p, now).unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "park".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
        store.persist(&ex).await.unwrap();
        let id = ex.id().clone();
        (store, id)
    }

    let (store, id) = parked_with_pending_boom(OnFailure::FailExecution).await;
    let clock = Arc::new(FakeClock::new());
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(AcceptPolicy)
            .register_fn("park", |_c: ExecutionContext| async { panic!("fail-fast must cancel park") })
            .register(ScriptedExecutor::new("boom").timeout())
            .register_fn("child", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"c"))
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    assert_eq!(within(handle.wait()).await, ExecutionState::Failed);

    let (store, id) = parked_with_pending_boom(OnFailure::FailSubtree).await;
    let clock = Arc::new(FakeClock::new());
    let park_hits = Arc::new(AtomicU32::new(0));
    let ph = park_hits.clone();
    let handle = within(
        Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(4)
            .policy(AcceptPolicy)
            .register_fn("park", move |_c: ExecutionContext| {
                ph.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"p")) }
            })
            .register(ScriptedExecutor::new("boom").timeout())
            .register_fn("child", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"c"))
            })
            .build()
            .resume(&id),
    )
    .await
    .unwrap();
    within(async {
        loop {
            if matches!(
                handle.inspect().await.node(&NodeId::new("boom")).map(|n| &n.state),
                Some(NodeState::TimedOut)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(park_hits.load(Ordering::SeqCst), 0);
    clock.advance(DELAY);
    assert_eq!(within(handle.wait()).await, ExecutionState::Completed);
    assert_eq!(park_hits.load(Ordering::SeqCst), 1);
}

