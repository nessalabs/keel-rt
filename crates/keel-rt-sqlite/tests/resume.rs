//! Crash-resume against a real sqlite file. Not a mock of StateStore.
//!
//! Recipe: nested current_thread runtime, forget the handle so Drop does
//! not Cancel, drop the tokio runtime (aborts tasks), reopen the file.

use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Clock, CompleteError, Event, Execution, ExecutionContext, ExecutionId,
    ExecutionSnapshot, ExecutionState, Executor, ExecutorId, Join, LeaseEpoch, NodeError, NodeId,
    NodeOutcome, NodeState, OnFailure, OwnerId, Recover, Resume, ResumeError, RetryPolicy, Runtime,
    StateStore, StoreError, Timestamp, WorkflowDefinition, DEFAULT_LEASE_TTL, MAX_SNAPSHOT_ERROR,
    SCHEMA_VERSION,
};
use keel_rt_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(5);

fn tmp() -> PathBuf {
    let p = std::env::temp_dir().join(format!("keel-rt-sqlite-crash-{}.db", ExecutionId::new()));
    let _ = std::fs::remove_file(&p);
    p
}

fn current_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current_thread runtime")
}

async fn wait_node(
    store: &SqliteStore,
    id: &ExecutionId,
    node: &str,
    pred: impl Fn(&NodeState) -> bool,
) {
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(snap) = store.get(id).await.unwrap() {
                if let Some(n) = snap.node(&NodeId::new(node)) {
                    if pred(&n.state) {
                        return;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("node state");
}

#[test]
fn crash_during_b_running_reinvokes_b_not_a() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("ea", |_ctx: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"A"))
                })
                .register(ScriptedExecutor::new("eb").hang(false))
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "ea")
                .node("b", "eb")
                .edge("a", "b")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "b", |s| matches!(s, NodeState::Running { .. })).await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let a_runs = Arc::new(AtomicU32::new(0));
    let b_runs = Arc::new(AtomicU32::new(0));
    let ac = a_runs.clone();
    let bc = b_runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("ea", move |_ctx: ExecutionContext| {
                ac.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) }
            })
            .register_fn("eb", move |ctx: ExecutionContext| {
                bc.fetch_add(1, Ordering::SeqCst);
                let attempt = ctx.attempt;
                async move {
                    assert_eq!(attempt, 2);
                    NodeOutcome::Succeeded(Bytes::from_static(b"B"))
                }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(a_runs.load(Ordering::SeqCst), 0);
        assert_eq!(b_runs.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

/// Recover persist must land Ready-now before StartNode. Stall returning
/// from that persist so drive cannot dispatch; drop Runtime; Continue
/// re-invokes. Without recover persist, get() stays Failed and Continue
/// does not re-run — this test must fail.
#[test]
fn retry_failed_recover_persist_then_kill_before_startnode_continue_reinvokes() {
    let path = tmp();
    let recovered = Arc::new(tokio::sync::Notify::new());
    let hold = Arc::new(tokio::sync::Notify::new());
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_ctx: ExecutionContext| async {
                    NodeOutcome::failed("boom")
                })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            assert_eq!(handle.wait().await, ExecutionState::Failed);
            id
        });
        drop(rt);
        let stall = StallAfterFirstPersist {
            inner: store.clone(),
            recovered: recovered.clone(),
            hold: hold.clone(),
            first: AtomicU32::new(0),
        };
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(stall)
                .register_fn("a", |_ctx: ExecutionContext| async {
                    panic!("StartNode must not run; recover persist is stalled")
                })
                .build();
            let resume_id = id.clone();
            let task =
                tokio::spawn(
                    async move { runtime.resume_with(&resume_id, Recover::RetryFailed).await },
                );
            tokio::time::timeout(BOUND, recovered.notified())
                .await
                .expect("recover persist must commit");
            let snap = store.get(&id).await.unwrap().unwrap();
            let node = snap.node(&NodeId::new("a")).unwrap();
            assert!(
                matches!(node.state, NodeState::Ready { runnable_at: None }),
                "get() after recover persist must be Ready-now, got {:?}",
                node.state
            );
            assert_ne!(snap.state, ExecutionState::Failed);
            assert!(node.resume_token.is_none());
            assert!(node.last_error.is_some());
            task.abort();
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", move |_ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "Continue from Ready-never-Running must re-invoke"
        );
    });
    let _ = std::fs::remove_file(&path);
}

/// Writes the first persist (recover snapshot), then never returns so
/// Restore / StartNode cannot run.
struct StallAfterFirstPersist {
    inner: SqliteStore,
    recovered: Arc<tokio::sync::Notify>,
    hold: Arc<tokio::sync::Notify>,
    first: AtomicU32,
}

#[async_trait::async_trait]
impl StateStore for StallAfterFirstPersist {
    // The fault gate must preserve the real adapter's lease operations;
    // default no-op claims cannot authorize writes to a previously leased row.
    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, keel_rt::ClaimError> {
        self.inner.claim(id, owner, now).await
    }

    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), keel_rt::ClaimError> {
        self.inner.heartbeat(id, epoch, now).await
    }

    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.inner.release(id, epoch).await
    }

    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        self.inner.release_now(id, epoch);
    }

    fn release_owner_now(&self, owner: &OwnerId) {
        self.inner.release_owner_now(owner);
    }

    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        self.inner.persist_with_events(exec, events).await?;
        if self.first.fetch_add(1, Ordering::SeqCst) == 0 {
            self.recovered.notify_waiters();
            self.hold.notified().await;
        }
        Ok(())
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

fn fail_subtree_all_done_map_reduce() -> WorkflowDefinition {
    WorkflowDefinition::builder("wf")
        .on_failure(OnFailure::FailSubtree)
        .node("p1", "e")
        .node("p2", "e")
        .node("join", "j")
        .edge("p1", "join")
        .edge("p2", "join")
        .join("join", Join::AllDone)
        .build()
        .unwrap()
}

/// FailSubtree AllDone map-reduce × many: recover persist lands p1 Ready-now
/// (join Pending, p2 still Succeeded), kill before StartNode, Continue
/// re-invokes p1 and the join. Waiting until Running would not prove persist.
#[test]
fn retry_failed_all_done_map_reduce_recover_persist_kill_before_startnode_many() {
    const N: usize = 16;
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let def = fail_subtree_all_done_map_reduce();
    let ids = {
        let rt = current_rt();
        let ids = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("e", |ctx: ExecutionContext| async move {
                    if ctx.node_id.as_str() == "p1" {
                        NodeOutcome::failed("page")
                    } else {
                        NodeOutcome::Succeeded(Bytes::from_static(b"p2"))
                    }
                })
                .register_fn("j", |_ctx: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"join"))
                })
                .build();
            let mut ids = Vec::with_capacity(N);
            for i in 0..N {
                let handle = runtime.start(def.clone()).unwrap();
                let id = handle.execution_id().clone();
                assert_eq!(
                    handle.wait().await,
                    ExecutionState::Completed,
                    "exec {i} first run"
                );
                ids.push(id);
            }
            ids
        });
        drop(rt);
        ids
    };
    for (i, id) in ids.iter().enumerate() {
        let recovered = Arc::new(tokio::sync::Notify::new());
        let hold = Arc::new(tokio::sync::Notify::new());
        let stall = StallAfterFirstPersist {
            inner: store.clone(),
            recovered: recovered.clone(),
            hold: hold.clone(),
            first: AtomicU32::new(0),
        };
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(stall)
                .register_fn("e", |_ctx: ExecutionContext| async {
                    panic!("StartNode must not run; recover persist is stalled")
                })
                .register_fn("j", |_ctx: ExecutionContext| async {
                    panic!("join must not dispatch before recover persist returns")
                })
                .build();
            let resume_id = id.clone();
            let task =
                tokio::spawn(
                    async move { runtime.resume_with(&resume_id, Recover::RetryFailed).await },
                );
            tokio::time::timeout(BOUND, recovered.notified())
                .await
                .unwrap_or_else(|_| panic!("exec {i}: recover persist must commit"));
            let snap = store.get(id).await.unwrap().unwrap();
            let p1 = snap.node(&NodeId::new("p1")).unwrap();
            assert!(
                matches!(p1.state, NodeState::Ready { runnable_at: None }),
                "exec {i}: get() after recover persist must be Ready-now, got {:?}",
                p1.state
            );
            assert!(
                matches!(
                    snap.node(&NodeId::new("p2")).unwrap().state,
                    NodeState::Succeeded
                ),
                "exec {i}: succeeded page stays Succeeded"
            );
            assert!(
                matches!(
                    snap.node(&NodeId::new("join")).unwrap().state,
                    NodeState::Pending
                ),
                "exec {i}: AllDone join is Pending after recover, got {:?}",
                snap.node(&NodeId::new("join")).unwrap().state
            );
            assert_ne!(snap.state, ExecutionState::Failed);
            assert_ne!(snap.state, ExecutionState::Completed);
            task.abort();
        });
        drop(rt);
    }
    drop(store);
    let store = SqliteStore::open(&path).unwrap();
    let p1_runs = Arc::new(AtomicU32::new(0));
    let p2_runs = Arc::new(AtomicU32::new(0));
    let join_runs = Arc::new(AtomicU32::new(0));
    let (c1, c2, cj) = (p1_runs.clone(), p2_runs.clone(), join_runs.clone());
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("e", move |ctx: ExecutionContext| {
                if ctx.node_id.as_str() == "p1" {
                    c1.fetch_add(1, Ordering::SeqCst);
                } else {
                    c2.fetch_add(1, Ordering::SeqCst);
                }
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .register_fn("j", move |_ctx: ExecutionContext| {
                cj.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"join2")) }
            })
            .build();
        for (i, id) in ids.iter().enumerate() {
            let handle = runtime.resume(id).await.unwrap();
            assert_eq!(
                handle.wait().await,
                ExecutionState::Succeeded,
                "exec {i} Continue after recover persist"
            );
        }
    });
    assert_eq!(
        p1_runs.load(Ordering::SeqCst) as usize,
        N,
        "Continue from Ready-never-Running must re-invoke each failed page"
    );
    assert_eq!(p2_runs.load(Ordering::SeqCst), 0, "succeeded pages stay");
    assert_eq!(
        join_runs.load(Ordering::SeqCst) as usize,
        N,
        "AllDone join re-runs once per recovered execution"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_diamond_join_runs_writer_once() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("src", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"s"))
                })
                .register_fn("sum", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"u"))
                })
                .register(ScriptedExecutor::new("crit").hang(false))
                .register_fn("writer", |_c: ExecutionContext| async {
                    panic!("writer must not run before both sides")
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("src", "src")
                .node("sum", "sum")
                .node("crit", "crit")
                .node("writer", "writer")
                .edge("src", "sum")
                .edge("src", "crit")
                .edge("sum", "writer")
                .edge("crit", "writer")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "crit", |s| {
                matches!(s, NodeState::Running { .. })
            })
            .await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let writes = Arc::new(AtomicU32::new(0));
    let w = writes.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("src", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"s"))
            })
            .register_fn("sum", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"u"))
            })
            .register_fn("crit", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"c"))
            })
            .register_fn("writer", move |_c: ExecutionContext| {
                w.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"w")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_while_waiting_keeps_the_same_token() {
    let path = tmp();
    let (id, token) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |ctx: ExecutionContext| async move {
                    NodeOutcome::Waiting {
                        token: ctx.resume_token,
                    }
                })
                .register_fn("b", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"B"))
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .node("b", "b")
                .edge("a", "b")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "a", |s| matches!(s, NodeState::Waiting { .. })).await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("a"))
                .unwrap()
                .resume_token
                .clone()
                .expect("token");
            std::mem::forget(handle);
            drop(runtime);
            (id, token)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("waiting node must not re-run")
            })
            .register_fn("b", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"B"))
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
        handle
            .resume(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
            )
            .await
            .unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_during_retry_delay_does_not_fire_early() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .policy(RetryPolicy::new(3, Duration::from_millis(50)))
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Failed(keel_rt::NodeError::new("boom"))
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "a", |s| {
                matches!(
                    s,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
            .await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, Duration::from_millis(50)))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "must wait for runnable_at");
        clock.advance(Duration::from_millis(50));
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_after_fail_fast_stays_failed() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Failed(keel_rt::NodeError::new("boom"))
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            tokio::time::timeout(BOUND, async {
                loop {
                    if let Some(s) = store.get(&id).await.unwrap() {
                        if s.state == ExecutionState::Failed {
                            return;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("failed persist");
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("failed execution must not resurrect")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Failed);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_fail_subtree_pages_stay_failed() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("p1", |_c: ExecutionContext| async {
                    NodeOutcome::Failed(keel_rt::NodeError::new("page"))
                })
                .register(ScriptedExecutor::new("p2").hang(false))
                .register_fn("red", |_c: ExecutionContext| async {
                    panic!("reducer waits for p2")
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .on_failure(OnFailure::FailSubtree)
                .node("p1", "p1")
                .node("p2", "p2")
                .node("red", "red")
                .join("red", Join::AllDone)
                .edge("p1", "red")
                .edge("p2", "red")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "p1", |s| matches!(s, NodeState::Failed)).await;
            wait_node(&store, &id, "p2", |s| {
                matches!(s, NodeState::Running { .. })
            })
            .await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let p1 = Arc::new(AtomicU32::new(0));
    let c1 = p1.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("p1", move |_c: ExecutionContext| {
                c1.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"no")) }
            })
            .register_fn("p2", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"p2"))
            })
            .register_fn("red", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"r"))
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Completed);
        assert_eq!(p1.load(Ordering::SeqCst), 0);
    });
    let _ = std::fs::remove_file(&path);
}

/// Engine down: Waiting on disk, new Runtime, `complete` (no handle) drives
/// the successor. Other-binary path without HTTP.
#[test]
fn complete_after_sqlite_kill_new_runtime_unblocks_wait() {
    let path = tmp();
    let token = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let token = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("next", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"next"))
                })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("hold", "wait")
                        .node("next", "next")
                        .edge("hold", "next")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "hold", |s| {
                matches!(s, NodeState::Waiting { .. })
            })
            .await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("hold"))
                .unwrap()
                .resume_token
                .clone()
                .expect("token");
            std::mem::forget(handle);
            drop(runtime);
            token
        });
        drop(rt);
        drop(store);
        token
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("next", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build();
        let id = token.execution_id().clone();
        runtime
            .complete(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
            )
            .await
            .unwrap();
        tokio::time::timeout(BOUND, async {
            loop {
                if let Some(snap) = store.get(&id).await.unwrap() {
                    if snap.state == ExecutionState::Succeeded {
                        assert_eq!(
                            snap.node(&NodeId::new("next"))
                                .and_then(|n| n.output.clone()),
                            Some(Bytes::from_static(b"next"))
                        );
                        assert_eq!(
                            snap.node(&NodeId::new("hold"))
                                .and_then(|n| n.output.clone()),
                            Some(Bytes::from_static(b"gate"))
                        );
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("complete must drive successor");
    });
    let _ = std::fs::remove_file(&path);
}

/// Lease fence: while A owns the Waiting file, B complete is ClaimedElsewhere.
/// After drop Runtime A the lease is released and B may complete.
#[test]
fn two_runtimes_same_file_both_may_complete() {
    let path = tmp();
    let store_a = SqliteStore::open(&path).unwrap();
    let store_b = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let ra = Runtime::builder().store(store_a.clone()).build();
        let handle = ra
            .start(
                WorkflowDefinition::builder("wf")
                    .node("hold", "wait")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        wait_node(&store_a, &id, "hold", |s| {
            matches!(s, NodeState::Waiting { .. })
        })
        .await;
        let token = store_a
            .get(&id)
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("hold"))
            .unwrap()
            .resume_token
            .clone()
            .expect("token");
        let rb = Runtime::builder().store(store_b).build();
        let outcome = Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"g")));
        match rb.complete(token.clone(), outcome.clone()).await {
            Err(CompleteError::ClaimedElsewhere) => {}
            Ok(()) => panic!("live lease must fence B complete"),
            Err(e) => panic!("expected ClaimedElsewhere, got {e}"),
        }
        std::mem::forget(handle);
        drop(ra);
        rb.complete(token, outcome)
            .await
            .expect("B claims after drop Runtime A");
        tokio::time::timeout(BOUND, async {
            loop {
                if let Some(snap) = store_a.get(&id).await.unwrap() {
                    if snap.state == ExecutionState::Succeeded {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("B complete must persist Succeeded");
    });
    let _ = std::fs::remove_file(&path);
}

/// Engine-down complete timing (store path, no HTTP / no secret).
#[test]
fn complete_after_sqlite_kill_reports_ms() {
    let path = tmp();
    let (id, token) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("next", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"next"))
                })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("hold", "wait")
                        .node("next", "next")
                        .edge("hold", "next")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "hold", |s| {
                matches!(s, NodeState::Waiting { .. })
            })
            .await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("hold"))
                .unwrap()
                .resume_token
                .clone()
                .expect("token");
            std::mem::forget(handle);
            drop(runtime);
            (id, token)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("next", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"next"))
            })
            .build();
        let started = std::time::Instant::now();
        runtime
            .complete(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"gate"))),
            )
            .await
            .unwrap();
        tokio::time::timeout(BOUND, async {
            loop {
                if let Some(snap) = store.get(&id).await.unwrap() {
                    if snap.state == ExecutionState::Succeeded {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("complete must drive successor");
        eprintln!(
            "sqlite_complete_after_crash elapsed_ms={:.3}",
            started.elapsed().as_secs_f64() * 1000.0
        );
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn resume_twice_live_is_already_active() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
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
        let handle = runtime.start(def).unwrap();
        let id = handle.execution_id().clone();
        match runtime.resume(&id).await {
            Err(ResumeError::AlreadyActive) => {}
            Ok(_) => panic!("expected AlreadyActive"),
            Err(e) => panic!("{e}"),
        }
        handle.cancel().await;
        handle.wait().await;
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn process_restart_is_new_runtime_same_file() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
            id
        });
        drop(rt);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("terminal resume must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn resume_unknown_id_on_file() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        match runtime
            .resume(&ExecutionId::parse("exec-missing").unwrap())
            .await
        {
            Err(ResumeError::UnknownExecution) => {}
            Err(e) => panic!("{e}"),
            Ok(_) => panic!("expected UnknownExecution"),
        }
    });
    let _ = std::fs::remove_file(&path);
}

/// Runtime A registered `slow` and left it Running on sqlite. Runtime B does
/// not register that id: resume is `UnregisteredExecutors`, not a
/// `launch_slot` panic. Runtime C with the adapter re-invokes and finishes.
#[test]
fn resume_running_custom_without_adapter_is_unregistered() {
    use std::future::Future;
    use std::pin::Pin;

    struct Slow;
    impl Executor for Slow {
        fn id(&self) -> ExecutorId {
            ExecutorId::new("slow")
        }
        fn execute<'a>(
            &'a self,
            _ctx: ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + 'a>> {
            Box::pin(async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) })
        }
    }

    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let mut ex = Execution::new(
            WorkflowDefinition::builder("wf")
                .node("slow", "slow")
                .build()
                .unwrap(),
        );
        let p = AcceptPolicy;
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "slow".into(),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.snapshot().node(&NodeId::new("slow")).map(|n| &n.state),
            Some(NodeState::Running { .. })
        ));
        let rt = current_rt();
        rt.block_on(async {
            store.persist(&ex).await.unwrap();
        });
        drop(rt);
        drop(store);
        ex.id().clone()
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let bare = Runtime::builder().store(store.clone()).build();
        match bare.resume(&id).await {
            Err(ResumeError::UnregisteredExecutors(missing)) => {
                let names: Vec<&str> = missing.0.iter().map(|e| e.as_str()).collect();
                assert_eq!(names, vec!["slow"], "{names:?}");
            }
            Err(e) => panic!("{e}"),
            Ok(_) => panic!("expected UnregisteredExecutors"),
        }
        let still = store.get(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                still.node(&NodeId::new("slow")).map(|n| &n.state),
                Some(NodeState::Running { .. })
            ),
            "fail-closed resume must not rewrite Running: {:?}",
            still.node(&NodeId::new("slow")).map(|n| &n.state)
        );
        let with = Runtime::builder().store(store).register(Slow).build();
        let handle = with.resume(&id).await.expect("adapter present");
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn sqlite_persist_failed_huge_last_error_is_capped() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let mut ex = Execution::new(
        WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap(),
    );
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
    let huge = "q".repeat(MAX_SNAPSHOT_ERROR + 4096);
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "a".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Failed(NodeError { message: huge })),
        },
        &p,
        now,
    )
    .unwrap();
    let rt = current_rt();
    rt.block_on(async {
        store.persist(&ex).await.unwrap();
        let snap = store.get(ex.id()).await.unwrap().unwrap();
        let msg = snap
            .node(&NodeId::new("a"))
            .and_then(|n| n.last_error.clone())
            .expect("last_error");
        assert!(
            msg.message.len() <= MAX_SNAPSHOT_ERROR,
            "sqlite last_error {} > MAX_SNAPSHOT_ERROR",
            msg.message.len()
        );
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_after_terminal_cas_before_emit_keeps_terminal() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution, Timestamp};
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
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
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
            },
            &p,
            now,
        )
        .unwrap();
        let rt = current_rt();
        rt.block_on(async {
            store.persist(&ex).await.unwrap();
        });
        drop(rt);
        drop(store);
        ex.id().clone()
    };
    let store = SqliteStore::open(&path).unwrap();
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", move |_c: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"no")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_after_running_persist_releases_lock_and_reinvokes() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register(ScriptedExecutor::new("a").hang(false))
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "a", |s| matches!(s, NodeState::Running { .. })).await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).expect("file must not stay locked");
    let runs = Arc::new(AtomicU32::new(0));
    let c = runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", move |ctx: ExecutionContext| {
                c.fetch_add(1, Ordering::SeqCst);
                let attempt = ctx.attempt;
                async move {
                    assert_eq!(attempt, 2);
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn two_runtimes_same_file_are_not_fenced() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution, Timestamp};
    let path = tmp();
    let store_a = SqliteStore::open(&path).unwrap();
    let store_b = SqliteStore::open(&path).unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
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
    let rt = current_rt();
    rt.block_on(async {
        store_a.persist(&ex).await.unwrap();
        let runs = Arc::new(AtomicU32::new(0));
        let ca = runs.clone();
        let cb = runs.clone();
        let runtime_a = Runtime::builder()
            .store(store_a.clone())
            .register_fn("a", move |_c: ExecutionContext| {
                ca.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"A")) }
            })
            .build();
        let runtime_b = Runtime::builder()
            .store(store_b)
            .register_fn("a", move |_c: ExecutionContext| {
                cb.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"B")) }
            })
            .build();
        let ha = runtime_a.resume(ex.id()).await.unwrap();
        match runtime_b.resume(ex.id()).await {
            Err(ResumeError::ClaimedElsewhere) => {}
            Ok(_) => panic!("live lease must fence B resume"),
            Err(e) => panic!("expected ClaimedElsewhere, got {e}"),
        }
        let _ = ha.wait().await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "only the owning Runtime re-invokes"
        );
        let keep = store_a.get(ex.id()).await.unwrap().unwrap().revision;
        let mut older = store_a.get(ex.id()).await.unwrap().unwrap();
        older.revision = 0;
        match store_a.put(&older).await {
            Err(keel_rt::StoreError::Stale {
                found,
                attempted: 0,
            }) if found == keep => {}
            other => panic!("{other:?}"),
        }
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn two_runtimes_lease_ttl_then_second_claims() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let clock = Arc::new(FakeClock::new());
    let rt = current_rt();
    rt.block_on(async {
        let def = WorkflowDefinition::builder("wf")
            .node("hold", "wait")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&ex).await.unwrap();
        let a = OwnerId::new();
        store.claim(ex.id(), &a, clock.now()).await.unwrap();
        let rb = Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .build();
        match rb.resume(ex.id()).await {
            Err(ResumeError::ClaimedElsewhere) => {}
            Ok(_) => panic!("before TTL B must fail"),
            Err(e) => panic!("expected ClaimedElsewhere, got {e}"),
        }
        clock.advance(DEFAULT_LEASE_TTL);
        rb.resume(ex.id())
            .await
            .expect("after TTL B claims")
            .cancel()
            .await;
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn stale_epoch_persist_is_rejected() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let mut ex = Execution::new(
            WorkflowDefinition::builder("wf")
                .node("a", "e")
                .build()
                .unwrap(),
        );
        let a = OwnerId::new();
        let b = OwnerId::new();
        let e1 = store.claim(ex.id(), &a, Timestamp(0)).await.unwrap();
        assert_eq!(e1, LeaseEpoch(1));
        ex.set_fence_epoch(e1.0);
        store.persist(&ex).await.unwrap();
        let e2 = store
            .claim(
                ex.id(),
                &b,
                Timestamp(0).saturating_add(DEFAULT_LEASE_TTL + Duration::from_millis(1)),
            )
            .await
            .unwrap();
        assert_eq!(e2, LeaseEpoch(2));
        let err = store.persist(&ex).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::StaleEpoch {
                found: 2,
                attempted: 1
            }
        );
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn concurrent_resume_same_runtime_one_already_active() {
    use keel_rt::Execution;
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("a", "a")
        .build()
        .unwrap();
    let exec = Execution::new(def);
    let rt = current_rt();
    rt.block_on(async {
        store.persist(&exec).await.unwrap();
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let (a, b) = tokio::join!(runtime.resume(exec.id()), runtime.resume(exec.id()));
        let oks = [&a, &b].iter().filter(|r| r.is_ok()).count();
        let actives = [&a, &b]
            .iter()
            .filter(|r| matches!(r, Err(ResumeError::AlreadyActive)))
            .count();
        assert_eq!(oks, 1);
        assert_eq!(actives, 1);
        let handle = a.ok().or(b.ok()).unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fat_bytes_sqlite_round_trip_preserves_bytes() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution, Timestamp};
    let path = tmp();
    let fat = Bytes::from(vec![9u8; 64 * 1024]);
    let store = SqliteStore::open(&path).unwrap();
    let def = WorkflowDefinition::builder("wf")
        .node("fat", "fat")
        .node("join", "join")
        .edge("fat", "join")
        .build()
        .unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(
        ApplyCmd::StartNode {
            node_id: "fat".into(),
        },
        &p,
        now,
    )
    .unwrap();
    ex.apply(
        ApplyCmd::FinishNode {
            node_id: "fat".into(),
            attempt: 1,
            outcome: Ok(NodeOutcome::Succeeded(fat.clone())),
        },
        &p,
        now,
    )
    .unwrap();
    let rt = current_rt();
    rt.block_on(async {
        store.persist(&ex).await.unwrap();
        drop(store);
        let store = SqliteStore::open(&path).unwrap();
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("fat", |_c: ExecutionContext| async {
                panic!("succeeded fat node must not re-run")
            })
            .register_fn("join", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"j"))
            })
            .build();
        let handle = runtime.resume(ex.id()).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        let out = store
            .get(ex.id())
            .await
            .unwrap()
            .unwrap()
            .node(&NodeId::new("fat"))
            .and_then(|n| n.output.clone())
            .expect("fat");
        assert_eq!(out.as_ref(), fat.as_ref());
    });
    let _ = std::fs::remove_file(&path);
}

fn hourglass_def(n: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("hourglass").node("neck", "neck");
    for i in 0..n {
        let a = format!("a{i}");
        let sink = format!("b{i}");
        b = b
            .node(a.as_str(), "src")
            .node(sink.as_str(), "sink")
            .edge(a.as_str(), "neck")
            .edge("neck", sink.as_str());
    }
    b.build().unwrap()
}

#[test]
fn crash_hourglass_neck_running_resume_runs_sinks_not_sources() {
    let path = tmp();
    let n = 4usize;
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("src", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"s"))
                })
                .register(ScriptedExecutor::new("neck").hang(false))
                .register_fn("sink", |_c: ExecutionContext| async {
                    panic!("sinks stay pending until neck succeeds")
                })
                .build();
            let handle = runtime.start(hourglass_def(n)).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "neck", |s| {
                matches!(s, NodeState::Running { .. })
            })
            .await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let src_runs = Arc::new(AtomicU32::new(0));
    let sink_runs = Arc::new(AtomicU32::new(0));
    let sc = src_runs.clone();
    let kc = sink_runs.clone();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("src", move |_c: ExecutionContext| {
                sc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"s")) }
            })
            .register_fn("neck", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"n"))
            })
            .register_fn("sink", move |_c: ExecutionContext| {
                kc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"b")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(
            src_runs.load(Ordering::SeqCst),
            0,
            "sources already succeeded"
        );
        assert_eq!(sink_runs.load(Ordering::SeqCst), n as u32);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fat_payloads_64kib_times_eight_persist_resume() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution, Timestamp};
    let path = tmp();
    let n = 8usize;
    let fat = Bytes::from(vec![7u8; 64 * 1024]);
    let store = SqliteStore::open(&path).unwrap();
    let mut b = WorkflowDefinition::builder("fatn").node("join", "join");
    for i in 0..n {
        let id = format!("f{i}");
        b = b.node(id.as_str(), "fat").edge(id.as_str(), "join");
    }
    let def = b.build().unwrap();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    for i in 0..n {
        let nid = format!("f{i}");
        ex.apply(
            ApplyCmd::StartNode {
                node_id: nid.clone().into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: nid.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(fat.clone())),
            },
            &p,
            now,
        )
        .unwrap();
    }
    let rt = current_rt();
    rt.block_on(async {
        store.persist(&ex).await.unwrap();
        drop(store);
        let store = SqliteStore::open(&path).unwrap();
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("fat", |_c: ExecutionContext| async {
                panic!("succeeded fat node must not re-run")
            })
            .register_fn("join", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"j"))
            })
            .build();
        let handle = runtime.resume(ex.id()).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        let snap = store.get(ex.id()).await.unwrap().unwrap();
        for i in 0..n {
            let out = snap
                .node(&NodeId::new(format!("f{i}")))
                .and_then(|n| n.output.clone())
                .expect("fat node");
            assert_eq!(out.as_ref(), fat.as_ref());
        }
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn concurrent_persist_two_executions_same_file_no_panic() {
    use keel_rt::{Execution, StoreError};
    let path = tmp();
    let store_a = SqliteStore::open(&path).unwrap();
    let store_b = SqliteStore::open(&path).unwrap();
    let ex_a = Execution::new(
        WorkflowDefinition::builder("wa")
            .node("a", "a")
            .build()
            .unwrap(),
    );
    let ex_b = Execution::new(
        WorkflowDefinition::builder("wb")
            .node("b", "b")
            .build()
            .unwrap(),
    );
    let id_a = ex_a.id().clone();
    let id_b = ex_b.id().clone();
    let ha = std::thread::spawn(move || {
        let rt = current_rt();
        rt.block_on(store_a.persist(&ex_a))
    });
    let hb = std::thread::spawn(move || {
        let rt = current_rt();
        rt.block_on(store_b.persist(&ex_b))
    });
    let ra = ha
        .join()
        .expect("writer A panicked (SQLITE_BUSY must be typed)");
    let rb = hb
        .join()
        .expect("writer B panicked (SQLITE_BUSY must be typed)");
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        for (r, id) in [(&ra, &id_a), (&rb, &id_b)] {
            match r {
                Ok(()) => {
                    assert!(
                        store.get(id).await.unwrap().is_some(),
                        "successful persist must not lose the row"
                    );
                }
                Err(StoreError::Message(m)) => {
                    assert!(
                        m.to_lowercase().contains("locked") || m.to_lowercase().contains("busy"),
                        "unexpected persist error: {m}"
                    );
                }
                Err(e) => panic!("unexpected persist error: {e}"),
            }
        }
    });
    let _ = std::fs::remove_file(&path);
}

/// 256-wide: one node Succeeded (dirty write), 256 workers still Pending in
/// the file from the first snapshot. Kill, resume — none of the Pending
/// rows are missing (would fail if incremental persist DELETEd unchanged nodes).
#[test]
fn incremental_persist_256_wide_succeeded_does_not_drop_pending() {
    use keel_rt::{AcceptPolicy, ApplyCmd, Execution, Timestamp};
    let path = tmp();
    let n = 256usize;
    let mut b = WorkflowDefinition::builder("wide-inc")
        .node("ok", "ok")
        .node("hold", "hold");
    for i in 0..n {
        let id = format!("w{i}");
        b = b.node(id.as_str(), "w").edge("hold", id.as_str());
    }
    let def = b.build().unwrap();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let exec = Execution::new(def);
        let id = exec.id().clone();
        let rt = current_rt();
        rt.block_on(async {
            store.persist(&exec).await.unwrap();
            let def = store.workflow_definition(&id).await.unwrap().unwrap();
            let snap = store.get(&id).await.unwrap().unwrap();
            let mut exec = Execution::from_snapshot(def, snap).unwrap();
            exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
                .unwrap();
            store.persist(&exec).await.unwrap();
            let def = store.workflow_definition(&id).await.unwrap().unwrap();
            let snap = store.get(&id).await.unwrap().unwrap();
            let mut exec = Execution::from_snapshot(def, snap).unwrap();
            exec.apply(
                ApplyCmd::StartNode {
                    node_id: "ok".into(),
                },
                &AcceptPolicy,
                Timestamp(0),
            )
            .unwrap();
            exec.apply(
                ApplyCmd::FinishNode {
                    node_id: "ok".into(),
                    attempt: 1,
                    outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
                },
                &AcceptPolicy,
                Timestamp(0),
            )
            .unwrap();
            assert!(
                exec.dirty_nodes().iter().any(|(nid, n)| {
                    nid.as_str() == "ok" && matches!(n.state, NodeState::Succeeded)
                }),
                "ok must be the dirty Succeeded slot"
            );
            store.persist(&exec).await.unwrap();
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let hold_runs = Arc::new(AtomicU32::new(0));
    let hc = hold_runs.clone();
    let worker_runs = Arc::new(AtomicU32::new(0));
    let wc = worker_runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(
            snap.nodes.len(),
            n + 2,
            "256 Pending workers must still be rows"
        );
        assert!(matches!(
            snap.node(&NodeId::new("ok")).unwrap().state,
            NodeState::Succeeded
        ));
        for i in 0..n {
            match &snap.node(&NodeId::new(format!("w{i}"))).unwrap().state {
                NodeState::Pending => {}
                other => panic!("w{i} must still be Pending, got {other:?}"),
            }
        }
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("ok", |_c: ExecutionContext| async {
                panic!("succeeded ok must not re-run")
            })
            .register_fn("hold", move |_c: ExecutionContext| {
                hc.fetch_add(1, Ordering::SeqCst);
                async {
                    std::future::pending::<()>().await;
                    NodeOutcome::Succeeded(Bytes::from_static(b"h"))
                }
            })
            .register_fn("w", move |_c: ExecutionContext| {
                wc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"w")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        wait_node(&store, &id, "hold", |s| {
            matches!(s, NodeState::Running { .. })
        })
        .await;
        for i in 0..n {
            let st = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new(format!("w{i}")))
                .unwrap()
                .state
                .clone();
            assert!(
                matches!(st, NodeState::Pending),
                "resume must not drop Pending w{i}: {st:?}"
            );
        }
        assert_eq!(worker_runs.load(Ordering::SeqCst), 0);
        assert_eq!(hold_runs.load(Ordering::SeqCst), 1);
        std::mem::forget(handle);
    });
    let _ = std::fs::remove_file(&path);
}

struct FailFirstTerminal {
    inner: SqliteStore,
    n: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl StateStore for FailFirstTerminal {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }
    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        if exec.state().is_terminal() {
            let k = self.n.fetch_add(1, Ordering::SeqCst) + 1;
            if k == 1 {
                return Err(StoreError::Message("busy terminal".into()));
            }
        }
        self.inner.persist_with_events(exec, events).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

/// `wait()` Succeeded after a transient terminal persist Err used to leave the
/// sqlite file Running (last_persisted advanced on Err; Shutdown did not flush).
/// Resume then re-invoked a job the caller already observed as done.
#[test]
fn transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded() {
    for fast in [false, true] {
        let path = tmp();
        let inner = if fast {
            SqliteStore::open_fast(&path).unwrap()
        } else {
            SqliteStore::open(&path).unwrap()
        };
        let id = {
            let store = FailFirstTerminal {
                inner: inner.clone(),
                n: std::sync::atomic::AtomicU32::new(0),
            };
            let rt = current_rt();
            let id = rt.block_on(async {
                let runtime = Runtime::builder()
                    .store(store)
                    .register_fn("a", |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                    })
                    .build();
                let handle = runtime
                    .start(
                        WorkflowDefinition::builder("wf")
                            .node("a", "a")
                            .build()
                            .unwrap(),
                    )
                    .unwrap();
                let id = handle.execution_id().clone();
                assert_eq!(handle.wait().await, ExecutionState::Succeeded);
                drop(runtime);
                tokio::time::timeout(BOUND, async {
                    loop {
                        if let Some(s) = inner.get(&id).await.unwrap() {
                            if s.state == ExecutionState::Succeeded {
                                return;
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("Shutdown must flush Succeeded to sqlite after transient persist Err");
                id
            });
            drop(rt);
            id
        };
        let store = if fast {
            SqliteStore::open_fast(&path).unwrap()
        } else {
            SqliteStore::open(&path).unwrap()
        };
        assert!(
            store.event_count(&id).unwrap() >= 3,
            "shutdown persist Ok must write event rows for the durable snapshot"
        );
        let rt = current_rt();
        rt.block_on(async {
            assert_eq!(
                store.get(&id).await.unwrap().unwrap().state,
                ExecutionState::Succeeded
            );
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_c: ExecutionContext| async {
                    panic!("succeeded must not re-run after shutdown flush")
                })
                .build();
            let handle = runtime.resume(&id).await.unwrap();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        });
        let _ = std::fs::remove_file(&path);
    }
}

struct FailFirstCancel {
    inner: SqliteStore,
    n: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl StateStore for FailFirstCancel {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }
    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        if exec.state() == ExecutionState::Cancelled {
            let k = self.n.fetch_add(1, Ordering::SeqCst) + 1;
            if k == 1 {
                return Err(StoreError::Message("busy cancel".into()));
            }
        }
        self.inner.persist_with_events(exec, events).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

/// Drop-cancel persist Err once used to leave sqlite Running; resume re-invoked
/// work the handle cancelled. Shutdown must flush Cancelled.
#[test]
fn transient_cancel_persist_err_shutdown_flushes_sqlite_cancelled() {
    let path = tmp();
    let inner = SqliteStore::open(&path).unwrap();
    let id = {
        let store = FailFirstCancel {
            inner: inner.clone(),
            n: std::sync::atomic::AtomicU32::new(0),
        };
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .register(ScriptedExecutor::new("a").hang(false))
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_node(&inner, &id, "a", |s| matches!(s, NodeState::Running { .. })).await;
            drop(handle);
            tokio::time::timeout(BOUND, async {
                loop {
                    if let Some(s) = inner.get(&id).await.unwrap() {
                        if s.state == ExecutionState::Cancelled {
                            return;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancel persist retry on Shutdown");
            drop(runtime);
            id
        });
        drop(rt);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("cancelled must not re-invoke")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Cancelled);
    });
    let _ = std::fs::remove_file(&path);
}

struct FailFirstTwoTerminal {
    inner: SqliteStore,
    n: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl StateStore for FailFirstTwoTerminal {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }
    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        if exec.state().is_terminal() {
            let k = self.n.fetch_add(1, Ordering::SeqCst) + 1;
            if k <= 2 {
                return Err(StoreError::Message("SQLITE_BUSY terminal twice".into()));
            }
        }
        self.inner.persist_with_events(exec, events).await
    }
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

/// Command persist Err + first Shutdown persist Err used to leave sqlite
/// Running after wait() Succeeded. Shutdown must keep retrying.
#[test]
fn transient_terminal_persist_err_twice_shutdown_retries_sqlite() {
    let path = tmp();
    let inner = SqliteStore::open(&path).unwrap();
    let id = {
        let store = FailFirstTwoTerminal {
            inner: inner.clone(),
            n: std::sync::atomic::AtomicU32::new(0),
        };
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
            drop(runtime);
            tokio::time::timeout(BOUND, async {
                loop {
                    if let Some(s) = inner.get(&id).await.unwrap() {
                        if s.state == ExecutionState::Succeeded {
                            return;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Shutdown must retry past a second terminal persist Err");
            id
        });
        drop(rt);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("succeeded must not re-run after double persist Err + Shutdown")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn waiting_resume_does_not_append_event_rows() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let id = rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
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
        let handle = runtime.start(def).unwrap();
        let id = handle.execution_id().clone();
        handle.wait_stable().await;
        id
    });
    drop(rt);
    let n = store.event_count(&id).unwrap();
    assert!(n >= 2, "start + NodeWaiting, got {n}");
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("Waiting must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        handle.wait_stable().await;
        assert_eq!(
            store.event_count(&id).unwrap(),
            n,
            "no-op Waiting resume must not persist a new event batch"
        );
        std::mem::forget(handle);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn event_rows_in_snapshot_txn_are_not_used_for_resume() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let id = rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap();
        let handle = runtime.start(def).unwrap();
        let id = handle.execution_id().clone();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        id
    });
    drop(rt);
    let n = store.event_count(&id).unwrap();
    assert!(n >= 3, "started + node events + succeeded, got {n}");
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("resume is snapshot, not event replay")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

fn exec_succeeded_event(exec: &Execution) -> Event {
    Event::ExecutionSucceeded {
        execution_id: exec.id().clone(),
        workflow_id: exec.definition().id().clone(),
        at: Timestamp(0),
        schema_version: SCHEMA_VERSION,
    }
}

fn succeeded_copies(store: &SqliteStore, id: &ExecutionId) -> usize {
    store
        .event_bodies(id)
        .unwrap()
        .iter()
        .filter(|b| b.contains("ExecutionSucceeded"))
        .count()
}

/// Equal-revision persist used to INSERT events (MAX(seq)+1). Two Runtimes or
/// Shutdown retry after a committed terminal then appended copies.
#[test]
fn equal_revision_persist_does_not_grow_event_rows() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        let mut exec = Execution::new(def);
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        let ev = exec_succeeded_event(&exec);
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
        let n = store.event_count(exec.id()).unwrap();
        assert_eq!(n, 1);
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
        assert_eq!(
            store.event_count(exec.id()).unwrap(),
            n,
            "equal-revision must not append event rows"
        );
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
        assert_eq!(store.event_count(exec.id()).unwrap(), n);
    });
    drop(rt);
    let _ = std::fs::remove_file(&path);
}

/// Checkpoint SQLITE_BUSY after COMMIT used to fail persist. Shutdown retried
/// up to 8×, each hit equal-revision and appended ExecutionSucceeded.
#[test]
fn checkpoint_busy_after_terminal_commit_does_not_duplicate_events() {
    let path = tmp();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            SqliteStore::fail_next_wal_checkpoints(0);
        }
    }
    let _reset = Reset;
    SqliteStore::fail_next_wal_checkpoints(16);
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let id = rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let handle = runtime
            .start(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        drop(runtime);
        tokio::task::yield_now().await;
        id
    });
    drop(rt);
    SqliteStore::fail_next_wal_checkpoints(0);
    let n = store.event_count(&id).unwrap();
    let succeeded = succeeded_copies(&store, &id);
    assert_eq!(
        succeeded, 1,
        "ExecutionSucceeded copies must be one batch, not Shutdown retries; event_count={n} bodies={:?}",
        store.event_bodies(&id).unwrap()
    );
    assert!(n >= 3 && n <= 5, "one persist batch, got event_count={n}");
    let _ = std::fs::remove_file(&path);
}

/// COMMIT already wrote Succeeded. Checkpoint Err must not look like a missing
/// snapshot: persist returns Ok and resume sees Succeeded.
#[test]
fn checkpoint_busy_after_commit_is_persist_ok_and_resume_sees_succeeded() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let id = rt.block_on(async {
        let mut exec = Execution::new(
            WorkflowDefinition::builder("wf")
                .node("a", "e")
                .build()
                .unwrap(),
        );
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        exec.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        exec.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        assert_eq!(exec.state(), ExecutionState::Succeeded);
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                SqliteStore::fail_next_wal_checkpoints(0);
            }
        }
        let _reset = Reset;
        SqliteStore::fail_next_wal_checkpoints(1);
        store
            .persist_with_events(&exec, &[exec_succeeded_event(&exec)])
            .await
            .expect("COMMIT succeeded; checkpoint BUSY must not fail persist");
        SqliteStore::fail_next_wal_checkpoints(0);
        exec.id().clone()
    });
    drop(rt);
    drop(store);
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("e", |_c: ExecutionContext| async {
                panic!("terminal resume must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    let _ = std::fs::remove_file(&path);
}

/// Phase 5: `SqliteStore::open` is FULL. Crash while parked on T; reopen;
/// deadline is still on the snapshot; FakeClock must advance to dispatch.
#[test]
fn crash_resume_full_file_keeps_deadline() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    let delay = Duration::from_millis(50);
    let (id, t) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .policy(RetryPolicy::new(3, delay))
                .register_fn("a", |_c: ExecutionContext| async { NodeOutcome::TimedOut })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "a", |s| {
                matches!(
                    s,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
            .await;
            let t = match store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("a"))
            {
                Some(n) => match &n.state {
                    NodeState::Ready {
                        runnable_at: Some(at),
                    } => *at,
                    other => panic!("{other:?}"),
                },
                None => panic!("missing node"),
            };
            std::mem::forget(handle);
            drop(runtime);
            (id, t)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(
            snap.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(t)
            },
            "FULL file must keep snapshot T"
        );
        let fired = Arc::new(AtomicU32::new(0));
        let f = fired.clone();
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .policy(RetryPolicy::new(3, delay))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "must wait for T");
        clock.advance(delay);
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

/// Hunt: persist T, reopen, T still there (not dropped). Resume with now >= T
/// dispatches once, not twice.
#[test]
fn sqlite_deadline_persist_does_not_drop_or_double_fire() {
    let path = tmp();
    let delay = Duration::from_millis(50);
    let (id, t) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let mut ex = Execution::new(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            );
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
                other => panic!("{other:?}"),
            };
            store.persist(&ex).await.unwrap();
            (ex.id().clone(), t)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let clock = Arc::new(FakeClock::new());
    clock.set(t);
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(
            snap.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(t)
            },
            "reopen must not drop T"
        );
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock)
            .policy(RetryPolicy::new(3, delay))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(fired.load(Ordering::SeqCst), 1, "due T must not double-run");
    });
    let _ = std::fs::remove_file(&path);
}

/// Crash after TimedOut+Retry persisted, before dispatch: same as delay park.
#[test]
fn crash_after_timeout_persisted_before_dispatch_does_not_double_run() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    let delay = Duration::from_millis(80);
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .policy(RetryPolicy::new(2, delay))
                .register(
                    ScriptedExecutor::new("a")
                        .timeout()
                        .succeed(Bytes::from_static(b"ok")),
                )
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("a", "a")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "a", |s| {
                matches!(
                    s,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
            .await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                snap.node(&NodeId::new("a")).unwrap().state,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            ),
            "timeout persisted as Ready {{ T }}, not Waiting/TimedOut yet"
        );
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .policy(RetryPolicy::new(2, delay))
            .register_fn("a", move |c: ExecutionContext| {
                a.fetch_add(1, Ordering::SeqCst);
                let attempt = c.attempt;
                async move {
                    assert_eq!(attempt, 2, "no extra attempts invented");
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        clock.advance(delay);
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    });
    let _ = std::fs::remove_file(&path);
}

/// Incremental persist after a first snapshot must write `runnable_at` (dirty
/// slot). Reopen FULL file — T is still there.
#[test]
fn incremental_persist_does_not_drop_runnable_at() {
    let path = tmp();
    let delay = Duration::from_millis(50);
    let (id, t) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let mut ex = Execution::new(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            );
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
            store.persist(&ex).await.unwrap();
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
                other => panic!("{other:?}"),
            };
            store.persist(&ex).await.unwrap();
            (ex.id().clone(), t)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        assert_eq!(
            store
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
    });
    let _ = std::fs::remove_file(&path);
}

/// 256 parked T, crash, resume, one advance — each fires once.
#[test]
fn crash_resume_256_parked_advance_once_each_once() {
    let path = tmp();
    let n = 256usize;
    let clock = Arc::new(FakeClock::new());
    let delay = Duration::from_millis(1);
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let mut b = WorkflowDefinition::builder("wide-t");
            for i in 0..n {
                b = b.node(format!("w{i}"), "e");
            }
            let mut ex = Execution::new(b.build().unwrap());
            let p = RetryPolicy::new(2, delay);
            let now = Timestamp(0);
            ex.apply(ApplyCmd::Start, &p, now).unwrap();
            for i in 0..n {
                let nid = format!("w{i}");
                ex.apply(
                    ApplyCmd::StartNode {
                        node_id: nid.clone().into(),
                    },
                    &p,
                    now,
                )
                .unwrap();
                ex.apply(
                    ApplyCmd::FinishNode {
                        node_id: nid.into(),
                        attempt: 1,
                        outcome: Ok(NodeOutcome::TimedOut),
                    },
                    &p,
                    now,
                )
                .unwrap();
            }
            store.persist(&ex).await.unwrap();
            ex.id().clone()
        });
        drop(rt);
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let rt = current_rt();
    rt.block_on(async {
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
        assert_eq!(parked, n);
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .concurrency(32)
            .policy(RetryPolicy::new(2, delay))
            .register_fn("e", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        clock.advance(delay);
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(fired.load(Ordering::SeqCst), n as u32);
    });
    let _ = std::fs::remove_file(&path);
}

/// Persist/resume 256-wide Ready {{ T }} vs Ready now. Prints medians (n=5).
#[test]
fn persist_resume_256_runnable_at_set_vs_unset() {
    fn wide(n: usize) -> WorkflowDefinition {
        let mut b = WorkflowDefinition::builder("p256");
        for i in 0..n {
            b = b.node(format!("w{i}"), "e");
        }
        b.build().unwrap()
    }
    fn persist_shape(path: &std::path::Path, parked: bool) -> (Duration, usize, usize, u64) {
        let n = 256usize;
        let store = SqliteStore::open(path).unwrap();
        let rt = current_rt();
        let d = rt.block_on(async {
            let mut ex = Execution::new(wide(n));
            let p = RetryPolicy::new(2, Duration::from_millis(1));
            let now = Timestamp(0);
            ex.apply(ApplyCmd::Start, &p, now).unwrap();
            if parked {
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
            }
            let dirty = ex.dirty_nodes().len();
            let t0 = std::time::Instant::now();
            store.persist(&ex).await.unwrap();
            let elapsed = t0.elapsed();
            let bytes = store.node_json_bytes(ex.id()).unwrap();
            let cols = store.parked_deadline_count(ex.id()).unwrap();
            let snap = store.get(ex.id()).await.unwrap().unwrap();
            if parked {
                assert_eq!(cols, n as u64, "each parked row must store T in the column");
                let kept = snap
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
                assert_eq!(kept, n, "get() must restore Ready {{ T }} for every row");
                assert!(
                    snap.nodes.values().all(|n| n.last_error.is_some()),
                    "park persist must round-trip last_error"
                );
                let sample: String = {
                    let conn = rusqlite::Connection::open(path).unwrap();
                    conn.query_row(
                        "SELECT body FROM nodes WHERE execution_id = ?1 LIMIT 1",
                        rusqlite::params![ex.id().as_str()],
                        |r| r.get(0),
                    )
                    .unwrap()
                };
                assert!(
                    !sample.contains("runnable_at"),
                    "T is the column, got {sample}"
                );
                assert!(
                    sample.contains("last_error"),
                    "compact parked JSON keeps last_error, got {sample}"
                );
            } else {
                assert_eq!(cols, 0);
            }
            (elapsed, bytes, dirty, cols)
        });
        drop(rt);
        d
    }
    let mut set = Vec::new();
    let mut unset = Vec::new();
    let mut set_bytes = 0usize;
    let mut unset_bytes = 0usize;
    for i in 0..5 {
        let a = tmp();
        let b = tmp();
        let (dt, bytes, dirty, cols) = persist_shape(&a, true);
        assert_eq!(dirty, 256);
        assert_eq!(cols, 256);
        set.push(dt);
        set_bytes = bytes;
        let (dt, bytes, dirty, cols) = persist_shape(&b, false);
        assert_eq!(dirty, 256);
        assert_eq!(cols, 0);
        unset.push(dt);
        unset_bytes = bytes;
        let _ = std::fs::remove_file(&a);
        let _ = std::fs::remove_file(&b);
        let _ = i;
    }
    set.sort();
    unset.sort();
    eprintln!(
        "sqlite_persist_256 runnable_at_set={}ms ({} on-disk node-json B) runnable_at_unset={}ms ({} on-disk node-json B) (median n=5)",
        set[2].as_secs_f64() * 1000.0,
        set_bytes,
        unset[2].as_secs_f64() * 1000.0,
        unset_bytes
    );
    // Compact last_error (~38 B/row) is kept so inspect agrees. Nested T is not.
    assert!(
        set_bytes <= unset_bytes + 40 * 256 + 512,
        "parked Ready{{T}} JSON is Ready-now + short last_error, not nested T (got {set_bytes} vs {unset_bytes})"
    );
    assert!(
        set_bytes < 22_016,
        "parked 256 JSON must not return to the 22 016 B nested-T shape (got {set_bytes})"
    );
}

/// Fire one of 256 parked nodes; incremental persist must not write Ready now
/// over the other 255 `runnable_at` values.
#[test]
fn incremental_fire_does_not_overwrite_sibling_runnable_at() {
    let path = tmp();
    let delay = Duration::from_millis(10);
    let (id, t0, t_last) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let mut b = WorkflowDefinition::builder("sib");
            for i in 0..256 {
                b = b.node(format!("w{i}"), "e");
            }
            let mut ex = Execution::new(b.build().unwrap());
            let p = RetryPolicy::new(2, delay);
            ex.apply(ApplyCmd::Start, &p, Timestamp(0)).unwrap();
            for i in 0..256 {
                let nid = format!("w{i}");
                let now = if i == 0 { Timestamp(0) } else { Timestamp(50) };
                ex.apply(
                    ApplyCmd::StartNode {
                        node_id: nid.clone().into(),
                    },
                    &p,
                    now,
                )
                .unwrap();
                ex.apply(
                    ApplyCmd::FinishNode {
                        node_id: nid.into(),
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
            let def = store.workflow_definition(&id).await.unwrap().unwrap();
            let mut ex =
                Execution::from_snapshot(def, store.get(&id).await.unwrap().unwrap()).unwrap();
            ex.apply(
                ApplyCmd::RetryDue {
                    node_id: "w0".into(),
                },
                &p,
                Timestamp(10),
            )
            .unwrap();
            assert_eq!(ex.dirty_nodes().len(), 1, "only the fired slot is dirty");
            store.persist(&ex).await.unwrap();
            let t_last = match &ex.snapshot().node(&NodeId::new("w255")).unwrap().state {
                NodeState::Ready {
                    runnable_at: Some(at),
                } => *at,
                other => panic!("{other:?}"),
            };
            (id, Timestamp(10), t_last)
        });
        drop(rt);
        drop(store);
        out
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                snap.node(&NodeId::new("w0")).unwrap().state,
                NodeState::Ready { runnable_at: None }
            ),
            "fired slot is Ready now"
        );
        for i in 1..256 {
            match &snap.node(&NodeId::new(format!("w{i}"))).unwrap().state {
                NodeState::Ready {
                    runnable_at: Some(at),
                } => {
                    assert!(*at > t0, "sibling {i} must keep future T, got {at}");
                }
                other => panic!("sibling {i} {other:?}"),
            }
        }
        assert_eq!(
            snap.node(&NodeId::new("w255")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(t_last)
            }
        );
    });
    let _ = std::fs::remove_file(&path);
}

/// BUSY on a non-terminal park persist: disk unchanged, then persist Ok keeps T.
/// Checkpoint is not invoked on park persist.
#[test]
fn busy_on_park_persist_rolls_back_then_t_lands() {
    let path = tmp();
    let delay = Duration::from_millis(50);
    let store = SqliteStore::open_with_busy_timeout(&path, Duration::ZERO).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let mut ex = Execution::new(
            WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap(),
        );
        let p = RetryPolicy::new(3, delay);
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        store.persist(&ex).await.unwrap();
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
            other => panic!("{other:?}"),
        };
        SqliteStore::fail_next_wal_checkpoints(8);
        store.persist(&ex).await.unwrap();
        SqliteStore::fail_next_wal_checkpoints(0);
        assert_eq!(
            store
                .get(&ex.id())
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("a"))
                .unwrap()
                .state,
            NodeState::Ready {
                runnable_at: Some(t)
            },
            "park persist is non-terminal; checkpoint BUSY must not fail it"
        );
    });
    drop(rt);
    drop(store);

    let store = SqliteStore::open_with_busy_timeout(&path, Duration::ZERO).unwrap();
    let locker = rusqlite::Connection::open(&path).unwrap();
    locker.busy_timeout(Duration::ZERO).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let mut ex = Execution::new(
            WorkflowDefinition::builder("wf2")
                .node("b", "b")
                .build()
                .unwrap(),
        );
        let p = RetryPolicy::new(3, delay);
        ex.apply(ApplyCmd::Start, &p, Timestamp(0)).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            Timestamp(0),
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "b".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            Timestamp(0),
        )
        .unwrap();
        let err = store.persist(&ex).await;
        assert!(err.is_err(), "write lock must BUSY park persist: {err:?}");
    });
    locker.execute_batch("ROLLBACK").unwrap();
    drop(locker);
    drop(rt);
    let _ = std::fs::remove_file(&path);
}

/// WAL reader does not block a park persist (WAL). T is on disk.
#[test]
fn reader_lock_does_not_block_park_persist() {
    let path = tmp();
    let delay = Duration::from_millis(50);
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let id = rt.block_on(async {
        let mut ex = Execution::new(
            WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap(),
        );
        let p = RetryPolicy::new(3, delay);
        ex.apply(ApplyCmd::Start, &p, Timestamp(0)).unwrap();
        store.persist(&ex).await.unwrap();
        ex.id().clone()
    });
    let reader = rusqlite::Connection::open(&path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT COUNT(*) FROM nodes;")
        .unwrap();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        let mut ex =
            Execution::from_snapshot(store.workflow_definition(&id).await.unwrap().unwrap(), snap)
                .unwrap();
        let p = RetryPolicy::new(3, delay);
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            Timestamp(0),
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            Timestamp(0),
        )
        .unwrap();
        store.persist(&ex).await.unwrap();
        assert!(matches!(
            store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("a"))
                .unwrap()
                .state,
            NodeState::Ready {
                runnable_at: Some(_)
            }
        ));
    });
    reader.execute_batch("ROLLBACK").unwrap();
    drop(reader);
    drop(rt);
    drop(store);
    let _ = std::fs::remove_file(&path);
}

/// Many park wakes then terminal: WAL stays bounded (checkpoint after COMMIT).
#[test]
fn many_park_wakes_wal_stays_bounded() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let clock = Arc::new(FakeClock::new());
        let hits = Arc::new(AtomicU32::new(0));
        let h = hits.clone();
        let runtime = Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .policy(RetryPolicy::new(16, Duration::from_millis(1)))
            .register_fn("a", move |_c: ExecutionContext| {
                let n = h.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 15 {
                        NodeOutcome::TimedOut
                    } else {
                        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                    }
                }
            })
            .build();
        let handle = runtime
            .start(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        for _ in 0..16 {
            clock.advance(Duration::from_millis(1));
            tokio::task::yield_now().await;
        }
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    drop(rt);
    drop(store);
    let mut wal = path.clone();
    let mut s = wal.into_os_string();
    s.push("-wal");
    wal = std::path::PathBuf::from(s);
    let n = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    assert!(n < 2 * 1024 * 1024, "WAL after 16 park wakes: {n}");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&wal);
}
