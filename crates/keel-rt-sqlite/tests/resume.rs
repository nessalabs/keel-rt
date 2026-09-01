//! Crash-resume against a real sqlite file. Not a mock of StateStore.
//!
//! Recipe: nested current_thread runtime, forget the handle so Drop does
//! not Cancel, drop the tokio runtime (aborts tasks), reopen the file.

use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Event, Execution, ExecutionContext, ExecutionId, ExecutionSnapshot,
    ExecutionState, Join, NodeId, NodeOutcome, NodeState, OnFailure, Resume, ResumeError,
    RetryPolicy, Runtime, SCHEMA_VERSION, StateStore, StoreError, Timestamp, WorkflowDefinition,
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

async fn wait_node(store: &SqliteStore, id: &ExecutionId, node: &str, pred: impl Fn(&NodeState) -> bool) {
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
            wait_node(&store, &id, "crit", |s| matches!(s, NodeState::Running { .. }))
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
                matches!(s, NodeState::Ready { runnable_at: Some(_) })
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
            wait_node(&store, &id, "p2", |s| matches!(s, NodeState::Running { .. }))
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
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
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
    ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
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
        let hb = runtime_b.resume(ex.id()).await.unwrap();
        let _ = ha.wait().await;
        let _ = hb.wait().await;
        assert!(
            runs.load(Ordering::SeqCst) >= 2,
            "sqlite does not fence processes; both Runtimes re-invoke"
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
    ex.apply(ApplyCmd::StartNode { node_id: "fat".into() }, &p, now)
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
            wait_node(&store, &id, "neck", |s| matches!(s, NodeState::Running { .. }))
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
        assert_eq!(src_runs.load(Ordering::SeqCst), 0, "sources already succeeded");
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
    let ra = ha.join().expect("writer A panicked (SQLITE_BUSY must be typed)");
    let rb = hb.join().expect("writer B panicked (SQLITE_BUSY must be typed)");
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
                        m.to_lowercase().contains("locked")
                            || m.to_lowercase().contains("busy"),
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
        assert_eq!(snap.nodes.len(), n + 2, "256 Pending workers must still be rows");
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
        wait_node(&store, &id, "hold", |s| matches!(s, NodeState::Running { .. })).await;
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
    async fn get(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, StoreError> {
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
                let handle = runtime.start(
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
    async fn get(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, StoreError> {
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
            let handle = runtime.start(
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
    async fn get(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, StoreError> {
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
