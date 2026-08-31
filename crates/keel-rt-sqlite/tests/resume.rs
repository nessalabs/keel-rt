//! Crash-resume against a real sqlite file. Not a mock of StateStore.
//!
//! Recipe: nested current_thread runtime, forget the handle so Drop does
//! not Cancel, drop the tokio runtime (aborts tasks), reopen the file.

use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor};
use keel_rt::{
    ExecutionContext, ExecutionId, ExecutionState, Join, NodeId, NodeOutcome, NodeState,
    OnFailure, Resume, ResumeError, RetryPolicy, Runtime, StateStore, WorkflowDefinition,
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
