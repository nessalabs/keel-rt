//! Sqlite resume stress. Separate CI job (`stress-resume`). Not in coverage.
//!
//! `cargo test -p keel-rt-sqlite --test resume_stress -- --test-threads=1 --nocapture`

use bytes::Bytes;
use keel_rt::testing::ScriptedExecutor;
use keel_rt::{
    AcceptPolicy, ApplyCmd, Execution, ExecutionContext, ExecutionId, ExecutionState, NodeId,
    NodeOutcome, NodeState, Runtime, StateStore, Timestamp, WorkflowDefinition,
};
use keel_rt_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WIDE_BOUND: Duration = Duration::from_secs(30);
const LOOP_BOUND: Duration = Duration::from_secs(60);
const SEQ_BOUND: Duration = Duration::from_secs(90);

fn tmp() -> PathBuf {
    let p = std::env::temp_dir().join(format!("keel-rt-sqlite-stress-{}.db", ExecutionId::new()));
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
    tokio::time::timeout(Duration::from_secs(5), async {
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

fn wide_def(n: usize) -> WorkflowDefinition {
    let mut b = WorkflowDefinition::builder("wide")
        .node("src", "ok")
        .node("join", "ok");
    for i in 0..n {
        let id = format!("w{i}");
        b = b
            .node(id.as_str(), "ok")
            .edge("src", id.as_str())
            .edge(id.as_str(), "join");
    }
    b.build().unwrap()
}

fn diamond() -> WorkflowDefinition {
    WorkflowDefinition::builder("d")
        .node("src", "src")
        .node("sum", "sum")
        .node("crit", "crit")
        .node("writer", "writer")
        .edge("src", "sum")
        .edge("src", "crit")
        .edge("sum", "writer")
        .edge("crit", "writer")
        .build()
        .unwrap()
}

#[test]
fn resume_256_wide_snapshot_within_bound() {
    let path = tmp();
    let def = wide_def(256);
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .concurrency(32)
            .register_fn("ok", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let mut samples = Vec::new();
        for _ in 0..3 {
            let mut ex = Execution::new(def.clone());
            ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
                .unwrap();
            store.persist(&ex).await.unwrap();
            let t0 = Instant::now();
            let handle = tokio::time::timeout(WIDE_BOUND, runtime.resume(ex.id()))
                .await
                .expect("256-wide resume timed out")
                .unwrap();
            assert_eq!(
                tokio::time::timeout(WIDE_BOUND, handle.wait())
                    .await
                    .expect("256-wide wait timed out"),
                ExecutionState::Succeeded
            );
            samples.push(t0.elapsed());
        }
        samples.sort();
        eprintln!(
            "sqlite resume 256-wide (debug, n=3) median={:?} samples={:?}",
            samples[1], samples
        );
        assert!(
            samples[1] < WIDE_BOUND,
            "median {:?} exceeds {WIDE_BOUND:?}",
            samples[1]
        );
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn one_thousand_sequential_dags_resume_last() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        tokio::time::timeout(SEQ_BOUND, async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .build();
            let mut last = None;
            for i in 0..1000 {
                let def = WorkflowDefinition::builder(format!("wf-{i}"))
                    .node("a", "a")
                    .build()
                    .unwrap();
                let handle = runtime.start(def).unwrap();
                let id = handle.execution_id().clone();
                assert_eq!(handle.wait().await, ExecutionState::Succeeded);
                last = Some(id);
            }
            let id = last.expect("last");
            let t0 = Instant::now();
            let handle = runtime.resume(&id).await.unwrap();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
            eprintln!(
                "sqlite 1000 sequential small DAGs then resume last: {:?}",
                t0.elapsed()
            );
        })
        .await
        .expect("1000 sequential sqlite executions timed out");
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_resume_same_diamond_attempt_climbs_only_on_inflight() {
    let path = tmp();
    let crit_attempts = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
    let src_runs = Arc::new(AtomicU32::new(0));
    let n = 8usize;
    let mut last_id = None;
    for i in 0..n {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = last_id.clone();
        let crit_attempts = crit_attempts.clone();
        let src_runs = src_runs.clone();
        let out = rt.block_on(async {
            if i == 0 {
                let src = src_runs.clone();
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .register_fn("src", move |_c: ExecutionContext| {
                        src.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"s")) }
                    })
                    .register_fn("sum", |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"u"))
                    })
                    .register(ScriptedExecutor::new("crit").hang(false))
                    .register_fn("writer", |_c: ExecutionContext| async {
                        panic!("writer waits for crit")
                    })
                    .build();
                let handle = runtime.start(diamond()).unwrap();
                let id = handle.execution_id().clone();
                wait_node(&store, &id, "crit", |s| matches!(s, NodeState::Running { .. }))
                    .await;
                std::mem::forget(handle);
                drop(runtime);
                Some(id)
            } else if i + 1 < n {
                let rec = crit_attempts.clone();
                let src = src_runs.clone();
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .register_fn("src", move |_c: ExecutionContext| {
                        src.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"s")) }
                    })
                    .register_fn("sum", |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"u"))
                    })
                    .register_fn("crit", move |ctx: ExecutionContext| {
                        rec.lock().unwrap().push(ctx.attempt);
                        async {
                            std::future::pending::<()>().await;
                            NodeOutcome::Succeeded(Bytes::from_static(b"c"))
                        }
                    })
                    .register_fn("writer", |_c: ExecutionContext| async {
                        panic!("writer waits for crit")
                    })
                    .build();
                let handle = runtime.resume(id.as_ref().unwrap()).await.unwrap();
                wait_node(&store, id.as_ref().unwrap(), "crit", |s| {
                    matches!(s, NodeState::Running { .. })
                })
                .await;
                std::mem::forget(handle);
                drop(runtime);
                id
            } else {
                let rec = crit_attempts.clone();
                let src = src_runs.clone();
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .register_fn("src", move |_c: ExecutionContext| {
                        src.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"s")) }
                    })
                    .register_fn("sum", |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"u"))
                    })
                    .register_fn("crit", move |ctx: ExecutionContext| {
                        rec.lock().unwrap().push(ctx.attempt);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"c")) }
                    })
                    .register_fn("writer", |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"w"))
                    })
                    .build();
                let handle = tokio::time::timeout(LOOP_BOUND, runtime.resume(id.as_ref().unwrap()))
                    .await
                    .expect("final resume")
                    .unwrap();
                assert_eq!(
                    tokio::time::timeout(LOOP_BOUND, handle.wait())
                        .await
                        .expect("final wait"),
                    ExecutionState::Succeeded
                );
                id
            }
        });
        drop(rt);
        drop(store);
        last_id = out;
    }
    let attempts = crit_attempts.lock().unwrap().clone();
    assert!(
        !attempts.is_empty(),
        "in-flight crit must have been re-invoked"
    );
    for w in attempts.windows(2) {
        assert!(w[1] > w[0], "attempt must climb on the in-flight node: {attempts:?}");
    }
    assert_eq!(
        src_runs.load(Ordering::SeqCst),
        1,
        "succeeded src must not re-run across crashes"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fifty_start_crash_resume_diamonds_one_file() {
    let path = tmp();
    let t0 = Instant::now();
    for i in 0..50 {
        let store = SqliteStore::open(&path).expect("no leaked sqlite lock");
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
                    panic!("writer waits")
                })
                .build();
            let def = WorkflowDefinition::builder(format!("d-{i}"))
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
        let store = SqliteStore::open(&path).expect("no leaked sqlite lock");
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .register_fn("src", |_c: ExecutionContext| async {
                    panic!("src succeeded")
                })
                .register_fn("sum", |_c: ExecutionContext| async {
                    panic!("sum succeeded")
                })
                .register_fn("crit", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"c"))
                })
                .register_fn("writer", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"w"))
                })
                .build();
            let handle = runtime.resume(&id).await.unwrap();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        });
        drop(rt);
    }
    eprintln!(
        "sqlite 50 start-crash-resume diamonds on one file: {:?}",
        t0.elapsed()
    );
    assert!(
        t0.elapsed() < LOOP_BOUND,
        "50 diamond crash-resume loop exceeded {LOOP_BOUND:?}"
    );
    let _ = std::fs::remove_file(&path);
}
