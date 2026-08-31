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

// Tightened after WAL + dirty-row persist (this machine ~260 ms / ~280 ms).
// Headroom is ~15–30× for CI, not the old 30s/60s floors.
const WIDE_BOUND: Duration = Duration::from_secs(8);
const LOOP_BOUND: Duration = Duration::from_secs(10);
const SEQ_BOUND: Duration = Duration::from_secs(30);
const HOURGLASS_BOUND: Duration = Duration::from_secs(20);
const FAT_BOUND: Duration = Duration::from_secs(10);
const WAL_LOOP_BOUND: Duration = Duration::from_secs(40);
const WIDE_2K_BOUND: Duration = Duration::from_secs(120);

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
    tokio::time::timeout(Duration::from_secs(15), async {
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

fn wal_path(db: &std::path::Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}

fn wal_len(db: &std::path::Path) -> u64 {
    std::fs::metadata(wal_path(db)).map(|m| m.len()).unwrap_or(0)
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

fn resume_256_median(
    label: &str,
    open: impl Fn(&std::path::Path) -> keel_rt_sqlite::SqliteStore,
) -> Duration {
    let path = tmp();
    let def = wide_def(256);
    let store = open(&path);
    let rt = current_rt();
    let median = rt.block_on(async {
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
                .unwrap_or_else(|_| panic!("{label} 256-wide resume timed out"))
                .unwrap();
            assert_eq!(
                tokio::time::timeout(WIDE_BOUND, handle.wait())
                    .await
                    .unwrap_or_else(|_| panic!("{label} 256-wide wait timed out")),
                ExecutionState::Succeeded
            );
            samples.push(t0.elapsed());
        }
        samples.sort();
        eprintln!(
            "sqlite resume 256-wide {label} (debug, n=3) median={:?} samples={:?}",
            samples[1], samples
        );
        samples[1]
    });
    let _ = std::fs::remove_file(&path);
    median
}

#[test]
fn resume_256_wide_full_vs_normal() {
    let normal = resume_256_median("NORMAL", |p| SqliteStore::open_fast(p).unwrap());
    let full = resume_256_median("FULL", |p| SqliteStore::open(p).unwrap());
    eprintln!(
        "sqlite 256-wide resume NORMAL={:?} FULL={:?} (50% of 1.008s = 504ms)",
        normal, full
    );
    assert!(normal < WIDE_BOUND);
    assert!(full < WIDE_BOUND);
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
    let wal = wal_len(&path);
    eprintln!("sqlite 50-diamond WAL bytes after terminals: {wal}");
    assert!(
        wal < 8 * 1024 * 1024,
        "WAL grew without bound after 50 terminal diamonds: {wal} bytes"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path(&path));
}

#[test]
fn persist_256_wide_apply_within_bound() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let def = wide_def(256);
        let mut first = Execution::new(def.clone());
        let t_first = Instant::now();
        store.persist(&first).await.unwrap();
        let first_elapsed = t_first.elapsed();
        first
            .apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        let t_inc = Instant::now();
        store.persist(&first).await.unwrap();
        let inc_elapsed = t_inc.elapsed();
        eprintln!(
            "sqlite persist 256-wide first snapshot={:?} incremental after Start={:?}",
            first_elapsed, inc_elapsed
        );
        assert!(
            first_elapsed < WIDE_BOUND,
            "first persist {:?} exceeds {WIDE_BOUND:?}",
            first_elapsed
        );
        assert!(
            inc_elapsed < WIDE_BOUND,
            "incremental persist {:?} exceeds {WIDE_BOUND:?}",
            inc_elapsed
        );
        let snap = store.get(first.id()).await.unwrap().unwrap();
        assert_eq!(snap.nodes.len(), 258);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn crash_resume_hourglass_256_within_bound() {
    let path = tmp();
    let n = 256usize;
    let t0 = Instant::now();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .concurrency(32)
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
            .concurrency(32)
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
        let handle = tokio::time::timeout(HOURGLASS_BOUND, runtime.resume(&id))
            .await
            .expect("hourglass 256 resume timed out")
            .unwrap();
        assert_eq!(
            tokio::time::timeout(HOURGLASS_BOUND, handle.wait())
                .await
                .expect("hourglass 256 wait timed out"),
            ExecutionState::Succeeded
        );
        assert_eq!(src_runs.load(Ordering::SeqCst), 0);
        assert_eq!(sink_runs.load(Ordering::SeqCst), n as u32);
    });
    eprintln!(
        "sqlite hourglass-256 crash-resume: {:?}",
        t0.elapsed()
    );
    assert!(t0.elapsed() < HOURGLASS_BOUND);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn twenty_diamond_repeat_crash_resume_within_bound() {
    let path = tmp();
    let t0 = Instant::now();
    let n = 20usize;
    let mut last_id = None;
    for i in 0..n {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = last_id.clone();
        let out = rt.block_on(async {
            if i == 0 {
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
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .register_fn("src", |_c: ExecutionContext| async {
                        panic!("src succeeded")
                    })
                    .register_fn("sum", |_c: ExecutionContext| async {
                        panic!("sum succeeded")
                    })
                    .register_fn("crit", |_c: ExecutionContext| async {
                        std::future::pending::<()>().await;
                        NodeOutcome::Succeeded(Bytes::from_static(b"c"))
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
                let runtime = Runtime::builder()
                    .store(store.clone())
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
    eprintln!(
        "sqlite 20 diamond-repeat crash-resume: {:?}",
        t0.elapsed()
    );
    assert!(
        t0.elapsed() < WAL_LOOP_BOUND,
        "20 diamond-repeat exceeded {WAL_LOOP_BOUND:?}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fat_payloads_64kib_times_32_persist_resume_within_bound() {
    let path = tmp();
    let n = 32usize;
    let fat = Bytes::from(vec![3u8; 64 * 1024]);
    let store = SqliteStore::open(&path).unwrap();
    let mut b = WorkflowDefinition::builder("fatn").node("join", "join");
    for i in 0..n {
        let id = format!("f{i}");
        b = b.node(id.as_str(), "fat").edge(id.as_str(), "join");
    }
    let def = b.build().unwrap();
    let mut ex = Execution::new(def);
    ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    for i in 0..n {
        let nid = format!("f{i}");
        ex.apply(
            ApplyCmd::StartNode {
                node_id: nid.clone().into(),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: nid.into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(fat.clone())),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
    }
    let rt = current_rt();
    rt.block_on(async {
        let t0 = Instant::now();
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
        let handle = tokio::time::timeout(FAT_BOUND, runtime.resume(ex.id()))
            .await
            .expect("fat-32 resume timed out")
            .unwrap();
        assert_eq!(
            tokio::time::timeout(FAT_BOUND, handle.wait())
                .await
                .expect("fat-32 wait timed out"),
            ExecutionState::Succeeded
        );
        let elapsed = t0.elapsed();
        eprintln!("sqlite fat 64KiB × 32 persist+resume: {elapsed:?}");
        assert!(elapsed < FAT_BOUND);
        let snap = store.get(ex.id()).await.unwrap().unwrap();
        for i in 0..n {
            let out = snap
                .node(&NodeId::new(format!("f{i}")))
                .and_then(|node| node.output.clone())
                .expect("fat node");
            assert_eq!(out.as_ref(), fat.as_ref());
        }
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn two_hundred_start_crash_resume_wal_bounded() {
    let path = tmp();
    let t0 = Instant::now();
    let mut wal_peak = 0u64;
    for i in 0..200 {
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
        wal_peak = wal_peak.max(wal_len(&path));
    }
    let wal = wal_len(&path);
    eprintln!(
        "sqlite 200 start-crash-resume diamonds: {:?} wal_end={wal} wal_peak={wal_peak}",
        t0.elapsed()
    );
    assert!(
        t0.elapsed() < WAL_LOOP_BOUND,
        "200 diamond crash-resume exceeded {WAL_LOOP_BOUND:?}"
    );
    assert!(
        wal < 8 * 1024 * 1024 && wal_peak < 16 * 1024 * 1024,
        "WAL unbounded: end={wal} peak={wal_peak}"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path(&path));
}

fn resume_wide_snapshot(n: usize, bound: Duration) {
    let path = tmp();
    let def = wide_def(n);
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
        let mut ex = Execution::new(def);
        ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&ex).await.unwrap();
        let t0 = Instant::now();
        let handle = tokio::time::timeout(bound, runtime.resume(ex.id()))
            .await
            .unwrap_or_else(|_| panic!("{n}-wide resume timed out"))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(bound, handle.wait())
                .await
                .unwrap_or_else(|_| panic!("{n}-wide wait timed out")),
            ExecutionState::Succeeded
        );
        eprintln!(
            "sqlite resume {n}-wide snapshot: {:?}",
            t0.elapsed()
        );
        assert!(t0.elapsed() < bound);
    });
    let _ = std::fs::remove_file(&path);
}

/// Debug 10k-wide sqlite resume did not finish in ~60s on this machine class
/// (see benches/BASELINE.md). CI `stress-resume` gates 2k debug.
#[test]
fn resume_2k_wide_snapshot_debug_within_bound() {
    resume_wide_snapshot(2000, WIDE_2K_BOUND);
}

/// 10k-node sqlite snapshot resume. Release profile only — debug is too slow.
#[cfg(not(debug_assertions))]
#[test]
fn resume_10k_wide_snapshot_release_within_bound() {
    resume_wide_snapshot(10_000, Duration::from_secs(60));
}
