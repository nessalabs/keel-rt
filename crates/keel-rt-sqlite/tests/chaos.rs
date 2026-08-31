//! Standing sqlite chaos / load pack. Not coverage.
//!
//! High load and messy user scenarios against a **real file**. Fail-fast and
//! AND-join stay the library defaults. Two Runtimes on one file are unfenced
//! (ADR 0004) — hunt silent wrong terminals, not leases.
//!
//! `cargo test -p keel-rt-sqlite --test chaos -- --test-threads=1 --nocapture`

use async_trait::async_trait;
use bytes::Bytes;
use keel_rt::testing::{FakeClock, FaultySink, ScriptedExecutor};
use keel_rt::{
    AcceptPolicy, ApplyCmd, Execution, ExecutionContext, ExecutionId, ExecutionSnapshot,
    ExecutionState, NodeId, NodeOutcome, NodeState, Policy, PolicyDecision, Resume, RetryPolicy,
    Runtime, StateStore, StoreError, Timestamp, WorkflowDefinition,
};
use keel_rt_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const JOBS_BOUND: Duration = Duration::from_secs(30);
const WIDE_BOUND: Duration = Duration::from_secs(15);
const WIDE_2K_BOUND: Duration = Duration::from_secs(120);
const FARM_BOUND: Duration = Duration::from_secs(20);
const STORM_BOUND: Duration = Duration::from_secs(20);
const SQLITE_BOUND: Duration = Duration::from_secs(15);

fn tmp() -> PathBuf {
    let p = std::env::temp_dir().join(format!("keel-rt-sqlite-chaos-{}.db", ExecutionId::new()));
    rm_db(&p);
    p
}

fn wal_path(db: &std::path::Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}

fn wal_len(db: &std::path::Path) -> u64 {
    std::fs::metadata(wal_path(db)).map(|m| m.len()).unwrap_or(0)
}

fn rm_db(p: &std::path::Path) {
    let _ = std::fs::remove_file(p);
    let _ = std::fs::remove_file(wal_path(p));
    let mut shm = p.as_os_str().to_os_string();
    shm.push("-shm");
    let _ = std::fs::remove_file(PathBuf::from(shm));
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
    tokio::time::timeout(Duration::from_secs(10), async {
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

fn assert_no_silent_wrong_terminal(snap: &ExecutionSnapshot, def: &WorkflowDefinition) {
    if snap.state == ExecutionState::Succeeded {
        for n in def.nodes() {
            let body = snap.node(&n.id).unwrap_or_else(|| panic!("missing {}", n.id.as_str()));
            assert!(
                !matches!(body.state, NodeState::Pending | NodeState::Ready { .. } | NodeState::Running { .. } | NodeState::Waiting { .. }),
                "Succeeded execution has live node {}",
                n.id.as_str()
            );
        }
    }
    if let Some(w) = snap.node(&NodeId::new("writer")) {
        if matches!(w.state, NodeState::Succeeded) {
            for pred in ["sum", "crit"] {
                if let Some(p) = snap.node(&NodeId::new(pred)) {
                    assert!(
                        matches!(p.state, NodeState::Succeeded),
                        "writer Succeeded with {pred} {:?}",
                        p.state
                    );
                }
            }
        }
    }
}

/// Yields inside `persist` of a Cancelled snapshot so the test can Drop the
/// handle while sqlite is in `persist`. Production SqliteStore does not yield;
/// this is the interleaving the scheduler already allows at `.await`.
#[derive(Clone)]
struct PersistGate {
    inner: SqliteStore,
    started: Arc<Notify>,
    proceed: Arc<Notify>,
}

impl PersistGate {
    fn new(inner: SqliteStore) -> Self {
        Self {
            inner,
            started: Arc::new(Notify::new()),
            proceed: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl StateStore for PersistGate {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        if exec.state() == ExecutionState::Cancelled {
            self.started.notify_one();
            tokio::task::yield_now().await;
            self.proceed.notified().await;
        }
        self.inner.persist(exec).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

/// Commits via sqlite, then panics. CatchUnwind must not roll back the COMMIT
/// and must not invent a different terminal on resume.
struct PanicAfterPersist {
    inner: SqliteStore,
    hits: AtomicU32,
}

#[async_trait]
impl StateStore for PanicAfterPersist {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let terminal = exec.state().is_terminal();
        self.inner.persist(exec).await?;
        self.hits.fetch_add(1, Ordering::SeqCst);
        if terminal {
            panic!("chaos persist panics after sqlite COMMIT of terminal");
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

struct PanicPolicy;

impl Policy for PanicPolicy {
    fn decide(&self, _o: &NodeOutcome, _a: u32) -> PolicyDecision {
        panic!("chaos policy exploded");
    }
}

/// Thousands of short jobs, one sqlite file. Sequential start+wait so we
/// measure persist/resume load, not a 50-diamond start/drop loop.
#[test]
fn two_thousand_short_jobs_one_file() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let t0 = Instant::now();
    rt.block_on(async {
        tokio::time::timeout(JOBS_BOUND, async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .build();
            let mut last = None;
            for i in 0..2000 {
                let def = WorkflowDefinition::builder(format!("j-{i}"))
                    .node("a", "a")
                    .build()
                    .unwrap();
                let handle = runtime.start(def).unwrap();
                let id = handle.execution_id().clone();
                assert_eq!(handle.wait().await, ExecutionState::Succeeded);
                last = Some(id);
            }
            let id = last.unwrap();
            let handle = runtime.resume(&id).await.unwrap();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        })
        .await
        .expect("2000 short sqlite jobs timed out");
    });
    eprintln!("chaos 2000 short jobs one file: {:?}", t0.elapsed());
    rm_db(&path);
}

/// Crash while a 256-wide AND-join worker is Running. Resume: succeeded
/// workers skipped, join runs once, fail-fast unchanged.
#[test]
fn wide_256_and_join_crash_resume() {
    let path = tmp();
    let n = 256usize;
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .concurrency(32)
                .register_fn("ok", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .register(ScriptedExecutor::new("hang").hang(false))
                .build();
            let mut b = WorkflowDefinition::builder("w256")
                .node("src", "ok")
                .node("hang", "hang")
                .node("join", "ok")
                .edge("src", "hang")
                .edge("hang", "join");
            for i in 0..n {
                let id = format!("w{i}");
                b = b
                    .node(id.as_str(), "ok")
                    .edge("src", id.as_str())
                    .edge(id.as_str(), "join");
            }
            let handle = runtime.start(b.build().unwrap()).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "hang", |s| matches!(s, NodeState::Running { .. })).await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let join_runs = Arc::new(AtomicU32::new(0));
    let jc = join_runs.clone();
    let hang_runs = Arc::new(AtomicU32::new(0));
    let hc = hang_runs.clone();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .concurrency(32)
            .register_fn("ok", move |ctx: ExecutionContext| {
                let jc = jc.clone();
                async move {
                    if ctx.node_id.as_str() == "join" {
                        jc.fetch_add(1, Ordering::SeqCst);
                    }
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            })
            .register_fn("hang", move |_c: ExecutionContext| {
                hc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"h")) }
            })
            .build();
        let handle = tokio::time::timeout(WIDE_BOUND, runtime.resume(&id))
            .await
            .expect("256-wide AND-join resume")
            .unwrap();
        assert_eq!(
            tokio::time::timeout(WIDE_BOUND, handle.wait())
                .await
                .expect("256-wide AND-join wait"),
            ExecutionState::Succeeded
        );
        assert_eq!(hang_runs.load(Ordering::SeqCst), 1);
        assert_eq!(join_runs.load(Ordering::SeqCst), 1);
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(snap.nodes.len(), n + 3);
    });
    rm_db(&path);
}

/// Sqlite-bound: resume a 256-wide snapshot while other executions start on
/// the same file. Not a start/drop loop — the slow path is snapshot load +
/// persist under concurrent writers. Idle resume of a twin snapshot is
/// recorded so this is not confused with the 50-diamond start/drop bound.
#[test]
fn wide_256_resume_under_concurrent_starts_is_sqlite_bound() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let mut idle = Execution::new(wide_def(256));
    idle.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    let idle_id = idle.id().clone();
    let mut load = Execution::new(wide_def(256));
    load.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    let load_id = load.id().clone();
    let rt = current_rt();
    rt.block_on(async {
        store.persist(&idle).await.unwrap();
        store.persist(&load).await.unwrap();
        let runtime = Runtime::builder()
            .store(store.clone())
            .concurrency(32)
            .register_fn("ok", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"a"))
            })
            .build();
        let t_idle = Instant::now();
        let h_idle = runtime.resume(&idle_id).await.unwrap();
        assert_eq!(h_idle.wait().await, ExecutionState::Succeeded);
        let idle_elapsed = t_idle.elapsed();
        let t_load = Instant::now();
        let resume = async {
            let handle = runtime.resume(&load_id).await.unwrap();
            assert_eq!(handle.wait().await, ExecutionState::Succeeded);
            t_load.elapsed()
        };
        let starts = async {
            let mut n_ok = 0u32;
            for i in 0..32 {
                let d = WorkflowDefinition::builder(format!("s-{i}"))
                    .node("a", "a")
                    .build()
                    .unwrap();
                let h = runtime.start(d).unwrap();
                assert_eq!(h.wait().await, ExecutionState::Succeeded);
                n_ok += 1;
            }
            n_ok
        };
        let (load_elapsed, n_ok) = tokio::join!(resume, starts);
        eprintln!(
            "chaos sqlite-bound 256-wide idle resume {idle_elapsed:?}; under 32 concurrent starts {load_elapsed:?} starts={n_ok}"
        );
        assert_eq!(n_ok, 32);
        assert!(
            idle_elapsed < SQLITE_BOUND,
            "idle 256-wide resume {idle_elapsed:?}"
        );
        assert!(
            load_elapsed < SQLITE_BOUND,
            "sqlite-bound resume {load_elapsed:?}"
        );
        let snap = store.get(&load_id).await.unwrap().unwrap();
        assert_eq!(snap.state, ExecutionState::Succeeded);
        assert_eq!(snap.nodes.len(), 258);
    });
    rm_db(&path);
}

/// Crash after persisting a 2k-wide Ready AND-join snapshot (no live workers).
/// Resume must join once. Live hang-one-of-2k is start/drop bound; this path
/// is sqlite snapshot load. Debug 2k idle resume is also gated in
/// `resume_2k_wide_snapshot_debug_within_bound`.
#[test]
fn wide_2k_and_join_crash_resume_of_ready() {
    let path = tmp();
    let n = 2000usize;
    let join_runs = Arc::new(AtomicU32::new(0));
    let jc = join_runs.clone();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let mut wide = Execution::new(wide_def(n));
        wide.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        let id = wide.id().clone();
        current_rt().block_on(async {
            store.persist(&wide).await.unwrap();
        });
        drop(store);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    let t0 = Instant::now();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .concurrency(32)
            .register_fn("ok", move |ctx: ExecutionContext| {
                let jc = jc.clone();
                async move {
                    if ctx.node_id.as_str() == "join" {
                        jc.fetch_add(1, Ordering::SeqCst);
                    }
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                }
            })
            .build();
        let handle = tokio::time::timeout(WIDE_2K_BOUND, runtime.resume(&id))
            .await
            .expect("2k-wide AND-join resume")
            .unwrap();
        assert_eq!(
            tokio::time::timeout(WIDE_2K_BOUND, handle.wait())
                .await
                .expect("2k-wide AND-join wait"),
            ExecutionState::Succeeded
        );
        assert_eq!(join_runs.load(Ordering::SeqCst), 1);
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(snap.nodes.len(), n + 2);
        assert_eq!(snap.state, ExecutionState::Succeeded);
    });
    eprintln!("chaos 2k-wide Ready crash-resume: {:?}", t0.elapsed());
    assert!(t0.elapsed() < WIDE_2K_BOUND);
    rm_db(&path);
}

/// Two OS threads, one file: 256-wide resume vs a storm of starts.
/// SQLITE_BUSY must be typed (no panic). Final wide snapshot is a real terminal.
#[test]
fn two_threads_wide_resume_and_starts_no_wrong_terminal() {
    let path = tmp();
    let store_a = SqliteStore::open(&path).unwrap();
    let store_b = SqliteStore::open(&path).unwrap();
    let def = wide_def(256);
    let mut wide = Execution::new(def);
    wide.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    let wide_id = wide.id().clone();
    current_rt().block_on(async {
        store_a.persist(&wide).await.unwrap();
    });
    let pa = path.clone();
    let wide_id_a = wide_id.clone();
    let ha = std::thread::spawn(move || {
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store_a)
                .concurrency(32)
                .register_fn("ok", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .build();
            match runtime.resume(&wide_id_a).await {
                Ok(handle) => {
                    let st = handle.wait().await;
                    assert!(st.is_terminal(), "{st}");
                }
                Err(e) => panic!("wide resume typed-or-ok, got {e}"),
            }
        });
    });
    let hb = std::thread::spawn(move || {
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store_b)
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"a"))
                })
                .build();
            for i in 0..16 {
                let d = WorkflowDefinition::builder(format!("t-{i}"))
                    .node("a", "a")
                    .build()
                    .unwrap();
                let h = runtime.start(d).unwrap();
                let st = h.wait().await;
                assert!(st.is_terminal() || st == ExecutionState::Succeeded, "{st}");
            }
        });
    });
    ha.join().expect("thread A panicked");
    hb.join().expect("thread B panicked");
    let store = SqliteStore::open(&path).unwrap();
    current_rt().block_on(async {
        let snap = store.get(&wide_id).await.unwrap().unwrap();
        let def = store.workflow_definition(&wide_id).await.unwrap().unwrap();
        Execution::from_snapshot(def.clone(), snap.clone()).unwrap();
        assert_no_silent_wrong_terminal(&snap, &def);
        assert!(snap.state.is_terminal() || snap.state == ExecutionState::Running);
    });
    rm_db(&pa);
}

/// Diamond farm: retry + HITL Waiting, crash, shuffled token resume.
#[test]
fn diamond_farm_retry_hitl_shuffled_resume() {
    let path = tmp();
    let n = 8usize;
    let clock = Arc::new(FakeClock::new());
    let ids = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .policy(RetryPolicy::new(3, Duration::from_millis(40)))
                .register_fn("src", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"s"))
                })
                .register_fn("sum", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"u"))
                })
                .register_fn("crit", |ctx: ExecutionContext| async move {
                    if ctx.node_id.as_str() == "crit" && ctx.attempt == 1 {
                        NodeOutcome::Failed(keel_rt::NodeError::new("once"))
                    } else {
                        NodeOutcome::Waiting {
                            token: ctx.resume_token,
                        }
                    }
                })
                .register_fn("writer", |_c: ExecutionContext| async {
                    panic!("writer waits for both sides")
                })
                .build();
            let mut ids = Vec::new();
            for i in 0..n {
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
                wait_node(&store, &id, "crit", |s| {
                    matches!(s, NodeState::Ready { runnable_at: Some(_) } | NodeState::Waiting { .. })
                })
                .await;
                std::mem::forget(handle);
                ids.push(id);
            }
            drop(runtime);
            ids
        });
        drop(rt);
        drop(store);
        out
    };
    clock.advance(Duration::from_millis(40));
    let store = SqliteStore::open(&path).unwrap();
    let writes = Arc::new(AtomicU32::new(0));
    let rt = current_rt();
    rt.block_on(async {
        tokio::time::timeout(FARM_BOUND, async {
            let mut order: Vec<usize> = (0..n).collect();
            order.reverse();
            for i in order {
                let w = writes.clone();
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .clock(clock.clone())
                    .policy(RetryPolicy::new(3, Duration::from_millis(40)))
                    .register_fn("src", |_c: ExecutionContext| async {
                        panic!("src succeeded")
                    })
                    .register_fn("sum", |_c: ExecutionContext| async {
                        panic!("sum succeeded")
                    })
                    .register_fn("crit", |ctx: ExecutionContext| async move {
                        NodeOutcome::Waiting {
                            token: ctx.resume_token,
                        }
                    })
                    .register_fn("writer", move |_c: ExecutionContext| {
                        w.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"w")) }
                    })
                    .build();
                let handle = runtime.resume(&ids[i]).await.unwrap();
                let st = handle.wait_stable().await;
                if st == ExecutionState::Waiting {
                    let snap = handle.inspect().await;
                    let token = snap
                        .node(&NodeId::new("crit"))
                        .and_then(|n| match &n.state {
                            NodeState::Waiting { token, .. } => Some(token.clone()),
                            _ => None,
                        })
                        .expect("crit token");
                    handle
                        .resume(
                            token,
                            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"c"))),
                        )
                        .await
                        .unwrap();
                    assert_eq!(handle.wait().await, ExecutionState::Succeeded);
                } else {
                    assert_eq!(st, ExecutionState::Succeeded);
                }
            }
        })
        .await
        .expect("diamond farm shuffled resume");
        assert_eq!(writes.load(Ordering::SeqCst), n as u32);
    });
    rm_db(&path);
}

/// Burst of work, idle at a Waiting gate, crash, resume, second burst.
/// AND-join: wave B must not run before the gate.
#[test]
fn burst_idle_burst_crash_resume_sqlite() {
    let path = tmp();
    let wave = 32usize;
    let (id, token) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let mut b = WorkflowDefinition::builder("burst").node("gate", "gate");
            for i in 0..wave {
                let a = format!("a{i}");
                let c = format!("b{i}");
                b = b
                    .node(a.as_str(), "a")
                    .node(c.as_str(), "b")
                    .edge(a.as_str(), "gate")
                    .edge("gate", c.as_str());
            }
            let runtime = Runtime::builder()
                .store(store.clone())
                .concurrency(8)
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"a"))
                })
                .register_fn("gate", |ctx: ExecutionContext| async move {
                    NodeOutcome::Waiting {
                        token: ctx.resume_token,
                    }
                })
                .register_fn("b", |_c: ExecutionContext| async {
                    panic!("wave B before gate")
                })
                .build();
            let handle = runtime.start(b.build().unwrap()).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "gate", |s| matches!(s, NodeState::Waiting { .. })).await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("gate"))
                .and_then(|n| match &n.state {
                    NodeState::Waiting { token, .. } => Some(token.clone()),
                    _ => None,
                })
                .unwrap();
            std::mem::forget(handle);
            drop(runtime);
            (id, token)
        });
        drop(rt);
        drop(store);
        out
    };
    let b_runs = Arc::new(AtomicU32::new(0));
    let bc = b_runs.clone();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .concurrency(8)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("wave A succeeded")
            })
            .register_fn("gate", |_c: ExecutionContext| async {
                panic!("gate must not re-run")
            })
            .register_fn("b", move |_c: ExecutionContext| {
                bc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"b")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
        assert_eq!(b_runs.load(Ordering::SeqCst), 0);
        handle
            .resume(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
            )
            .await
            .unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(b_runs.load(Ordering::SeqCst), wave as u32);
    });
    rm_db(&path);
}

/// Mixed 64KiB and 1-byte payloads survive persist+crash+resume; AND-join sees both.
#[test]
fn mixed_fat_and_tiny_payloads_crash_resume() {
    let path = tmp();
    let fat = Bytes::from(vec![0xABu8; 64 * 1024]);
    let tiny = Bytes::from_static(b"x");
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("fat", {
                    let fat = fat.clone();
                    move |_c: ExecutionContext| {
                        let fat = fat.clone();
                        async move { NodeOutcome::Succeeded(fat) }
                    }
                })
                .register_fn("tiny", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"x"))
                })
                .register(ScriptedExecutor::new("hang").hang(false))
                .register_fn("join", |_c: ExecutionContext| async {
                    panic!("join waits")
                })
                .build();
            let def = WorkflowDefinition::builder("mix")
                .node("fat", "fat")
                .node("tiny", "tiny")
                .node("hang", "hang")
                .node("join", "join")
                .edge("fat", "join")
                .edge("tiny", "join")
                .edge("hang", "join")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "hang", |s| matches!(s, NodeState::Running { .. })).await;
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
            .store(store.clone())
            .register_fn("fat", |_c: ExecutionContext| async {
                panic!("fat succeeded")
            })
            .register_fn("tiny", |_c: ExecutionContext| async {
                panic!("tiny succeeded")
            })
            .register_fn("hang", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"h"))
            })
            .register_fn("join", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"j"))
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_eq!(
            snap.node(&NodeId::new("fat")).unwrap().output.as_ref().unwrap().as_ref(),
            fat.as_ref()
        );
        assert_eq!(
            snap.node(&NodeId::new("tiny")).unwrap().output.as_ref().unwrap().as_ref(),
            tiny.as_ref()
        );
    });
    rm_db(&path);
}

/// Start-crash-resume storm on one file (short jobs + diamonds). WAL bounded.
#[test]
fn start_crash_resume_storm_one_file() {
    let path = tmp();
    let t0 = Instant::now();
    for i in 0..24 {
        let store = SqliteStore::open(&path).expect("lock");
        let rt = current_rt();
        let id = rt.block_on(async {
            if i % 3 == 0 {
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .register(ScriptedExecutor::new("a").hang(false))
                    .build();
                let def = WorkflowDefinition::builder(format!("s-{i}"))
                    .node("a", "a")
                    .build()
                    .unwrap();
                let handle = runtime.start(def).unwrap();
                let id = handle.execution_id().clone();
                wait_node(&store, &id, "a", |s| matches!(s, NodeState::Running { .. })).await;
                std::mem::forget(handle);
                drop(runtime);
                id
            } else {
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
                        panic!("writer")
                    })
                    .build();
                let handle = runtime.start(diamond()).unwrap();
                let id = handle.execution_id().clone();
                wait_node(&store, &id, "crit", |s| matches!(s, NodeState::Running { .. }))
                    .await;
                std::mem::forget(handle);
                drop(runtime);
                id
            }
        });
        drop(rt);
        drop(store);
        let store = SqliteStore::open(&path).expect("lock");
        let rt = current_rt();
        rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                })
                .register_fn("src", |_c: ExecutionContext| async {
                    panic!("src")
                })
                .register_fn("sum", |_c: ExecutionContext| async {
                    panic!("sum")
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
    let wal = wal_len(&path);
    eprintln!(
        "chaos start-crash-resume storm 24: {:?} wal={wal}",
        t0.elapsed()
    );
    assert!(t0.elapsed() < STORM_BOUND);
    assert!(
        wal < 8 * 1024 * 1024,
        "WAL unbounded after storm terminals: {wal} bytes"
    );
    rm_db(&path);
}

/// Product clock: 1pm Waiting + 4pm delay, AND-join writer. Crash between.
#[test]
fn and_join_1pm_waiting_4pm_delay_crash_resume() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    let one_pm = Duration::from_secs(13 * 3600);
    let four_pm = Duration::from_secs(16 * 3600);
    clock.advance(one_pm);
    let (id, token) = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let out = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .register_fn("hitl", |ctx: ExecutionContext| async move {
                    NodeOutcome::Waiting {
                        token: ctx.resume_token,
                    }
                })
                .register(ScriptedExecutor::new("late").delay_succeed(
                    four_pm - one_pm,
                    Bytes::from_static(b"4pm"),
                ))
                .register_fn("writer", |_c: ExecutionContext| async {
                    panic!("writer AND-joins")
                })
                .build();
            let def = WorkflowDefinition::builder("day")
                .node("hitl", "hitl")
                .node("late", "late")
                .node("writer", "writer")
                .edge("hitl", "writer")
                .edge("late", "writer")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            wait_node(&store, &id, "hitl", |s| matches!(s, NodeState::Waiting { .. })).await;
            wait_node(&store, &id, "late", |s| matches!(s, NodeState::Running { .. })).await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("hitl"))
                .and_then(|n| match &n.state {
                    NodeState::Waiting { token, .. } => Some(token.clone()),
                    _ => None,
                })
                .unwrap();
            std::mem::forget(handle);
            drop(runtime);
            (id, token)
        });
        drop(rt);
        drop(store);
        out
    };
    let writes = Arc::new(AtomicU32::new(0));
    let w = writes.clone();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .clock(clock.clone())
            .register_fn("hitl", |_c: ExecutionContext| async {
                panic!("hitl keeps token")
            })
            .register(ScriptedExecutor::new("late").delay_succeed(
                four_pm - one_pm,
                Bytes::from_static(b"4pm"),
            ))
            .register_fn("writer", move |_c: ExecutionContext| {
                w.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"w")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        handle
            .resume(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"1pm"))),
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(writes.load(Ordering::SeqCst), 0, "AND-join waits for 4pm");
        clock.advance(four_pm - one_pm);
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    });
    rm_db(&path);
}

/// Drop handle while persist of Cancel is in flight (gated yield). File is
/// Cancelled, not a silent Succeeded. Graph cancel, not process crash.
#[test]
fn drop_handle_mid_persist_cancels_not_succeed() {
    let path = tmp();
    let inner = SqliteStore::open(&path).unwrap();
    let gate = PersistGate::new(inner.clone());
    let started = gate.started.clone();
    let proceed = gate.proceed.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(gate)
            .register(ScriptedExecutor::new("a").hang(false))
            .build();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "a")
            .build()
            .unwrap();
        let handle = runtime.start(def).unwrap();
        let id = handle.execution_id().clone();
        wait_node(&inner, &id, "a", |s| matches!(s, NodeState::Running { .. })).await;
        let notified = started.notified();
        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), notified)
            .await
            .expect("cancel persist did not start");
        proceed.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
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
        .expect("drop mid-persist must persist Cancelled");
        drop(runtime);
        let store = SqliteStore::open(&path).unwrap();
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("cancelled must not re-invoke as success")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Cancelled);
    });
    rm_db(&path);
}

/// Executor panic: sqlite snapshot is Failed; resume does not resurrect.
#[test]
fn executor_panic_sqlite_resume_stays_failed() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("a", |_c: ExecutionContext| async { panic!("boom") })
                .build();
            let def = WorkflowDefinition::builder("wf")
                .node("a", "a")
                .build()
                .unwrap();
            let handle = runtime.start(def).unwrap();
            let id = handle.execution_id().clone();
            assert_eq!(handle.wait().await, ExecutionState::Failed);
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
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("failed must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Failed);
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Failed
        );
    });
    rm_db(&path);
}

/// Sink panic during emit after sqlite persist: snapshot still Succeeded.
#[test]
fn sink_panic_during_sqlite_persist_still_durable() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .sink(FaultySink::panic_on_nth(1))
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
        drop(runtime);
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
    });
    rm_db(&path);
}

/// Persist panics after sqlite COMMIT of the terminal. File keeps Succeeded;
/// resume does not re-run. CatchUnwind must not invent Failed/Cancelled.
#[test]
fn persist_panic_after_sqlite_commit_keeps_succeeded() {
    let path = tmp();
    let inner = SqliteStore::open(&path).unwrap();
    let store = PanicAfterPersist {
        inner: inner.clone(),
        hits: AtomicU32::new(0),
    };
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
        drop(runtime);
        id
    });
    drop(rt);
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("succeeded must not re-run after persist panic")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    rm_db(&path);
}

/// Policy panic fail-fasts; sqlite resume stays Failed (do not reverse fail-fast).
#[test]
fn policy_panic_during_sqlite_put_stays_failed() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .policy(PanicPolicy)
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
        assert_eq!(handle.wait().await, ExecutionState::Failed);
        drop(runtime);
        let store = SqliteStore::open(&path).unwrap();
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_c: ExecutionContext| async {
                panic!("fail-fast must not resurrect")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Failed);
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Failed
        );
    });
    rm_db(&path);
}

/// Two Runtimes, one file, same Running diamond: unfenced re-invoke is allowed;
/// the file must not show writer Succeeded with a live predecessor.
#[test]
fn two_runtimes_diamond_no_silent_wrong_terminal() {
    let path = tmp();
    let store_a = SqliteStore::open(&path).unwrap();
    let store_b = SqliteStore::open(&path).unwrap();
    let def = diamond();
    let mut ex = Execution::new(def);
    let p = AcceptPolicy;
    let now = Timestamp(0);
    ex.apply(ApplyCmd::Start, &p, now).unwrap();
    ex.apply(ApplyCmd::StartNode { node_id: "src".into() }, &p, now)
        .unwrap();
    current_rt().block_on(async {
        store_a.persist(&ex).await.unwrap();
        let runtime_a = Runtime::builder()
            .store(store_a.clone())
            .register_fn("src", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"s"))
            })
            .register_fn("sum", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"u"))
            })
            .register_fn("crit", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"c"))
            })
            .register_fn("writer", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"w"))
            })
            .build();
        let runtime_b = Runtime::builder()
            .store(store_b)
            .register_fn("src", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"s"))
            })
            .register_fn("sum", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"u"))
            })
            .register_fn("crit", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"c"))
            })
            .register_fn("writer", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"w"))
            })
            .build();
        let ha = runtime_a.resume(ex.id()).await.unwrap();
        let hb = runtime_b.resume(ex.id()).await.unwrap();
        let _ = ha.wait().await;
        let _ = hb.wait().await;
        let snap = store_a.get(ex.id()).await.unwrap().unwrap();
        let def = store_a
            .workflow_definition(ex.id())
            .await
            .unwrap()
            .unwrap();
        Execution::from_snapshot(def.clone(), snap.clone()).unwrap();
        assert_no_silent_wrong_terminal(&snap, &def);
        assert!(snap.state.is_terminal());
    });
    rm_db(&path);
}
