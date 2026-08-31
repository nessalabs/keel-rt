//! Property crash-inject + constructed sqlite killers. Not coverage.
//!
//! Process death: `mem::forget(handle)`, drop Runtime, drop the nested tokio
//! runtime, reopen the file. Drop without forget is cancel, not crash.
//!
//! `cargo test -p keel-rt-sqlite --test crash_inject -- --test-threads=1 --nocapture`

use async_trait::async_trait;
use bytes::Bytes;
use keel_rt::testing::{FakeClock, ScriptedExecutor};
use keel_rt::{
    AcceptPolicy, ApplyCmd, ApplyError, Clock, Execution, ExecutionContext, ExecutionId,
    ExecutionSnapshot, ExecutionState, Join, NodeId, NodeOutcome, NodeState, OnFailure, Resume,
    ResumeError, RetryPolicy, Runtime, SnapshotError, StateStore, StoreError, Timestamp,
    WorkflowDefinition,
};
use keel_rt_sqlite::SqliteStore;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const INNER: Duration = Duration::from_secs(4);
const SUITE_BUDGET: Duration = Duration::from_secs(60);
const MIN_SEEDS: u32 = 100;
const MAX_SEEDS: u32 = 256;

fn tmp() -> PathBuf {
    let p = std::env::temp_dir().join(format!("keel-rt-crash-inject-{}.db", ExecutionId::new()));
    rm_db(&p);
    p
}

fn wal_path(db: &Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}

fn rm_db(p: &Path) {
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

fn open_store(path: &Path, fast: bool) -> SqliteStore {
    if fast {
        SqliteStore::open_fast(path).unwrap()
    } else {
        SqliteStore::open(path).unwrap()
    }
}

struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[(self.next() as usize) % xs.len()]
    }

    fn bool(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

#[derive(Clone, Copy, Debug)]
enum DagKind {
    Chain,
    Diamond,
    WideJoin,
    Hourglass,
    FailSubtreeAllDone,
    HitlWaiting,
    RetryDelay,
}

#[derive(Clone, Copy, Debug)]
enum CrashKind {
    AfterSnapshot,
    MidRunning,
    AtWaiting,
    RetryReady,
    AfterFailed,
    AfterCancel,
    CleanWait,
}

impl CrashKind {
    fn label(self) -> &'static str {
        match self {
            Self::AfterSnapshot => "after-persist",
            Self::MidRunning => "mid-Running",
            Self::AtWaiting => "Waiting",
            Self::RetryReady => "retry-Ready",
            Self::AfterFailed => "after-Failed",
            Self::AfterCancel => "after-cancel",
            Self::CleanWait => "wait-then-Shutdown",
        }
    }
}

struct FailNthTerminal {
    inner: SqliteStore,
    n: AtomicU32,
    fail_first: u32,
}

#[async_trait]
impl StateStore for FailNthTerminal {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        if exec.state().is_terminal() && self.fail_first > 0 {
            let k = self.n.fetch_add(1, Ordering::SeqCst) + 1;
            if k <= self.fail_first {
                return Err(StoreError::Message("database or disk is full".into()));
            }
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

struct FailReadyDelayOnce {
    inner: SqliteStore,
    fired: AtomicU32,
}

#[async_trait]
impl StateStore for FailReadyDelayOnce {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.put(snapshot).await
    }
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let delayed = exec.snapshot().nodes.values().any(|n| {
            matches!(
                n.state,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            )
        });
        if delayed && self.fired.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StoreError::Message("busy Ready delay".into()));
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

async fn wait_snap(
    store: &SqliteStore,
    id: &ExecutionId,
    pred: impl Fn(&ExecutionSnapshot) -> bool,
) -> ExecutionSnapshot {
    tokio::time::timeout(INNER, async {
        loop {
            if let Some(s) = store.get(id).await.unwrap() {
                if pred(&s) {
                    return s;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot predicate")
}

struct CrashOut {
    id: ExecutionId,
    snap: ExecutionSnapshot,
    now: Timestamp,
    wait_status: Option<ExecutionState>,
}

#[test]
fn randomized_crash_inject_sqlite() {
    let t0 = Instant::now();
    let mut n = 0u32;
    let mut seed = 0xC0FFEE_u64;
    while t0.elapsed() < SUITE_BUDGET && n < MAX_SEEDS {
        seed = seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(n as u64 + 1);
        match std::panic::catch_unwind(|| run_seed(seed)) {
            Ok(Ok(())) => n += 1,
            Ok(Err(e)) => {
                panic!(
                    "crash_inject failed after {n} seeds ({:?}): {e}",
                    t0.elapsed()
                )
            }
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| p.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                panic!(
                    "crash_inject panic after {n} seeds ({:?}): {msg}",
                    t0.elapsed()
                );
            }
        }
    }
    assert!(
        n >= MIN_SEEDS,
        "only {n} seeds in {:?}; need {MIN_SEEDS}",
        t0.elapsed()
    );
    eprintln!("crash_inject seeds={n} elapsed={:?}", t0.elapsed());
}

fn run_seed(seed: u64) -> Result<(), String> {
    let mut rng = XorShift::new(seed);
    let dag = rng.pick(&[
        DagKind::Chain,
        DagKind::Diamond,
        DagKind::WideJoin,
        DagKind::Hourglass,
        DagKind::FailSubtreeAllDone,
        DagKind::HitlWaiting,
        DagKind::RetryDelay,
    ]);
    let crash = match dag {
        DagKind::HitlWaiting => rng.pick(&[
            CrashKind::AtWaiting,
            CrashKind::AfterSnapshot,
            CrashKind::AfterCancel,
            CrashKind::CleanWait,
        ]),
        DagKind::RetryDelay => rng.pick(&[
            CrashKind::RetryReady,
            CrashKind::AfterSnapshot,
            CrashKind::CleanWait,
            CrashKind::MidRunning,
        ]),
        DagKind::FailSubtreeAllDone => rng.pick(&[
            CrashKind::MidRunning,
            CrashKind::AfterFailed,
            CrashKind::AfterSnapshot,
            CrashKind::AfterCancel,
        ]),
        DagKind::Chain => rng.pick(&[
            CrashKind::AfterSnapshot,
            CrashKind::MidRunning,
            CrashKind::AfterCancel,
            CrashKind::CleanWait,
            CrashKind::AfterFailed,
        ]),
        _ => rng.pick(&[
            CrashKind::AfterSnapshot,
            CrashKind::MidRunning,
            CrashKind::AfterCancel,
            CrashKind::CleanWait,
        ]),
    };
    let fast = rng.bool();
    let fail_first = if matches!(crash, CrashKind::CleanWait) {
        rng.pick(&[0u32, 0, 1, 2])
    } else {
        0
    };
    let unicode = matches!(dag, DagKind::Chain) && rng.bool();
    let ctx = format!(
        "seed={seed:#x} dag={dag:?} crash={} open_fast={fast} fail_first={fail_first} unicode={unicode}",
        crash.label()
    );
    run_scenario(dag, crash, fast, fail_first, unicode).map_err(|e| format!("{ctx}: {e}"))
}

fn run_scenario(
    dag: DagKind,
    crash: CrashKind,
    fast: bool,
    fail_first: u32,
    unicode: bool,
) -> Result<(), String> {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(1_000));
    let out = {
        let inner = open_store(&path, fast);
        let store: Arc<dyn StateStore> = if fail_first > 0 {
            Arc::new(FailNthTerminal {
                inner: inner.clone(),
                n: AtomicU32::new(0),
                fail_first,
            })
        } else {
            Arc::new(inner.clone())
        };
        let rt = current_rt();
        let out = rt.block_on(drive_until_crash(
            store,
            inner.clone(),
            clock.clone(),
            dag,
            crash,
            unicode,
        ));
        drop(rt);
        drop(inner);
        out
    };
    let out = match out {
        Ok(o) => o,
        Err(e) => {
            rm_db(&path);
            return Err(e);
        }
    };
    let store = open_store(&path, fast);
    let file_snap = {
        let rt = current_rt();
        let s = rt.block_on(async { store.get(&out.id).await.unwrap() });
        drop(rt);
        s
    };
    if let (Some(status), Some(s)) = (out.wait_status, file_snap.as_ref()) {
        if s.state == ExecutionState::Running
            && matches!(status, ExecutionState::Succeeded | ExecutionState::Cancelled)
        {
            rm_db(&path);
            return Err(format!(
                "wait() returned {status:?} but file is still Running"
            ));
        }
    }
    let crash_snap = file_snap.unwrap_or(out.snap);
    let resume_clock = Arc::new(FakeClock::new());
    resume_clock.set(out.now);
    let join_runs = Arc::new(AtomicU32::new(0));
    let join_name = match dag {
        DagKind::Diamond | DagKind::WideJoin | DagKind::FailSubtreeAllDone => Some("join"),
        DagKind::Hourglass => Some("neck"),
        _ => None,
    };
    let rt = current_rt();
    let result = rt.block_on(recover(
        store,
        resume_clock,
        &out.id,
        &crash_snap,
        dag,
        crash,
        join_name,
        join_runs,
    ));
    drop(rt);
    rm_db(&path);
    result
}

fn chain_ids(unicode: bool) -> (String, String, String) {
    if unicode {
        (
            "α-节点-🔗".into(),
            "β-mid".into(),
            "γ-end".into(),
        )
    } else {
        ("a".into(), "b".into(), "c".into())
    }
}

fn build_def(dag: DagKind, unicode: bool) -> WorkflowDefinition {
    match dag {
        DagKind::Chain => {
            let (a, b, c) = chain_ids(unicode);
            WorkflowDefinition::builder("wf")
                .node(a.clone(), "a")
                .node(b.clone(), "b")
                .node(c.clone(), "c")
                .edge(a, b.clone())
                .edge(b, c)
                .build()
                .unwrap()
        }
        DagKind::Diamond => WorkflowDefinition::builder("wf")
            .node("src", "src")
            .node("l", "l")
            .node("r", "r")
            .node("join", "join")
            .edge("src", "l")
            .edge("src", "r")
            .edge("l", "join")
            .edge("r", "join")
            .build()
            .unwrap(),
        DagKind::WideJoin => {
            let mut bld = WorkflowDefinition::builder("wf").node("join", "join");
            for i in 0..4 {
                let n = format!("p{i}");
                bld = bld.node(n.clone(), n.clone()).edge(n, "join");
            }
            bld.build().unwrap()
        }
        DagKind::Hourglass => WorkflowDefinition::builder("wf")
            .node("s1", "s1")
            .node("s2", "s2")
            .node("neck", "neck")
            .node("t1", "t1")
            .node("t2", "t2")
            .edge("s1", "neck")
            .edge("s2", "neck")
            .edge("neck", "t1")
            .edge("neck", "t2")
            .build()
            .unwrap(),
        DagKind::FailSubtreeAllDone => WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("p1", "p1")
            .node("p2", "p2")
            .node("join", "join")
            .join("join", Join::AllDone)
            .edge("p1", "join")
            .edge("p2", "join")
            .build()
            .unwrap(),
        DagKind::HitlWaiting => WorkflowDefinition::builder("wf")
            .node("w", "w")
            .node("j", "j")
            .edge("w", "j")
            .build()
            .unwrap(),
        DagKind::RetryDelay => WorkflowDefinition::builder("wf")
            .node("r", "r")
            .build()
            .unwrap(),
    }
}

fn hang_executor(dag: DagKind, crash: CrashKind) -> Option<&'static str> {
    if !matches!(
        crash,
        CrashKind::MidRunning | CrashKind::AfterCancel | CrashKind::AfterFailed
    ) {
        return None;
    }
    match (dag, crash) {
        (DagKind::Chain, _) => Some("b"),
        (DagKind::Diamond, _) => Some("l"),
        (DagKind::WideJoin, _) => Some("p0"),
        (DagKind::Hourglass, _) => Some("neck"),
        (DagKind::FailSubtreeAllDone, _) => Some("p2"),
        (DagKind::RetryDelay, CrashKind::MidRunning | CrashKind::AfterCancel) => Some("r"),
        _ => None,
    }
}

fn register_start(
    mut b: keel_rt::RuntimeBuilder,
    dag: DagKind,
    crash: CrashKind,
) -> keel_rt::RuntimeBuilder {
    let hang = hang_executor(dag, crash);
    let execs: &[&str] = match dag {
        DagKind::Chain => &["a", "b", "c"],
        DagKind::Diamond => &["src", "l", "r", "join"],
        DagKind::WideJoin => &["p0", "p1", "p2", "p3", "join"],
        DagKind::Hourglass => &["s1", "s2", "neck", "t1", "t2"],
        DagKind::FailSubtreeAllDone => &["p1", "p2", "join"],
        DagKind::HitlWaiting => &["w", "j"],
        DagKind::RetryDelay => &["r"],
    };
    for id in execs {
        if *id == "w" && matches!(dag, DagKind::HitlWaiting) {
            b = b.register_fn("w", |ctx: ExecutionContext| async move {
                NodeOutcome::Waiting {
                    token: ctx.resume_token,
                }
            });
            continue;
        }
        if *id == "r" && matches!(dag, DagKind::RetryDelay) && hang != Some("r") {
            b = b.register(
                ScriptedExecutor::new("r")
                    .fail("once")
                    .succeed(Bytes::from_static(b"ok")),
            );
            continue;
        }
        if *id == "p1" && matches!(dag, DagKind::FailSubtreeAllDone) {
            b = b.register_fn("p1", |_c: ExecutionContext| async {
                NodeOutcome::Failed(keel_rt::NodeError::new("page"))
            });
            continue;
        }
        if *id == "a" && matches!(dag, DagKind::Chain) && matches!(crash, CrashKind::AfterFailed) {
            b = b.register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Failed(keel_rt::NodeError::new("boom"))
            });
            continue;
        }
        if hang == Some(*id) {
            let exec = (*id).to_string();
            b = b.register_fn(exec, |_c: ExecutionContext| async {
                std::future::pending::<()>().await;
                NodeOutcome::Succeeded(Bytes::from_static(b"hang"))
            });
            continue;
        }
        let exec = (*id).to_string();
        b = b.register_fn(exec, |_c: ExecutionContext| async {
            NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
        });
    }
    b
}

async fn drive_until_crash(
    store: Arc<dyn StateStore>,
    inner: SqliteStore,
    clock: Arc<FakeClock>,
    dag: DagKind,
    crash: CrashKind,
    unicode: bool,
) -> Result<CrashOut, String> {
    let mut b = Runtime::builder()
        .store_arc(store)
        .clock(clock.clone())
        .concurrency(if matches!(dag, DagKind::WideJoin) { 2 } else { 8 });
    if matches!(dag, DagKind::RetryDelay) {
        b = b.policy(RetryPolicy::new(3, Duration::from_millis(40)));
    }
    b = register_start(b, dag, crash);
    let runtime = b.build();
    let handle = runtime.start(build_def(dag, unicode)).map_err(|e| e.to_string())?;
    let id = handle.execution_id().clone();

    let snap = match crash {
        CrashKind::AfterSnapshot => wait_snap(&inner, &id, |s| !s.nodes.is_empty()).await,
        CrashKind::MidRunning => wait_snap(&inner, &id, |s| s.running_count() > 0).await,
        CrashKind::AtWaiting => {
            wait_snap(&inner, &id, |s| {
                s.waiting_count() > 0 || s.state == ExecutionState::Waiting
            })
            .await
        }
        CrashKind::RetryReady => wait_snap(&inner, &id, |s| {
            s.nodes.values().any(|n| {
                matches!(
                    n.state,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
        })
        .await,
        CrashKind::AfterFailed => wait_snap(&inner, &id, |s| {
            s.nodes
                .values()
                .any(|n| matches!(n.state, NodeState::Failed))
                || s.state == ExecutionState::Failed
        })
        .await,
        CrashKind::AfterCancel => {
            wait_snap(&inner, &id, |s| !s.nodes.is_empty()).await;
            handle.cancel().await;
            wait_snap(&inner, &id, |s| s.state == ExecutionState::Cancelled).await
        }
        CrashKind::CleanWait => {
            if matches!(dag, DagKind::HitlWaiting) {
                let s = wait_snap(&inner, &id, |s| s.state == ExecutionState::Waiting).await;
                let token = s
                    .nodes
                    .values()
                    .find_map(|n| match &n.state {
                        NodeState::Waiting { token, .. } => Some(token.clone()),
                        _ => n.resume_token.clone(),
                    })
                    .ok_or_else(|| "waiting token".to_string())?;
                handle
                    .resume(
                        token,
                        Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
            }
            if matches!(dag, DagKind::RetryDelay) {
                wait_snap(&inner, &id, |s| {
                    s.nodes.values().any(|n| {
                        matches!(
                            n.state,
                            NodeState::Ready {
                                runnable_at: Some(_)
                            }
                        )
                    })
                })
                .await;
                clock.advance(Duration::from_millis(80));
            }
            let status = tokio::time::timeout(INNER, handle.wait())
                .await
                .map_err(|_| "wait timeout".to_string())?;
            drop(runtime);
            tokio::time::timeout(INNER, async {
                loop {
                    if let Some(s) = inner.get(&id).await.unwrap() {
                        if s.state != ExecutionState::Running {
                            return s;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| "Shutdown did not flush after wait()".to_string())?;
            let snap = inner.get(&id).await.unwrap().unwrap();
            return Ok(CrashOut {
                id,
                snap,
                now: clock.now(),
                wait_status: Some(status),
            });
        }
    };
    std::mem::forget(handle);
    drop(runtime);
    Ok(CrashOut {
        id,
        snap,
        now: clock.now(),
        wait_status: None,
    })
}

async fn recover(
    store: SqliteStore,
    clock: Arc<FakeClock>,
    id: &ExecutionId,
    crash_snap: &ExecutionSnapshot,
    dag: DagKind,
    crash: CrashKind,
    join_name: Option<&str>,
    join_runs: Arc<AtomicU32>,
) -> Result<(), String> {
    let mut b = Runtime::builder()
        .store(store.clone())
        .clock(clock.clone())
        .concurrency(if matches!(dag, DagKind::WideJoin) { 2 } else { 8 });
    if matches!(dag, DagKind::RetryDelay) {
        b = b.policy(RetryPolicy::new(3, Duration::from_millis(40)));
    }
    let mut registered: HashSet<String> = HashSet::new();
    for (nid, n) in crash_snap.iter_nodes() {
        let exec_id = executor_id_for(dag, nid.as_str());
        if !registered.insert(exec_id.clone()) {
            continue;
        }
        match &n.state {
            NodeState::Succeeded
            | NodeState::Failed
            | NodeState::TimedOut
            | NodeState::Cancelled
            | NodeState::Waiting { .. } => {
                let label = nid.as_str().to_string();
                b = b.register_fn(exec_id, move |_c: ExecutionContext| {
                    let label = label.clone();
                    async move { panic!("{label} must not re-run") }
                });
            }
            _ => {
                if join_name == Some(nid.as_str()) {
                    let c = join_runs.clone();
                    b = b.register_fn(exec_id, move |_c: ExecutionContext| {
                        c.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Bytes::from_static(b"j")) }
                    });
                } else {
                    b = b.register_fn(exec_id, |_c: ExecutionContext| async {
                        NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
                    });
                }
            }
        }
    }
    for extra in executor_ids(dag) {
        if registered.insert(extra.to_string()) {
            b = b.register_fn(extra.to_string(), |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            });
        }
    }
    let runtime = b.build();
    let handle = match runtime.resume(id).await {
        Ok(h) => h,
        Err(ResumeError::UnknownExecution) => {
            return Err("resume UnknownExecution after a legal persist crash".into());
        }
        Err(e) => return Err(e.to_string()),
    };

    let tokens: Vec<_> = crash_snap
        .nodes
        .iter()
        .filter_map(|(_, n)| match &n.state {
            NodeState::Waiting { token, .. } => Some(token.clone()),
            _ => None,
        })
        .collect();
    if !tokens.is_empty() && crash_snap.state != ExecutionState::Cancelled {
        let stable = tokio::time::timeout(INNER, handle.wait_stable())
            .await
            .map_err(|_| "wait_stable timeout".to_string())?;
        if stable == ExecutionState::Waiting {
            for t in &tokens {
                let live = handle.inspect().await;
                let still = live.node(t.node_id()).and_then(|n| match &n.state {
                    NodeState::Waiting { token, .. } => Some(token.clone()),
                    _ => n.resume_token.clone(),
                });
                if let Some(tok) = still {
                    if tok != *t {
                        return Err("Waiting token changed across resume".into());
                    }
                    handle
                        .resume(
                            tok,
                            Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"go"))),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
    }

    if matches!(crash, CrashKind::RetryReady)
        || crash_snap.nodes.values().any(|n| {
            matches!(
                n.state,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            )
        })
    {
        let due = crash_snap.nodes.values().find_map(|n| match n.state {
            NodeState::Ready {
                runnable_at: Some(at),
            } => Some(at),
            _ => None,
        });
        if let Some(at) = due {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            if clock.now() < at {
                clock.set(at);
                clock.advance(Duration::from_millis(1));
            }
        }
    }

    let status = tokio::time::timeout(INNER, handle.wait_stable())
        .await
        .map_err(|_| "resume hung (not terminal/Waiting)".to_string())?;
    if !(status.is_terminal() || status == ExecutionState::Waiting) {
        return Err(format!("resume ended in {status:?}"));
    }
    let live = handle.inspect().await;
    if status.is_terminal() && live.running_count() != 0 {
        return Err(format!(
            "permit leak: terminal {status:?} running_count={}",
            live.running_count()
        ));
    }
    if live.waiting_count()
        != live
            .nodes
            .values()
            .filter(|n| matches!(n.state, NodeState::Waiting { .. }))
            .count()
    {
        return Err("waiting_count mismatch".into());
    }
    if let Some(j) = join_name {
        let n = join_runs.load(Ordering::SeqCst);
        if n > 1 {
            return Err(format!("AND-join {j} ran {n} times"));
        }
    }
    if matches!(dag, DagKind::Chain)
        && crash_snap.state == ExecutionState::Failed
        && status != ExecutionState::Failed
    {
        return Err(format!("fail-fast was Failed, resume got {status:?}"));
    }
    Ok(())
}

fn executor_id_for(dag: DagKind, node: &str) -> String {
    match dag {
        DagKind::Chain => {
            if node == "a" || node.starts_with('α') {
                "a".into()
            } else if node == "b" || node.starts_with('β') {
                "b".into()
            } else {
                "c".into()
            }
        }
        _ => node.to_string(),
    }
}

fn executor_ids(dag: DagKind) -> &'static [&'static str] {
    match dag {
        DagKind::Chain => &["a", "b", "c"],
        DagKind::Diamond => &["src", "l", "r", "join"],
        DagKind::WideJoin => &["p0", "p1", "p2", "p3", "join"],
        DagKind::Hourglass => &["s1", "s2", "neck", "t1", "t2"],
        DagKind::FailSubtreeAllDone => &["p1", "p2", "join"],
        DagKind::HitlWaiting => &["w", "j"],
        DagKind::RetryDelay => &["r"],
    }
}

#[test]
fn clock_jump_backward_after_runnable_at_persist_does_not_fire() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(10_000));
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .clock(clock.clone())
                .policy(RetryPolicy::new(3, Duration::from_millis(80)))
                .register(
                    ScriptedExecutor::new("a")
                        .fail("once")
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
            wait_snap(&store, &id, |s| {
                s.node(&NodeId::new("a")).is_some_and(|n| {
                    matches!(
                        n.state,
                        NodeState::Ready {
                            runnable_at: Some(_)
                        }
                    )
                })
            })
            .await;
            clock.set(Timestamp(50));
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
    let resume_clock = Arc::new(FakeClock::new());
    resume_clock.set(Timestamp(50));
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .clock(resume_clock.clone())
            .policy(RetryPolicy::new(3, Duration::from_millis(80)))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "backward clock jump must not treat saturating_sub(0) as due"
        );
        resume_clock.set(Timestamp(10_080));
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    });
    rm_db(&path);
}

#[test]
fn clock_jump_forward_over_staggered_retries_sqlite() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(1_000));
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .clock(clock.clone())
            .concurrency(2)
            .policy(RetryPolicy::new(3, Duration::from_millis(100)))
            .register(
                ScriptedExecutor::new("fast")
                    .fail("f")
                    .succeed(Bytes::from_static(b"fok")),
            )
            .register(
                ScriptedExecutor::new("slow")
                    .then(keel_rt::testing::ScriptedAction::Delay {
                        delay: Duration::from_millis(50),
                        then: Box::new(keel_rt::testing::ScriptedAction::Fail("s".into())),
                    })
                    .succeed(Bytes::from_static(b"sok")),
            )
            .build();
        let handle = runtime
            .start(
                WorkflowDefinition::builder("wf")
                    .node("fast", "fast")
                    .node("slow", "slow")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let id = handle.execution_id().clone();
        wait_snap(&store, &id, |s| {
            s.node(&NodeId::new("fast")).is_some_and(|n| {
                matches!(
                    n.state,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
        })
        .await;
        clock.advance(Duration::from_millis(50));
        wait_snap(&store, &id, |s| {
            s.node(&NodeId::new("slow")).is_some_and(|n| {
                matches!(
                    n.state,
                    NodeState::Ready {
                        runnable_at: Some(_)
                    }
                )
            })
        })
        .await;
        clock.advance(Duration::from_secs(10));
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    drop(rt);
    rm_db(&path);
}

#[test]
fn stale_finish_node_after_resume_attempt_bump_is_noop() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
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
        wait_snap(&store, &id, |s| {
            matches!(
                s.node(&NodeId::new("a")).map(|n| &n.state),
                Some(NodeState::Running { attempt: 1 })
            )
        })
        .await;
        std::mem::forget(handle);
        drop(runtime);
        let def = store.workflow_definition(&id).await.unwrap().unwrap();
        let snap = store.get(&id).await.unwrap().unwrap();
        let mut exec = Execution::from_snapshot(def, snap).unwrap();
        exec.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        assert!(matches!(
            exec.snapshot().node(&NodeId::new("a")).unwrap().state,
            NodeState::Running { attempt: 2 }
        ));
        let rev = exec.revision();
        exec.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Failed(keel_rt::NodeError::new("stale"))),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        assert_eq!(exec.revision(), rev, "stale FinishNode must no-op");
        assert!(matches!(
            exec.snapshot().node(&NodeId::new("a")).unwrap().state,
            NodeState::Running { attempt: 2 }
        ));
        assert_ne!(exec.state(), ExecutionState::Failed);
    });
    drop(rt);
    rm_db(&path);
}

#[test]
fn duplicate_hitl_complete_after_sqlite_resume() {
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
            wait_snap(&store, &id, |s| s.state == ExecutionState::Waiting).await;
            let token = store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&NodeId::new("a"))
                .unwrap()
                .resume_token
                .clone()
                .unwrap();
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
                panic!("waiting must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
        handle
            .resume(
                token.clone(),
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"one"))),
            )
            .await
            .unwrap();
        assert_eq!(handle.wait_stable().await, ExecutionState::Succeeded);
        match handle
            .resume(
                token,
                Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"two"))),
            )
            .await
        {
            Err(ApplyError::ConflictingComplete) => {}
            other => panic!("expected ConflictingComplete, got {other:?}"),
        }
    });
    rm_db(&path);
}

#[test]
fn cancel_persisted_survives_crash_and_resume() {
    let path = tmp();
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
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
            wait_snap(&store, &id, |s| s.running_count() > 0).await;
            handle.cancel().await;
            wait_snap(&store, &id, |s| s.state == ExecutionState::Cancelled).await;
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
                panic!("persisted Cancelled must not re-invoke")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Cancelled);
    });
    rm_db(&path);
}

#[test]
fn two_resume_plus_start_same_runtime_already_active() {
    let path = tmp();
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |ctx: ExecutionContext| async move {
                NodeOutcome::Waiting {
                    token: ctx.resume_token,
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
        let id = handle.execution_id().clone();
        match runtime.resume(&id).await {
            Err(ResumeError::AlreadyActive) => {}
            Ok(_) => panic!("resume of live start expected AlreadyActive"),
            Err(e) => panic!("resume of live start: {e}"),
        }
        match runtime.resume(&id).await {
            Err(ResumeError::AlreadyActive) => {}
            Ok(_) => panic!("second resume expected AlreadyActive"),
            Err(e) => panic!("second resume: {e}"),
        }
        let other = runtime
            .start(
                WorkflowDefinition::builder("wf")
                    .node("a", "a")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        assert_ne!(other.execution_id(), &id, "start always mints a new id");
        handle.cancel().await;
        handle.wait().await;
        other.cancel().await;
        other.wait().await;
    });
    drop(rt);
    rm_db(&path);
}

#[test]
fn non_terminal_ready_delay_persist_err_then_crash_skips_uncommitted_delay() {
    let path = tmp();
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(1_000));
    let id = {
        let inner = SqliteStore::open(&path).unwrap();
        let store = FailReadyDelayOnce {
            inner: inner.clone(),
            fired: AtomicU32::new(0),
        };
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store)
                .clock(clock.clone())
                .policy(RetryPolicy::new(3, Duration::from_millis(5_000)))
                .register_fn("a", |_c: ExecutionContext| async {
                    NodeOutcome::Failed(keel_rt::NodeError::new("once"))
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
            wait_snap(&inner, &id, |s| {
                matches!(
                    s.node(&NodeId::new("a")).map(|n| &n.state),
                    Some(NodeState::Running { .. })
                )
            })
            .await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            std::mem::forget(handle);
            drop(runtime);
            id
        });
        drop(rt);
        drop(inner);
        id
    };
    let store = SqliteStore::open(&path).unwrap();
    let fired = Arc::new(AtomicU32::new(0));
    let f = fired.clone();
    let resume_clock = Arc::new(FakeClock::new());
    resume_clock.set(Timestamp(1_000));
    let rt = current_rt();
    rt.block_on(async {
        let snap = store.get(&id).await.unwrap().unwrap();
        let delayed = snap.node(&NodeId::new("a")).is_some_and(|n| {
            matches!(
                n.state,
                NodeState::Ready {
                    runnable_at: Some(_)
                }
            )
        });
        assert!(
            !delayed,
            "uncommitted Ready delay must not be on disk after persist Err + crash"
        );
        let runtime = Runtime::builder()
            .store(store)
            .clock(resume_clock)
            .policy(RetryPolicy::new(3, Duration::from_millis(5_000)))
            .register_fn("a", move |_c: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert!(
            fired.load(Ordering::SeqCst) >= 1,
            "resume from last Running snapshot re-invokes immediately (delay was not durable)"
        );
    });
    rm_db(&path);
}

#[test]
fn wal_truncated_after_succeeded_commit_still_succeeded_full() {
    let path = tmp();
    let id = {
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
            wait_snap(&store, &id, |s| s.state == ExecutionState::Succeeded).await;
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let wal = wal_path(&path);
    if wal.exists() {
        std::fs::write(&wal, b"").unwrap();
    }
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
                panic!("Succeeded after WAL truncate must not re-run")
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    rm_db(&path);
}

#[test]
fn definition_mismatch_on_resume_fail_closed() {
    let path = tmp();
    let id = {
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
            id
        });
        drop(rt);
        drop(store);
        id
    };
    let other = WorkflowDefinition::builder("wf")
        .node("b", "b")
        .build()
        .unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE definitions SET body = ?1",
            [other.durable_bytes()],
        )
        .unwrap();
    }
    let store = SqliteStore::open(&path).unwrap();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("a", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .register_fn("b", |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        match runtime.resume(&id).await {
            Err(ResumeError::Snapshot(SnapshotError::DefinitionHashMismatch))
            | Err(ResumeError::Snapshot(SnapshotError::MissingNode(_)))
            | Err(ResumeError::Snapshot(SnapshotError::UnknownNode(_)))
            | Err(ResumeError::Store(_)) => {}
            Ok(_) => panic!("definition mismatch must fail closed, got Ok"),
            Err(e) => panic!("definition mismatch must fail closed, got {e}"),
        }
    });
    rm_db(&path);
}

#[test]
fn unicode_and_long_node_ids_survive_crash_resume() {
    let path = tmp();
    let long = "n".repeat(200);
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("ea", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"A"))
                })
                .register(ScriptedExecutor::new("eb").hang(false))
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("α-节点-🔗", "ea")
                        .node(long.clone(), "eb")
                        .edge("α-节点-🔗", long.clone())
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_snap(&store, &id, |s| {
                s.node(&NodeId::new(&long))
                    .is_some_and(|n| matches!(n.state, NodeState::Running { .. }))
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
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("ea", |_c: ExecutionContext| async {
                panic!("unicode succeeded must not re-run")
            })
            .register_fn("eb", |ctx: ExecutionContext| async move {
                assert_eq!(ctx.attempt, 2);
                NodeOutcome::Succeeded(Bytes::from_static(b"B"))
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    rm_db(&path);
}

#[test]
fn empty_and_64kib_join_inputs_after_resume() {
    let path = tmp();
    let fat = Bytes::from(vec![7u8; 64 * 1024]);
    let id = {
        let store = SqliteStore::open(&path).unwrap();
        let rt = current_rt();
        let fat_c = fat.clone();
        let id = rt.block_on(async {
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("empty", |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::new())
                })
                .register_fn("fat", move |_c: ExecutionContext| {
                    let fat = fat_c.clone();
                    async move { NodeOutcome::Succeeded(fat) }
                })
                .register(ScriptedExecutor::new("join").hang(false))
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .node("empty", "empty")
                        .node("fat", "fat")
                        .node("join", "join")
                        .edge("empty", "join")
                        .edge("fat", "join")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_snap(&store, &id, |s| {
                s.node(&NodeId::new("join"))
                    .is_some_and(|n| matches!(n.state, NodeState::Running { .. }))
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
    let fat_c = fat.clone();
    let rt = current_rt();
    rt.block_on(async {
        let runtime = Runtime::builder()
            .store(store)
            .register_fn("empty", |_c: ExecutionContext| async {
                panic!("empty succeeded must not re-run")
            })
            .register_fn("fat", |_c: ExecutionContext| async {
                panic!("fat succeeded must not re-run")
            })
            .register_fn("join", move |ctx: ExecutionContext| {
                let fat = fat_c.clone();
                async move {
                    let empty = ctx.inputs.get(&NodeId::new("empty")).cloned().unwrap();
                    let got = ctx.inputs.get(&NodeId::new("fat")).cloned().unwrap();
                    assert_eq!(empty.len(), 0);
                    assert_eq!(got, fat);
                    NodeOutcome::Succeeded(Bytes::from_static(b"j"))
                }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
    });
    rm_db(&path);
}

#[test]
fn fifo_64_ready_concurrency_1_crash_resume_all_run_join_once() {
    let path = tmp();
    let n = 64usize;
    let id = {
        let store = SqliteStore::open_fast(&path).unwrap();
        let rt = current_rt();
        let id = rt.block_on(async {
            let mut b = Runtime::builder()
                .store(store.clone())
                .concurrency(1)
                .register(ScriptedExecutor::new("p0").hang(false));
            for i in 1..n {
                let name = format!("p{i}");
                b = b.register_fn(name, |_c: ExecutionContext| async {
                    NodeOutcome::Succeeded(Bytes::from_static(b"p"))
                });
            }
            b = b.register_fn("join", |_c: ExecutionContext| async {
                panic!("join must wait for AND")
            });
            let mut def = WorkflowDefinition::builder("wf").node("join", "join");
            for i in 0..n {
                let name = format!("p{i}");
                def = def.node(name.clone(), name.clone()).edge(name, "join");
            }
            let runtime = b.build();
            let handle = runtime.start(def.build().unwrap()).unwrap();
            let id = handle.execution_id().clone();
            wait_snap(&store, &id, |s| {
                s.node(&NodeId::new("p0"))
                    .is_some_and(|n| matches!(n.state, NodeState::Running { .. }))
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
    let store = SqliteStore::open_fast(&path).unwrap();
    let join_runs = Arc::new(AtomicU32::new(0));
    let jc = join_runs.clone();
    let p0_runs = Arc::new(AtomicU32::new(0));
    let p0c = p0_runs.clone();
    let rt = current_rt();
    rt.block_on(async {
        let mut b = Runtime::builder().store(store).concurrency(1);
        b = b.register_fn("p0", move |ctx: ExecutionContext| {
            p0c.fetch_add(1, Ordering::SeqCst);
            let attempt = ctx.attempt;
            async move {
                assert_eq!(attempt, 2);
                NodeOutcome::Succeeded(Bytes::from_static(b"p"))
            }
        });
        for i in 1..n {
            let name = format!("p{i}");
            b = b.register_fn(name, |_c: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"p"))
            });
        }
        b = b.register_fn("join", move |_c: ExecutionContext| {
            jc.fetch_add(1, Ordering::SeqCst);
            async { NodeOutcome::Succeeded(Bytes::from_static(b"j")) }
        });
        let runtime = b.build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(p0_runs.load(Ordering::SeqCst), 1);
        assert_eq!(join_runs.load(Ordering::SeqCst), 1, "AND-join once");
    });
    rm_db(&path);
}

#[test]
fn open_and_open_fast_process_kill_after_running_commit_both_reinvoke() {
    for fast in [false, true] {
        let path = tmp();
        let id = {
            let store = open_store(&path, fast);
            let rt = current_rt();
            let id = rt.block_on(async {
                let runtime = Runtime::builder()
                    .store(store.clone())
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
                wait_snap(&store, &id, |s| s.running_count() > 0).await;
                std::mem::forget(handle);
                drop(runtime);
                id
            });
            drop(rt);
            drop(store);
            id
        };
        let store = open_store(&path, fast);
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
        rm_db(&path);
    }
}

#[test]
fn fail_subtree_sibling_running_crash_keeps_failed_page() {
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
                .register_fn("join", |_c: ExecutionContext| async {
                    panic!("reducer waits for p2")
                })
                .build();
            let handle = runtime
                .start(
                    WorkflowDefinition::builder("wf")
                        .on_failure(OnFailure::FailSubtree)
                        .node("p1", "p1")
                        .node("p2", "p2")
                        .node("join", "join")
                        .join("join", Join::AllDone)
                        .edge("p1", "join")
                        .edge("p2", "join")
                        .build()
                        .unwrap(),
                )
                .unwrap();
            let id = handle.execution_id().clone();
            wait_snap(&store, &id, |s| {
                s.node(&NodeId::new("p1"))
                    .is_some_and(|n| matches!(n.state, NodeState::Failed))
                    && s.node(&NodeId::new("p2"))
                        .is_some_and(|n| matches!(n.state, NodeState::Running { .. }))
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
    let join = Arc::new(AtomicU32::new(0));
    let jc = join.clone();
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
            .register_fn("join", move |_c: ExecutionContext| {
                jc.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"j")) }
            })
            .build();
        let handle = runtime.resume(&id).await.unwrap();
        assert_eq!(handle.wait().await, ExecutionState::Completed);
        assert_eq!(p1.load(Ordering::SeqCst), 0);
        assert_eq!(join.load(Ordering::SeqCst), 1);
    });
    rm_db(&path);
}
