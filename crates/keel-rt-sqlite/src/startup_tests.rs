//! Real process death at startup boundaries. SQL barriers are test-only.
use super::*;
use keel_rt::{ExecutionState, FakeClock, NodeOutcome, Runtime};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

const CHILD_MODE: &str = "KEEL_SQLITE_STARTUP_CHILD";
const CHILD_PATH: &str = "KEEL_SQLITE_STARTUP_PATH";

#[test]
fn concurrent_initialization_cannot_turn_a_placeholder_read_into_corruption() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    for definition_read in [false, true] {
        let path =
            std::env::temp_dir().join(format!("keel-startup-reader-{}.db", ExecutionId::new()));
        let reader = SqliteStore::open(&path).unwrap();
        let writer = SqliteStore::open(&path).unwrap();
        let mut exec = Execution::new(definition());
        let id = exec.id().clone();
        current_runtime().block_on(async {
            let epoch = writer
                .claim(&id, &OwnerId::new(), Timestamp(0))
                .await
                .unwrap();
            exec.set_fence_epoch(epoch.0);
        });
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut fired = false;
        reader
            .lock()
            .unwrap()
            .authorizer(Some(move |ctx: AuthContext<'_>| {
                if matches!(
                    ctx.action,
                    AuthAction::Read {
                        table_name: "nodes",
                        ..
                    }
                ) && !fired
                {
                    fired = true;
                    // Snapshot metadata has been read; pause before the child-row query.
                    ready_tx.send(()).unwrap();
                    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Authorization::Allow
            }));
        let write = std::thread::spawn(move || {
            ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            current_runtime().block_on(writer.persist(&exec)).unwrap();
            done_tx.send(()).unwrap();
        });
        current_runtime().block_on(async {
            if definition_read {
                assert!(reader.workflow_definition(&id).await.unwrap().is_none());
            } else {
                assert!(reader.get(&id).await.unwrap().is_none());
            }
        });
        write.join().unwrap();
        reader
            .lock()
            .unwrap()
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        current_runtime().block_on(async {
            assert_eq!(reader.get(&id).await.unwrap().unwrap().nodes.len(), 2);
            assert!(reader.workflow_definition(&id).await.unwrap().is_some());
        });
        drop(reader);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn failed_commit_rolls_back_claim_first_snapshot_and_durable_initialization() {
    for boundary in ["claim", "persist", "initialize"] {
        let path =
            std::env::temp_dir().join(format!("keel-startup-commit-{}.db", ExecutionId::new()));
        let store = SqliteStore::open(&path).unwrap();
        current_runtime().block_on(async {
            let mut exec = Execution::new(definition());
            let owner = OwnerId::new();
            if boundary == "persist" {
                let epoch = store.claim(exec.id(), &owner, Timestamp(0)).await.unwrap();
                exec.set_fence_epoch(epoch.0);
            }
            let target = if boundary == "claim" { "executions" } else { "nodes" };
            store.lock().unwrap().execute_batch(&format!(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE deferred_child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
                 CREATE TRIGGER deferred_failure AFTER INSERT ON {target} BEGIN INSERT INTO deferred_child VALUES (99); END;"
            )).unwrap();
            let failed = match boundary {
                "claim" => store.claim(exec.id(), &owner, Timestamp(0)).await.is_err(),
                "persist" => store.persist(&exec).await.is_err(),
                _ => store.initialize(&exec, &owner, Timestamp(0)).await.is_err(),
            };
            assert!(failed, "{boundary}: COMMIT must fail its deferred constraint");
            assert!(store.get(exec.id()).await.unwrap().is_none());
            {
                let conn = store.lock().unwrap();
                assert!(conn.is_autocommit(), "{boundary}: failed COMMIT left an open transaction");
                for table in ["executions", "definitions", "nodes", "deferred_child"] {
                    let count: usize = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0)).unwrap();
                    let expected = usize::from(boundary == "persist" && table == "executions");
                    assert_eq!(count, expected, "{boundary}: {table} rolled back after COMMIT failure");
                }
                conn.execute_batch("DROP TRIGGER deferred_failure").unwrap();
            }
            match boundary {
                "claim" => {
                    let epoch = store.claim(exec.id(), &owner, Timestamp(0)).await.unwrap();
                    exec.set_fence_epoch(epoch.0);
                    store.persist(&exec).await.unwrap();
                }
                "persist" => store.persist(&exec).await.unwrap(),
                _ => { store.initialize(&exec, &owner, Timestamp(0)).await.unwrap(); }
            }
            assert_eq!(store.get(exec.id()).await.unwrap().unwrap(), exec.snapshot());
        });
        drop(store);
        std::fs::remove_file(path).unwrap();
    }
}

fn definition() -> WorkflowDefinition {
    WorkflowDefinition::builder("startup-kill")
        .node("a", "e")
        .node("b", "e")
        .edge("a", "b")
        .build()
        .unwrap()
}

fn current_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}

struct KillChild {
    child: Child,
    path: PathBuf,
}

impl KillChild {
    fn start(mode: &str, fast: bool) -> Self {
        let path =
            std::env::temp_dir().join(format!("keel-startup-kill-{}.db", ExecutionId::new()));
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "startup_tests::crash_child", "--nocapture"])
            .env(CHILD_MODE, mode)
            .env(CHILD_PATH, &path)
            .env("KEEL_SQLITE_STARTUP_FAST", if fast { "1" } else { "0" })
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        Self { child, path }
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !sidecar(&self.path, ".ready").exists() {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    sidecar(&self.path, ".ready").exists(),
                    "child exited before boundary: {status}"
                );
            }
            assert!(
                Instant::now() < deadline,
                "child did not reach startup boundary"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }

    fn id(&self) -> ExecutionId {
        ExecutionId::parse(std::fs::read_to_string(sidecar(&self.path, ".id")).unwrap()).unwrap()
    }
}

impl Drop for KillChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for suffix in ["", "-wal", "-shm", ".ready", ".id", ".snapshot"] {
            let _ = std::fs::remove_file(sidecar(&self.path, suffix));
        }
    }
}

// Invoked only by a subprocess. The ordinary test invocation is a no-op.
#[test]
fn crash_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var(CHILD_PATH).unwrap());
    let store = if std::env::var("KEEL_SQLITE_STARTUP_FAST").unwrap() == "1" {
        SqliteStore::open_fast(&path).unwrap()
    } else {
        SqliteStore::open(&path).unwrap()
    };
    current_runtime().block_on(async {
        if mode == "ack" {
            let runtime = Runtime::builder().store(store.clone()).register_fn("e", |_| async {
                panic!("executor ran before initial acknowledgement crash")
            }).build();
            let handle = runtime.start_durable(definition()).await.unwrap();
            std::fs::write(sidecar(&path, ".id"), handle.execution_id().as_str()).unwrap();
            std::fs::write(sidecar(&path, ".ready"), b"acknowledged").unwrap();
            // No destructor runs and the current-thread scheduler never polls Start.
            std::process::exit(0);
        }
        let mut exec = Execution::new(definition());
        std::fs::write(sidecar(&path, ".id"), exec.id().as_str()).unwrap();
        std::fs::write(sidecar(&path, ".snapshot"), serde_json::to_vec(&exec.snapshot()).unwrap()).unwrap();
        if mode == "claim" || mode == "persist-txn" {
            let epoch = store.claim(exec.id(), &OwnerId::new(), Timestamp(0)).await.unwrap();
            exec.set_fence_epoch(epoch.0);
        }
        if mode == "claim" {
            std::fs::write(sidecar(&path, ".ready"), b"claimed").unwrap();
            std::process::exit(0);
        }
        if mode == "initialized" {
            store.initialize(&exec, &OwnerId::new(), Timestamp(0)).await.unwrap();
            std::fs::write(sidecar(&path, ".ready"), b"committed without acknowledgement").unwrap();
            std::process::exit(0);
        }
        let ready = sidecar(&path, ".ready");
        {
            let conn = store.lock().unwrap();
            conn.create_scalar_function("startup_barrier", 0, rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                move |_| -> rusqlite::Result<i64> {
                    std::fs::write(&ready, b"transaction open").unwrap();
                    loop { std::thread::park(); }
                }).unwrap();
            conn.execute_batch("CREATE TRIGGER startup_barrier AFTER INSERT ON nodes BEGIN SELECT startup_barrier(); END;").unwrap();
        }
        if mode == "persist-txn" { store.persist(&exec).await.unwrap(); }
        else { assert_eq!(mode, "initialize-txn"); store.initialize(&exec, &OwnerId::new(), Timestamp(0)).await.unwrap(); }
        panic!("transaction barrier unexpectedly returned");
    });
}

#[test]
fn process_death_after_claim_keeps_absence_and_live_lease() {
    let mut child = KillChild::start("claim", false);
    child.wait_ready();
    assert!(child.child.wait().unwrap().success());
    let store = SqliteStore::open(&child.path).unwrap();
    current_runtime().block_on(async {
        let id = child.id();
        assert!(store.get(&id).await.unwrap().is_none());
        assert!(store.workflow_definition(&id).await.unwrap().is_none());
        assert_eq!(
            store.claim(&id, &OwnerId::new(), Timestamp(1)).await,
            Err(ClaimError::ClaimedElsewhere)
        );
        assert_eq!(
            store
                .claim(&id, &OwnerId::new(), Timestamp(30_001))
                .await
                .unwrap(),
            LeaseEpoch(2)
        );
    });
}

#[test]
fn process_kill_inside_first_snapshot_transaction_rolls_back_every_row() {
    for mode in ["persist-txn", "initialize-txn"] {
        let mut child = KillChild::start(mode, false);
        child.wait_ready();
        let store = SqliteStore::open(&child.path).unwrap();
        current_runtime().block_on(async {
            assert!(
                store.get(&child.id()).await.unwrap().is_none(),
                "uncommitted rows cannot be read"
            );
        });
        drop(store);
        child.kill();
        let store = SqliteStore::open(&child.path).unwrap();
        current_runtime().block_on(async {
            assert!(store.get(&child.id()).await.unwrap().is_none());
            assert!(store
                .workflow_definition(&child.id())
                .await
                .unwrap()
                .is_none());
            {
                let conn = store.lock().unwrap();
                for table in ["nodes", "definitions", "events"] {
                    let count: usize = conn
                        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                        .unwrap();
                    assert_eq!(count, 0, "{mode}: {table} rolled back");
                }
                conn.execute_batch("DROP TRIGGER startup_barrier").unwrap();
            }
            let snap =
                serde_json::from_slice(&std::fs::read(sidecar(&child.path, ".snapshot")).unwrap())
                    .unwrap();
            let mut exec = Execution::from_snapshot(definition(), snap).unwrap();
            let epoch = if mode == "persist-txn" {
                let epoch = store
                    .claim(exec.id(), &OwnerId::new(), Timestamp(30_001))
                    .await
                    .unwrap();
                exec.set_fence_epoch(epoch.0);
                store.persist(&exec).await.unwrap();
                epoch
            } else {
                store
                    .initialize(&exec, &OwnerId::new(), Timestamp(30_001))
                    .await
                    .unwrap()
            };
            store.release(exec.id(), epoch).await.unwrap();
            let runtime = Runtime::builder()
                .store(store.clone())
                .register_fn("e", |_| async {
                    NodeOutcome::Succeeded(Default::default())
                })
                .build();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), async {
                    runtime.resume(exec.id()).await.unwrap().wait().await
                })
                .await
                .unwrap(),
                ExecutionState::Succeeded
            );
        });
    }
}

#[test]
fn committed_startup_survives_process_exit_before_dispatch_with_or_without_ack() {
    for mode in ["initialized", "ack"] {
        for fast in [false, true] {
            let mut child = KillChild::start(mode, fast);
            child.wait_ready();
            assert!(child.child.wait().unwrap().success());
            let store = SqliteStore::open(&child.path).unwrap();
            current_runtime().block_on(async {
                let id = child.id();
                let snap = store.get(&id).await.unwrap().unwrap();
                assert_eq!(snap.state, ExecutionState::Created);
                assert_eq!(snap.revision, 0);
                assert_eq!(snap.nodes.len(), 2);
                assert!(snap.nodes.values().all(|n| n.state == NodeState::Pending));
                assert!(store.workflow_definition(&id).await.unwrap().is_some());
                let clock = Arc::new(FakeClock::new());
                // Runtime::start_durable uses system time; initialize above uses zero.
                let lease_until: i64 = store
                    .lock()
                    .unwrap()
                    .query_row(
                        "SELECT lease_until FROM executions WHERE id = ?1",
                        [id.as_str()],
                        |r| r.get(0),
                    )
                    .unwrap();
                clock.set(Timestamp(lease_until as u64 + 1));
                let runs = Arc::new(AtomicU32::new(0));
                let counter = runs.clone();
                let runtime = Runtime::builder()
                    .store(store.clone())
                    .clock(clock)
                    .register_fn("e", move |_| {
                        counter.fetch_add(1, Ordering::SeqCst);
                        async { NodeOutcome::Succeeded(Default::default()) }
                    })
                    .build();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), async {
                        runtime.resume(&id).await.unwrap().wait().await
                    })
                    .await
                    .unwrap(),
                    ExecutionState::Succeeded
                );
                assert_eq!(runs.load(Ordering::SeqCst), 2);
            });
        }
    }
}
