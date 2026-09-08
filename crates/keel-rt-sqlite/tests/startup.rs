use keel_rt::{
    AcceptPolicy, ApplyCmd, ClaimError, Execution, ExecutionId, ExecutionState, MemoryStore,
    OwnerId, ResumeError, Runtime, StateStore, StoreError, Timestamp, WorkflowDefinition,
};
use keel_rt_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use std::path::PathBuf;

struct Database(PathBuf);

#[tokio::test]
async fn temporary_and_in_memory_databases_cannot_acknowledge_durability() {
    for path in [
        ":memory:",
        "",
        "file:keel-startup-memory?mode=memory&cache=shared",
    ] {
        let store = SqliteStore::open(path).unwrap();
        let exec = execution();
        assert_eq!(
            store.initialize(&exec, &OwnerId::new(), Timestamp(0)).await,
            Err(keel_rt::InitializeError::Unsupported)
        );
        assert!(store.get(exec.id()).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn durable_handle_drop_still_cancels_waiting_execution() {
    let db = Database::new();
    let store = db.open();
    let runtime = Runtime::builder().store(store.clone()).build();
    let definition = WorkflowDefinition::builder("drop-durable")
        .node("wait", "wait")
        .build()
        .unwrap();
    let handle = runtime.start_durable(definition).await.unwrap();
    let id = handle.execution_id().clone();
    assert_eq!(handle.wait_stable().await, ExecutionState::Waiting);
    drop(handle);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store.get(&id).await.unwrap().unwrap().state == ExecutionState::Cancelled {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn durable_ack_has_a_created_snapshot_and_definition_before_dispatch() {
    for fast in [false, true] {
        let db = Database::new();
        let store = if fast {
            SqliteStore::open_fast(&db.0).unwrap()
        } else {
            db.open()
        };
        let runtime = Runtime::builder()
            .store(store.clone())
            .register_fn("a", |_| async {
                keel_rt::NodeOutcome::Succeeded(Default::default())
            })
            .build();
        let handle = runtime
            .start_durable(execution().definition().clone())
            .await
            .unwrap();
        let id = handle.execution_id().clone();
        let initial = store.get(&id).await.unwrap().unwrap();
        assert_eq!(initial.state, ExecutionState::Created);
        assert_eq!(initial.revision, 0);
        assert_eq!(initial.nodes.len(), 1);
        assert!(store.workflow_definition(&id).await.unwrap().is_some());
        assert_eq!(handle.wait().await, ExecutionState::Succeeded);
        assert_eq!(
            store.get(&id).await.unwrap().unwrap().state,
            ExecutionState::Succeeded
        );
    }
}

#[tokio::test]
async fn initialize_rejects_existing_ids_and_non_created_executions() {
    let db = Database::new();
    let store = db.open();
    let mut exec = execution();
    let first = OwnerId::new();
    let epoch = store.initialize(&exec, &first, Timestamp(0)).await.unwrap();
    for owner in [&first, &OwnerId::new()] {
        assert_eq!(
            store.initialize(&exec, owner, Timestamp(30_001)).await,
            Err(keel_rt::InitializeError::AlreadyExists)
        );
    }
    let row: (String, u64) = db
        .conn()
        .query_row("SELECT owner, epoch FROM executions", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(row, (first.as_str().into(), epoch.0));
    store.release(exec.id(), epoch).await.unwrap();
    assert_eq!(
        store.initialize(&exec, &first, Timestamp(30_001)).await,
        Err(keel_rt::InitializeError::AlreadyExists)
    );
    exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
        .unwrap();
    assert!(matches!(
        store.initialize(&exec, &first, Timestamp(0)).await,
        Err(keel_rt::InitializeError::Store(_))
    ));
    let reservation = execution();
    store
        .claim(reservation.id(), &first, Timestamp(0))
        .await
        .unwrap();
    assert_eq!(
        store.initialize(&reservation, &first, Timestamp(0)).await,
        Err(keel_rt::InitializeError::AlreadyExists)
    );
    assert!(store.get(reservation.id()).await.unwrap().is_none());
}

#[tokio::test]
async fn durable_initialization_failure_rolls_back_lease_definition_and_nodes() {
    let db = Database::new();
    let store = db.open();
    db.conn().execute_batch("CREATE TRIGGER reject_node BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT, 'disk failed'); END;").unwrap();
    let runtime = Runtime::builder()
        .store(store.clone())
        .register_fn("a", |_| async {
            panic!("failed initialization must never run work")
        })
        .build();
    assert!(matches!(
        runtime
            .start_durable(execution().definition().clone())
            .await,
        Err(keel_rt::DurableStartError::Initialization(
            keel_rt::InitializeError::Store(_)
        ))
    ));
    for table in ["executions", "definitions", "nodes", "events"] {
        let count: usize = db
            .conn()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "{table} must be rolled back");
    }
    db.conn().execute_batch("DROP TRIGGER reject_node").unwrap();
    let exec = execution();
    store
        .initialize(&exec, &OwnerId::new(), Timestamp(0))
        .await
        .unwrap();
    assert_eq!(
        store.get(exec.id()).await.unwrap().unwrap(),
        exec.snapshot()
    );
}

#[test]
fn competing_initializers_commit_one_execution_without_overwriting_the_owner() {
    let db = Database::new();
    let stores = [db.open(), db.open()];
    let exec = execution();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let threads: Vec<_> = stores
            .iter()
            .map(|store| {
                let barrier = &barrier;
                let copy =
                    Execution::from_snapshot(exec.definition().clone(), exec.snapshot()).unwrap();
                scope.spawn(move || {
                    let owner = OwnerId::new();
                    barrier.wait();
                    let result = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(store.initialize(&copy, &owner, Timestamp(0)));
                    (owner, result)
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| *r == Err(keel_rt::InitializeError::AlreadyExists))
            .count(),
        1
    );
    let winner = &results.iter().find(|(_, r)| r.is_ok()).unwrap().0;
    let owner: String = db
        .conn()
        .query_row("SELECT owner FROM executions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(owner, winner.as_str());
}

impl Database {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("keel-startup-{}.db", ExecutionId::new())))
    }

    fn open(&self) -> SqliteStore {
        SqliteStore::open(&self.0).unwrap()
    }

    fn conn(&self) -> Connection {
        Connection::open(&self.0).unwrap()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

fn execution() -> Execution {
    Execution::new(
        WorkflowDefinition::builder("startup")
            .node("a", "a")
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn lease_only_reopen_is_absent_without_releasing_ownership() {
    let db = Database::new();
    let id = ExecutionId::new();
    let owner = OwnerId::new();
    let epoch = db.open().claim(&id, &owner, Timestamp(0)).await.unwrap();
    let store = db.open();
    let memory = MemoryStore::new();
    memory.claim(&id, &owner, Timestamp(0)).await.unwrap();
    for adapter in [&store as &dyn StateStore, &memory] {
        assert!(adapter.get(&id).await.unwrap().is_none());
        assert!(adapter.workflow_definition(&id).await.unwrap().is_none());
        assert_eq!(
            adapter.claim(&id, &OwnerId::new(), Timestamp(1)).await,
            Err(ClaimError::ClaimedElsewhere)
        );
    }
    let runtime = Runtime::builder().store(store.clone()).build();
    assert!(matches!(
        runtime.resume(&id).await,
        Err(ResumeError::UnknownExecution)
    ));
    let row: (String, u64, u64) = db
        .conn()
        .query_row(
            "SELECT owner, epoch, lease_until FROM executions WHERE id = ?1",
            [id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (owner.as_str().into(), epoch.0, 30_000));
    let next = store
        .claim(&id, &OwnerId::new(), Timestamp(30_001))
        .await
        .unwrap();
    assert_eq!(next.0, epoch.0 + 1);
    assert!(store.get(&id).await.unwrap().is_none());
    store.release(&id, next).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_none());
    assert!(store.workflow_definition(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn claimed_revision_zero_initializes_and_resumes_with_all_nodes() {
    for start_first in [false, true] {
        let db = Database::new();
        let store = db.open();
        let mut exec = execution();
        let owner = OwnerId::new();
        let epoch = store.claim(exec.id(), &owner, Timestamp(0)).await.unwrap();
        exec.set_fence_epoch(epoch.0);
        assert!(
            matches!(store.put(&exec.snapshot()).await, Err(StoreError::Message(s)) if s.contains("persist first"))
        );
        let events = if start_first {
            exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
                .unwrap()
                .events
        } else {
            vec![]
        };
        store.persist_with_events(&exec, &events).await.unwrap();
        store.persist_with_events(&exec, &events).await.unwrap();
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap(),
            exec.snapshot()
        );
        assert_eq!(
            store
                .workflow_definition(exec.id())
                .await
                .unwrap()
                .unwrap()
                .content_hash(),
            exec.definition().content_hash()
        );
        let event_count: usize = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            event_count,
            events.len(),
            "equal revision cannot append twice"
        );
        assert_eq!(
            store.claim(exec.id(), &OwnerId::new(), Timestamp(1)).await,
            Err(ClaimError::ClaimedElsewhere)
        );
        store.release(exec.id(), epoch).await.unwrap();
        drop(store);
        let reopened = db.open();
        assert_eq!(
            reopened.get(exec.id()).await.unwrap().unwrap(),
            exec.snapshot()
        );
        let rt = Runtime::builder()
            .store(reopened)
            .register_fn("a", |_| async {
                keel_rt::NodeOutcome::Succeeded(Default::default())
            })
            .build();
        assert_eq!(
            rt.resume(exec.id()).await.unwrap().wait().await,
            ExecutionState::Succeeded
        );
    }
}

#[tokio::test]
async fn old_owner_cannot_initialize_after_takeover() {
    let db = Database::new();
    let store = db.open();
    let mut exec = execution();
    let first = store
        .claim(exec.id(), &OwnerId::new(), Timestamp(0))
        .await
        .unwrap();
    let second = store
        .claim(exec.id(), &OwnerId::new(), Timestamp(30_001))
        .await
        .unwrap();
    exec.set_fence_epoch(first.0);
    assert_eq!(
        store.persist(&exec).await,
        Err(StoreError::StaleEpoch {
            found: second.0,
            attempted: first.0
        })
    );
    assert!(store.get(exec.id()).await.unwrap().is_none());
    exec.set_fence_epoch(second.0);
    store.persist(&exec).await.unwrap();
    assert_eq!(
        store.get(exec.id()).await.unwrap().unwrap(),
        exec.snapshot()
    );
}

#[tokio::test]
async fn partial_or_corrupt_placeholders_are_not_hidden_or_overwritten() {
    for corruption in [
        "UPDATE executions SET revision = 1",
        "UPDATE executions SET schema_version = 999",
        "UPDATE executions SET workflow_id = 'real'",
        "UPDATE executions SET state = '\"Running\"'",
        "UPDATE executions SET state = 'broken'",
        "UPDATE executions SET node_order = '[\"a\"]'",
        "UPDATE executions SET node_order = 'broken'",
        "INSERT INTO nodes SELECT id, 'a', '{}', NULL FROM executions",
        "INSERT INTO events SELECT id, 1, '{}' FROM executions",
    ] {
        let db = Database::new();
        let store = db.open();
        let mut exec = execution();
        let epoch = store
            .claim(exec.id(), &OwnerId::new(), Timestamp(0))
            .await
            .unwrap();
        exec.set_fence_epoch(epoch.0);
        db.conn().execute_batch(corruption).unwrap();
        assert!(store.get(exec.id()).await.is_err(), "{corruption}");
        assert!(
            store.workflow_definition(exec.id()).await.is_err(),
            "{corruption}"
        );
        assert!(store.persist(&exec).await.is_err(), "{corruption}");
    }
}

#[tokio::test]
async fn real_snapshot_with_missing_definition_still_fails_recovery() {
    let db = Database::new();
    let store = db.open();
    let exec = execution();
    store.persist(&exec).await.unwrap();
    db.conn().execute("DELETE FROM definitions", []).unwrap();
    assert!(store.get(exec.id()).await.unwrap().is_some());
    assert!(store.workflow_definition(exec.id()).await.is_err());
    assert!(matches!(
        Runtime::builder()
            .store(store)
            .build()
            .resume(exec.id())
            .await,
        Err(ResumeError::Store(_))
    ));
}

#[tokio::test]
async fn failed_initial_snapshot_transaction_preserves_placeholder() {
    let db = Database::new();
    let store = db.open();
    let mut exec = execution();
    let epoch = store
        .claim(exec.id(), &OwnerId::new(), Timestamp(0))
        .await
        .unwrap();
    exec.set_fence_epoch(epoch.0);
    db.conn().execute_batch("CREATE TRIGGER reject_node BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT, 'disk failed'); END;").unwrap();
    assert!(store.persist(&exec).await.is_err());
    assert!(db.open().get(exec.id()).await.unwrap().is_none());
    assert!(store
        .workflow_definition(exec.id())
        .await
        .unwrap()
        .is_none());
    let definitions: usize = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM definitions WHERE hash = ?1",
            params![exec.definition().content_hash().as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(definitions, 0, "definition insert must roll back too");
    db.conn().execute_batch("DROP TRIGGER reject_node").unwrap();
    store.persist(&exec).await.unwrap();
    assert_eq!(
        store.get(exec.id()).await.unwrap().unwrap(),
        exec.snapshot()
    );
}
