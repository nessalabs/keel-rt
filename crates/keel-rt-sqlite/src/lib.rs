//! File-backed [`StateStore`] for keel-rt. The kernel never names this crate.
//!
//! Delete this package without editing `scheduler.rs`. Persist is inline
//! (no queue) — ADR 0001 still applies.
//!
//! # Durability
//!
//! Default [`SqliteStore::open`] uses WAL + **`synchronous=FULL`**. A COMMIT
//! that returned is durable across process kill **and** machine power loss of
//! that txn. Crash-after-CAS tests prove the process-kill half.
//!
//! [`SqliteStore::open_fast`] is `synchronous=NORMAL`: process kill after
//! COMMIT is still recovered (same crash tests); an OS crash or power loss
//! may drop the last WAL frames. Use it only when you have measured that you
//! need the extra speed. 256-wide resume under FULL still meets the ≥50%
//! cut vs the pre-opt 1.008 s baseline (`benches/BASELINE.md`).
//!
//! One `BEGIN IMMEDIATE` … `COMMIT` per `persist`/`put` call. The scheduler
//! already persists once per event (Start+dispatch is one event, not a sqlite
//! merge of two turns). After the first write, only [`Execution::dirty_nodes`]
//! rows are upserted — unchanged Pending rows are not deleted (ADR 0002).
//! `wal_checkpoint(TRUNCATE)` is **best-effort after COMMIT** of a terminal
//! snapshot, never inside the transaction. Checkpoint `Err` (SQLITE_BUSY)
//! does not fail persist: the snapshot is already durable. Equal-revision
//! persist does not insert event rows.

use async_trait::async_trait;
use keel_rt::{
    Event, Execution, ExecutionId, ExecutionSnapshot, StateStore, StoreError,
    WorkflowDefinition,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Test-only: next N `wal_checkpoint(TRUNCATE)` calls return SQLITE_BUSY.
/// Production persist must still treat COMMIT as Ok (checkpoint is best-effort).
static FAIL_NEXT_CHECKPOINTS: AtomicU32 = AtomicU32::new(0);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS executions (
  id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  definition_hash TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  state TEXT NOT NULL,
  node_order TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
  execution_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  body TEXT NOT NULL,
  PRIMARY KEY (execution_id, node_id)
);
CREATE TABLE IF NOT EXISTS definitions (
  hash TEXT PRIMARY KEY,
  body BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  execution_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  body TEXT NOT NULL,
  PRIMARY KEY (execution_id, seq)
);
";

/// How hard sqlite fsyncs on COMMIT. See crate docs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteSynchronous {
    /// Process-kill after COMMIT. OS crash / power loss may lose the last txn.
    Normal,
    /// `PRAGMA synchronous=FULL`. Last COMMIT survives machine power loss.
    Full,
}

/// One connection, shared. Sync sqlite work runs inside async persist
/// the same way [`keel_rt::MemoryStore`] blocks — no persist queue.
///
/// [`Self::open_with_busy_timeout`] bounds `SQLITE_BUSY`: a locked file is a
/// typed [`StoreError`], not a hang. Default open waits up to 5s and uses
/// `synchronous=FULL`. [`Self::open_fast`] is `NORMAL` (process-kill only).
#[derive(Clone)]
pub struct SqliteStore {
    path: PathBuf,
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with(path, Duration::from_secs(5), SqliteSynchronous::Full)
    }

    /// Same as [`Self::open`] (`synchronous=FULL`). Explicit name for callers.
    pub fn durable(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open(path)
    }

    /// `synchronous=NORMAL`. Process-kill after COMMIT still recovers; power
    /// loss may lose the last WAL frames. Faster than [`Self::open`] on this
    /// machine (~259 ms vs ~421 ms 256-wide resume debug).
    pub fn open_fast(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with(path, Duration::from_secs(5), SqliteSynchronous::Normal)
    }

    /// Same as [`Self::open`], with a caller-visible busy bound.
    /// Tests use `Duration::ZERO` so a locked file is a typed error, not a wait.
    pub fn open_with_busy_timeout(
        path: impl AsRef<Path>,
        busy: Duration,
    ) -> Result<Self, StoreError> {
        Self::open_with(path, busy, SqliteSynchronous::Full)
    }

    /// [`Self::open_fast`] with a caller-visible busy bound.
    pub fn open_fast_with_busy_timeout(
        path: impl AsRef<Path>,
        busy: Duration,
    ) -> Result<Self, StoreError> {
        Self::open_with(path, busy, SqliteSynchronous::Normal)
    }

    /// [`Self::durable`] with a caller-visible busy bound.
    pub fn durable_with_busy_timeout(
        path: impl AsRef<Path>,
        busy: Duration,
    ) -> Result<Self, StoreError> {
        Self::open_with_busy_timeout(path, busy)
    }

    pub fn open_with(
        path: impl AsRef<Path>,
        busy: Duration,
        sync: SqliteSynchronous,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path).map_err(store_err)?;
        conn.busy_timeout(busy).map_err(store_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(store_err)?;
        let sync_val = match sync {
            SqliteSynchronous::Normal => "NORMAL",
            SqliteSynchronous::Full => "FULL",
        };
        conn.pragma_update(None, "synchronous", sync_val)
            .map_err(store_err)?;
        conn.pragma_update(None, "wal_autocheckpoint", 1000)
            .map_err(store_err)?;
        conn.execute_batch(SCHEMA).map_err(store_err)?;
        Ok(Self {
            path,
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Adapter inspect. Resume never calls this.
    pub fn event_count(&self, id: &ExecutionId) -> Result<u64, StoreError> {
        let conn = self.lock()?;
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE execution_id = ?1",
                params![id.as_str()],
                |row| row.get(0),
            )
            .map_err(store_err)?;
        Ok(n as u64)
    }

    /// Adapter inspect. Resume never calls this.
    pub fn event_bodies(&self, id: &ExecutionId) -> Result<Vec<String>, StoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT body FROM events WHERE execution_id = ?1 ORDER BY seq",
            )
            .map_err(store_err)?;
        let rows = stmt
            .query_map(params![id.as_str()], |row| row.get(0))
            .map_err(store_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(store_err)?);
        }
        Ok(out)
    }

    /// Next `n` terminal WAL checkpoints return SQLITE_BUSY. Hidden for tests.
    #[doc(hidden)]
    pub fn fail_next_wal_checkpoints(n: u32) {
        FAIL_NEXT_CHECKPOINTS.store(n, Ordering::SeqCst);
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        Ok(self.conn.lock().unwrap_or_else(|p| p.into_inner()))
    }

    fn write_snapshot(
        conn: &Connection,
        snapshot: &ExecutionSnapshot,
        definition: Option<&WorkflowDefinition>,
    ) -> Result<(), StoreError> {
        conn.execute("BEGIN IMMEDIATE", []).map_err(store_err)?;
        let r = (|| {
            insert_definition(conn, definition)?;
            upsert_full_snapshot(conn, snapshot)
        })();
        finish_tx(conn, r).map(|_| ())
    }

    fn persist_exec(conn: &Connection, exec: &Execution) -> Result<(), StoreError> {
        Self::persist_exec_with_events(conn, exec, &[])
    }

    fn persist_exec_with_events(
        conn: &Connection,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        conn.execute("BEGIN IMMEDIATE", []).map_err(store_err)?;
        let r = (|| {
            insert_definition(conn, Some(exec.definition()))?;
            let found: Option<i64> = conn
                .query_row(
                    "SELECT revision FROM executions WHERE id = ?1",
                    params![exec.id().as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(store_err)?;
            match found {
                Some(found) if found as u64 > exec.revision() => Err(StoreError::Stale {
                    found: found as u64,
                    attempted: exec.revision(),
                }),
                Some(found) if found as u64 == exec.revision() => {
                    // Snapshot already on disk. Do not append events (MAX(seq)+1
                    // has no identity key). Shutdown retry / a second Runtime
                    // would otherwise duplicate ExecutionSucceeded.
                    let _ = events;
                    Ok(())
                }
                Some(_) => {
                    upsert_execution_meta(conn, exec)?;
                    upsert_dirty_nodes(conn, exec)?;
                    insert_events(conn, exec.id(), events)?;
                    Ok(())
                }
                None => {
                    insert_new_snapshot(conn, &exec.snapshot())?;
                    insert_events(conn, exec.id(), events)?;
                    Ok(())
                }
            }
        })();
        let committed = finish_tx(conn, r)?;
        // TRUNCATE only after COMMIT. Never checkpoint an open transaction
        // (that would be a durability bug, not a speedup).
        // Checkpoint is best-effort: COMMIT already made the snapshot durable.
        // SQLITE_BUSY here must not look like "this snapshot is not on disk"
        // (scheduler would keep pending_events and retry, duplicating rows).
        if committed && exec.state().is_terminal() {
            debug_assert!(conn.is_autocommit());
            let _ = checkpoint_wal(conn);
        }
        Ok(())
    }
}

fn store_err(e: rusqlite::Error) -> StoreError {
    StoreError::Message(e.to_string())
}

fn json_err(e: serde_json::Error) -> StoreError {
    StoreError::Message(e.to_string())
}

fn finish_tx(conn: &Connection, r: Result<(), StoreError>) -> Result<bool, StoreError> {
    match r {
        Ok(()) => {
            conn.execute("COMMIT", []).map_err(store_err)?;
            Ok(true)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(e)
        }
    }
}

fn checkpoint_wal(conn: &Connection) -> Result<(), StoreError> {
    let remaining = FAIL_NEXT_CHECKPOINTS.load(Ordering::SeqCst);
    if remaining > 0 {
        FAIL_NEXT_CHECKPOINTS.store(remaining.saturating_sub(1), Ordering::SeqCst);
        return Err(StoreError::Message("SQLITE_BUSY checkpoint".into()));
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(store_err)
}

fn insert_events(
    conn: &Connection,
    execution_id: &ExecutionId,
    events: &[Event],
) -> Result<(), StoreError> {
    if events.is_empty() {
        return Ok(());
    }
    let next: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE execution_id = ?1",
            params![execution_id.as_str()],
            |row| row.get(0),
        )
        .map_err(store_err)?;
    let mut stmt = conn
        .prepare("INSERT INTO events (execution_id, seq, body) VALUES (?1, ?2, ?3)")
        .map_err(store_err)?;
    for (i, ev) in events.iter().enumerate() {
        let body = serde_json::to_string(ev).map_err(json_err)?;
        stmt.execute(params![execution_id.as_str(), next + i as i64, body])
            .map_err(store_err)?;
    }
    Ok(())
}

fn insert_definition(
    conn: &Connection,
    definition: Option<&WorkflowDefinition>,
) -> Result<(), StoreError> {
    let Some(def) = definition else {
        return Ok(());
    };
    conn.execute(
        "INSERT OR IGNORE INTO definitions (hash, body) VALUES (?1, ?2)",
        params![def.content_hash().as_str(), def.durable_bytes()],
    )
    .map_err(store_err)?;
    Ok(())
}

fn upsert_execution_row(
    conn: &Connection,
    snapshot: &ExecutionSnapshot,
) -> Result<(), StoreError> {
    let state = serde_json::to_string(&snapshot.state).map_err(json_err)?;
    let order = serde_json::to_string(&snapshot.node_order).map_err(json_err)?;
    conn.execute(
        "INSERT INTO executions
           (id, revision, schema_version, definition_hash, workflow_id, state, node_order)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(id) DO UPDATE SET
           revision = excluded.revision,
           schema_version = excluded.schema_version,
           definition_hash = excluded.definition_hash,
           workflow_id = excluded.workflow_id,
           state = excluded.state,
           node_order = excluded.node_order
         WHERE executions.revision < excluded.revision",
        params![
            snapshot.execution_id.as_str(),
            snapshot.revision as i64,
            snapshot.schema_version as i64,
            snapshot.definition_hash.as_str(),
            snapshot.workflow_id.as_str(),
            state,
            order,
        ],
    )
    .map_err(store_err)?;
    Ok(())
}

fn upsert_execution_meta(conn: &Connection, exec: &Execution) -> Result<(), StoreError> {
    let order: Vec<_> = exec
        .definition()
        .nodes()
        .iter()
        .map(|n| n.id.clone())
        .collect();
    let meta = ExecutionSnapshot {
        schema_version: keel_rt::SCHEMA_VERSION,
        revision: exec.revision(),
        execution_id: exec.id().clone(),
        workflow_id: exec.definition().id().clone(),
        state: exec.state(),
        nodes: std::collections::HashMap::new(),
        node_order: order,
        definition_hash: exec.definition().content_hash(),
    };
    upsert_execution_row(conn, &meta)
}

fn upsert_full_snapshot(
    conn: &Connection,
    snapshot: &ExecutionSnapshot,
) -> Result<(), StoreError> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT revision FROM executions WHERE id = ?1",
            params![snapshot.execution_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(store_err)?;
    match found {
        Some(found) if found as u64 > snapshot.revision => Err(StoreError::Stale {
            found: found as u64,
            attempted: snapshot.revision,
        }),
        Some(found) if found as u64 == snapshot.revision => Ok(()),
        Some(_) => {
            upsert_execution_row(conn, snapshot)?;
            conn.execute(
                "DELETE FROM nodes WHERE execution_id = ?1",
                params![snapshot.execution_id.as_str()],
            )
            .map_err(store_err)?;
            insert_nodes(conn, snapshot)?;
            Ok(())
        }
        None => Err(StoreError::Message(
            "put requires an existing execution (persist first)".into(),
        )),
    }
}

fn insert_new_snapshot(
    conn: &Connection,
    snapshot: &ExecutionSnapshot,
) -> Result<(), StoreError> {
    upsert_execution_row(conn, snapshot)?;
    insert_nodes(conn, snapshot)
}

fn insert_nodes(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT OR REPLACE INTO nodes (execution_id, node_id, body) VALUES (?1, ?2, ?3)",
        )
        .map_err(store_err)?;
    for (id, node) in &snapshot.nodes {
        let body = serde_json::to_string(node).map_err(json_err)?;
        stmt.execute(params![
            snapshot.execution_id.as_str(),
            id.as_str(),
            body
        ])
        .map_err(store_err)?;
    }
    Ok(())
}

fn upsert_dirty_nodes(conn: &Connection, exec: &Execution) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT OR REPLACE INTO nodes (execution_id, node_id, body) VALUES (?1, ?2, ?3)",
        )
        .map_err(store_err)?;
    for (id, node) in exec.dirty_nodes() {
        let body = serde_json::to_string(&node).map_err(json_err)?;
        stmt.execute(params![exec.id().as_str(), id.as_str(), body])
            .map_err(store_err)?;
    }
    Ok(())
}

fn load_snapshot(
    conn: &Connection,
    id: &ExecutionId,
) -> Result<Option<ExecutionSnapshot>, StoreError> {
    let row: Option<(i64, i64, String, String, String, String)> = conn
        .query_row(
            "SELECT revision, schema_version, definition_hash, workflow_id, state, node_order
             FROM executions WHERE id = ?1",
            params![id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(store_err)?;
    let Some((revision, schema_version, definition_hash, workflow_id, state, order)) = row else {
        return Ok(None);
    };
    let mut stmt = conn
        .prepare_cached("SELECT node_id, body FROM nodes WHERE execution_id = ?1")
        .map_err(store_err)?;
    let mut rows = stmt.query(params![id.as_str()]).map_err(store_err)?;
    let mut nodes = std::collections::HashMap::new();
    while let Some(row) = rows.next().map_err(store_err)? {
        let nid: String = row.get(0).map_err(store_err)?;
        let body: String = row.get(1).map_err(store_err)?;
        let node: keel_rt::NodeSnapshot =
            serde_json::from_str(&body).map_err(json_err)?;
        nodes.insert(keel_rt::NodeId::new(nid), node);
    }
    Ok(Some(ExecutionSnapshot {
        schema_version: schema_version as u32,
        revision: revision as u64,
        execution_id: id.clone(),
        workflow_id: keel_rt::WorkflowId::new(workflow_id),
        state: serde_json::from_str(&state).map_err(json_err)?,
        nodes,
        node_order: serde_json::from_str(&order).map_err(json_err)?,
        definition_hash: keel_rt::DefinitionHash::parse(&definition_hash)
            .map_err(|e| StoreError::Message(e.to_string()))?,
    }))
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        let conn = self.lock()?;
        Self::write_snapshot(&conn, snapshot, None)
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        let conn = self.lock()?;
        load_snapshot(&conn, id)
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let _ = exec.definition().content_hash();
        let conn = self.lock()?;
        Self::persist_exec(&conn, exec)
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[Event],
    ) -> Result<(), StoreError> {
        let _ = exec.definition().content_hash();
        let conn = self.lock()?;
        Self::persist_exec_with_events(&conn, exec, events)
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        let conn = self.lock()?;
        let hash: Option<String> = conn
            .query_row(
                "SELECT definition_hash FROM executions WHERE id = ?1",
                params![id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(store_err)?;
        let Some(hash) = hash else {
            return Ok(None);
        };
        let body: Vec<u8> = conn
            .query_row(
                "SELECT body FROM definitions WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(store_err)?
            .ok_or_else(|| StoreError::Message("definition body missing".into()))?;
        WorkflowDefinition::from_durable_bytes(&body)
            .map(Some)
            .map_err(|e| StoreError::Message(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_rt::{AcceptPolicy, ApplyCmd, Timestamp};

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!("keel-rt-sqlite-unit-{}.db", ExecutionId::new()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn one_node() -> Execution {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        Execution::new(def)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persist_keeps_definition_beside_snapshot() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        let def = store
            .workflow_definition(exec.id())
            .await
            .unwrap()
            .expect("definition");
        assert_eq!(def.id().as_str(), "wf");
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().definition_hash,
            def.content_hash()
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_put_does_not_clobber() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        store.persist(&exec).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        let keep = store.get(exec.id()).await.unwrap().unwrap().revision;
        let mut older = store.get(exec.id()).await.unwrap().unwrap();
        older.revision = 0;
        let err = store.put(&older).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::Stale {
                found: keep,
                attempted: 0
            }
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn put_without_persist_errors() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let exec = one_node();
        let err = store.put(&exec.snapshot()).await.unwrap_err();
        assert!(matches!(err, StoreError::Message(m) if m.contains("persist first")));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reopen_file_sees_the_same_snapshot() {
        let path = tmp();
        let exec = one_node();
        let id = exec.id().clone();
        {
            let store = SqliteStore::open(&path).unwrap();
            store.persist(&exec).await.unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert!(store.get(&id).await.unwrap().is_some());
        assert!(store.workflow_definition(&id).await.unwrap().is_some());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn poisoned_mutex_recovers() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = store.conn.lock().unwrap();
            panic!("poison sqlite");
        }));
        assert!(poisoned.is_err());
        store.persist(&one_node()).await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncated_file_is_typed_error() {
        let path = tmp();
        std::fs::write(&path, b"not a sqlite database").unwrap();
        let err = match SqliteStore::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("truncated file must not open"),
        };
        assert!(matches!(err, StoreError::Message(_)));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_file_opens_as_new_store() {
        let path = tmp();
        std::fs::write(&path, b"").unwrap();
        let store = SqliteStore::open(&path).unwrap();
        let id = ExecutionId::parse("exec-missing").unwrap();
        assert!(store.get(&id).await.unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn locked_file_is_typed_error_not_panic() {
        let path = tmp();
        {
            let _init = SqliteStore::open(&path).unwrap();
        }
        let blocker = Connection::open(&path).unwrap();
        blocker
            .busy_timeout(Duration::from_millis(0))
            .unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let err = match SqliteStore::open_with_busy_timeout(&path, Duration::ZERO) {
            Err(e) => e,
            Ok(store) => store.persist(&one_node()).await.unwrap_err(),
        };
        assert!(
            matches!(&err, StoreError::Message(m) if m.contains("locked") || m.contains("busy")),
            "{err}"
        );
        drop(blocker);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_snapshot_json_is_typed_error() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        {
            let conn = store.lock().unwrap();
            conn.execute(
                "UPDATE nodes SET body = '{not-json' WHERE execution_id = ?1",
                params![exec.id().as_str()],
            )
            .unwrap();
        }
        let err = store.get(exec.id()).await.unwrap_err();
        assert!(matches!(err, StoreError::Message(_)));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn schema_version_in_json_fail_closed() {
        use keel_rt::{SCHEMA_VERSION, SnapshotError};
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        {
            let conn = store.lock().unwrap();
            conn.execute(
                "UPDATE executions SET schema_version = 99 WHERE id = ?1",
                params![exec.id().as_str()],
            )
            .unwrap();
        }
        let loaded = store.get(exec.id()).await.unwrap().unwrap();
        let def = store
            .workflow_definition(exec.id())
            .await
            .unwrap()
            .unwrap();
        match Execution::from_snapshot(def, loaded) {
            Err(SnapshotError::SchemaMismatch {
                found: 99,
                expected: SCHEMA_VERSION,
            }) => {}
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wal_mode_is_enabled() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mode: String = {
            let conn = store.lock().unwrap();
            conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(mode.to_lowercase(), "wal");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncated_wal_does_not_invent_a_terminal() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        store.persist(&exec).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        drop(store);
        let wal = {
            let mut s = path.as_os_str().to_os_string();
            s.push("-wal");
            std::path::PathBuf::from(s)
        };
        if wal.exists() {
            std::fs::write(&wal, b"torn").unwrap();
        }
        let store = match SqliteStore::open(&path) {
            Ok(s) => s,
            Err(_) => {
                let _ = std::fs::remove_file(&path);
                return;
            }
        };
        match store.get(exec.id()).await {
            Ok(None) => {}
            Ok(Some(snap)) => {
                assert_ne!(
                    snap.state,
                    keel_rt::ExecutionState::Succeeded,
                    "torn WAL must not invent a terminal"
                );
            }
            Err(_) => {}
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persist_under_lock_returns_within_busy_bound() {
        let path = tmp();
        {
            let _init = SqliteStore::open(&path).unwrap();
        }
        let blocker = Connection::open(&path).unwrap();
        blocker.busy_timeout(Duration::from_millis(0)).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let t0 = std::time::Instant::now();
        let err = match SqliteStore::open_with_busy_timeout(&path, Duration::from_millis(50)) {
            Err(e) => e,
            Ok(store) => store.persist(&one_node()).await.unwrap_err(),
        };
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "{:?}",
            t0.elapsed()
        );
        assert!(matches!(err, StoreError::Message(_)));
        drop(blocker);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_persist_keeps_pending_nodes() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        store.persist(&exec).await.unwrap();
        let def = store
            .workflow_definition(exec.id())
            .await
            .unwrap()
            .unwrap();
        let snap = store.get(exec.id()).await.unwrap().unwrap();
        let mut exec = Execution::from_snapshot(def, snap).unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        assert_eq!(exec.dirty_nodes().len(), 1);
        store.persist(&exec).await.unwrap();
        let snap = store.get(exec.id()).await.unwrap().unwrap();
        assert_eq!(snap.nodes.len(), 2);
        assert!(matches!(
            snap.node(&keel_rt::NodeId::new("b")).unwrap().state,
            keel_rt::NodeState::Pending
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal() {
        let path = tmp();
        let exec = one_node();
        let id = exec.id().clone();
        {
            let store = SqliteStore::open(&path).unwrap();
            store.persist(&exec).await.unwrap();
            {
                let conn = store.lock().unwrap();
                conn.execute("BEGIN IMMEDIATE", []).unwrap();
                conn.execute(
                    "UPDATE executions SET state = '\"Succeeded\"', revision = 99 WHERE id = ?1",
                    params![id.as_str()],
                )
                .unwrap();
                conn.execute(
                    "UPDATE nodes SET body = '{\"state\":\"Succeeded\",\"output\":null,\"attempt\":1,\"resume_token\":null,\"last_error\":null}' WHERE execution_id = ?1",
                    params![id.as_str()],
                )
                .unwrap();
            }
            drop(store);
        }
        let store = SqliteStore::open(&path).unwrap();
        match store.get(&id).await {
            Ok(None) => {}
            Ok(Some(snap)) => {
                assert_ne!(
                    snap.state,
                    keel_rt::ExecutionState::Succeeded,
                    "uncommitted put must not invent a terminal"
                );
                assert_ne!(snap.revision, 99);
            }
            Err(_) => {}
        }
        let _ = std::fs::remove_file(&path);
    }

    fn pragma_sync(store: &SqliteStore) -> i64 {
        let conn = store.lock().unwrap();
        conn.query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap()
    }

    async fn reload(store: &SqliteStore, id: &ExecutionId) -> Execution {
        let def = store.workflow_definition(id).await.unwrap().unwrap();
        let snap = store.get(id).await.unwrap().unwrap();
        Execution::from_snapshot(def, snap).unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_default_is_synchronous_full() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        // sqlite: 0=OFF 1=NORMAL 2=FULL 3=EXTRA
        assert_eq!(pragma_sync(&store), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_fast_is_synchronous_normal() {
        let path = tmp();
        let store = SqliteStore::open_fast(&path).unwrap();
        assert_eq!(pragma_sync(&store), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_is_synchronous_full() {
        let path = tmp();
        let store = SqliteStore::durable(&path).unwrap();
        assert_eq!(pragma_sync(&store), 2);
        store.persist(&one_node()).await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_persist_does_not_delete_unchanged_rows() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        let id = exec.id().clone();
        store.persist(&exec).await.unwrap();
        let n0: i64 = {
            let conn = store.lock().unwrap();
            conn.query_row(
                "SELECT COUNT(*) FROM nodes WHERE execution_id = ?1",
                params![id.as_str()],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(n0, 2);
        let mut exec = reload(&store, &id).await;
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        let n1: i64 = {
            let conn = store.lock().unwrap();
            conn.query_row(
                "SELECT COUNT(*) FROM nodes WHERE execution_id = ?1",
                params![id.as_str()],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(n1, n0, "incremental persist must not DELETE unchanged nodes");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn second_uncommitted_persist_does_not_merge_into_first_commit() {
        let path = tmp();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        let id = exec.id().clone();
        let first_rev;
        {
            let store = SqliteStore::open(&path).unwrap();
            store.persist(&exec).await.unwrap();
            let mut exec = reload(&store, &id).await;
            exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
                .unwrap();
            store.persist(&exec).await.unwrap();
            first_rev = store.get(&id).await.unwrap().unwrap().revision;
            assert!(first_rev > 0);
            {
                let conn = store.lock().unwrap();
                conn.execute("BEGIN IMMEDIATE", []).unwrap();
                conn.execute(
                    "UPDATE executions SET state = '\"Succeeded\"', revision = 99 WHERE id = ?1",
                    params![id.as_str()],
                )
                .unwrap();
            }
            drop(store);
        }
        let store = SqliteStore::open(&path).unwrap();
        let snap = store.get(&id).await.unwrap().unwrap();
        assert_ne!(snap.state, keel_rt::ExecutionState::Succeeded);
        assert_eq!(snap.revision, first_rev);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_put_after_incremental_dirty_rows_loses() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        store.persist(&exec).await.unwrap();
        let mut older = store.get(exec.id()).await.unwrap().unwrap();
        let mut exec = reload(&store, exec.id()).await;
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        let keep = store.get(exec.id()).await.unwrap().unwrap().revision;
        older.revision = 0;
        let err = store.put(&older).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::Stale {
                found: keep,
                attempted: 0
            }
        );
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().revision,
            keep
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn equal_revision_persist_with_events_does_not_duplicate_rows() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        let ev = Event::ExecutionStarted {
            execution_id: exec.id().clone(),
            workflow_id: exec.definition().id().clone(),
            at: Timestamp(0),
            schema_version: keel_rt::SCHEMA_VERSION,
        };
        store.persist_with_events(&exec, std::slice::from_ref(&ev)).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
        let n = store.event_count(exec.id()).unwrap();
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
        assert_eq!(
            store.event_count(exec.id()).unwrap(),
            n,
            "equal-revision persist_with_events must not append event rows"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn checkpoint_runs_only_after_commit_of_terminal() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        store.persist(&exec).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        assert!(!exec.state().is_terminal());
        {
            let conn = store.lock().unwrap();
            assert!(
                conn.is_autocommit(),
                "non-terminal persist must COMMIT before returning"
            );
        }
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
                outcome: Ok(keel_rt::NodeOutcome::Succeeded(bytes::Bytes::from_static(
                    b"ok",
                ))),
            },
            &AcceptPolicy,
            Timestamp(0),
        )
        .unwrap();
        assert!(exec.state().is_terminal());
        store.persist(&exec).await.unwrap();
        {
            let conn = store.lock().unwrap();
            assert!(conn.is_autocommit());
        }
        drop(store);
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().state,
            keel_rt::ExecutionState::Succeeded
        );
        let _ = std::fs::remove_file(&path);
    }
}
