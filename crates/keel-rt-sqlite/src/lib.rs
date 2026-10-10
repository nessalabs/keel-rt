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
//! Lease columns (`owner`, `epoch`, `lease_until`) live on `executions`.
//! Existing files get `ALTER TABLE` on open — kernel `SCHEMA_VERSION` stays 1.
//! `claim` uses `BEGIN IMMEDIATE` and never `INSERT OR REPLACE`.
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
    ClaimError, Event, Execution, ExecutionId, ExecutionSnapshot, InitializeError, LeaseEpoch,
    NodeSnapshot, NodeState, OwnerId, StateStore, StoreError, Timestamp, WorkflowDefinition,
    DEFAULT_LEASE_TTL, SCHEMA_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(test)]
mod startup_tests;

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
  node_order TEXT NOT NULL,
  owner TEXT,
  epoch INTEGER,
  lease_until INTEGER
);
CREATE TABLE IF NOT EXISTS nodes (
  execution_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  body TEXT NOT NULL,
  runnable_at INTEGER,
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
        ensure_runnable_at_column(&conn)?;
        ensure_lease_columns(&conn)?;
        Ok(Self {
            path,
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// On-disk `SUM(LENGTH(body))` for one execution. Resume never calls this.
    pub fn node_json_bytes(&self, id: &ExecutionId) -> Result<usize, StoreError> {
        let conn = self.lock()?;
        let n: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(LENGTH(body)), 0) FROM nodes WHERE execution_id = ?1",
                params![id.as_str()],
                |row| row.get(0),
            )
            .map_err(store_err)?;
        Ok(n as usize)
    }

    /// Rows with a `runnable_at` column. Resume never calls this.
    pub fn parked_deadline_count(&self, id: &ExecutionId) -> Result<u64, StoreError> {
        let conn = self.lock()?;
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE execution_id = ?1 AND runnable_at IS NOT NULL",
                params![id.as_str()],
                |row| row.get(0),
            )
            .map_err(store_err)?;
        Ok(n as u64)
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
            .prepare("SELECT body FROM events WHERE execution_id = ?1 ORDER BY seq")
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
            reject_fence(conn, exec)?;
            insert_definition(conn, Some(exec.definition()))?;
            let found = snapshot_revision(conn, exec.id())?;
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
                    reject_conflict(conn, &exec.snapshot())
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

fn ensure_lease_columns(conn: &Connection) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(executions)")
        .map_err(store_err)?;
    let cols = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(store_err)?;
    let mut have_owner = false;
    let mut have_epoch = false;
    let mut have_until = false;
    for c in cols {
        match c.map_err(store_err)?.as_str() {
            "owner" => have_owner = true,
            "epoch" => have_epoch = true,
            "lease_until" => have_until = true,
            _ => {}
        }
    }
    if !have_owner {
        conn.execute("ALTER TABLE executions ADD COLUMN owner TEXT", [])
            .map_err(store_err)?;
    }
    if !have_epoch {
        conn.execute("ALTER TABLE executions ADD COLUMN epoch INTEGER", [])
            .map_err(store_err)?;
    }
    if !have_until {
        conn.execute("ALTER TABLE executions ADD COLUMN lease_until INTEGER", [])
            .map_err(store_err)?;
    }
    Ok(())
}

fn lease_until_ms(now: Timestamp) -> i64 {
    i64::try_from(now.saturating_add(DEFAULT_LEASE_TTL).as_millis()).unwrap_or(i64::MAX)
}

fn lease_live_ms(until: Option<i64>, now: Timestamp) -> bool {
    match until {
        Some(u) => u > i64::try_from(now.as_millis()).unwrap_or(i64::MAX),
        None => false,
    }
}

fn reject_fence(conn: &Connection, exec: &Execution) -> Result<(), StoreError> {
    let epoch: Option<(Option<i64>, bool)> = conn
        .prepare_cached("SELECT epoch, owner IS NOT NULL FROM executions WHERE id = ?1")
        .map_err(store_err)?
        .query_row(params![exec.id().as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(store_err)?;
    let Some((Some(found), owned)) = epoch else {
        return Ok(());
    };
    if found <= 0 {
        return Ok(());
    }
    let found = found as u64;
    let attempted = exec.fence_epoch().unwrap_or(0);
    if attempted != found || !owned {
        return Err(StoreError::StaleEpoch { found, attempted });
    }
    Ok(())
}

fn claim_conn(
    conn: &Connection,
    id: &ExecutionId,
    owner: &OwnerId,
    now: Timestamp,
) -> Result<LeaseEpoch, ClaimError> {
    conn.execute("BEGIN IMMEDIATE", [])
        .map_err(|e| ClaimError::Store(store_err(e)))?;
    let r = (|| {
        let row: Option<(Option<String>, Option<i64>, Option<i64>)> = conn
            .query_row(
                "SELECT owner, epoch, lease_until FROM executions WHERE id = ?1",
                params![id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(store_err)?;
        let until = lease_until_ms(now);
        match row {
            None => {
                conn.execute(
                    "INSERT INTO executions
                       (id, revision, schema_version, definition_hash, workflow_id, state, node_order, owner, epoch, lease_until)
                     VALUES (?1, 0, ?2, '', '', '\"Created\"', '[]', ?3, 1, ?4)",
                    params![id.as_str(), SCHEMA_VERSION as i64, owner.as_str(), until],
                )
                .map_err(store_err)?;
                Ok(LeaseEpoch(1))
            }
            Some((cur_owner, cur_epoch, cur_until)) => {
                let epoch = cur_epoch.unwrap_or(0) as u64;
                let live = lease_live_ms(cur_until, now);
                let other = cur_owner.as_deref().is_some_and(|o| o != owner.as_str());
                if other && live {
                    return Err(ClaimError::ClaimedElsewhere);
                }
                if !other && live && epoch > 0 {
                    conn.execute(
                        "UPDATE executions SET lease_until = ?2 WHERE id = ?1",
                        params![id.as_str(), until],
                    )
                    .map_err(store_err)?;
                    return Ok(LeaseEpoch(epoch));
                }
                let new_epoch = epoch.saturating_add(1).max(1);
                conn.execute(
                    "UPDATE executions SET owner = ?2, epoch = ?3, lease_until = ?4 WHERE id = ?1",
                    params![id.as_str(), owner.as_str(), new_epoch as i64, until],
                )
                .map_err(store_err)?;
                Ok(LeaseEpoch(new_epoch))
            }
        }
    })();
    match r {
        Ok(epoch) => {
            finish_tx(conn, Ok(()))?;
            Ok(epoch)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(e)
        }
    }
}

fn heartbeat_conn(
    conn: &Connection,
    id: &ExecutionId,
    epoch: LeaseEpoch,
    now: Timestamp,
) -> Result<(), ClaimError> {
    conn.execute("BEGIN IMMEDIATE", [])
        .map_err(|e| ClaimError::Store(store_err(e)))?;
    let r = (|| {
        let n = conn
            .execute(
                "UPDATE executions SET lease_until = ?3 WHERE id = ?1 AND epoch = ?2 AND owner IS NOT NULL",
                params![id.as_str(), epoch.0 as i64, lease_until_ms(now)],
            )
            .map_err(store_err)?;
        if n == 0 {
            Err(ClaimError::ClaimedElsewhere)
        } else {
            Ok(())
        }
    })();
    match r {
        Ok(()) => {
            conn.execute("COMMIT", [])
                .map_err(|e| ClaimError::Store(store_err(e)))?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(e)
        }
    }
}

// Keep the generation after release: resetting it would let an old token
// become valid again when a future owner claims this execution.
fn release_conn(conn: &Connection, id: &ExecutionId, epoch: LeaseEpoch) {
    let _ = conn.execute(
        "UPDATE executions SET owner = NULL, lease_until = NULL
         WHERE id = ?1 AND epoch = ?2 AND owner IS NOT NULL",
        params![id.as_str(), epoch.0 as i64],
    );
}

fn release_owner_conn(conn: &Connection, owner: &OwnerId) {
    let _ = conn.execute(
        "UPDATE executions SET owner = NULL, lease_until = NULL
         WHERE owner = ?1",
        params![owner.as_str()],
    );
}

fn ensure_runnable_at_column(conn: &Connection) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(nodes)")
        .map_err(store_err)?;
    let cols = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(store_err)?;
    for c in cols {
        if c.map_err(store_err)? == "runnable_at" {
            return Ok(());
        }
    }
    conn.execute("ALTER TABLE nodes ADD COLUMN runnable_at INTEGER", [])
        .map_err(store_err)?;
    Ok(())
}

/// T lives in `nodes.runnable_at` (INTEGER ms). JSON stays the compact
/// Ready-now shape so parked rows are not 2.5× Ready-now. A short
/// `last_error` (`{"message":"timed out"}`) stays in the body so live
/// inspect and crash-resume inspect agree. Values that do not fit i64
/// stay in JSON (Timestamp::MAX).
fn node_row_parts(node: &NodeSnapshot) -> Result<(String, Option<i64>), StoreError> {
    let mut stored = node.clone();
    let col = match node.state {
        NodeState::Ready {
            runnable_at: Some(at),
        } => match i64::try_from(at.as_millis()) {
            Ok(ms) => {
                stored.state = NodeState::Ready { runnable_at: None };
                Some(ms)
            }
            Err(_) => None,
        },
        _ => None,
    };
    let body = serde_json::to_string(&stored).map_err(json_err)?;
    Ok((body, col))
}

fn node_from_row(body: &str, runnable_at: Option<i64>) -> Result<NodeSnapshot, StoreError> {
    let mut node: NodeSnapshot = serde_json::from_str(body).map_err(json_err)?;
    if let Some(ms) = runnable_at {
        if matches!(node.state, NodeState::Ready { .. }) {
            node.state = NodeState::Ready {
                runnable_at: Some(Timestamp::from_millis(ms as u64)),
            };
        }
    }
    Ok(node)
}

fn insert_node_row(
    stmt: &mut rusqlite::CachedStatement<'_>,
    execution_id: &str,
    node_id: &str,
    node: &NodeSnapshot,
) -> Result<(), StoreError> {
    let (body, t) = node_row_parts(node)?;
    stmt.execute(params![execution_id, node_id, body, t])
        .map_err(store_err)?;
    Ok(())
}

fn store_err(e: rusqlite::Error) -> StoreError {
    StoreError::Message(e.to_string())
}

fn json_err(e: serde_json::Error) -> StoreError {
    StoreError::Message(e.to_string())
}

fn finish_tx(conn: &Connection, r: Result<(), StoreError>) -> Result<bool, StoreError> {
    // Some COMMIT errors leave the transaction open. Roll back those too so a
    // failed first persist cannot expose uncommitted state on this connection.
    match r.and_then(|()| conn.execute("COMMIT", []).map(|_| ()).map_err(store_err)) {
        Ok(()) => Ok(true),
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
    let hash = def.content_hash();
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM definitions WHERE hash = ?1",
            params![hash.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(store_err)?;
    if exists.is_some() {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO definitions (hash, body) VALUES (?1, ?2)",
        params![hash.as_str(), def.durable_bytes()],
    )
    .map_err(store_err)?;
    Ok(())
}

fn upsert_execution_row(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
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
         WHERE executions.revision < excluded.revision
            OR (executions.revision = 0 AND executions.definition_hash = '')",
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

fn reject_conflict(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
    if load_snapshot(conn, &snapshot.execution_id)?.as_ref() == Some(snapshot) {
        Ok(())
    } else {
        Err(StoreError::Conflict {
            revision: snapshot.revision,
        })
    }
}

fn upsert_full_snapshot(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
    let found = snapshot_revision(conn, &snapshot.execution_id)?;
    match found {
        Some(found) if found as u64 > snapshot.revision => Err(StoreError::Stale {
            found: found as u64,
            attempted: snapshot.revision,
        }),
        Some(found) if found as u64 == snapshot.revision => reject_conflict(conn, snapshot),
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
            "put requires an initialized execution (persist first)".into(),
        )),
    }
}

fn insert_new_snapshot(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
    upsert_execution_row(conn, snapshot)?;
    insert_nodes(conn, snapshot)
}

fn insert_nodes(conn: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT OR REPLACE INTO nodes (execution_id, node_id, body, runnable_at)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(store_err)?;
    for (id, node) in &snapshot.nodes {
        insert_node_row(&mut stmt, snapshot.execution_id.as_str(), id.as_str(), node)?;
    }
    Ok(())
}

fn upsert_dirty_nodes(conn: &Connection, exec: &Execution) -> Result<(), StoreError> {
    let mut upd = conn
        .prepare_cached(
            "UPDATE nodes SET runnable_at = ?3
             WHERE execution_id = ?1 AND node_id = ?2 AND body = ?4",
        )
        .map_err(store_err)?;
    let mut ins = conn
        .prepare_cached(
            "INSERT OR REPLACE INTO nodes (execution_id, node_id, body, runnable_at)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(store_err)?;
    for (id, node) in exec.dirty_nodes() {
        let (body, t) = node_row_parts(&node)?;
        if t.is_some() {
            let n = upd
                .execute(params![exec.id().as_str(), id.as_str(), t, body])
                .map_err(store_err)?;
            if n > 0 {
                continue;
            }
        }
        ins.execute(params![exec.id().as_str(), id.as_str(), body, t])
            .map_err(store_err)?;
    }
    Ok(())
}

type SnapshotRow = (i64, i64, String, String, String, String);

// Keep the incremental write path cheap: do not load the whole node order just
// to compare revisions. Reservations need the full validation below once.
fn snapshot_revision(conn: &Connection, id: &ExecutionId) -> Result<Option<i64>, StoreError> {
    let row: Option<(i64, bool)> = conn
        .prepare_cached("SELECT revision, definition_hash = '' FROM executions WHERE id = ?1")
        .map_err(store_err)?
        .query_row(params![id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()
        .map_err(store_err)?;
    match row {
        Some((_, true)) => Ok(snapshot_row(conn, id)?.map(|row| row.0)),
        other => Ok(other.map(|(revision, _)| revision)),
    }
}

/// Lease reservation rows are not snapshots. Only the exact empty reservation
/// shape is absent; malformed or partially initialized rows remain errors.
fn snapshot_row(conn: &Connection, id: &ExecutionId) -> Result<Option<SnapshotRow>, StoreError> {
    let row: Option<SnapshotRow> = conn
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
    if let Some((revision, schema, hash, workflow, state, order)) = &row {
        if hash.is_empty() {
            let empty_children: bool = conn
                .query_row(
                    "SELECT NOT EXISTS (SELECT 1 FROM nodes WHERE execution_id = ?1)
                    AND NOT EXISTS (SELECT 1 FROM events WHERE execution_id = ?1)",
                    params![id.as_str()],
                    |row| row.get(0),
                )
                .map_err(store_err)?;
            if *revision == 0
                && *schema == SCHEMA_VERSION as i64
                && workflow.is_empty()
                && serde_json::from_str::<keel_rt::ExecutionState>(state).map_err(json_err)?
                    == keel_rt::ExecutionState::Created
                && serde_json::from_str::<Vec<keel_rt::NodeId>>(order)
                    .map_err(json_err)?
                    .is_empty()
                && empty_children
            {
                return Ok(None);
            }
            return Err(StoreError::Message("invalid lease placeholder".into()));
        }
    }
    Ok(row)
}

fn load_snapshot(
    conn: &Connection,
    id: &ExecutionId,
) -> Result<Option<ExecutionSnapshot>, StoreError> {
    let Some((revision, schema_version, definition_hash, workflow_id, state, order)) =
        snapshot_row(conn, id)?
    else {
        return Ok(None);
    };
    let mut stmt = conn
        .prepare_cached("SELECT node_id, body, runnable_at FROM nodes WHERE execution_id = ?1")
        .map_err(store_err)?;
    let mut rows = stmt.query(params![id.as_str()]).map_err(store_err)?;
    let mut nodes = std::collections::HashMap::new();
    while let Some(row) = rows.next().map_err(store_err)? {
        let nid: String = row.get(0).map_err(store_err)?;
        let body: String = row.get(1).map_err(store_err)?;
        let at: Option<i64> = row.get(2).map_err(store_err)?;
        let node = node_from_row(&body, at)?;
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
    async fn initialize(
        &self,
        exec: &Execution,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, InitializeError> {
        if exec.revision() != 0 || exec.state() != keel_rt::ExecutionState::Created {
            return Err(StoreError::Message(
                "initialize requires a fresh Created execution".into(),
            )
            .into());
        }
        // Hashing is CPU work and must not lengthen the SQLite write lock.
        let _ = exec.definition().content_hash();
        let snapshot = exec.snapshot();
        let mut conn = self.lock()?;
        if conn.path().is_none_or(str::is_empty) {
            return Err(InitializeError::Unsupported);
        }
        // Transaction Drop rolls back on error, panic, or a failed COMMIT.
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(store_err)?;
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM executions WHERE id = ?1)",
                params![exec.id().as_str()],
                |row| row.get(0),
            )
            .map_err(store_err)?;
        if exists {
            return Err(InitializeError::AlreadyExists);
        }
        insert_definition(&tx, Some(exec.definition()))?;
        insert_new_snapshot(&tx, &snapshot)?;
        tx.execute(
            "UPDATE executions SET owner = ?2, epoch = 1, lease_until = ?3 WHERE id = ?1",
            params![exec.id().as_str(), owner.as_str(), lease_until_ms(now)],
        )
        .map_err(store_err)?;
        tx.commit().map_err(store_err)?;
        Ok(LeaseEpoch(1))
    }

    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        let conn = self.lock()?;
        Self::write_snapshot(&conn, snapshot, None)
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        let mut conn = self.lock()?;
        // Metadata, reservation validation, and node rows must describe one
        // committed revision even when another connection initializes/writes.
        let tx = conn.transaction().map_err(store_err)?;
        let snapshot = load_snapshot(&tx, id)?;
        tx.commit().map_err(store_err)?;
        Ok(snapshot)
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
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(store_err)?;
        let Some((_, _, hash, _, _, _)) = snapshot_row(&tx, id)? else {
            return Ok(None);
        };
        let body: Vec<u8> = tx
            .query_row(
                "SELECT body FROM definitions WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(store_err)?
            .ok_or_else(|| StoreError::Message("definition body missing".into()))?;
        tx.commit().map_err(store_err)?;
        drop(conn);
        let definition = WorkflowDefinition::from_durable_bytes(&body)
            .map_err(|e| StoreError::Message(e.to_string()))?;
        Ok(Some(definition))
    }

    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        let conn = self.lock().map_err(ClaimError::Store)?;
        claim_conn(&conn, id, owner, now)
    }

    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        let conn = self.lock().map_err(ClaimError::Store)?;
        heartbeat_conn(&conn, id, epoch, now)
    }

    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.release_now(id, epoch);
        Ok(())
    }

    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        if let Ok(conn) = self.lock() {
            release_conn(&conn, id, epoch);
        }
    }

    fn release_owner_now(&self, owner: &OwnerId) {
        if let Ok(conn) = self.lock() {
            release_owner_conn(&conn, owner);
        }
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
        blocker.busy_timeout(Duration::from_millis(0)).unwrap();
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
        use keel_rt::{SnapshotError, SCHEMA_VERSION};
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
        let def = store.workflow_definition(exec.id()).await.unwrap().unwrap();
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
        let def = store.workflow_definition(exec.id()).await.unwrap().unwrap();
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
        assert_eq!(
            n1, n0,
            "incremental persist must not DELETE unchanged nodes"
        );
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
        assert_eq!(store.get(exec.id()).await.unwrap().unwrap().revision, keep);
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
        store
            .persist_with_events(&exec, std::slice::from_ref(&ev))
            .await
            .unwrap();
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

    #[tokio::test(flavor = "current_thread")]
    async fn parked_ready_t_uses_column_omits_nested_json_keeps_last_error() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        let p = keel_rt::RetryPolicy::new(3, Duration::from_millis(50));
        let now = Timestamp(0);
        exec.apply(ApplyCmd::Start, &p, now).unwrap();
        exec.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        exec.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(keel_rt::NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
        let live_snap = exec.snapshot();
        let live = live_snap.node(&keel_rt::NodeId::new("a")).unwrap();
        assert!(live.last_error.is_some(), "live inspect has last_error");
        let t = match &live.state {
            keel_rt::NodeState::Ready {
                runnable_at: Some(at),
            } => *at,
            other => panic!("{other:?}"),
        };
        store.persist(&exec).await.unwrap();
        let (body, col): (String, Option<i64>) = {
            let conn = store.lock().unwrap();
            conn.query_row(
                "SELECT body, runnable_at FROM nodes WHERE execution_id = ?1",
                params![exec.id().as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };
        assert!(
            !body.contains("runnable_at"),
            "T is the column, not nested JSON, got {body}"
        );
        assert!(
            body.contains("last_error"),
            "compact Ready JSON keeps a short last_error, got {body}"
        );
        assert_eq!(col, Some(t.as_millis() as i64));
        let loaded = store.get(exec.id()).await.unwrap().unwrap();
        let node = loaded.node(&keel_rt::NodeId::new("a")).unwrap();
        assert_eq!(
            node.state,
            keel_rt::NodeState::Ready {
                runnable_at: Some(t)
            }
        );
        assert!(
            node.last_error.is_some(),
            "sqlite get() must restore last_error so inspect agrees with MemoryStore"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Production path: `persist()` → `upsert_dirty_nodes`. `put` is a full
    /// DELETE+INSERT and does not prove a T-only column UPDATE.
    #[tokio::test(flavor = "current_thread")]
    async fn dirty_persist_ready_t_to_t_prime_updates_only_runnable_at() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let mut exec = one_node();
        let p = keel_rt::RetryPolicy::new(3, Duration::from_millis(50));
        let now = Timestamp(0);
        exec.apply(ApplyCmd::Start, &p, now).unwrap();
        exec.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        exec.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(keel_rt::NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
        store.persist(&exec).await.unwrap();
        let body0: String = {
            let conn = store.lock().unwrap();
            conn.query_row(
                "SELECT body FROM nodes WHERE execution_id = ?1",
                params![exec.id().as_str()],
                |r| r.get(0),
            )
            .unwrap()
        };
        // Crash-resume shape: dirty empty, revision matches disk. Then only T
        // moves — persist() must UPDATE runnable_at and reuse the body.
        let mut live =
            Execution::from_snapshot(exec.definition().clone(), exec.snapshot()).unwrap();
        let later = Timestamp(9_000);
        live.retarget_ready_deadline(&keel_rt::NodeId::new("a"), Some(later))
            .unwrap();
        assert_eq!(live.dirty_nodes().len(), 1);
        store.persist(&live).await.unwrap();
        let (body1, col): (String, Option<i64>) = {
            let conn = store.lock().unwrap();
            conn.query_row(
                "SELECT body, runnable_at FROM nodes WHERE execution_id = ?1",
                params![exec.id().as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };
        assert_eq!(
            body1, body0,
            "dirty Ready{{T→T'}} persist must reuse JSON body"
        );
        assert_eq!(col, Some(9_000));
        let loaded = store.get(exec.id()).await.unwrap().unwrap();
        let node = loaded.node(&keel_rt::NodeId::new("a")).unwrap();
        assert_eq!(
            node.state,
            keel_rt::NodeState::Ready {
                runnable_at: Some(later)
            }
        );
        assert!(node.last_error.is_some());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_nested_runnable_at_json_loads_when_column_null() {
        let path = tmp();
        let id = ExecutionId::new();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE executions (
                   id TEXT PRIMARY KEY, revision INTEGER, schema_version INTEGER,
                   definition_hash TEXT, workflow_id TEXT, state TEXT, node_order TEXT);
                 CREATE TABLE nodes (
                   execution_id TEXT NOT NULL, node_id TEXT NOT NULL, body TEXT NOT NULL,
                   PRIMARY KEY (execution_id, node_id));
                 CREATE TABLE definitions (hash TEXT PRIMARY KEY, body BLOB);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO executions
                   (id, revision, schema_version, definition_hash, workflow_id, state, node_order)
                 VALUES (?1, 1, 1, 'legacy', 'wf', '\"Running\"', '[\"a\"]')",
                params![id.as_str()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO nodes (execution_id, node_id, body) VALUES (?1, 'a', ?2)",
                params![
                    id.as_str(),
                    r#"{"state":{"Ready":{"runnable_at":77}},"attempt":1}"#
                ],
            )
            .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .node(&keel_rt::NodeId::new("a"))
                .unwrap()
                .state,
            keel_rt::NodeState::Ready {
                runnable_at: Some(Timestamp(77))
            }
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lease_columns_migrate_on_legacy_executions_table() {
        let path = tmp();
        let id = ExecutionId::new();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE executions (
                   id TEXT PRIMARY KEY, revision INTEGER, schema_version INTEGER,
                   definition_hash TEXT, workflow_id TEXT, state TEXT, node_order TEXT);
                 CREATE TABLE nodes (
                   execution_id TEXT NOT NULL, node_id TEXT NOT NULL, body TEXT NOT NULL,
                   PRIMARY KEY (execution_id, node_id));
                 CREATE TABLE definitions (hash TEXT PRIMARY KEY, body BLOB);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO executions
                   (id, revision, schema_version, definition_hash, workflow_id, state, node_order)
                 VALUES (?1, 1, 1, 'legacy', 'wf', '\"Running\"', '[\"a\"]')",
                params![id.as_str()],
            )
            .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        let a = OwnerId::new();
        let e = store.claim(&id, &a, Timestamp(0)).await.unwrap();
        assert_eq!(e, LeaseEpoch(1));
        match store.claim(&id, &OwnerId::new(), Timestamp(0)).await {
            Err(ClaimError::ClaimedElsewhere) => {}
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_does_not_use_insert_or_replace() {
        let lib = include_str!("lib.rs");
        let claim = lib.split("fn claim_conn").nth(1).expect("claim_conn");
        let claim = claim.split("fn heartbeat_conn").next().unwrap();
        assert!(
            !claim.contains("INSERT OR REPLACE"),
            "claim must not use INSERT OR REPLACE"
        );
        assert!(claim.contains("BEGIN IMMEDIATE"));
        assert!(claim.contains("INSERT INTO executions"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_owner_claim_refreshes_without_bumping_epoch() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let id = ExecutionId::new();
        let owner = OwnerId::new();
        let e1 = store.claim(&id, &owner, Timestamp(0)).await.unwrap();
        let e2 = store.claim(&id, &owner, Timestamp(1)).await.unwrap();
        assert_eq!(e1, e2);
        store.heartbeat(&id, e1, Timestamp(2)).await.unwrap();
        store.release(&id, e1).await.unwrap();
        store
            .claim(&id, &OwnerId::new(), Timestamp(2))
            .await
            .unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_wrong_epoch_is_claimed_elsewhere() {
        let path = tmp();
        let store = SqliteStore::open(&path).unwrap();
        let id = ExecutionId::new();
        let owner = OwnerId::new();
        store.claim(&id, &owner, Timestamp(0)).await.unwrap();
        match store.heartbeat(&id, LeaseEpoch(99), Timestamp(0)).await {
            Err(ClaimError::ClaimedElsewhere) => {}
            other => panic!("{other:?}"),
        }
        store.release_now(&id, LeaseEpoch(99));
        store.release_owner_now(&owner);
        let _ = std::fs::remove_file(&path);
    }
}
