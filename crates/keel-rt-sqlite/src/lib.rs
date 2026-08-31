//! File-backed [`StateStore`] for keel-rt. The kernel never names this crate.
//!
//! Delete this package without editing `scheduler.rs`. Persist is inline
//! (no queue) — ADR 0001 still applies.

use async_trait::async_trait;
use keel_rt::{
    Execution, ExecutionId, ExecutionSnapshot, StateStore, StoreError, WorkflowDefinition,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS executions (
  id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  definition_hash TEXT NOT NULL,
  snapshot_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS definitions (
  hash TEXT PRIMARY KEY,
  body BLOB NOT NULL
);
";

/// One connection, shared. Sync sqlite work runs inside async persist
/// the same way [`keel_rt::MemoryStore`] blocks — no persist queue.
#[derive(Clone)]
pub struct SqliteStore {
    path: PathBuf,
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path).map_err(store_err)?;
        conn.busy_timeout(Duration::from_secs(5))
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

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        Ok(self.conn.lock().unwrap_or_else(|p| p.into_inner()))
    }

    fn write_snapshot(
        conn: &Connection,
        snapshot: &ExecutionSnapshot,
        definition: Option<&WorkflowDefinition>,
    ) -> Result<(), StoreError> {
        if let Some(def) = definition {
            conn.execute(
                "INSERT OR IGNORE INTO definitions (hash, body) VALUES (?1, ?2)",
                params![def.content_hash().as_str(), def.durable_bytes()],
            )
            .map_err(store_err)?;
        }
        let json = serde_json::to_string(snapshot).map_err(|e| StoreError::Message(e.to_string()))?;
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
                let n = conn
                    .execute(
                        "UPDATE executions SET revision = ?1, schema_version = ?2,
                         definition_hash = ?3, snapshot_json = ?4
                         WHERE id = ?5 AND revision < ?1",
                        params![
                            snapshot.revision as i64,
                            snapshot.schema_version as i64,
                            snapshot.definition_hash.as_str(),
                            json,
                            snapshot.execution_id.as_str(),
                        ],
                    )
                    .map_err(store_err)?;
                if n == 0 {
                    return Err(StoreError::Message("lost cas race".into()));
                }
                Ok(())
            }
            None => {
                if definition.is_none() {
                    return Err(StoreError::Message(
                        "put requires an existing execution (persist first)".into(),
                    ));
                }
                conn.execute(
                    "INSERT INTO executions
                     (id, revision, schema_version, definition_hash, snapshot_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        snapshot.execution_id.as_str(),
                        snapshot.revision as i64,
                        snapshot.schema_version as i64,
                        snapshot.definition_hash.as_str(),
                        json,
                    ],
                )
                .map_err(store_err)?;
                Ok(())
            }
        }
    }
}

fn store_err(e: rusqlite::Error) -> StoreError {
    StoreError::Message(e.to_string())
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        let conn = self.lock()?;
        Self::write_snapshot(&conn, snapshot, None)
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                "SELECT snapshot_json FROM executions WHERE id = ?1",
                params![id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(store_err)?;
        match json {
            None => Ok(None),
            Some(json) => serde_json::from_str(&json)
                .map(Some)
                .map_err(|e| StoreError::Message(e.to_string())),
        }
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let _ = exec.definition().content_hash();
        let snap = exec.snapshot();
        let conn = self.lock()?;
        Self::write_snapshot(&conn, &snap, Some(exec.definition()))
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
}
