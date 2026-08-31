use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::ExecutionId;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::Execution;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("state store error: {0}")]
    Message(String),
    #[error("stale snapshot put: store has revision {found}, attempted {attempted}")]
    Stale { found: u64, attempted: u64 },
}

fn reject_stale(found: u64, attempted: u64) -> Result<(), StoreError> {
    if found > attempted {
        Err(StoreError::Stale { found, attempted })
    } else {
        Ok(())
    }
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError>;
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError>;

    /// Cheap skip for `snapshot()` + `put` on the apply path.
    fn is_noop(&self) -> bool {
        false
    }

    /// Persist the live aggregate. Default builds a full snapshot and `put`s it.
    /// [`MemoryStore`] updates only dirty node slots after the first write.
    /// See `docs/adr/0002-store-persist-live-aggregate.md`.
    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.put(&exec.snapshot()).await
    }

    /// Definition last persisted with this execution. Default: none.
    /// The store does not interpret DAG readiness; it returns the bytes' DAG.
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        let _ = id;
        Ok(None)
    }
}

struct Stored {
    snap: ExecutionSnapshot,
    definition: WorkflowDefinition,
}

#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<HashMap<ExecutionId, Stored>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recover from a poisoned mutex instead of panicking the scheduler.
    /// A panic while a `put`/`get`/`persist` held the lock used to kill the
    /// next persist via `expect` even though `CatchUnwind` caught the first
    /// panic. Poison means the last holder panicked; the map is still usable.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ExecutionId, Stored>> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        let mut g = self.lock();
        if let Some(stored) = g.get(&snapshot.execution_id) {
            reject_stale(stored.snap.revision, snapshot.revision)?;
            if stored.snap.revision == snapshot.revision {
                return Ok(());
            }
        }
        match g.get_mut(&snapshot.execution_id) {
            Some(stored) => stored.snap = snapshot.clone(),
            None => {
                // put without a prior persist cannot invent a definition.
                return Err(StoreError::Message(
                    "put requires an existing execution (persist first)".into(),
                ));
            }
        }
        Ok(())
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        Ok(self.lock().get(id).map(|s| s.snap.clone()))
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        // Mutex is not held across `.await`. The async fn awaits nothing while
        // `g` is live; poison recovery stays in `lock()`.
        let mut g = self.lock();
        if let Some(stored) = g.get(exec.id()) {
            reject_stale(stored.snap.revision, exec.revision())?;
            if stored.snap.revision == exec.revision() {
                return Ok(());
            }
        }
        match g.get_mut(exec.id()) {
            Some(stored) => {
                stored.snap.revision = exec.revision();
                stored.snap.state = exec.state();
                stored.snap.definition_hash = exec.definition().content_hash();
                stored.definition = exec.definition().clone();
                for slot in exec.dirty_slots() {
                    let id = exec.node_id_at(*slot).clone();
                    stored.snap.nodes.insert(id, exec.node_snapshot_at(*slot));
                }
            }
            None => {
                g.insert(
                    exec.id().clone(),
                    Stored {
                        snap: exec.snapshot(),
                        definition: exec.definition().clone(),
                    },
                );
            }
        }
        Ok(())
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        Ok(self.lock().get(id).map(|s| s.definition.clone()))
    }
}

#[derive(Clone, Default)]
pub struct NoopStore;

#[async_trait]
impl StateStore for NoopStore {
    async fn put(&self, _snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        Ok(())
    }

    async fn get(&self, _id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        Ok(None)
    }

    fn is_noop(&self) -> bool {
        true
    }
}

#[async_trait]
impl StateStore for Arc<dyn StateStore> {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        (**self).put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        (**self).get(id).await
    }

    fn is_noop(&self) -> bool {
        (**self).is_noop()
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        (**self).persist(exec).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        (**self).workflow_definition(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::definition::WorkflowDefinition;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn one_node() -> Execution {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        Execution::new(def)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn poisoned_mutex_recovers_on_next_persist() {
        let store = MemoryStore::new();
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _g = store.inner.lock().unwrap();
            panic!("poison memory store");
        }));
        assert!(poisoned.is_err());

        let exec = one_node();
        store
            .persist(&exec)
            .await
            .expect("poisoned MemoryStore must recover via into_inner");
        assert!(store.get(exec.id()).await.unwrap().is_some());
        store.put(&exec.snapshot()).await.unwrap();
        assert!(store.get(exec.id()).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persist_keeps_definition_beside_snapshot() {
        let store = MemoryStore::new();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        let def = store
            .workflow_definition(exec.id())
            .await
            .unwrap()
            .expect("definition stored");
        assert_eq!(def.id().as_str(), "wf");
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().definition_hash,
            def.content_hash()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_put_does_not_clobber() {
        use crate::domain::policy::AcceptPolicy;
        use crate::domain::state::ApplyCmd;
        use crate::domain::time::Timestamp;

        let store = MemoryStore::new();
        let mut exec = one_node();
        store.persist(&exec).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        let keep_rev = store.get(exec.id()).await.unwrap().unwrap().revision;
        let mut older = store.get(exec.id()).await.unwrap().unwrap();
        older.revision = 0;
        let err = store.put(&older).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::Stale {
                found: keep_rev,
                attempted: 0
            }
        );
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().revision,
            keep_rev
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn equal_revision_put_is_idempotent() {
        let store = MemoryStore::new();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        let snap = store.get(exec.id()).await.unwrap().unwrap();
        store.put(&snap).await.unwrap();
        assert_eq!(store.get(exec.id()).await.unwrap().unwrap(), snap);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn put_without_persist_does_not_invent_a_definition() {
        let store = MemoryStore::new();
        let exec = one_node();
        let err = store.put(&exec.snapshot()).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Message(ref m) if m.contains("persist first")),
            "{err:?}"
        );
        assert!(store.get(exec.id()).await.unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_persist_does_not_clobber() {
        use crate::domain::policy::AcceptPolicy;
        use crate::domain::state::ApplyCmd;
        use crate::domain::time::Timestamp;

        let store = MemoryStore::new();
        let mut exec = one_node();
        store.persist(&exec).await.unwrap();
        exec.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        store.persist(&exec).await.unwrap();
        let found = store.get(exec.id()).await.unwrap().unwrap().revision;
        assert!(found > 0);
        exec.revision = 0;
        let err = store.persist(&exec).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::Stale {
                found,
                attempted: 0
            }
        );
        assert_eq!(
            store.get(exec.id()).await.unwrap().unwrap().revision,
            found
        );
    }
}
