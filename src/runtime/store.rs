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
}

struct Stored {
    snap: ExecutionSnapshot,
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
        self.lock().insert(
            snapshot.execution_id.clone(),
            Stored {
                snap: snapshot.clone(),
            },
        );
        Ok(())
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        Ok(self.lock().get(id).map(|s| s.snap.clone()))
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let mut g = self.lock();
        match g.get_mut(exec.id()) {
            Some(stored) => {
                stored.snap.revision = exec.revision();
                stored.snap.state = exec.state();
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
                    },
                );
            }
        }
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::definition::WorkflowDefinition;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[tokio::test(flavor = "current_thread")]
    async fn poisoned_mutex_recovers_on_next_persist() {
        let store = MemoryStore::new();
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _g = store.inner.lock().unwrap();
            panic!("poison memory store");
        }));
        assert!(poisoned.is_err());

        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        let exec = Execution::new(def);
        store
            .persist(&exec)
            .await
            .expect("poisoned MemoryStore must recover via into_inner");
        assert!(store.get(exec.id()).await.unwrap().is_some());
        store.put(&exec.snapshot()).await.unwrap();
        assert!(store.get(exec.id()).await.unwrap().is_some());
    }
}
