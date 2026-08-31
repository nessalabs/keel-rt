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
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner.lock().expect("memory store").insert(
            snapshot.execution_id.clone(),
            Stored {
                snap: snapshot.clone(),
            },
        );
        Ok(())
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        Ok(self
            .inner
            .lock()
            .expect("memory store")
            .get(id)
            .map(|s| s.snap.clone()))
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        let mut g = self.inner.lock().expect("memory store");
        match g.get_mut(exec.id()) {
            Some(stored) => {
                stored.snap.revision = exec.revision();
                stored.snap.state = exec.state();
                for slot in exec.dirty_slots() {
                    let id = exec.node_id_at(*slot).clone();
                    stored
                        .snap
                        .nodes
                        .insert(id, exec.node_snapshot_at(*slot));
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
