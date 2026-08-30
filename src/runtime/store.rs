use crate::domain::ids::ExecutionId;
use crate::domain::snapshot::ExecutionSnapshot;
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
}

#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<HashMap<ExecutionId, ExecutionSnapshot>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.inner
            .lock()
            .expect("memory store")
            .insert(snapshot.execution_id.clone(), snapshot.clone());
        Ok(())
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        Ok(self.inner.lock().expect("memory store").get(id).cloned())
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
}
