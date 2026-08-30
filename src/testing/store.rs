use crate::domain::ids::ExecutionId;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::runtime::store::{MemoryStore, StateStore, StoreError};
use crate::testing::failpoint;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Memory store that fails on the Nth `put` (1-based). In-memory apply is not rolled back.
pub struct FailingStore {
    inner: MemoryStore,
    fail_on_nth_put: usize,
    puts: AtomicUsize,
}

impl FailingStore {
    pub fn fail_on_nth_put(n: usize) -> Self {
        Self {
            inner: MemoryStore::new(),
            fail_on_nth_put: n,
            puts: AtomicUsize::new(0),
        }
    }

    pub fn inner(&self) -> &MemoryStore {
        &self.inner
    }

    pub fn puts(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StateStore for FailingStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        if failpoint::take("store.put") {
            return Err(StoreError::Message("failpoint store.put".into()));
        }
        let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_nth_put {
            return Err(StoreError::Message(format!("failing store: put #{n}")));
        }
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
}

/// Records every put in order. Optional get sequence for scripted reads.
#[derive(Clone, Default)]
pub struct SequenceStore {
    inner: MemoryStore,
    puts: Arc<Mutex<Vec<ExecutionSnapshot>>>,
}

impl SequenceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn puts(&self) -> Vec<ExecutionSnapshot> {
        self.puts.lock().expect("sequence store").clone()
    }
}

#[async_trait]
impl StateStore for SequenceStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        self.puts.lock().expect("sequence store").push(snapshot.clone());
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }
}
