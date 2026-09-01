use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::ExecutionId;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::Execution;
use crate::runtime::store::{MemoryStore, StateStore, StoreError};
use crate::testing::failpoint;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Memory store that fails on the Nth `put` (1-based). In-memory apply is not rolled back.
pub struct FailingStore {
    inner: MemoryStore,
    fail_on_nth_put: usize,
    fail_all: bool,
    puts: AtomicUsize,
}

impl FailingStore {
    pub fn fail_on_nth_put(n: usize) -> Self {
        Self {
            inner: MemoryStore::new(),
            fail_on_nth_put: n,
            fail_all: false,
            puts: AtomicUsize::new(0),
        }
    }

    /// Every `put` fails. In-memory apply must still progress.
    pub fn fail_all() -> Self {
        Self {
            inner: MemoryStore::new(),
            fail_on_nth_put: 0,
            fail_all: true,
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
        if self.fail_all || n == self.fail_on_nth_put {
            return Err(StoreError::Message(format!("failing store: put #{n}")));
        }
        self.inner.put(snapshot).await
    }

    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError> {
        self.inner.get(id).await
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[crate::domain::events::Event],
    ) -> Result<(), StoreError> {
        if failpoint::take("store.put") {
            return Err(StoreError::Message("failpoint store.put".into()));
        }
        let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_all || n == self.fail_on_nth_put {
            return Err(StoreError::Message(format!("failing store: put #{n}")));
        }
        self.inner.persist_with_events(exec, events).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}

/// Records every persist in order. Optional get sequence for scripted reads.
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

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        self.persist_with_events(exec, &[]).await
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        _events: &[crate::domain::events::Event],
    ) -> Result<(), StoreError> {
        self.puts
            .lock()
            .expect("sequence store")
            .push(exec.snapshot());
        self.inner.persist_with_events(exec, _events).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.inner.workflow_definition(id).await
    }
}
