use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::ExecutionId;
use crate::domain::snapshot::ExecutionSnapshot;
use crate::domain::state::Execution;
use crate::domain::time::Timestamp;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// Default lease length. Store `claim` / `heartbeat` use Clock `now` + this.
/// Not a wall sleep in apply.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Fencing token. Persist / complete with a stale epoch is rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LeaseEpoch(pub u64);

/// Runtime identity for a store lease. Two Runtimes never share an owner.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OwnerId(Arc<str>);

impl OwnerId {
    pub fn new() -> Self {
        Self(Arc::from(format!("owner-{}", ExecutionId::new().as_str())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for OwnerId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ClaimError {
    #[error("execution claimed elsewhere")]
    ClaimedElsewhere,
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("state store error: {0}")]
    Message(String),
    #[error("stale snapshot put: store has revision {found}, attempted {attempted}")]
    Stale { found: u64, attempted: u64 },
    #[error("stale lease epoch: store has {found}, attempted {attempted}")]
    StaleEpoch { found: u64, attempted: u64 },
}

/// An opt-in durable store could not initialize a new execution.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InitializeError {
    #[error("store does not support durable initialization")]
    Unsupported,
    #[error("execution already exists")]
    AlreadyExists,
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn reject_stale(found: u64, attempted: u64) -> Result<(), StoreError> {
    if found > attempted {
        Err(StoreError::Stale { found, attempted })
    } else {
        Ok(())
    }
}

fn lease_until(now: Timestamp) -> Timestamp {
    now.saturating_add(DEFAULT_LEASE_TTL)
}

fn lease_live(until: Timestamp, now: Timestamp) -> bool {
    until > now
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError>;
    /// Last persisted snapshot. A lease reserved before its first snapshot is
    /// absent (`None`), not a malformed snapshot. Absence does not release its
    /// lease or prove that an executor has never run.
    async fn get(&self, id: &ExecutionId) -> Result<Option<ExecutionSnapshot>, StoreError>;

    /// Atomically reserve a fresh execution ID and commit its Created snapshot,
    /// all nodes, and workflow definition, returning the owner's lease epoch.
    /// Success guarantees recovery after process death under the adapter's
    /// documented durability mode. Existing IDs must return `AlreadyExists`.
    /// Default is unsupported; `persist` alone cannot promise durable recovery.
    ///
    /// Implementations must roll back an incomplete transaction on error/panic
    /// or cancellation. If commit happened before cancellation or a lost reply,
    /// retain the recoverable snapshot; its lease may expire normally. Never
    /// spawn background initialization that outlives the cancelled future.
    async fn initialize(
        &self,
        exec: &Execution,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, InitializeError> {
        let _ = (exec, owner, now);
        Err(InitializeError::Unsupported)
    }

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

    /// Same as [`Self::persist`]. File adapters may write `events` in the
    /// snapshot transaction. The kernel never resumes from those rows.
    /// Adapters must override this to see `events`; the default drops them.
    /// Do not insert the same slice again when the stored revision already
    /// matches (`SqliteStore` skips equal-revision inserts).
    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[crate::domain::events::Event],
    ) -> Result<(), StoreError> {
        let _ = events;
        self.persist(exec).await
    }

    /// Definition last persisted with this execution. A lease-only reservation
    /// has no definition. A missing definition for a real snapshot is an error
    /// in adapters that persist definitions. Default: none.
    /// The store does not interpret DAG readiness; it returns the bytes' DAG.
    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        let _ = id;
        Ok(None)
    }

    /// Take store-level ownership of one execution. A live lease for another
    /// owner is [`ClaimError::ClaimedElsewhere`]. Same owner refreshes.
    /// Default: epoch 1 (test / noop stores).
    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        let _ = (id, owner, now);
        Ok(LeaseEpoch(1))
    }

    /// Extend a live lease. Wrong epoch → [`ClaimError::ClaimedElsewhere`].
    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        let _ = (id, epoch, now);
        Ok(())
    }

    /// Drop a lease. Wrong epoch is a no-op.
    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        let _ = (id, epoch);
        Ok(())
    }

    /// Sync release for [`crate::Runtime`] Drop. Default no-op.
    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        let _ = (id, epoch);
    }

    /// Release every lease held by `owner`. Used on Runtime Drop.
    fn release_owner_now(&self, owner: &OwnerId) {
        let _ = owner;
    }
}

struct Stored {
    snap: ExecutionSnapshot,
    definition: std::sync::Arc<WorkflowDefinition>,
}

struct Lease {
    owner: OwnerId,
    epoch: u64,
    until: Timestamp,
}

#[derive(Default)]
struct MemoryInner {
    snaps: HashMap<ExecutionId, Stored>,
    leases: HashMap<ExecutionId, Lease>,
}

#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<MemoryInner>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recover from a poisoned mutex instead of panicking the scheduler.
    /// A panic while a `put`/`get`/`persist` held the lock used to kill the
    /// next persist via `expect` even though `CatchUnwind` caught the first
    /// panic. Poison means the last holder panicked; the map is still usable.
    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn reject_fence(inner: &MemoryInner, exec: &Execution) -> Result<(), StoreError> {
        let Some(lease) = inner.leases.get(exec.id()) else {
            return Ok(());
        };
        let attempted = exec.fence_epoch().unwrap_or(0);
        if attempted != lease.epoch {
            return Err(StoreError::StaleEpoch {
                found: lease.epoch,
                attempted,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn put(&self, snapshot: &ExecutionSnapshot) -> Result<(), StoreError> {
        let mut g = self.lock();
        if let Some(stored) = g.snaps.get(&snapshot.execution_id) {
            reject_stale(stored.snap.revision, snapshot.revision)?;
            if stored.snap.revision == snapshot.revision {
                return Ok(());
            }
        }
        match g.snaps.get_mut(&snapshot.execution_id) {
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
        Ok(self.lock().snaps.get(id).map(|s| s.snap.clone()))
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        // Mutex is not held across `.await`. The async fn awaits nothing while
        // `g` is live; poison recovery stays in `lock()`.
        let mut g = self.lock();
        Self::reject_fence(&g, exec)?;
        if let Some(stored) = g.snaps.get(exec.id()) {
            reject_stale(stored.snap.revision, exec.revision())?;
            if stored.snap.revision == exec.revision() {
                return Ok(());
            }
        }
        match g.snaps.get_mut(exec.id()) {
            Some(stored) => {
                stored.snap.revision = exec.revision();
                stored.snap.state = exec.state();
                for slot in exec.dirty_slots() {
                    let id = exec.node_id_at(*slot).clone();
                    stored.snap.nodes.insert(id, exec.node_snapshot_at(*slot));
                }
            }
            None => {
                g.snaps.insert(
                    exec.id().clone(),
                    Stored {
                        snap: exec.snapshot(),
                        definition: exec.definition_arc().clone(),
                    },
                );
            }
        }
        Ok(())
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[crate::domain::events::Event],
    ) -> Result<(), StoreError> {
        let _ = events;
        self.persist(exec).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        Ok(self.lock().snaps.get(id).map(|s| (*s.definition).clone()))
    }

    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        let mut g = self.lock();
        if let Some(lease) = g.leases.get(id) {
            if lease.owner != *owner && lease_live(lease.until, now) {
                return Err(ClaimError::ClaimedElsewhere);
            }
            if lease.owner == *owner && lease_live(lease.until, now) {
                let epoch = lease.epoch;
                g.leases.get_mut(id).unwrap().until = lease_until(now);
                return Ok(LeaseEpoch(epoch));
            }
        }
        let epoch = g
            .leases
            .get(id)
            .map(|l| l.epoch.saturating_add(1))
            .unwrap_or(1);
        g.leases.insert(
            id.clone(),
            Lease {
                owner: owner.clone(),
                epoch,
                until: lease_until(now),
            },
        );
        Ok(LeaseEpoch(epoch))
    }

    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        let mut g = self.lock();
        match g.leases.get_mut(id) {
            Some(lease) if lease.epoch == epoch.0 => {
                lease.until = lease_until(now);
                Ok(())
            }
            _ => Err(ClaimError::ClaimedElsewhere),
        }
    }

    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        self.release_now(id, epoch);
        Ok(())
    }

    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        let mut g = self.lock();
        if g.leases.get(id).is_some_and(|l| l.epoch == epoch.0) {
            g.leases.remove(id);
        }
    }

    fn release_owner_now(&self, owner: &OwnerId) {
        let mut g = self.lock();
        g.leases.retain(|_, l| l.owner != *owner);
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

    async fn initialize(
        &self,
        exec: &Execution,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, InitializeError> {
        (**self).initialize(exec, owner, now).await
    }

    fn is_noop(&self) -> bool {
        (**self).is_noop()
    }

    async fn persist(&self, exec: &Execution) -> Result<(), StoreError> {
        (**self).persist(exec).await
    }

    async fn persist_with_events(
        &self,
        exec: &Execution,
        events: &[crate::domain::events::Event],
    ) -> Result<(), StoreError> {
        (**self).persist_with_events(exec, events).await
    }

    async fn workflow_definition(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        (**self).workflow_definition(id).await
    }

    async fn claim(
        &self,
        id: &ExecutionId,
        owner: &OwnerId,
        now: Timestamp,
    ) -> Result<LeaseEpoch, ClaimError> {
        (**self).claim(id, owner, now).await
    }

    async fn heartbeat(
        &self,
        id: &ExecutionId,
        epoch: LeaseEpoch,
        now: Timestamp,
    ) -> Result<(), ClaimError> {
        (**self).heartbeat(id, epoch, now).await
    }

    async fn release(&self, id: &ExecutionId, epoch: LeaseEpoch) -> Result<(), StoreError> {
        (**self).release(id, epoch).await
    }

    fn release_now(&self, id: &ExecutionId, epoch: LeaseEpoch) {
        (**self).release_now(id, epoch);
    }

    fn release_owner_now(&self, owner: &OwnerId) {
        (**self).release_owner_now(owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::definition::WorkflowDefinition;
    use crate::domain::ids::ExecutionId;
    use crate::domain::time::Timestamp;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::time::Duration;

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
    async fn persist_with_events_is_persist() {
        let store = MemoryStore::new();
        let exec = one_node();
        store
            .persist_with_events(&exec, &[])
            .await
            .expect("MemoryStore ignores the event slice");
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
        let stored = store.get(exec.id()).await.unwrap().unwrap();
        assert!(
            stored.definition_hash.is_empty() || stored.definition_hash == def.content_hash(),
            "MemoryStore may leave hash empty until a file adapter computes it"
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
        assert_eq!(store.get(exec.id()).await.unwrap().unwrap().revision, found);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn memory_store_parked_ready_round_trips_last_error() {
        use crate::domain::ids::NodeId;
        use crate::domain::outcome::NodeOutcome;
        use crate::domain::policy::RetryPolicy;
        use crate::domain::state::{ApplyCmd, NodeState};
        use crate::domain::time::Timestamp;
        use std::time::Duration;

        let store = MemoryStore::new();
        let mut exec = one_node();
        let p = RetryPolicy::new(3, Duration::from_millis(50));
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
                outcome: Ok(NodeOutcome::TimedOut),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(
            exec.snapshot()
                .node(&NodeId::new("a"))
                .unwrap()
                .last_error
                .is_some(),
            "live inspect has last_error on the retry park"
        );
        store.persist(&exec).await.unwrap();
        let loaded = store.get(exec.id()).await.unwrap().unwrap();
        let node = loaded.node(&NodeId::new("a")).unwrap();
        assert!(matches!(
            node.state,
            NodeState::Ready {
                runnable_at: Some(_)
            }
        ));
        assert!(
            node.last_error.is_some(),
            "MemoryStore persist/get must keep last_error on Ready{{T}}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_second_owner_is_claimed_elsewhere() {
        let store = MemoryStore::new();
        let id = ExecutionId::new();
        let a = OwnerId::new();
        let b = OwnerId::new();
        let e1 = store.claim(&id, &a, Timestamp(0)).await.unwrap();
        assert_eq!(e1, LeaseEpoch(1));
        match store.claim(&id, &b, Timestamp(0)).await {
            Err(ClaimError::ClaimedElsewhere) => {}
            other => panic!("{other:?}"),
        }
        let again = store.claim(&id, &a, Timestamp(0)).await.unwrap();
        assert_eq!(
            again,
            LeaseEpoch(1),
            "same owner refresh must not bump epoch"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_after_ttl_steals_and_bumps_epoch() {
        let store = MemoryStore::new();
        let id = ExecutionId::new();
        let a = OwnerId::new();
        let b = OwnerId::new();
        store.claim(&id, &a, Timestamp(0)).await.unwrap();
        match store
            .claim(&id, &b, Timestamp(DEFAULT_LEASE_TTL.as_millis() as u64 - 1))
            .await
        {
            Err(ClaimError::ClaimedElsewhere) => {}
            other => panic!("before TTL B must fail, got {other:?}"),
        }
        let e2 = store
            .claim(&id, &b, Timestamp(DEFAULT_LEASE_TTL.as_millis() as u64))
            .await
            .unwrap();
        assert_eq!(e2, LeaseEpoch(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_epoch_persist_is_rejected() {
        let store = MemoryStore::new();
        let mut exec = one_node();
        let a = OwnerId::new();
        let b = OwnerId::new();
        let e1 = store.claim(exec.id(), &a, Timestamp(0)).await.unwrap();
        exec.set_fence_epoch(e1.0);
        store.persist(&exec).await.unwrap();
        let e2 = store
            .claim(
                exec.id(),
                &b,
                Timestamp(0).saturating_add(DEFAULT_LEASE_TTL + Duration::from_millis(1)),
            )
            .await
            .unwrap();
        assert_eq!(e2, LeaseEpoch(2));
        let err = store.persist(&exec).await.unwrap_err();
        assert_eq!(
            err,
            StoreError::StaleEpoch {
                found: 2,
                attempted: 1
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_wrong_epoch_is_claimed_elsewhere() {
        let store = MemoryStore::new();
        let id = ExecutionId::new();
        let owner = OwnerId::new();
        store.claim(&id, &owner, Timestamp(0)).await.unwrap();
        match store.heartbeat(&id, LeaseEpoch(99), Timestamp(0)).await {
            Err(ClaimError::ClaimedElsewhere) => {}
            other => panic!("{other:?}"),
        }
        store
            .heartbeat(&id, LeaseEpoch(1), Timestamp(0))
            .await
            .unwrap();
        store.release(&id, LeaseEpoch(99)).await.unwrap();
        store.release_now(&id, LeaseEpoch(99));
        store.release(&id, LeaseEpoch(1)).await.unwrap();
        let e = store
            .claim(&id, &OwnerId::new(), Timestamp(0))
            .await
            .unwrap();
        assert_eq!(e, LeaseEpoch(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn noop_store_lease_defaults() {
        let store = NoopStore;
        let id = ExecutionId::new();
        let owner = OwnerId::new();
        let e = store.claim(&id, &owner, Timestamp(0)).await.unwrap();
        assert_eq!(e, LeaseEpoch(1));
        store.heartbeat(&id, e, Timestamp(0)).await.unwrap();
        store.release(&id, e).await.unwrap();
        store.release_now(&id, e);
        store.release_owner_now(&owner);
        assert!(store.is_noop());
        assert_eq!(OwnerId::default().as_str().starts_with("owner-"), true);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persist_without_lease_does_not_require_epoch() {
        let store = MemoryStore::new();
        let exec = one_node();
        store.persist(&exec).await.unwrap();
        store.persist(&exec).await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn release_owner_drops_all_leases() {
        let store = MemoryStore::new();
        let owner = OwnerId::new();
        let a = ExecutionId::new();
        let b = ExecutionId::new();
        store.claim(&a, &owner, Timestamp(0)).await.unwrap();
        store.claim(&b, &owner, Timestamp(0)).await.unwrap();
        store.release_owner_now(&owner);
        store
            .claim(&a, &OwnerId::new(), Timestamp(0))
            .await
            .unwrap();
    }
}
