//! Apply loop. Current-thread analog: one event channel, FIFO ready queue.
//! This module never awaits `execute()` and does not name resource types.

use crate::domain::definition::WorkflowDefinition;
use crate::domain::events::Event as KernelEvent;
use crate::domain::ids::{NodeId, NodeSlot};
use crate::domain::policy::Policy;
use crate::domain::state::{ApplyCmd, Execution, ExecutionState};
use crate::runtime::executor::{ExecutionContext, Executor, ExecutorRegistry};
use crate::runtime::inject::{Event, EventTx};
use crate::runtime::sink::EventSink;
use crate::runtime::spawn::{CatchUnwind, SpawnSet};
use crate::runtime::store::StateStore;
use crate::runtime::time::{Clock, Timestamp};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Scheduler {
    exec: Execution,
    policy: Arc<dyn Policy>,
    store: Arc<dyn StateStore>,
    sink: Arc<dyn EventSink>,
    clock: Arc<dyn Clock>,
    spawn: SpawnSet,
    ready: VecDeque<NodeSlot>,
    queued: Vec<u8>,
    available: usize,
    held: Vec<u8>,
    executors: Vec<Option<Arc<dyn Executor>>>,
    cancel: CancellationToken,
    state_tx: watch::Sender<ExecutionState>,
    last_persisted: u64,
    pending_events: Vec<KernelEvent>,
}

/// Extra persist attempts on `Event::Shutdown` after the command that produced
/// the snapshot already tried once. Eight rides out a recovering backend
/// without turning Drop into an infinite hang.
const SHUTDOWN_PERSIST_ATTEMPTS: u32 = 8;

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        definition: WorkflowDefinition,
        policy: Arc<dyn Policy>,
        store: Arc<dyn StateStore>,
        sink: Arc<dyn EventSink>,
        registry: ExecutorRegistry,
        clock: Arc<dyn Clock>,
        tx: EventTx,
        concurrency: usize,
        cancel: CancellationToken,
        state_tx: watch::Sender<ExecutionState>,
    ) -> Self {
        let n = definition.len();
        let executors: Vec<Option<Arc<dyn Executor>>> = (0..n)
            .map(|i| registry.get(definition.executor_at(NodeSlot(i))))
            .collect();
        let exec = Execution::new(definition);
        let _ = state_tx.send(exec.state());
        Self {
            spawn: SpawnSet::new(tx.clone(), n),
            exec,
            policy,
            store,
            sink,
            clock,
            ready: VecDeque::new(),
            queued: vec![0u8; n],
            available: concurrency.max(1),
            held: vec![0u8; n],
            executors,
            cancel,
            state_tx,
            last_persisted: 0,
            pending_events: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_execution(
        exec: Execution,
        policy: Arc<dyn Policy>,
        store: Arc<dyn StateStore>,
        sink: Arc<dyn EventSink>,
        registry: ExecutorRegistry,
        clock: Arc<dyn Clock>,
        tx: EventTx,
        concurrency: usize,
        cancel: CancellationToken,
        state_tx: watch::Sender<ExecutionState>,
    ) -> Self {
        let n = exec.definition().len();
        let last_persisted = exec.revision();
        let executors: Vec<Option<Arc<dyn Executor>>> = (0..n)
            .map(|i| registry.get(exec.definition().executor_at(NodeSlot(i))))
            .collect();
        let _ = state_tx.send(exec.state());
        Self {
            spawn: SpawnSet::new(tx.clone(), n),
            exec,
            policy,
            store,
            sink,
            clock,
            ready: VecDeque::new(),
            queued: vec![0u8; n],
            available: concurrency.max(1),
            held: vec![0u8; n],
            executors,
            cancel,
            state_tx,
            // Snapshot on disk is already this revision. A no-op Waiting
            // restore must not BEGIN IMMEDIATE just to write zero events.
            last_persisted,
            pending_events: Vec::new(),
        }
    }

    pub(crate) fn next_deadline(&self) -> Option<(Timestamp, NodeId)> {
        self.exec.next_deadline()
    }

    pub(crate) fn execution_id(&self) -> crate::domain::ids::ExecutionId {
        self.exec.id().clone()
    }

    fn enqueue_dispatchable(&mut self) {
        let n = self.exec.definition().len();
        for i in 0..n {
            let slot = NodeSlot(i);
            if self.exec.is_dispatchable_slot(slot) {
                self.enqueue_slot(slot);
            }
        }
    }

    /// Apply one inbox / timer / bound event. Returns true when the
    /// Runtime drive loop should exit. Does not wait.
    pub(crate) async fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Start => {
                self.apply_cmd(ApplyCmd::Start);
                self.dispatch();
                self.persist_then_emit().await;
            }
            Event::Restore => {
                if self.exec.state() == ExecutionState::Created {
                    self.apply_cmd(ApplyCmd::Start);
                } else {
                    self.enqueue_dispatchable();
                }
                self.dispatch();
                self.persist_then_emit().await;
            }
            Event::NodeFinished {
                slot,
                node_id,
                attempt,
                result,
            } => {
                self.release_permit_slot(slot);
                self.spawn.forget(slot);
                match result {
                    Ok(outcome) => {
                        self.apply_cmd(ApplyCmd::FinishNode {
                            node_id,
                            attempt,
                            outcome: Ok(outcome),
                        });
                    }
                    Err(msg) => {
                        self.apply_cmd(ApplyCmd::FinishNode {
                            node_id,
                            attempt,
                            outcome: Err(msg),
                        });
                    }
                }
                self.dispatch();
                self.persist_then_emit().await;
            }
            Event::Resume {
                token,
                resume,
                reply,
            } => {
                let r = self.apply_cmd_result(ApplyCmd::Resume { token, resume });
                let _ = reply.send(r);
                self.dispatch();
                self.persist_then_emit().await;
            }
            Event::Cancel => {
                self.cancel.cancel();
                self.spawn.abort_all();
                self.apply_cmd(ApplyCmd::Cancel);
                self.persist_then_emit().await;
            }
            Event::Inspect { reply } => {
                let _ = reply.send(self.exec.snapshot());
            }
            Event::Timer { node_id } => {
                self.apply_cmd(ApplyCmd::RetryDue { node_id });
                // Re-arm: fire every other due deadline (equal timestamps).
                let now = self.clock.now();
                while let Some((at, id)) = self.exec.next_deadline() {
                    if at > now {
                        break;
                    }
                    self.apply_cmd(ApplyCmd::RetryDue { node_id: id });
                }
                self.dispatch();
                self.persist_then_emit().await;
            }
            Event::ForceCancelBound => {
                warn!("cancel bound elapsed; aborting remaining execute tasks");
                self.spawn.abort_all();
                self.apply_cmd(ApplyCmd::ForceCancelRunning);
                self.persist_then_emit().await;
            }
            Event::Shutdown => {
                self.spawn.abort_all();
                debug_assert_eq!(self.spawn.inflight_len(), 0);
                // Last chance: a failed persist on the command that made
                // this execution terminal left `last_persisted` behind.
                // One extra attempt is not enough when the store is still
                // recovering. Bound the loop so a permanently failing store
                // cannot hang Drop; Phase 1 still allows in-memory terminal
                // with a non-durable file when every attempt returns Err.
                self.persist_then_emit_n(SHUTDOWN_PERSIST_ATTEMPTS).await;
                return true;
            }
        }
        false
    }

    fn apply_cmd(&mut self, cmd: ApplyCmd) {
        let _ = self.apply_cmd_result(cmd);
    }

    fn apply_cmd_result(&mut self, cmd: ApplyCmd) -> Result<(), crate::domain::state::ApplyError> {
        let now = self.clock.now();
        let effect = self.exec.apply(cmd, self.policy.as_ref(), now)?;
        self.pending_events.extend(effect.events);
        for slot in effect.newly_runnable {
            self.enqueue_slot(slot);
        }
        for slot in effect.to_abort {
            self.release_permit_slot(slot);
            self.spawn.abort_node(slot);
        }
        let _ = self.state_tx.send(self.exec.state());
        Ok(())
    }

    fn dispatch(&mut self) {
        while self.available > 0 {
            let Some(slot) = self.ready.pop_front() else {
                break;
            };
            self.queued[slot.0] = 0;
            if !self.exec.is_dispatchable_slot(slot) {
                continue;
            }
            let id = self.exec.node_id_at(slot).clone();
            self.apply_cmd(ApplyCmd::StartNode {
                node_id: id.clone(),
            });
            self.available -= 1;
            self.held[slot.0] = 1;
            self.launch_slot(slot, id);
        }
    }

    /// Persist the durable snapshot, then announce. A failed persist keeps
    /// in-memory apply and does not emit (do not announce a non-durable fact
    /// on the sink). `last_persisted` advances only on persist `Ok`. Shutdown
    /// retries a recovering store so a clean `wait`/Drop matches the file.
    /// No persist queue — ADR 0001 still applies.
    async fn persist_then_emit(&mut self) {
        self.persist_then_emit_n(1).await;
    }

    async fn persist_then_emit_n(&mut self, attempts: u32) {
        if self.store.is_noop() {
            let events = std::mem::take(&mut self.pending_events);
            self.emit_events(&events);
            return;
        }
        for _ in 0..attempts {
            if self.persist_snapshot().await {
                let events = std::mem::take(&mut self.pending_events);
                self.emit_events(&events);
                return;
            }
            // Persist Err/panic: keep pending_events. Taking them on failure
            // dropped ExecutionStarted after a later persist Ok of the same
            // snapshot (and Shutdown retried with an empty slice).
        }
    }

    async fn persist_snapshot(&mut self) -> bool {
        if self.exec.revision() == self.last_persisted {
            return true;
        }
        match CatchUnwind(AssertUnwindSafe(
            self.store
                .persist_with_events(&self.exec, &self.pending_events),
        ))
        .await
        {
            Ok(Ok(())) => {
                self.exec.clear_dirty();
                self.last_persisted = self.exec.revision();
                true
            }
            Ok(Err(e)) => {
                debug!(error = %e, "StateStore::put failed; in-memory state kept");
                // Do not advance last_persisted: the next persist_then_emit
                // (including Shutdown) must retry this revision. Treating Err
                // as durable left the file at Running after wait() Succeeded.
                false
            }
            Err(_) => {
                debug!("StateStore::persist panicked; in-memory state kept");
                false
            }
        }
    }

    fn emit_events(&self, events: &[KernelEvent]) {
        for ev in events {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Err(e) = self.sink.try_emit(ev) {
                    debug!(error = %e, "EventSink::try_emit failed; persist already Ok");
                }
            }))
            .is_err()
            {
                debug!("EventSink::emit panicked; apply already progressed");
            }
        }
    }

    fn enqueue_slot(&mut self, slot: NodeSlot) {
        if self.queued[slot.0] == 0 {
            self.queued[slot.0] = 1;
            self.ready.push_back(slot);
        }
    }

    fn launch_slot(&mut self, slot: NodeSlot, id: NodeId) {
        // Documented invariant panics. Public `Runtime::start` rejects
        // unregistered ids (`start_unknown_executor_errors_and_nothing_runs`);
        // `dispatch_node` always issues a resume token. Do not add a dead
        // defensive branch here — see `docs/FAILURE_CATALOG.md`.
        let exec = self.executors[slot.0]
            .clone()
            .expect("Runtime::start rejected unregistered executor ids");
        let token = self
            .exec
            .resume_token_at(slot)
            .expect("dispatch issues a resume token");
        let ctx = ExecutionContext {
            execution_id: self.exec.id().clone(),
            node_id: id,
            attempt: self.exec.attempt_at(slot),
            inputs: self.exec.inputs_for_slot(slot),
            cancel: self.cancel.child_token(),
            resume_token: token,
            clock: self.clock.clone(),
        };
        self.spawn.spawn(slot, exec, ctx);
    }

    fn release_permit_slot(&mut self, slot: NodeSlot) {
        if self.held[slot.0] == 1 {
            self.held[slot.0] = 0;
            self.available += 1;
        }
    }
}
