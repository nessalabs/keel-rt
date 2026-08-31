//! Apply loop. Current-thread analog: one event channel, FIFO ready queue.
//! This module never awaits `execute()` and does not name resource types.

use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{NodeId, NodeSlot};
use crate::domain::policy::Policy;
use crate::domain::state::{ApplyCmd, Execution, ExecutionState};
use crate::runtime::executor::{ExecutionContext, Executor, ExecutorRegistry};
use crate::runtime::inject::{Event, EventTx};
use crate::runtime::park::ChannelPark;
use crate::runtime::sink::EventSink;
use crate::runtime::spawn::SpawnSet;
use crate::runtime::store::StateStore;
use crate::runtime::time::Clock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Scheduler {
    exec: Execution,
    policy: Arc<dyn Policy>,
    store: Arc<dyn StateStore>,
    sink: Arc<dyn EventSink>,
    clock: Arc<dyn Clock>,
    park: ChannelPark,
    spawn: SpawnSet,
    ready: VecDeque<NodeSlot>,
    queued: Vec<u8>,
    available: usize,
    held: Vec<u8>,
    executors: Vec<Option<Arc<dyn Executor>>>,
    cancel: CancellationToken,
    state_tx: watch::Sender<ExecutionState>,
    cancel_bound: Duration,
    tx: EventTx,
    bound_armed: bool,
    last_persisted: u64,
}

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        definition: WorkflowDefinition,
        policy: Arc<dyn Policy>,
        store: Arc<dyn StateStore>,
        sink: Arc<dyn EventSink>,
        registry: ExecutorRegistry,
        clock: Arc<dyn Clock>,
        park: ChannelPark,
        tx: EventTx,
        concurrency: usize,
        cancel: CancellationToken,
        state_tx: watch::Sender<ExecutionState>,
        cancel_bound: Duration,
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
            park,
            ready: VecDeque::new(),
            queued: vec![0u8; n],
            available: concurrency.max(1),
            held: vec![0u8; n],
            executors,
            cancel,
            state_tx,
            cancel_bound,
            tx,
            bound_armed: false,
            last_persisted: 0,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let timer = self.exec.next_deadline();
            let Some(event) = self.park.recv(timer).await else {
                break;
            };
            if self.handle_event(event).await {
                break;
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Start => {
                self.apply_cmd(ApplyCmd::Start);
                self.dispatch();
                self.persist_after_event().await;
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
                self.persist_after_event().await;
            }
            Event::Resume {
                token,
                resume,
                reply,
            } => {
                let r = self.apply_cmd_result(ApplyCmd::Resume { token, resume });
                let _ = reply.send(r);
                self.dispatch();
                self.persist_after_event().await;
            }
            Event::Cancel => {
                self.cancel.cancel();
                self.apply_cmd(ApplyCmd::Cancel);
                self.arm_cancel_bound();
                self.persist_after_event().await;
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
                self.persist_after_event().await;
            }
            Event::ForceCancelBound => {
                warn!(
                    execution_id = %self.exec.id(),
                    bound_ms = self.cancel_bound.as_millis() as u64,
                    "cancel bound elapsed; aborting remaining execute tasks"
                );
                self.spawn.abort_all();
                self.apply_cmd(ApplyCmd::ForceCancelRunning);
                self.persist_after_event().await;
            }
            Event::Shutdown => {
                self.spawn.abort_all();
                return true;
            }
        }
        false
    }

    fn apply_cmd(&mut self, cmd: ApplyCmd) {
        let _ = self.apply_cmd_result(cmd);
    }

    fn apply_cmd_result(
        &mut self,
        cmd: ApplyCmd,
    ) -> Result<(), crate::domain::state::ApplyError> {
        let now = self.clock.now();
        let effect = self.exec.apply(cmd, self.policy.as_ref(), now)?;
        for ev in &effect.events {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.sink.emit(ev);
            }))
            .is_err()
            {
                debug!("EventSink::emit panicked; apply already progressed");
            }
        }
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
            let now = self.clock.now();
            if !self.exec.is_ready_now_slot(slot, now) {
                continue;
            }
            let id = self.exec.node_id_at(slot).clone();
            if self
                .apply_cmd_result(ApplyCmd::StartNode { node_id: id.clone() })
                .is_err()
            {
                continue;
            }
            self.available -= 1;
            self.held[slot.0] = 1;
            self.launch_slot(slot, id);
        }
    }

    async fn persist_after_event(&mut self) {
        if self.store.is_noop() {
            return;
        }
        if self.exec.revision() == self.last_persisted {
            return;
        }
        if let Err(e) = self.store.persist(&self.exec).await {
            debug!(error = %e, "StateStore::put failed; in-memory state kept");
        } else {
            self.exec.clear_dirty();
        }
        self.last_persisted = self.exec.revision();
    }

    fn enqueue_slot(&mut self, slot: NodeSlot) {
        if self.queued[slot.0] == 0 {
            self.queued[slot.0] = 1;
            self.ready.push_back(slot);
        }
    }

    fn launch_slot(&mut self, slot: NodeSlot, id: NodeId) {
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

    fn arm_cancel_bound(&mut self) {
        if self.bound_armed {
            return;
        }
        self.bound_armed = true;
        let tx = self.tx.clone();
        let bound = self.cancel_bound;
        // Wall time, not Clock: hang-bound must fire even if FakeClock is paused.
        tokio::spawn(async move {
            tokio::time::sleep(bound).await;
            let _ = tx.send(Event::ForceCancelBound);
        });
        self.spawn.abort_all();
    }
}
