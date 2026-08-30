//! Apply loop. Current-thread analog: one event channel, FIFO ready queue.
//! This module never awaits `execute()` and does not name resource types.

use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::NodeId;
use crate::domain::outcome::NodeOutcome;
use crate::domain::policy::Policy;
use crate::domain::state::{ApplyCmd, Execution, ExecutionState};
use crate::runtime::executor::{ExecutionContext, ExecutorRegistry};
use crate::runtime::inject::{Event, EventTx, JoinKind};
use crate::runtime::park::{ChannelPark, Park};
use crate::runtime::sink::EventSink;
use crate::runtime::spawn::SpawnSet;
use crate::runtime::store::StateStore;
use crate::runtime::time::Clock;
use std::collections::{HashSet, VecDeque};
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
    registry: ExecutorRegistry,
    clock: Arc<dyn Clock>,
    park: ChannelPark,
    spawn: SpawnSet,
    ready: VecDeque<NodeId>,
    queued: HashSet<NodeId>,
    available: usize,
    held: HashSet<NodeId>,
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
        let exec = Execution::new(definition);
        let _ = state_tx.send(exec.state());
        Self {
            spawn: SpawnSet::new(tx.clone()),
            exec,
            policy,
            store,
            sink,
            registry,
            clock,
            park,
            ready: VecDeque::new(),
            queued: HashSet::new(),
            available: concurrency.max(1),
            held: HashSet::new(),
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
                node_id,
                attempt,
                result,
            } => {
                self.release_permit(&node_id);
                self.spawn.forget(&node_id);
                match result {
                    Ok(outcome) => {
                        self.apply_cmd(ApplyCmd::FinishNode {
                            node_id,
                            attempt,
                            outcome: Ok(outcome),
                        });
                    }
                    Err(JoinKind::Panic(msg)) => {
                        self.apply_cmd(ApplyCmd::FinishNode {
                            node_id,
                            attempt,
                            outcome: Err(msg),
                        });
                    }
                    Err(JoinKind::Cancelled) => {
                        if !self.exec.is_cancelled() {
                            self.apply_cmd(ApplyCmd::FinishNode {
                                node_id,
                                attempt,
                                outcome: Ok(NodeOutcome::failed("cancelled")),
                            });
                        }
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
            self.sink.emit(ev);
        }
        for id in effect.newly_runnable {
            self.enqueue(id);
        }
        for id in &effect.to_abort {
            self.release_permit(id);
            self.spawn.abort_node(id);
        }
        let _ = self.state_tx.send(self.exec.state());
        Ok(())
    }

    fn dispatch(&mut self) {
        while self.available > 0 {
            let Some(id) = self.ready.pop_front() else {
                break;
            };
            self.queued.remove(&id);
            let now = self.clock.now();
            if !self.exec.is_ready_now(&id, now) {
                continue;
            }
            if self
                .apply_cmd_result(ApplyCmd::StartNode { node_id: id.clone() })
                .is_err()
            {
                continue;
            }
            self.available -= 1;
            self.held.insert(id.clone());
            self.launch(&id);
        }
    }

    async fn persist_after_event(&mut self) {
        if self.store.is_noop() {
            return;
        }
        if self.exec.revision() == self.last_persisted {
            return;
        }
        let snap = self.exec.snapshot();
        if let Err(e) = self.store.put(&snap).await {
            debug!(error = %e, "StateStore::put failed; in-memory state kept");
        }
        self.last_persisted = self.exec.revision();
    }

    fn enqueue(&mut self, id: NodeId) {
        if self.queued.insert(id.clone()) {
            self.ready.push_back(id);
        }
    }

    fn launch(&mut self, id: &NodeId) {
        let Some(executor_id) = self.exec.executor_id(id) else {
            return;
        };
        let Some(exec) = self.registry.get(executor_id) else {
            let attempt = self.exec.attempt(id).unwrap_or(1);
            let _ = self.tx.send(Event::NodeFinished {
                node_id: id.clone(),
                attempt,
                result: Ok(NodeOutcome::failed(format!(
                    "no executor registered for {executor_id}"
                ))),
            });
            return;
        };
        let token = self
            .exec
            .resume_token(id)
            .expect("dispatch issues a resume token");
        let ctx = ExecutionContext {
            execution_id: self.exec.id().clone(),
            node_id: id.clone(),
            attempt: self.exec.attempt(id).unwrap_or(1),
            inputs: self.exec.inputs_for(id),
            cancel: self.cancel.child_token(),
            resume_token: token,
            clock: self.clock.clone(),
        };
        self.spawn.spawn(exec, ctx);
    }

    fn release_permit(&mut self, id: &NodeId) {
        if self.held.remove(id) {
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
        tokio::spawn(async move {
            tokio::time::sleep(bound).await;
            let _ = tx.send(Event::ForceCancelBound);
        });
        self.spawn.abort_all();
    }
}
