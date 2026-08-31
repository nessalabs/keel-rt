use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, ExecutorId, NodeId, NodeSlot, ResumeToken, WorkflowId};
use crate::domain::outcome::{NodeError, NodeOutcome, Resume};
use crate::domain::policy::{Policy, PolicyDecision};
use crate::domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SCHEMA_VERSION};
use crate::domain::time::Timestamp;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Pending,
    Ready { runnable_at: Option<Timestamp> },
    Running { attempt: u32 },
    Waiting { token: ResumeToken, attempt: u32 },
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl NodeState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    pub fn is_ready_now(&self, now: Timestamp) -> bool {
        match self {
            Self::Ready { runnable_at: None } => true,
            Self::Ready {
                runnable_at: Some(at),
            } => *at <= now,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionState {
    Created,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl ExecutionState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug)]
pub struct NodeRuntime {
    pub(crate) state: NodeState,
    pub(crate) output: Option<Bytes>,
    pub(crate) attempt: u32,
    pub(crate) last_error: Option<NodeError>,
    pub(crate) resume_token: Option<ResumeToken>,
    /// Next start should reuse `attempt` (Reinvoke).
    pub(crate) reinvoke: bool,
    pub(crate) last_outcome: Option<NodeOutcome>,
}

impl Default for NodeRuntime {
    fn default() -> Self {
        Self {
            state: NodeState::Pending,
            output: None,
            attempt: 0,
            last_error: None,
            resume_token: None,
            reinvoke: false,
            last_outcome: None,
        }
    }
}

/// Execution aggregate. Apply is synchronous and pure w.r.t. I/O.
#[derive(Clone, Debug)]
pub struct Execution {
    pub(crate) id: ExecutionId,
    pub(crate) workflow_id: WorkflowId,
    pub(crate) definition: WorkflowDefinition,
    pub(crate) state: ExecutionState,
    nodes: Vec<NodeRuntime>,
    pub(crate) revision: u64,
    pub(crate) cancelled: bool,
    next_deadline: Option<(Timestamp, NodeSlot)>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ApplyError {
    #[error("illegal transition: {0}")]
    Illegal(String),
    #[error("unknown node: {0}")]
    UnknownNode(NodeId),
    #[error("resume after cancel")]
    ResumeAfterCancel,
    #[error("conflicting resume complete")]
    ConflictingComplete,
    #[error("resume token does not match a waiting node")]
    TokenMismatch,
    #[error("execution is not waiting on that token")]
    NotWaiting,
}

#[derive(Clone, Debug)]
pub enum ApplyCmd {
    Start,
    StartNode { node_id: NodeId },
    FinishNode { node_id: NodeId, attempt: u32, outcome: Result<NodeOutcome, String> },
    Resume { token: ResumeToken, resume: Resume },
    Cancel,
    RetryDue { node_id: NodeId },
    ForceCancelRunning,
}

#[derive(Clone, Debug, Default)]
pub struct ApplyEffect {
    pub events: Vec<crate::domain::events::DomainEvent>,
    pub newly_runnable: Vec<NodeId>,
    pub to_abort: Vec<NodeId>,
    pub changed: bool,
}

impl Execution {
    pub fn new(definition: WorkflowDefinition) -> Self {
        let nodes = (0..definition.len()).map(|_| NodeRuntime::default()).collect();
        Self {
            id: ExecutionId::new(),
            workflow_id: definition.id.clone(),
            definition,
            state: ExecutionState::Created,
            nodes,
            revision: 0,
            cancelled: false,
            next_deadline: None,
        }
    }

    pub fn id(&self) -> &ExecutionId {
        &self.id
    }

    pub fn state(&self) -> ExecutionState {
        self.state
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    pub fn node(&self, id: &NodeId) -> Option<&NodeRuntime> {
        let slot = self.definition.slot(id)?;
        self.nodes.get(slot.0)
    }

    pub fn is_ready_now(&self, id: &NodeId, now: Timestamp) -> bool {
        self.node(id)
            .map(|n| n.state.is_ready_now(now))
            .unwrap_or(false)
    }

    pub fn executor_id(&self, id: &NodeId) -> Option<&ExecutorId> {
        let slot = self.definition.slot(id)?;
        Some(self.definition.executor_at(slot))
    }

    pub fn attempt(&self, id: &NodeId) -> Option<u32> {
        self.node(id).map(|n| n.attempt)
    }

    pub fn resume_token(&self, id: &NodeId) -> Option<ResumeToken> {
        self.node(id).and_then(|n| n.resume_token.clone())
    }

    /// Next retry deadline. Maintained when a node enters `Ready { runnable_at: Some }`.
    pub fn next_deadline(&self) -> Option<(Timestamp, NodeId)> {
        self.next_deadline
            .map(|(ts, slot)| (ts, self.definition.id_at(slot).clone()))
    }

    pub fn inputs_for(&self, id: &NodeId) -> HashMap<NodeId, Bytes> {
        let Some(slot) = self.definition.slot(id) else {
            return HashMap::new();
        };
        let mut out = HashMap::new();
        for pred in self.definition.pred_slots(slot) {
            let n = &self.nodes[pred.0];
            if matches!(n.state, NodeState::Succeeded) {
                if let Some(bytes) = &n.output {
                    out.insert(self.definition.id_at(*pred).clone(), bytes.clone());
                }
            }
        }
        out
    }

    pub fn snapshot(&self) -> ExecutionSnapshot {
        let nodes = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                (
                    self.definition.id_at(NodeSlot(i)).clone(),
                    NodeSnapshot {
                        state: n.state.clone(),
                        output: n.output.clone(),
                        attempt: n.attempt,
                        resume_token: n.resume_token.clone(),
                        last_error: n.last_error.clone(),
                    },
                )
            })
            .collect();
        ExecutionSnapshot {
            schema_version: SCHEMA_VERSION,
            revision: self.revision,
            execution_id: self.id.clone(),
            workflow_id: self.workflow_id.clone(),
            state: self.state,
            nodes,
        }
    }

    pub fn apply(
        &mut self,
        cmd: ApplyCmd,
        policy: &dyn Policy,
        now: Timestamp,
    ) -> Result<ApplyEffect, ApplyError> {
        let prev_state = self.state;
        let mut effect = ApplyEffect::default();
        match cmd {
            ApplyCmd::Start => {
                if self.state != ExecutionState::Created {
                    return Err(ApplyError::Illegal("start only from Created".into()));
                }
                self.state = ExecutionState::Running;
                effect.events.push(crate::domain::events::DomainEvent::ExecutionStarted {
                    execution_id: self.id.clone(),
                });
                let nsrc = self.definition.source_slots().len();
                for i in 0..nsrc {
                    let src = self.definition.source_slots()[i];
                    self.mark_ready(src, None, &mut effect);
                }
                effect.changed = true;
            }
            ApplyCmd::StartNode { node_id } => {
                self.dispatch_node(&node_id, &mut effect)?;
                effect.changed = true;
            }
            ApplyCmd::FinishNode {
                node_id,
                attempt,
                outcome,
            } => {
                self.finish_node(&node_id, attempt, outcome, policy, now, &mut effect)?;
            }
            ApplyCmd::Resume { token, resume } => {
                self.resume(token, resume, policy, now, &mut effect)?;
            }
            ApplyCmd::Cancel => {
                if self.cancelled && self.state == ExecutionState::Cancelled {
                    return Ok(effect);
                }
                self.cancel_graph(&mut effect);
                effect.changed = true;
            }
            ApplyCmd::RetryDue { node_id } => {
                let Some(slot) = self.definition.slot(&node_id) else {
                    return Ok(effect);
                };
                let due = matches!(
                    self.nodes[slot.0].state,
                    NodeState::Ready {
                        runnable_at: Some(at),
                    } if at <= now
                );
                if !due {
                    return Ok(effect);
                }
                self.nodes[slot.0].state = NodeState::Ready { runnable_at: None };
                self.clear_deadline_if(slot);
                effect.newly_runnable.push(node_id);
                effect.changed = true;
            }
            ApplyCmd::ForceCancelRunning => {
                if !self.nodes.iter().any(|n| matches!(n.state, NodeState::Running { .. })) {
                    return Ok(effect);
                }
                self.force_cancel_running(&mut effect);
                effect.changed = true;
            }
        }
        if !effect.changed {
            return Ok(effect);
        }
        self.recompute_execution_state(prev_state, &mut effect);
        self.revision += 1;
        Ok(effect)
    }

    fn mark_ready(&mut self, slot: NodeSlot, runnable_at: Option<Timestamp>, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        let n = &mut self.nodes[slot.0];
        if n.state.is_terminal() || matches!(n.state, NodeState::Running { .. }) {
            return;
        }
        n.state = NodeState::Ready { runnable_at };
        let id = self.definition.id_at(slot).clone();
        effect.events.push(DomainEvent::NodeReady {
            node_id: id.clone(),
            runnable_at,
        });
        if let Some(at) = runnable_at {
            self.note_deadline(slot, at);
        } else {
            effect.newly_runnable.push(id);
        }
    }

    fn dispatch_node(&mut self, id: &NodeId, effect: &mut ApplyEffect) -> Result<(), ApplyError> {
        use crate::domain::events::DomainEvent;
        let slot = self
            .definition
            .slot(id)
            .ok_or_else(|| ApplyError::UnknownNode(id.clone()))?;
        let n = &mut self.nodes[slot.0];
        match &n.state {
            NodeState::Ready { runnable_at } if runnable_at.is_none() => {}
            other => {
                return Err(ApplyError::Illegal(format!(
                    "dispatch {id} from {other:?}"
                )));
            }
        }
        if !n.reinvoke {
            n.attempt = n.attempt.saturating_add(1);
        }
        n.reinvoke = false;
        let attempt = n.attempt;
        let token = ResumeToken::issue(self.id.clone(), id.clone(), attempt);
        n.resume_token = Some(token);
        n.state = NodeState::Running { attempt };
        effect.events.push(DomainEvent::NodeStarted {
            node_id: id.clone(),
            attempt,
        });
        Ok(())
    }

    fn finish_node(
        &mut self,
        id: &NodeId,
        attempt: u32,
        outcome: Result<NodeOutcome, String>,
        policy: &dyn Policy,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) -> Result<(), ApplyError> {
        if self.cancelled {
            return Ok(());
        }
        let slot = self
            .definition
            .slot(id)
            .ok_or_else(|| ApplyError::UnknownNode(id.clone()))?;
        match &self.nodes[slot.0].state {
            NodeState::Running { attempt: a } if *a == attempt => {}
            // Stale attempt, already terminal, or late join: ignore. Do not
            // double-apply or bump revision.
            _ => return Ok(()),
        }

        let outcome = match outcome {
            Ok(o) => o,
            Err(panic_msg) => NodeOutcome::Failed(NodeError::new(format!("panic: {panic_msg}"))),
        };

        effect.changed = true;
        self.apply_outcome(slot, id, outcome, policy, now, effect)
    }

    fn apply_outcome(
        &mut self,
        slot: NodeSlot,
        id: &NodeId,
        outcome: NodeOutcome,
        policy: &dyn Policy,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) -> Result<(), ApplyError> {
        use crate::domain::events::DomainEvent;
        let attempt = self.nodes[slot.0].attempt;

        let decision = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            policy.decide(&outcome, attempt)
        })) {
            Ok(d) => d,
            Err(_) => {
                tracing::error!(node = %id, "Policy::decide panicked");
                self.fail_node(slot, id, NodeError::new("policy panicked"), effect);
                self.fail_fast(effect);
                return Ok(());
            }
        };
        if matches!(decision, PolicyDecision::Retry { .. })
            && matches!(
                outcome,
                NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }
            )
        {
            self.fail_node(slot, id, NodeError::new("illegal policy Retry after Succeeded/Waiting"), effect);
            self.fail_fast(effect);
            return Ok(());
        }

        match (&outcome, decision) {
            (NodeOutcome::Succeeded(bytes), PolicyDecision::Accept) => {
                let n = &mut self.nodes[slot.0];
                n.output = Some(bytes.clone());
                n.last_outcome = Some(outcome.clone());
                n.state = NodeState::Succeeded;
                n.last_error = None;
                effect.events.push(DomainEvent::NodeSucceeded {
                    node_id: id.clone(),
                });
                self.ready_successors(slot, effect);
            }
            (NodeOutcome::Waiting { token }, PolicyDecision::Accept) => {
                let n = &mut self.nodes[slot.0];
                let token = n.resume_token.clone().unwrap_or_else(|| token.clone());
                n.state = NodeState::Waiting {
                    token: token.clone(),
                    attempt,
                };
                n.resume_token = Some(token.clone());
                n.last_outcome = Some(NodeOutcome::Waiting {
                    token: token.clone(),
                });
                effect.events.push(DomainEvent::NodeWaiting {
                    node_id: id.clone(),
                    token,
                });
            }
            (NodeOutcome::Failed(err), PolicyDecision::Accept) => {
                self.fail_node(slot, id, err.clone(), effect);
                self.fail_fast(effect);
            }
            (NodeOutcome::TimedOut, PolicyDecision::Accept) => {
                let n = &mut self.nodes[slot.0];
                n.state = NodeState::TimedOut;
                n.last_error = Some(NodeError::new("timed out"));
                n.last_outcome = Some(outcome);
                effect.events.push(DomainEvent::NodeTimedOut {
                    node_id: id.clone(),
                });
                self.fail_fast(effect);
            }
            (NodeOutcome::Failed(_) | NodeOutcome::TimedOut, PolicyDecision::Retry { delay }) => {
                let n = &mut self.nodes[slot.0];
                n.last_error = match &outcome {
                    NodeOutcome::Failed(e) => Some(e.clone()),
                    _ => Some(NodeError::new("timed out")),
                };
                n.last_outcome = Some(outcome);
                let at = if delay.is_zero() {
                    None
                } else {
                    Some(now.saturating_add(delay))
                };
                n.state = NodeState::Ready { runnable_at: at };
                effect.events.push(DomainEvent::NodeReady {
                    node_id: id.clone(),
                    runnable_at: at,
                });
                if let Some(at) = at {
                    self.note_deadline(slot, at);
                } else {
                    effect.newly_runnable.push(id.clone());
                }
            }
            (_, PolicyDecision::Reject) => {
                self.fail_node(
                    slot,
                    id,
                    NodeError::new("policy rejected outcome"),
                    effect,
                );
                self.fail_fast(effect);
            }
            (NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }, PolicyDecision::Retry { .. }) => {
                unreachable!("illegal retry handled above");
            }
        }
        Ok(())
    }

    fn fail_node(&mut self, slot: NodeSlot, id: &NodeId, err: NodeError, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        let n = &mut self.nodes[slot.0];
        n.state = NodeState::Failed;
        n.last_error = Some(err.clone());
        n.last_outcome = Some(NodeOutcome::Failed(err.clone()));
        self.clear_deadline_if(slot);
        effect.events.push(DomainEvent::NodeFailed {
            node_id: id.clone(),
            error: err,
        });
    }

    /// After policy Accepts Failed/TimedOut (or Rejects): non-terminal nodes
    /// Cancelled, execution Failed. Sets the same abort flag as [`Self::cancel_graph`].
    fn fail_fast(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        self.cancelled = true;
        for i in 0..self.nodes.len() {
            if self.nodes[i].state.is_terminal() {
                continue;
            }
            let running = matches!(self.nodes[i].state, NodeState::Running { .. });
            self.nodes[i].state = NodeState::Cancelled;
            let id = self.definition.id_at(NodeSlot(i)).clone();
            if running {
                effect.to_abort.push(id.clone());
            }
            effect.events.push(DomainEvent::NodeCancelled { node_id: id });
        }
        self.next_deadline = None;
        self.state = ExecutionState::Failed;
    }

    fn ready_successors(&mut self, succeeded: NodeSlot, effect: &mut ApplyEffect) {
        let nsucc = self.definition.succ_slots(succeeded).len();
        for i in 0..nsucc {
            let succ = self.definition.succ_slots(succeeded)[i];
            if self.all_preds_succeeded(succ) && matches!(self.nodes[succ.0].state, NodeState::Pending)
            {
                self.mark_ready(succ, None, effect);
            }
        }
    }

    fn all_preds_succeeded(&self, slot: NodeSlot) -> bool {
        self.definition
            .pred_slots(slot)
            .iter()
            .all(|p| matches!(self.nodes[p.0].state, NodeState::Succeeded))
    }

    fn resume(
        &mut self,
        token: ResumeToken,
        resume: Resume,
        policy: &dyn Policy,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) -> Result<(), ApplyError> {
        if self.cancelled || self.state == ExecutionState::Cancelled {
            return Err(ApplyError::ResumeAfterCancel);
        }
        if self.state.is_terminal() {
            if let Resume::Complete(ref outcome) = resume {
                if let Some(n) = self.node(&token.node_id) {
                    if matches!(n.state, NodeState::Succeeded) {
                        if let Some(prev) = &n.last_outcome {
                            if prev.equivalent(outcome) {
                                return Ok(());
                            }
                        }
                        return Err(ApplyError::ConflictingComplete);
                    }
                }
            }
            return Err(ApplyError::ResumeAfterCancel);
        }

        let slot = self
            .definition
            .slot(&token.node_id)
            .ok_or_else(|| ApplyError::UnknownNode(token.node_id.clone()))?;
        let node = &self.nodes[slot.0];

        match &node.state {
            NodeState::Succeeded => {
                if let Resume::Complete(ref outcome) = resume {
                    if let Some(prev) = &node.last_outcome {
                        if prev.equivalent(outcome) {
                            return Ok(());
                        }
                    }
                    return Err(ApplyError::ConflictingComplete);
                }
                return Err(ApplyError::NotWaiting);
            }
            NodeState::Waiting { token: t, .. } => {
                if t != &token {
                    return Err(ApplyError::TokenMismatch);
                }
            }
            NodeState::Cancelled => return Err(ApplyError::ResumeAfterCancel),
            _ => return Err(ApplyError::NotWaiting),
        }

        match resume {
            Resume::Complete(outcome) => {
                self.nodes[slot.0].state = NodeState::Running {
                    attempt: token.attempt,
                };
                effect.changed = true;
                self.apply_outcome(slot, &token.node_id, outcome, policy, now, effect)?;
            }
            Resume::Reinvoke => {
                self.nodes[slot.0].reinvoke = true;
                self.nodes[slot.0].state = NodeState::Ready { runnable_at: None };
                effect.newly_runnable.push(token.node_id);
                effect.changed = true;
            }
        }
        Ok(())
    }

    fn cancel_graph(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        self.cancelled = true;
        for i in 0..self.nodes.len() {
            if self.nodes[i].state.is_terminal() {
                continue;
            }
            let running = matches!(self.nodes[i].state, NodeState::Running { .. });
            self.nodes[i].state = NodeState::Cancelled;
            let id = self.definition.id_at(NodeSlot(i)).clone();
            if running {
                effect.to_abort.push(id.clone());
            }
            effect.events.push(DomainEvent::NodeCancelled { node_id: id });
        }
        self.next_deadline = None;
        self.state = ExecutionState::Cancelled;
    }

    fn force_cancel_running(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        for i in 0..self.nodes.len() {
            if matches!(self.nodes[i].state, NodeState::Running { .. }) {
                self.nodes[i].state = NodeState::Cancelled;
                let id = self.definition.id_at(NodeSlot(i)).clone();
                effect.to_abort.push(id.clone());
                effect.events.push(DomainEvent::NodeCancelled { node_id: id });
            }
        }
        if !self.state.is_terminal() {
            self.state = ExecutionState::Cancelled;
            self.cancelled = true;
        }
        self.next_deadline = None;
    }

    fn note_deadline(&mut self, slot: NodeSlot, at: Timestamp) {
        match self.next_deadline {
            None => self.next_deadline = Some((at, slot)),
            Some((t, _)) if at < t => self.next_deadline = Some((at, slot)),
            _ => {}
        }
    }

    fn clear_deadline_if(&mut self, slot: NodeSlot) {
        if self.next_deadline.map(|(_, s)| s) == Some(slot) {
            self.rebuild_deadline();
        }
    }

    fn rebuild_deadline(&mut self) {
        self.next_deadline = None;
        for (i, n) in self.nodes.iter().enumerate() {
            if let NodeState::Ready {
                runnable_at: Some(at),
            } = n.state
            {
                match self.next_deadline {
                    None => self.next_deadline = Some((at, NodeSlot(i))),
                    Some((t, _)) if at < t => self.next_deadline = Some((at, NodeSlot(i))),
                    _ => {}
                }
            }
        }
    }

    fn derive_state(&self) -> ExecutionState {
        let mut any_running = false;
        let mut any_ready_now = false;
        let mut any_ready_later = false;
        let mut any_waiting = false;
        let mut all_succeeded = true;
        let mut any_failed = false;

        for n in &self.nodes {
            match &n.state {
                NodeState::Running { .. } => {
                    any_running = true;
                    all_succeeded = false;
                }
                NodeState::Ready { runnable_at: None } => {
                    any_ready_now = true;
                    all_succeeded = false;
                }
                NodeState::Ready {
                    runnable_at: Some(_),
                } => {
                    any_ready_later = true;
                    all_succeeded = false;
                }
                NodeState::Waiting { .. } => {
                    any_waiting = true;
                    all_succeeded = false;
                }
                NodeState::Pending => {
                    all_succeeded = false;
                }
                NodeState::Succeeded => {}
                NodeState::Failed | NodeState::TimedOut => {
                    any_failed = true;
                    all_succeeded = false;
                }
                NodeState::Cancelled => {
                    all_succeeded = false;
                }
            }
        }

        if any_failed {
            ExecutionState::Failed
        } else if all_succeeded {
            ExecutionState::Succeeded
        } else if self.cancelled {
            ExecutionState::Cancelled
        } else if any_running || any_ready_now || any_ready_later {
            ExecutionState::Running
        } else if any_waiting {
            ExecutionState::Waiting
        } else {
            self.state
        }
    }

    fn recompute_execution_state(&mut self, prev: ExecutionState, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        let next = self.derive_state();
        if next != prev {
            match next {
                ExecutionState::Failed => {
                    effect.events.push(DomainEvent::ExecutionFailed {
                        execution_id: self.id.clone(),
                    });
                }
                ExecutionState::Cancelled => {
                    effect.events.push(DomainEvent::ExecutionCancelled {
                        execution_id: self.id.clone(),
                    });
                }
                ExecutionState::Succeeded => {
                    effect.events.push(DomainEvent::ExecutionSucceeded {
                        execution_id: self.id.clone(),
                    });
                }
                ExecutionState::Waiting => {
                    effect.events.push(DomainEvent::ExecutionWaiting {
                        execution_id: self.id.clone(),
                    });
                }
                ExecutionState::Running | ExecutionState::Created => {}
            }
        }
        self.state = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::events::DomainEvent;
    use crate::domain::policy::AcceptPolicy;
    use bytes::Bytes;

    fn linear() -> Execution {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "ea")
            .node("b", "eb")
            .edge("a", "b")
            .build()
            .unwrap();
        Execution::new(def)
    }

    fn count_failed(effect: &ApplyEffect) -> usize {
        effect
            .events
            .iter()
            .filter(|e| matches!(e, DomainEvent::ExecutionFailed { .. }))
            .count()
    }

    #[test]
    fn start_marks_sources_ready() {
        let mut ex = linear();
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &AcceptPolicy, now).unwrap();
        assert_eq!(ex.state, ExecutionState::Running);
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready { runnable_at: None }
        ));
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Pending
        ));
    }

    #[test]
    fn join_and_requires_all_predecessors() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .node("c", "e")
            .edge("a", "c")
            .edge("b", "c")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"a"))),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("c")).unwrap().state,
            NodeState::Pending
        ));
        ex.apply(ApplyCmd::StartNode { node_id: "b".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "b".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"b"))),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("c")).unwrap().state,
            NodeState::Ready { .. }
        ));
    }

    #[test]
    fn fail_fast_cancels_dependents_one_execution_failed() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        let effect = ex
            .apply(
                ApplyCmd::FinishNode {
                    node_id: "a".into(),
                    attempt: 1,
                    outcome: Ok(NodeOutcome::failed("boom")),
                },
                &p,
                now,
            )
            .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Failed
        ));
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Cancelled
        ));
        assert_eq!(ex.state, ExecutionState::Failed);
        assert_eq!(count_failed(&effect), 1);
        assert!(ex.cancelled);

        let rev = ex.revision;
        let late = ex
            .apply(
                ApplyCmd::FinishNode {
                    node_id: "b".into(),
                    attempt: 1,
                    outcome: Err("cancelled".into()),
                },
                &p,
                now,
            )
            .unwrap();
        assert!(!late.changed);
        assert_eq!(ex.revision, rev);
        assert_eq!(count_failed(&late), 0);
    }

    #[test]
    fn retry_delay_is_ready_not_waiting() {
        use crate::domain::policy::RetryPolicy;
        use std::time::Duration;
        let mut ex = linear();
        let now = Timestamp(1000);
        let p = RetryPolicy::new(3, Duration::from_millis(50));
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("x")),
            },
            &p,
            now,
        )
        .unwrap();
        match &ex.node(&NodeId::new("a")).unwrap().state {
            NodeState::Ready {
                runnable_at: Some(at),
            } => assert_eq!(*at, Timestamp(1050)),
            NodeState::Waiting { .. } => panic!("retry delay must not be Waiting"),
            other => panic!("expected Ready with runnable_at, got {other:?}"),
        }
        assert_eq!(ex.state, ExecutionState::Running);
        let (at, id) = ex.next_deadline().expect("deadline on aggregate");
        assert_eq!(at, Timestamp(1050));
        assert_eq!(id.as_str(), "a");
    }

    #[test]
    fn illegal_retry_after_success_fails_node() {
        struct BadPolicy;
        impl Policy for BadPolicy {
            fn decide(&self, _o: &NodeOutcome, _a: u32) -> PolicyDecision {
                PolicyDecision::Retry {
                    delay: std::time::Duration::from_millis(1),
                }
            }
        }
        let mut ex = linear();
        let now = Timestamp(0);
        let p = BadPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"ok"))),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Failed
        ));
    }

    #[test]
    fn duplicate_equivalent_complete_is_noop() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        let token = ex
            .node(&NodeId::new("a"))
            .unwrap()
            .resume_token
            .clone()
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Waiting {
                    token: token.clone(),
                }),
            },
            &p,
            now,
        )
        .unwrap();
        let bytes = Bytes::from_static(b"x");
        ex.apply(
            ApplyCmd::Resume {
                token: token.clone(),
                resume: Resume::Complete(NodeOutcome::Succeeded(bytes.clone())),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Succeeded
        ));
        let rev = ex.revision;
        ex.apply(
            ApplyCmd::Resume {
                token: token.clone(),
                resume: Resume::Complete(NodeOutcome::Succeeded(bytes)),
            },
            &p,
            now,
        )
        .unwrap();
        assert_eq!(ex.node(&NodeId::new("a")).unwrap().attempt, 1);
        assert_eq!(ex.revision, rev, "noop resume must not bump revision");
    }

    #[test]
    fn resume_after_cancel_errors() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        let token = ex
            .node(&NodeId::new("a"))
            .unwrap()
            .resume_token
            .clone()
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Waiting {
                    token: token.clone(),
                }),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(ApplyCmd::Cancel, &p, now).unwrap();
        let err = ex
            .apply(
                ApplyCmd::Resume {
                    token,
                    resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"x"))),
                },
                &p,
                now,
            )
            .unwrap_err();
        assert_eq!(err, ApplyError::ResumeAfterCancel);
    }
}
