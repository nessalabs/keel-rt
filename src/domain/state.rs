use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, NodeId, ResumeToken, WorkflowId};
use crate::domain::outcome::{NodeError, NodeOutcome, Resume};
use crate::domain::policy::{Policy, PolicyDecision};
use crate::domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SCHEMA_VERSION};
use crate::runtime::time::Timestamp;
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
    pub state: NodeState,
    pub output: Option<Bytes>,
    pub attempt: u32,
    pub last_error: Option<NodeError>,
    pub resume_token: Option<ResumeToken>,
    /// Next start should reuse `attempt` (Reinvoke).
    pub reinvoke: bool,
    pub last_outcome: Option<NodeOutcome>,
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
    pub id: ExecutionId,
    pub workflow_id: WorkflowId,
    pub definition: WorkflowDefinition,
    pub state: ExecutionState,
    pub nodes: HashMap<NodeId, NodeRuntime>,
    pub revision: u64,
    pub cancelled: bool,
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
}

impl Execution {
    pub fn new(definition: WorkflowDefinition) -> Self {
        let nodes = definition
            .nodes()
            .iter()
            .map(|n| (n.id.clone(), NodeRuntime::default()))
            .collect();
        Self {
            id: ExecutionId::new(),
            workflow_id: definition.id.clone(),
            definition,
            state: ExecutionState::Created,
            nodes,
            revision: 0,
            cancelled: false,
        }
    }

    pub fn node(&self, id: &NodeId) -> Option<&NodeRuntime> {
        self.nodes.get(id)
    }

    pub fn inputs_for(&self, id: &NodeId) -> HashMap<NodeId, Bytes> {
        let mut out = HashMap::new();
        for pred in self.definition.predecessors(id) {
            if let Some(n) = self.nodes.get(&pred) {
                if matches!(n.state, NodeState::Succeeded) {
                    if let Some(bytes) = &n.output {
                        out.insert(pred, bytes.clone());
                    }
                }
            }
        }
        out
    }

    pub fn snapshot(&self) -> ExecutionSnapshot {
        let nodes = self
            .nodes
            .iter()
            .map(|(id, n)| {
                (
                    id.clone(),
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
        use crate::domain::events::DomainEvent;
        let mut effect = ApplyEffect::default();
        match cmd {
            ApplyCmd::Start => {
                if self.state != ExecutionState::Created {
                    return Err(ApplyError::Illegal("start only from Created".into()));
                }
                self.state = ExecutionState::Running;
                effect.events.push(DomainEvent::ExecutionStarted {
                    execution_id: self.id.clone(),
                });
                for src in self.definition.sources() {
                    self.mark_ready(&src, None, &mut effect);
                }
            }
            ApplyCmd::StartNode { node_id } => {
                self.dispatch_node(&node_id, &mut effect)?;
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
                self.cancel_graph(&mut effect);
            }
            ApplyCmd::RetryDue { node_id } => {
                if let Some(n) = self.nodes.get_mut(&node_id) {
                    if let NodeState::Ready {
                        runnable_at: Some(at),
                    } = n.state
                    {
                        if at <= now {
                            n.state = NodeState::Ready { runnable_at: None };
                            effect.newly_runnable.push(node_id);
                        }
                    }
                }
            }
            ApplyCmd::ForceCancelRunning => {
                self.force_cancel_running(&mut effect);
            }
        }
        self.recompute_execution_state(&mut effect);
        self.revision += 1;
        Ok(effect)
    }

    fn mark_ready(&mut self, id: &NodeId, runnable_at: Option<Timestamp>, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        if let Some(n) = self.nodes.get_mut(id) {
            if n.state.is_terminal() || matches!(n.state, NodeState::Running { .. }) {
                return;
            }
            n.state = NodeState::Ready { runnable_at };
            effect.events.push(DomainEvent::NodeReady {
                node_id: id.clone(),
                runnable_at,
            });
            if runnable_at.is_none() {
                effect.newly_runnable.push(id.clone());
            }
        }
    }

    fn dispatch_node(&mut self, id: &NodeId, effect: &mut ApplyEffect) -> Result<(), ApplyError> {
        use crate::domain::events::DomainEvent;
        let n = self
            .nodes
            .get_mut(id)
            .ok_or_else(|| ApplyError::UnknownNode(id.clone()))?;
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
            // Late finish after cancel: keep Cancelled, ignore outcome except panic->already failed path.
            if let Some(n) = self.nodes.get_mut(id) {
                if !n.state.is_terminal() {
                    n.state = NodeState::Cancelled;
                }
            }
            return Ok(());
        }

        let n = self
            .nodes
            .get(id)
            .ok_or_else(|| ApplyError::UnknownNode(id.clone()))?;
        match &n.state {
            NodeState::Running { attempt: a } if *a == attempt => {}
            NodeState::Cancelled => return Ok(()),
            other => {
                return Err(ApplyError::Illegal(format!(
                    "finish {id} from {other:?} attempt {attempt}"
                )));
            }
        }

        let outcome = match outcome {
            Ok(o) => o,
            Err(panic_msg) => NodeOutcome::Failed(NodeError::new(format!("panic: {panic_msg}"))),
        };

        self.apply_outcome(id, outcome, policy, now, effect)
    }

    fn apply_outcome(
        &mut self,
        id: &NodeId,
        outcome: NodeOutcome,
        policy: &dyn Policy,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) -> Result<(), ApplyError> {
        use crate::domain::events::DomainEvent;
        let attempt = self.nodes.get(id).map(|n| n.attempt).unwrap_or(0);

        // Illegal Retry after Succeeded / Waiting fails the node.
        let decision = policy.decide(&outcome, attempt);
        if matches!(decision, PolicyDecision::Retry { .. })
            && matches!(
                outcome,
                NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }
            )
        {
            self.fail_node(
                id,
                NodeError::new("illegal policy Retry after Succeeded/Waiting"),
                effect,
            );
            self.fail_fast(effect);
            return Ok(());
        }

        match (&outcome, decision) {
            (NodeOutcome::Succeeded(bytes), PolicyDecision::Accept) => {
                if let Some(n) = self.nodes.get_mut(id) {
                    n.output = Some(bytes.clone());
                    n.last_outcome = Some(outcome.clone());
                    n.state = NodeState::Succeeded;
                    n.last_error = None;
                }
                effect.events.push(DomainEvent::NodeSucceeded {
                    node_id: id.clone(),
                });
                self.ready_successors(id, effect);
            }
            (NodeOutcome::Waiting { token }, PolicyDecision::Accept) => {
                if let Some(n) = self.nodes.get_mut(id) {
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
            }
            (NodeOutcome::Failed(err), PolicyDecision::Accept) => {
                self.fail_node(id, err.clone(), effect);
                self.fail_fast(effect);
            }
            (NodeOutcome::TimedOut, PolicyDecision::Accept) => {
                if let Some(n) = self.nodes.get_mut(id) {
                    n.state = NodeState::TimedOut;
                    n.last_error = Some(NodeError::new("timed out"));
                    n.last_outcome = Some(outcome);
                }
                effect.events.push(DomainEvent::NodeTimedOut {
                    node_id: id.clone(),
                });
                self.fail_fast(effect);
            }
            (NodeOutcome::Failed(_) | NodeOutcome::TimedOut, PolicyDecision::Retry { delay }) => {
                if let Some(n) = self.nodes.get_mut(id) {
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
                    if at.is_none() {
                        effect.newly_runnable.push(id.clone());
                    }
                }
            }
            (NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }, PolicyDecision::Retry { .. }) => {
                unreachable!("illegal retry handled above");
            }
        }
        Ok(())
    }

    fn fail_node(&mut self, id: &NodeId, err: NodeError, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        if let Some(n) = self.nodes.get_mut(id) {
            n.state = NodeState::Failed;
            n.last_error = Some(err.clone());
            n.last_outcome = Some(NodeOutcome::Failed(err.clone()));
        }
        effect.events.push(DomainEvent::NodeFailed {
            node_id: id.clone(),
            error: err,
        });
    }

    /// After policy Accepts Failed/TimedOut: all non-terminal nodes Cancelled, execution Failed.
    fn fail_fast(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        for (id, n) in self.nodes.iter_mut() {
            if !n.state.is_terminal() {
                if matches!(n.state, NodeState::Running { .. }) {
                    effect.to_abort.push(id.clone());
                }
                n.state = NodeState::Cancelled;
                effect.events.push(DomainEvent::NodeCancelled {
                    node_id: id.clone(),
                });
            }
        }
        self.state = ExecutionState::Failed;
    }

    fn ready_successors(&mut self, succeeded: &NodeId, effect: &mut ApplyEffect) {
        let succs = self.definition.successors(succeeded);
        for succ in succs {
            if self.all_preds_succeeded(&succ) {
                if let Some(n) = self.nodes.get(&succ) {
                    if matches!(n.state, NodeState::Pending) {
                        self.mark_ready(&succ, None, effect);
                    }
                }
            }
        }
    }

    fn all_preds_succeeded(&self, id: &NodeId) -> bool {
        self.definition
            .predecessors(id)
            .into_iter()
            .all(|p| matches!(self.nodes.get(&p).map(|n| &n.state), Some(NodeState::Succeeded)))
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
            // Duplicate equivalent Complete after success is Ok noop.
            if let Resume::Complete(ref outcome) = resume {
                if let Some(n) = self.nodes.get(&token.node_id) {
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

        let node = self
            .nodes
            .get(&token.node_id)
            .ok_or_else(|| ApplyError::UnknownNode(token.node_id.clone()))?;

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
                if let Some(n) = self.nodes.get_mut(&token.node_id) {
                    n.state = NodeState::Running {
                        attempt: token.attempt,
                    };
                }
                self.apply_outcome(&token.node_id, outcome, policy, now, effect)?;
            }
            Resume::Reinvoke => {
                if let Some(n) = self.nodes.get_mut(&token.node_id) {
                    n.reinvoke = true;
                    n.state = NodeState::Ready { runnable_at: None };
                    effect.newly_runnable.push(token.node_id);
                }
            }
        }
        Ok(())
    }

    fn cancel_graph(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        self.cancelled = true;
        for (id, n) in self.nodes.iter_mut() {
            if !n.state.is_terminal() {
                if matches!(n.state, NodeState::Running { .. }) {
                    effect.to_abort.push(id.clone());
                }
                n.state = NodeState::Cancelled;
                effect.events.push(DomainEvent::NodeCancelled {
                    node_id: id.clone(),
                });
            }
        }
        self.state = ExecutionState::Cancelled;
    }

    fn force_cancel_running(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        for (id, n) in self.nodes.iter_mut() {
            if matches!(n.state, NodeState::Running { .. }) {
                n.state = NodeState::Cancelled;
                effect.to_abort.push(id.clone());
                effect.events.push(DomainEvent::NodeCancelled {
                    node_id: id.clone(),
                });
            }
        }
        if !self.state.is_terminal() {
            self.state = ExecutionState::Cancelled;
            self.cancelled = true;
        }
    }

    fn recompute_execution_state(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        if self.state == ExecutionState::Failed || self.state == ExecutionState::Cancelled {
            if self.state == ExecutionState::Failed {
                effect.events.push(DomainEvent::ExecutionFailed {
                    execution_id: self.id.clone(),
                });
            } else {
                effect.events.push(DomainEvent::ExecutionCancelled {
                    execution_id: self.id.clone(),
                });
            }
            return;
        }

        let mut any_running = false;
        let mut any_ready_now = false;
        let mut any_ready_later = false;
        let mut any_waiting = false;
        let mut all_succeeded = true;
        let mut any_failed = false;

        for n in self.nodes.values() {
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
                    // Blocked on predecessors (AND-join). Does not keep the
                    // execution Running — if the only live nodes are Waiting,
                    // the aggregate is Waiting so Handle/tests can resume.
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

        let next = if any_failed {
            ExecutionState::Failed
        } else if all_succeeded {
            ExecutionState::Succeeded
        } else if any_running || any_ready_now || any_ready_later {
            ExecutionState::Running
        } else if any_waiting {
            ExecutionState::Waiting
        } else {
            self.state
        };

        if next == ExecutionState::Succeeded && self.state != ExecutionState::Succeeded {
            effect.events.push(DomainEvent::ExecutionSucceeded {
                execution_id: self.id.clone(),
            });
        } else if next == ExecutionState::Waiting && self.state != ExecutionState::Waiting {
            effect.events.push(DomainEvent::ExecutionWaiting {
                execution_id: self.id.clone(),
            });
        }
        self.state = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn fail_fast_cancels_dependents() {
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
        ex.apply(
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
        // Force waiting
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
