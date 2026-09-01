use crate::domain::definition::WorkflowDefinition;
use crate::domain::ids::{ExecutionId, ExecutorId, NodeId, NodeSlot, ResumeToken, WorkflowId};
use crate::domain::outcome::{NodeError, NodeOutcome, Resume};
use crate::domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SCHEMA_VERSION};
use crate::domain::time::Timestamp;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Pending,
    /// Dispatchable now (`runnable_at: None`) or parked until Instant **T**
    /// (`Some(T)`). T is the snapshot deadline for retry backoff (and any
    /// other "not runnable until T" policy). Waiting is an executor yield, not a timer.
    Ready {
        runnable_at: Option<Timestamp>,
    },
    Running {
        attempt: u32,
    },
    Waiting {
        token: ResumeToken,
        attempt: u32,
    },
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

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("Pending"),
            Self::Ready { runnable_at: None } => f.write_str("Ready"),
            Self::Ready {
                runnable_at: Some(at),
            } => write!(f, "Ready({at})"),
            Self::Running { attempt } => write!(f, "Running({attempt})"),
            Self::Waiting { attempt, .. } => write!(f, "Waiting({attempt})"),
            Self::Succeeded => f.write_str("Succeeded"),
            Self::Failed => f.write_str("Failed"),
            Self::Cancelled => f.write_str("Cancelled"),
            Self::TimedOut => f.write_str("TimedOut"),
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
    /// Every node terminal, mixed outcomes, [`OnFailure::FailSubtree`] (no fail-fast).
    Completed,
}

impl ExecutionState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Completed
        )
    }

    /// Dashboard helper: the job finished and was not Failed/Cancelled.
    ///
    /// True for [`Succeeded`](Self::Succeeded) **and** [`Completed`](Self::Completed)
    /// (FailSubtree mixed terminals). False for Failed, Cancelled, Waiting,
    /// Running, Created. Do not use `== Succeeded` to mean “the pipeline is done ok.”
    pub fn is_successful_finish(self) -> bool {
        matches!(self, Self::Succeeded | Self::Completed)
    }
}

impl fmt::Display for ExecutionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Created => "Created",
            Self::Running => "Running",
            Self::Waiting => "Waiting",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::Completed => "Completed",
        })
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
    pub(crate) definition: Arc<WorkflowDefinition>,
    pub(crate) state: ExecutionState,
    pub(crate) nodes: Vec<NodeRuntime>,
    pub(crate) revision: u64,
    pub(crate) cancelled: bool,
    /// Set only by [`Self::fail_fast`] (`OnFailure::FailExecution`).
    pub(crate) fail_execution: bool,
    pub(crate) next_deadline: Option<(Timestamp, NodeSlot)>,
    /// Slots mutated since last persist / snapshot cache flush.
    pub(crate) dirty: Vec<u8>,
    pub(crate) dirty_list: Vec<NodeSlot>,
    pub(crate) n_pending: u32,
    pub(crate) n_ready: u32,
    pub(crate) n_running: u32,
    pub(crate) n_waiting: u32,
    pub(crate) n_succeeded: u32,
    pub(crate) n_failed: u32,
    pub(crate) n_cancelled: u32,
    /// Remaining unsatisfied AND-join predecessors per slot. Decremented
    /// once when a predecessor becomes Succeeded. Zero + Pending ⇒ Ready.
    pub(crate) remain: Vec<u32>,
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
    StartNode {
        node_id: NodeId,
    },
    FinishNode {
        node_id: NodeId,
        attempt: u32,
        outcome: Result<NodeOutcome, String>,
    },
    Resume {
        token: ResumeToken,
        resume: Resume,
    },
    Cancel,
    RetryDue {
        node_id: NodeId,
    },
    ForceCancelRunning,
}

/// Result of one [`Execution::apply`].
///
/// `newly_runnable` / `to_abort` are dense slots so the scheduler can enqueue
/// and abort without hashing [`NodeId`]. Apply-only drivers (no scheduler)
/// map them with [`Self::newly_runnable_ids`]. Slots are never public.
#[derive(Clone, Debug, Default)]
pub struct ApplyEffect {
    pub events: Vec<crate::domain::events::Event>,
    pub(crate) newly_runnable: Vec<NodeSlot>,
    pub(crate) to_abort: Vec<NodeSlot>,
    pub changed: bool,
}

impl ApplyEffect {
    /// Node ids that became immediately runnable. The scheduler does not use
    /// this; it reads slots. Public `inputs_for` is unchanged.
    pub fn newly_runnable_ids<'a>(
        &'a self,
        exec: &'a Execution,
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.newly_runnable
            .iter()
            .map(|&s| exec.node_id_at(s).clone())
    }
}

impl Execution {
    pub fn new(definition: WorkflowDefinition) -> Self {
        let n = definition.len();
        let nodes: Vec<NodeRuntime> = (0..n).map(|_| NodeRuntime::default()).collect();
        let dirty = vec![1u8; n];
        let dirty_list: Vec<NodeSlot> = (0..n).map(NodeSlot).collect();
        let remain: Vec<u32> = (0..n)
            .map(|i| definition.pred_slots(NodeSlot(i)).len() as u32)
            .collect();
        let definition = Arc::new(definition);
        let mut exec = Self {
            id: ExecutionId::new(),
            workflow_id: definition.id().clone(),
            definition,
            state: ExecutionState::Created,
            nodes,
            revision: 0,
            cancelled: false,
            fail_execution: false,
            next_deadline: None,
            dirty,
            dirty_list,
            n_pending: 0,
            n_ready: 0,
            n_running: 0,
            n_waiting: 0,
            n_succeeded: 0,
            n_failed: 0,
            n_cancelled: 0,
            remain,
        };
        for i in 0..n {
            exec.inc_kind(count_kind(&exec.nodes[i].state));
        }
        exec
    }

    pub fn id(&self) -> &ExecutionId {
        &self.id
    }

    /// Definition this execution was started from. Data, not slot state.
    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }

    pub(crate) fn definition_arc(&self) -> &Arc<WorkflowDefinition> {
        &self.definition
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

    pub(crate) fn is_dispatchable_slot(&self, slot: NodeSlot) -> bool {
        matches!(
            self.nodes[slot.0].state,
            NodeState::Ready { runnable_at: None }
        )
    }

    pub(crate) fn mark_dirty(&mut self, slot: NodeSlot) {
        if self.dirty[slot.0] == 0 {
            self.dirty[slot.0] = 1;
            self.dirty_list.push(slot);
        }
    }

    pub(crate) fn dirty_slots(&self) -> &[NodeSlot] {
        &self.dirty_list
    }

    /// Nodes that changed since create or the last successful persist.
    /// File adapters write these instead of cloning the whole graph (ADR 0002).
    pub fn dirty_nodes(&self) -> Vec<(NodeId, NodeSnapshot)> {
        self.dirty_list
            .iter()
            .map(|slot| {
                (
                    self.definition.id_at(*slot).clone(),
                    self.node_snapshot_at(*slot),
                )
            })
            .collect()
    }

    pub(crate) fn clear_dirty(&mut self) {
        for s in self.dirty_list.drain(..) {
            self.dirty[s.0] = 0;
        }
    }

    pub(crate) fn node_snapshot_at(&self, slot: NodeSlot) -> NodeSnapshot {
        let n = &self.nodes[slot.0];
        NodeSnapshot {
            state: n.state.clone(),
            output: n.output.clone(),
            attempt: n.attempt,
            resume_token: n.resume_token.clone(),
            last_error: n.last_error.clone(),
        }
    }

    pub(crate) fn node_id_at(&self, slot: NodeSlot) -> &NodeId {
        self.definition.id_at(slot)
    }

    pub(crate) fn set_state(&mut self, slot: NodeSlot, new: NodeState) {
        let new_kind = count_kind(&new);
        let old = std::mem::replace(&mut self.nodes[slot.0].state, new);
        self.dec_count(&old);
        self.inc_kind(new_kind);
        self.mark_dirty(slot);
    }

    fn dec_count(&mut self, s: &NodeState) {
        // Terminal states are never left; n_succeeded/failed/cancelled only increase.
        if matches!(s, NodeState::Pending) {
            self.n_pending -= 1;
        } else if matches!(s, NodeState::Ready { .. }) {
            self.n_ready -= 1;
        } else if matches!(s, NodeState::Running { .. }) {
            self.n_running -= 1;
        } else if matches!(s, NodeState::Waiting { .. }) {
            self.n_waiting -= 1;
        }
    }

    fn inc_kind(&mut self, kind: u8) {
        match kind {
            0 => self.n_pending += 1,
            1 => self.n_ready += 1,
            2 => self.n_running += 1,
            3 => self.n_waiting += 1,
            4 => self.n_succeeded += 1,
            5 => self.n_failed += 1,
            _ => self.n_cancelled += 1,
        }
    }

    pub fn executor_id(&self, id: &NodeId) -> Option<&ExecutorId> {
        let slot = self.definition.slot(id)?;
        Some(self.definition.executor_at(slot))
    }

    pub fn attempt(&self, id: &NodeId) -> Option<u32> {
        self.node(id).map(|n| n.attempt)
    }

    pub(crate) fn attempt_at(&self, slot: NodeSlot) -> u32 {
        self.nodes[slot.0].attempt
    }

    pub fn resume_token(&self, id: &NodeId) -> Option<ResumeToken> {
        self.node(id).and_then(|n| n.resume_token.clone())
    }

    pub(crate) fn resume_token_at(&self, slot: NodeSlot) -> Option<ResumeToken> {
        self.nodes[slot.0].resume_token.clone()
    }

    /// Next snapshot deadline T (`Ready { runnable_at: Some(T) }`). Park
    /// sleeps until this Instant. Waiting is not consulted.
    pub fn next_deadline(&self) -> Option<(Timestamp, NodeId)> {
        self.next_deadline
            .map(|(ts, slot)| (ts, self.definition.id_at(slot).clone()))
    }

    pub fn inputs_for(&self, id: &NodeId) -> HashMap<NodeId, Bytes> {
        let Some(slot) = self.definition.slot(id) else {
            return HashMap::new();
        };
        self.inputs_for_slot(slot)
    }

    pub(crate) fn inputs_for_slot(&self, slot: NodeSlot) -> HashMap<NodeId, Bytes> {
        let preds = self.definition.pred_slots(slot);
        let mut out = HashMap::with_capacity(preds.len());
        for pred in preds {
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
            node_order: (0..self.definition.len())
                .map(|i| self.definition.id_at(NodeSlot(i)).clone())
                .collect(),
            definition_hash: self.definition.hash_if_ready().unwrap_or_default(),
        }
    }
}

mod apply;
mod restore;

fn count_kind(s: &NodeState) -> u8 {
    match s {
        NodeState::Pending => 0,
        NodeState::Ready { .. } => 1,
        NodeState::Running { .. } => 2,
        NodeState::Waiting { .. } => 3,
        NodeState::Succeeded => 4,
        NodeState::Failed | NodeState::TimedOut => 5,
        NodeState::Cancelled => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::definition::{Join, OnFailure};
    use crate::domain::events::Event;
    use crate::domain::policy::{AcceptPolicy, Policy, PolicyDecision};
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
            .filter(|e| matches!(e, Event::ExecutionFailed { .. }))
            .count()
    }

    #[test]
    fn successful_finish_is_succeeded_or_completed() {
        assert!(ExecutionState::Succeeded.is_successful_finish());
        assert!(ExecutionState::Completed.is_successful_finish());
        assert!(!ExecutionState::Failed.is_successful_finish());
        assert!(!ExecutionState::Cancelled.is_successful_finish());
        assert!(!ExecutionState::Waiting.is_successful_finish());
        assert!(!ExecutionState::Running.is_successful_finish());
        assert!(!ExecutionState::Created.is_successful_finish());
    }

    #[test]
    fn start_marks_sources_ready() {
        let mut ex = linear();
        let now = Timestamp(0);
        let effect = ex.apply(ApplyCmd::Start, &AcceptPolicy, now).unwrap();
        let a = ex.definition.slot(&NodeId::new("a")).unwrap();
        assert_eq!(effect.newly_runnable, vec![a]);
        assert_eq!(
            effect.newly_runnable_ids(&ex).collect::<Vec<_>>(),
            vec![NodeId::new("a")]
        );
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
    fn fail_fast_aborts_running_sibling_by_slot() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
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
        let b = ex.definition.slot(&NodeId::new("b")).unwrap();
        assert_eq!(effect.to_abort, vec![b]);
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Cancelled
        ));
    }

    #[test]
    fn retry_delay_is_ready_not_waiting() {
        use crate::domain::policy::RetryPolicy;
        use std::time::Duration;
        let mut ex = linear();
        let now = Timestamp(1000);
        let p = RetryPolicy::new(3, Duration::from_millis(50));
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        assert_eq!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(Timestamp(1050))
            },
            "retry delay must be Ready {{ runnable_at }}, not Waiting"
        );
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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

    #[test]
    fn fail_subtree_diamond_completes_not_failed() {
        let def = WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("a", "e")
            .node("b", "e")
            .node("c", "e")
            .node("d", "e")
            .edge("a", "b")
            .edge("a", "c")
            .edge("b", "d")
            .edge("c", "d")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "c".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let effect = ex
            .apply(
                ApplyCmd::FinishNode {
                    node_id: "c".into(),
                    attempt: 1,
                    outcome: Ok(NodeOutcome::failed("boom")),
                },
                &p,
                now,
            )
            .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("d")).unwrap().state,
            NodeState::Cancelled
        ));
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Running { .. }
        ));
        assert_eq!(ex.state, ExecutionState::Running);
        assert_eq!(count_failed(&effect), 0);
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
        assert_eq!(ex.state, ExecutionState::Completed);
        assert!(!ex.cancelled);
    }

    #[test]
    fn all_done_ready_after_failed_pred() {
        let def = WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("a", "e")
            .node("b", "e")
            .node("j", "e")
            .edge("a", "j")
            .edge("b", "j")
            .join("j", Join::AllDone)
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
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
        assert!(matches!(
            ex.node(&NodeId::new("j")).unwrap().state,
            NodeState::Pending
        ));
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
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
            ex.node(&NodeId::new("j")).unwrap().state,
            NodeState::Ready { .. }
        ));
        assert_eq!(ex.inputs_for(&NodeId::new("j")).len(), 1);
        assert!(!ex
            .inputs_for(&NodeId::new("j"))
            .contains_key(&NodeId::new("a")));
    }

    #[test]
    fn double_cancel_is_noop_and_start_node_rejects_unknown_and_pending() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        assert!(!ex.is_cancelled());
        assert!(ex.is_ready_now(&NodeId::new("a"), now));
        assert!(!ex.is_ready_now(&NodeId::new("ghost"), now));
        assert!(ex.attempt(&NodeId::new("ghost")).is_none());
        assert!(ex.resume_token(&NodeId::new("ghost")).is_none());
        let pending = ex
            .apply(
                ApplyCmd::StartNode {
                    node_id: "b".into(),
                },
                &p,
                now,
            )
            .unwrap_err();
        assert!(matches!(pending, ApplyError::Illegal(_)));
        let unknown = ex
            .apply(
                ApplyCmd::StartNode {
                    node_id: "ghost".into(),
                },
                &p,
                now,
            )
            .unwrap_err();
        assert!(matches!(unknown, ApplyError::UnknownNode(_)));
        ex.apply(ApplyCmd::Cancel, &p, now).unwrap();
        assert!(ex.is_cancelled());
        let rev = ex.revision;
        let again = ex.apply(ApplyCmd::Cancel, &p, now).unwrap();
        assert!(!again.changed);
        assert_eq!(ex.revision, rev);
        assert_eq!(ex.state, ExecutionState::Cancelled);
    }

    #[test]
    fn retry_due_after_cancel_does_not_wake_dead_execution() {
        use crate::domain::policy::RetryPolicy;
        let mut ex = linear();
        let p = RetryPolicy::new(3, std::time::Duration::from_millis(10));
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("once")),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(_)
            }
        ));
        ex.apply(ApplyCmd::Cancel, &p, now).unwrap();
        let rev = ex.revision();
        let effect = ex
            .apply(
                ApplyCmd::RetryDue {
                    node_id: "a".into(),
                },
                &p,
                Timestamp(10),
            )
            .unwrap();
        assert!(!effect.changed);
        assert_eq!(ex.revision(), rev);
        assert_eq!(ex.state(), ExecutionState::Cancelled);
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Cancelled
        ));
    }

    #[test]
    fn dirty_nodes_lists_slots_changed_by_apply() {
        let mut ex = linear();
        assert_eq!(ex.dirty_nodes().len(), 2);
        ex.clear_dirty();
        assert!(ex.dirty_nodes().is_empty());
        ex.apply(ApplyCmd::Start, &AcceptPolicy, Timestamp(0))
            .unwrap();
        let dirty = ex.dirty_nodes();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0.as_str(), "a");
        assert!(matches!(dirty[0].1.state, NodeState::Ready { .. }));
    }

    #[test]
    fn force_cancel_running_aborts_and_cancels_execution() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let effect = ex.apply(ApplyCmd::ForceCancelRunning, &p, now).unwrap();
        let a = ex.definition.slot(&NodeId::new("a")).unwrap();
        assert_eq!(effect.to_abort, vec![a]);
        assert_eq!(ex.state, ExecutionState::Cancelled);
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Cancelled
        ));
    }

    #[test]
    fn fail_subtree_skips_already_cancelled_successor() {
        let def = WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("a", "e")
            .node("b", "e")
            .node("d", "e")
            .edge("a", "d")
            .edge("b", "d")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("a")),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("d")).unwrap().state,
            NodeState::Cancelled
        ));
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "b".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("b")),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("d")).unwrap().state,
            NodeState::Cancelled
        ));
        assert_eq!(ex.state, ExecutionState::Completed);
    }

    #[test]
    fn resume_failed_node_while_sibling_runs_is_resume_after_cancel() {
        let def = WorkflowDefinition::builder("wf")
            .on_failure(OnFailure::FailSubtree)
            .node("a", "e")
            .node("b", "e")
            .build()
            .unwrap();
        let mut ex = Execution::new(def);
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let token = ex.resume_token(&NodeId::new("a")).unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("a")),
            },
            &p,
            now,
        )
        .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Running { .. }
        ));
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

    #[test]
    fn resume_reinvoke_on_succeeded_is_not_waiting() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let token = ex.resume_token(&NodeId::new("a")).unwrap();
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
        let err = ex
            .apply(
                ApplyCmd::Resume {
                    token,
                    resume: Resume::Reinvoke,
                },
                &p,
                now,
            )
            .unwrap_err();
        assert_eq!(err, ApplyError::NotWaiting);
    }

    #[test]
    fn conflicting_complete_on_succeeded_node() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let token = ex.resume_token(&NodeId::new("a")).unwrap();
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
        let err = ex
            .apply(
                ApplyCmd::Resume {
                    token: token.clone(),
                    resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"other"))),
                },
                &p,
                now,
            )
            .unwrap_err();
        assert_eq!(err, ApplyError::ConflictingComplete);
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "b".into(),
            },
            &p,
            now,
        )
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
        assert_eq!(ex.state, ExecutionState::Succeeded);
        let rev = ex.revision;
        ex.apply(
            ApplyCmd::Resume {
                token: token.clone(),
                resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"a"))),
            },
            &p,
            now,
        )
        .unwrap();
        assert_eq!(
            ex.revision, rev,
            "equivalent complete after Succeeded is a no-op"
        );
        let err = ex
            .apply(
                ApplyCmd::Resume {
                    token: token.clone(),
                    resume: Resume::Complete(NodeOutcome::Succeeded(Bytes::from_static(b"zzz"))),
                },
                &p,
                now,
            )
            .unwrap_err();
        assert_eq!(err, ApplyError::ConflictingComplete);
        let err = ex
            .apply(
                ApplyCmd::Resume {
                    token,
                    resume: Resume::Reinvoke,
                },
                &p,
                now,
            )
            .unwrap_err();
        assert_eq!(err, ApplyError::ResumeAfterCancel);
    }

    #[test]
    fn resume_complete_on_failed_node_after_execution_failed() {
        let mut ex = linear();
        let now = Timestamp(0);
        let p = AcceptPolicy;
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let token = ex.resume_token(&NodeId::new("a")).unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::failed("a")),
            },
            &p,
            now,
        )
        .unwrap();
        assert_eq!(ex.state, ExecutionState::Failed);
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

    #[test]
    fn retry_timed_out_zero_delay_is_immediately_runnable() {
        use crate::domain::policy::RetryPolicy;
        use std::time::Duration;
        let mut ex = linear();
        let now = Timestamp(0);
        let p = RetryPolicy::new(3, Duration::ZERO);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(
            ApplyCmd::StartNode {
                node_id: "a".into(),
            },
            &p,
            now,
        )
        .unwrap();
        let effect = ex
            .apply(
                ApplyCmd::FinishNode {
                    node_id: "a".into(),
                    attempt: 1,
                    outcome: Ok(NodeOutcome::TimedOut),
                },
                &p,
                now,
            )
            .unwrap();
        assert!(!effect.newly_runnable.is_empty());
        assert!(matches!(
            ex.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready { runnable_at: None }
        ));
    }

    #[test]
    fn node_state_ready_now_overdue_and_non_ready() {
        assert!(NodeState::Ready { runnable_at: None }.is_ready_now(Timestamp(0)));
        assert!(NodeState::Ready {
            runnable_at: Some(Timestamp(1)),
        }
        .is_ready_now(Timestamp(5)));
        assert!(!NodeState::Ready {
            runnable_at: Some(Timestamp(10)),
        }
        .is_ready_now(Timestamp(5)));
        assert!(!NodeState::Pending.is_ready_now(Timestamp(0)));
        assert!(!NodeState::Succeeded.is_ready_now(Timestamp(0)));
    }
}
