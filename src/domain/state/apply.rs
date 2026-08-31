use super::*;
use crate::domain::definition::{Join, OnFailure};
use crate::domain::events::DomainEvent;
use crate::domain::policy::{Policy, PolicyDecision};

impl Execution {
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
                self.set_state(slot, NodeState::Ready { runnable_at: None });
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
        let st = &self.nodes[slot.0].state;
        if st.is_terminal() || matches!(st, NodeState::Running { .. }) {
            return;
        }
        self.set_state(slot, NodeState::Ready { runnable_at });
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
        match &self.nodes[slot.0].state {
            NodeState::Ready { runnable_at } if runnable_at.is_none() => {}
            other => {
                return Err(ApplyError::Illegal(format!(
                    "dispatch {id} from {other:?}"
                )));
            }
        }
        let attempt = {
            let n = &mut self.nodes[slot.0];
            if !n.reinvoke {
                n.attempt = n.attempt.saturating_add(1);
            }
            n.reinvoke = false;
            let attempt = n.attempt;
            let token = ResumeToken::issue(self.id.clone(), id.clone(), attempt);
            n.resume_token = Some(token);
            attempt
        };
        self.set_state(slot, NodeState::Running { attempt });
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
                {
                    let n = &mut self.nodes[slot.0];
                    n.output = Some(bytes.clone());
                    n.last_outcome = Some(outcome.clone());
                    n.last_error = None;
                }
                self.set_state(slot, NodeState::Succeeded);
                effect.events.push(DomainEvent::NodeSucceeded {
                    node_id: id.clone(),
                });
                self.ready_successors(slot, effect);
            }
            (NodeOutcome::Waiting { token }, PolicyDecision::Accept) => {
                let token = {
                    let n = &mut self.nodes[slot.0];
                    let token = n.resume_token.clone().unwrap_or_else(|| token.clone());
                    n.resume_token = Some(token.clone());
                    n.last_outcome = Some(NodeOutcome::Waiting {
                        token: token.clone(),
                    });
                    token
                };
                self.set_state(
                    slot,
                    NodeState::Waiting {
                        token: token.clone(),
                        attempt,
                    },
                );
                effect.events.push(DomainEvent::NodeWaiting {
                    node_id: id.clone(),
                    token,
                });
            }
            (NodeOutcome::Failed(err), PolicyDecision::Accept) => {
                self.fail_node(slot, id, err.clone(), effect);
                self.apply_on_failure(slot, effect);
            }
            (NodeOutcome::TimedOut, PolicyDecision::Accept) => {
                {
                    let n = &mut self.nodes[slot.0];
                    n.last_error = Some(NodeError::new("timed out"));
                    n.last_outcome = Some(outcome);
                }
                self.set_state(slot, NodeState::TimedOut);
                effect.events.push(DomainEvent::NodeTimedOut {
                    node_id: id.clone(),
                });
                self.apply_on_failure(slot, effect);
            }
            (NodeOutcome::Failed(_) | NodeOutcome::TimedOut, PolicyDecision::Retry { delay }) => {
                let at = if delay.is_zero() {
                    None
                } else {
                    Some(now.saturating_add(delay))
                };
                {
                    let n = &mut self.nodes[slot.0];
                    n.last_error = match &outcome {
                        NodeOutcome::Failed(e) => Some(e.clone()),
                        _ => Some(NodeError::new("timed out")),
                    };
                    n.last_outcome = Some(outcome);
                }
                self.set_state(slot, NodeState::Ready { runnable_at: at });
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
                self.apply_on_failure(slot, effect);
            }
            (NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }, PolicyDecision::Retry { .. }) => {
                unreachable!("illegal retry handled above");
            }
        }
        Ok(())
    }

    fn fail_node(&mut self, slot: NodeSlot, id: &NodeId, err: NodeError, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        {
            let n = &mut self.nodes[slot.0];
            n.last_error = Some(err.clone());
            n.last_outcome = Some(NodeOutcome::Failed(err.clone()));
        }
        self.set_state(slot, NodeState::Failed);
        self.clear_deadline_if(slot);
        effect.events.push(DomainEvent::NodeFailed {
            node_id: id.clone(),
            error: err,
        });
    }

    fn apply_on_failure(&mut self, slot: NodeSlot, effect: &mut ApplyEffect) {
        match self.definition.on_failure() {
            OnFailure::FailExecution => self.fail_fast(effect),
            OnFailure::FailSubtree => self.fail_subtree(slot, effect),
        }
    }

    /// After policy Accepts Failed/TimedOut (or Rejects): non-terminal nodes
    /// Cancelled, execution Failed. Sets the same abort flag as [`Self::cancel_graph`].
    fn cancel_non_terminals(&mut self, effect: &mut ApplyEffect) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].state.is_terminal() {
                continue;
            }
            let running = matches!(self.nodes[i].state, NodeState::Running { .. });
            self.set_state(NodeSlot(i), NodeState::Cancelled);
            let id = self.definition.id_at(NodeSlot(i)).clone();
            if running {
                effect.to_abort.push(id.clone());
            }
            effect.events.push(DomainEvent::NodeCancelled { node_id: id });
        }
        self.next_deadline = None;
    }

    fn fail_fast(&mut self, effect: &mut ApplyEffect) {
        self.cancelled = true;
        self.fail_execution = true;
        self.cancel_non_terminals(effect);
        self.state = ExecutionState::Failed;
    }

    /// Cancel AllSucceeded descendants of `origin` (already Failed/TimedOut).
    /// AllDone dependents stay Pending until every predecessor is terminal.
    /// Iterative — deep DAGs must not blow the stack.
    fn fail_subtree(&mut self, origin: NodeSlot, effect: &mut ApplyEffect) {
        let mut stack: Vec<NodeSlot> = self.definition.succ_slots(origin).to_vec();
        while let Some(succ) = stack.pop() {
            if self.nodes[succ.0].state.is_terminal() {
                continue;
            }
            match self.definition.join_at(succ) {
                Join::AllSucceeded => {
                    self.cancel_node(succ, effect);
                    stack.extend_from_slice(self.definition.succ_slots(succ));
                }
                Join::AllDone => {
                    if self.remain[succ.0] > 0 {
                        self.remain[succ.0] -= 1;
                    }
                    if self.remain[succ.0] == 0
                        && matches!(self.nodes[succ.0].state, NodeState::Pending)
                    {
                        self.mark_ready(succ, None, effect);
                    }
                }
            }
        }
    }

    fn cancel_node(&mut self, slot: NodeSlot, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        if self.nodes[slot.0].state.is_terminal() {
            return;
        }
        let running = matches!(self.nodes[slot.0].state, NodeState::Running { .. });
        self.set_state(slot, NodeState::Cancelled);
        self.clear_deadline_if(slot);
        let id = self.definition.id_at(slot).clone();
        if running {
            effect.to_abort.push(id.clone());
        }
        effect.events.push(DomainEvent::NodeCancelled { node_id: id });
    }

    fn ready_successors(&mut self, succeeded: NodeSlot, effect: &mut ApplyEffect) {
        let nsucc = self.definition.succ_slots(succeeded).len();
        for i in 0..nsucc {
            let succ = self.definition.succ_slots(succeeded)[i];
            debug_assert!(self.remain[succ.0] > 0);
            self.remain[succ.0] -= 1;
            if self.remain[succ.0] == 0
                && matches!(self.nodes[succ.0].state, NodeState::Pending)
            {
                self.mark_ready(succ, None, effect);
            }
        }
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
                if let Some(n) = self.node(token.node_id()) {
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
            .slot(token.node_id())
            .ok_or_else(|| ApplyError::UnknownNode(token.node_id().clone()))?;
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
                self.set_state(
                    slot,
                    NodeState::Running {
                        attempt: token.attempt(),
                    },
                );
                effect.changed = true;
                self.apply_outcome(slot, token.node_id(), outcome, policy, now, effect)?;
            }
            Resume::Reinvoke => {
                self.nodes[slot.0].reinvoke = true;
                self.set_state(slot, NodeState::Ready { runnable_at: None });
                effect.newly_runnable.push(token.node_id().clone());
                effect.changed = true;
            }
        }
        Ok(())
    }

    fn cancel_graph(&mut self, effect: &mut ApplyEffect) {
        self.cancelled = true;
        self.cancel_non_terminals(effect);
        self.state = ExecutionState::Cancelled;
    }

    fn force_cancel_running(&mut self, effect: &mut ApplyEffect) {
        use crate::domain::events::DomainEvent;
        for i in 0..self.nodes.len() {
            if matches!(self.nodes[i].state, NodeState::Running { .. }) {
                self.set_state(NodeSlot(i), NodeState::Cancelled);
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
        if self.fail_execution {
            return ExecutionState::Failed;
        }
        if self.cancelled {
            return ExecutionState::Cancelled;
        }
        let live = self.n_pending + self.n_ready + self.n_running + self.n_waiting;
        if live == 0 {
            if self.n_failed == 0 && self.n_cancelled == 0 {
                ExecutionState::Succeeded
            } else {
                ExecutionState::Completed
            }
        } else if self.n_running > 0 || self.n_ready > 0 {
            ExecutionState::Running
        } else if self.n_waiting > 0 {
            ExecutionState::Waiting
        } else {
            // Isolated Pending (should not happen if FailSubtree cancels
            // AllSucceeded dependents). Stay Running so wait() does not lie.
            ExecutionState::Running
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
                ExecutionState::Completed => {
                    effect.events.push(DomainEvent::ExecutionCompleted {
                        execution_id: self.id.clone(),
                    });
                }
                ExecutionState::Running | ExecutionState::Created => {}
            }
        }
        self.state = next;
    }
}
