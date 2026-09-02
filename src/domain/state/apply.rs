use super::*;
use crate::domain::definition::{Join, OnFailure};
use crate::domain::events::{Event, ExecKind, NodeKind};
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
                self.emit_exec(&mut effect, now, ExecKind::Started);
                let nsrc = self.definition.source_slots().len();
                for i in 0..nsrc {
                    let src = self.definition.source_slots()[i];
                    self.mark_ready(src, &mut effect);
                }
                effect.changed = true;
            }
            ApplyCmd::StartNode { node_id } => {
                self.dispatch_node(&node_id, now, &mut effect)?;
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
                // Already terminal (Succeeded / Failed / Completed / Cancelled):
                // no-op. Do not rewrite a finished run to Cancelled.
                if self.state.is_terminal() {
                    return Ok(effect);
                }
                self.cancel_graph(now, &mut effect);
                effect.changed = true;
            }
            ApplyCmd::RetryDue { node_id } => {
                if self.cancelled {
                    return Ok(effect);
                }
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
                effect.newly_runnable.push(slot);
                effect.changed = true;
            }
            ApplyCmd::ForceCancelRunning => {
                if !self
                    .nodes
                    .iter()
                    .any(|n| matches!(n.state, NodeState::Running { .. }))
                {
                    return Ok(effect);
                }
                self.force_cancel_running(now, &mut effect);
                effect.changed = true;
            }
            ApplyCmd::RetryFailed => {
                match self.state {
                    ExecutionState::Failed | ExecutionState::Completed => {}
                    other => {
                        return Err(ApplyError::Illegal(format!(
                            "RetryFailed only from Failed or Completed, got {other}"
                        )));
                    }
                }
                self.apply_retry_failed(&mut effect);
                effect.changed = true;
            }
        }
        if !effect.changed {
            return Ok(effect);
        }
        self.recompute_execution_state(prev_state, &mut effect, now);
        self.revision += 1;
        Ok(effect)
    }

    fn emit_exec(&self, effect: &mut ApplyEffect, now: Timestamp, kind: ExecKind) {
        effect.events.push(Event::exec(
            kind,
            self.id.clone(),
            self.workflow_id.clone(),
            now,
        ));
    }

    fn emit_node(
        &self,
        effect: &mut ApplyEffect,
        now: Timestamp,
        node_id: NodeId,
        attempt: u32,
        kind: NodeKind,
    ) {
        effect.events.push(Event::node(
            kind,
            self.id.clone(),
            self.workflow_id.clone(),
            node_id,
            attempt,
            now,
        ));
    }

    fn mark_ready(&mut self, slot: NodeSlot, effect: &mut ApplyEffect) {
        // Callers only pass Pending (Start sources; AllDone remain==0).
        // Ready is not a public Event (no NodeReady).
        self.set_state(slot, NodeState::Ready { runnable_at: None });
        effect.newly_runnable.push(slot);
    }

    fn dispatch_node(
        &mut self,
        id: &NodeId,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) -> Result<(), ApplyError> {
        let slot = self
            .definition
            .slot(id)
            .ok_or_else(|| ApplyError::UnknownNode(id.clone()))?;
        match &self.nodes[slot.0].state {
            NodeState::Ready { runnable_at } if runnable_at.is_none() => {}
            other => {
                return Err(ApplyError::Illegal(format!("dispatch {id} from {other:?}")));
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
        self.emit_node(effect, now, id.clone(), attempt, NodeKind::Started);
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
        let attempt = self.nodes[slot.0].attempt;

        let decision = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            policy.decide(&outcome, attempt)
        })) {
            Ok(d) => d,
            Err(_) => {
                tracing::error!(node = %id, "Policy::decide panicked");
                self.fail_node(slot, id, NodeError::new("policy panicked"), now, effect);
                self.fail_fast(now, effect);
                return Ok(());
            }
        };
        if matches!(decision, PolicyDecision::Retry { .. })
            && matches!(
                outcome,
                NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. }
            )
        {
            self.fail_node(
                slot,
                id,
                NodeError::new("illegal policy Retry after Succeeded/Waiting"),
                now,
                effect,
            );
            self.fail_fast(now, effect);
            return Ok(());
        }

        match decision {
            PolicyDecision::Retry { delay } => {
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
                    // Prior attempt token is dead. Waiting is the only state
                    // that resume Complete/Reinvoke consults. Dropping it here
                    // keeps Ready { T } node JSON off the stale token blob.
                    n.resume_token = None;
                }
                self.set_state(slot, NodeState::Ready { runnable_at: at });
                if let Some(at) = at {
                    self.note_deadline(slot, at);
                } else {
                    effect.newly_runnable.push(slot);
                }
                return Ok(());
            }
            PolicyDecision::Reject => {
                self.fail_node(
                    slot,
                    id,
                    NodeError::new("policy rejected outcome"),
                    now,
                    effect,
                );
                self.apply_on_failure(slot, now, effect);
                return Ok(());
            }
            PolicyDecision::Accept => {}
        }

        match outcome {
            NodeOutcome::Succeeded(bytes) => {
                {
                    let n = &mut self.nodes[slot.0];
                    n.output = Some(bytes.clone());
                    n.last_outcome = Some(NodeOutcome::Succeeded(bytes));
                    n.last_error = None;
                }
                self.set_state(slot, NodeState::Succeeded);
                self.emit_node(effect, now, id.clone(), attempt, NodeKind::Succeeded);
                self.ready_successors(slot, effect);
            }
            NodeOutcome::Waiting { token } => {
                let token = {
                    let n = &mut self.nodes[slot.0];
                    let token = n.resume_token.take().unwrap_or(token);
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
                self.emit_node(effect, now, id.clone(), attempt, NodeKind::Waiting(token));
            }
            NodeOutcome::Failed(err) => {
                self.fail_node(slot, id, err.clone(), now, effect);
                self.apply_on_failure(slot, now, effect);
            }
            NodeOutcome::TimedOut => {
                {
                    let n = &mut self.nodes[slot.0];
                    n.last_error = Some(NodeError::new("timed out"));
                    n.last_outcome = Some(NodeOutcome::TimedOut);
                }
                self.set_state(slot, NodeState::TimedOut);
                self.emit_node(effect, now, id.clone(), attempt, NodeKind::TimedOut);
                self.apply_on_failure(slot, now, effect);
            }
        }
        Ok(())
    }

    fn fail_node(
        &mut self,
        slot: NodeSlot,
        id: &NodeId,
        err: NodeError,
        now: Timestamp,
        effect: &mut ApplyEffect,
    ) {
        let attempt = self.nodes[slot.0].attempt;
        {
            let n = &mut self.nodes[slot.0];
            n.last_error = Some(err.clone());
            n.last_outcome = Some(NodeOutcome::Failed(err.clone()));
        }
        self.set_state(slot, NodeState::Failed);
        self.clear_deadline_if(slot);
        self.emit_node(effect, now, id.clone(), attempt, NodeKind::Failed(err));
    }

    fn apply_on_failure(&mut self, slot: NodeSlot, now: Timestamp, effect: &mut ApplyEffect) {
        match self.definition.on_failure() {
            OnFailure::FailExecution => self.fail_fast(now, effect),
            OnFailure::FailSubtree => self.fail_subtree(slot, now, effect),
        }
    }

    /// After policy Accepts Failed/TimedOut (or Rejects): non-terminal nodes
    /// Cancelled, execution Failed. Sets the same abort flag as [`Self::cancel_graph`].
    fn cancel_non_terminals(&mut self, now: Timestamp, effect: &mut ApplyEffect) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].state.is_terminal() {
                continue;
            }
            let running = matches!(self.nodes[i].state, NodeState::Running { .. });
            let attempt = self.nodes[i].attempt;
            self.set_state(NodeSlot(i), NodeState::Cancelled);
            let id = self.definition.id_at(NodeSlot(i)).clone();
            if running {
                effect.to_abort.push(NodeSlot(i));
            }
            self.emit_node(effect, now, id, attempt, NodeKind::Cancelled);
        }
        self.next_deadline = None;
    }

    fn fail_fast(&mut self, now: Timestamp, effect: &mut ApplyEffect) {
        self.cancelled = true;
        self.fail_execution = true;
        self.cancel_non_terminals(now, effect);
        self.state = ExecutionState::Failed;
    }

    /// Cancel AllSucceeded descendants of `origin` (already Failed/TimedOut).
    /// AllDone dependents stay Pending until every predecessor is terminal.
    /// Iterative — deep DAGs must not blow the stack.
    fn fail_subtree(&mut self, origin: NodeSlot, now: Timestamp, effect: &mut ApplyEffect) {
        let mut stack: Vec<NodeSlot> = self.definition.succ_slots(origin).to_vec();
        while let Some(succ) = stack.pop() {
            if self.nodes[succ.0].state.is_terminal() {
                continue;
            }
            match self.definition.join_at(succ) {
                Join::AllSucceeded => {
                    self.cancel_node(succ, now, effect);
                    stack.extend_from_slice(self.definition.succ_slots(succ));
                }
                Join::AllDone => {
                    if self.remain[succ.0] > 0 {
                        self.remain[succ.0] -= 1;
                    }
                    if self.remain[succ.0] == 0
                        && matches!(self.nodes[succ.0].state, NodeState::Pending)
                    {
                        self.mark_ready(succ, effect);
                    }
                }
            }
        }
    }

    fn cancel_node(&mut self, slot: NodeSlot, now: Timestamp, effect: &mut ApplyEffect) {
        // FailSubtree already skips terminals. AllSucceeded descendants cannot
        // be Running (they stay Pending until every predecessor succeeds).
        let attempt = self.nodes[slot.0].attempt;
        self.set_state(slot, NodeState::Cancelled);
        self.clear_deadline_if(slot);
        let id = self.definition.id_at(slot).clone();
        self.emit_node(effect, now, id, attempt, NodeKind::Cancelled);
    }

    fn ready_successors(&mut self, succeeded: NodeSlot, effect: &mut ApplyEffect) {
        let nsucc = self.definition.succ_slots(succeeded).len();
        for i in 0..nsucc {
            let succ = self.definition.succ_slots(succeeded)[i];
            debug_assert!(self.remain[succ.0] > 0);
            self.remain[succ.0] -= 1;
            if self.remain[succ.0] == 0 && matches!(self.nodes[succ.0].state, NodeState::Pending) {
                self.mark_ready(succ, effect);
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
                        if n.last_outcome
                            .as_ref()
                            .is_some_and(|prev| prev.equivalent(outcome))
                        {
                            return Ok(());
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
                    if node
                        .last_outcome
                        .as_ref()
                        .is_some_and(|prev| prev.equivalent(outcome))
                    {
                        return Ok(());
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
            NodeState::Cancelled | NodeState::Failed | NodeState::TimedOut => {
                return Err(ApplyError::ResumeAfterCancel);
            }
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
                effect.newly_runnable.push(slot);
                effect.changed = true;
            }
        }
        Ok(())
    }

    fn cancel_graph(&mut self, now: Timestamp, effect: &mut ApplyEffect) {
        self.cancelled = true;
        self.cancel_non_terminals(now, effect);
        self.state = ExecutionState::Cancelled;
    }

    fn force_cancel_running(&mut self, now: Timestamp, effect: &mut ApplyEffect) {
        for i in 0..self.nodes.len() {
            if matches!(self.nodes[i].state, NodeState::Running { .. }) {
                let attempt = self.nodes[i].attempt;
                self.set_state(NodeSlot(i), NodeState::Cancelled);
                let id = self.definition.id_at(NodeSlot(i)).clone();
                effect.to_abort.push(NodeSlot(i));
                self.emit_node(effect, now, id, attempt, NodeKind::Cancelled);
            }
        }
        if !self.state.is_terminal() {
            self.state = ExecutionState::Cancelled;
            self.cancelled = true;
        }
        self.next_deadline = None;
    }

    /// Test-only: move a parked Ready deadline so the next
    /// [`crate::StateStore::persist`] can `UPDATE runnable_at` without
    /// rewriting body. Production apply never changes T alone.
    #[cfg(any(test, feature = "test-util"))]
    pub fn retarget_ready_deadline(
        &mut self,
        node_id: &NodeId,
        runnable_at: Option<Timestamp>,
    ) -> Result<(), ApplyError> {
        let slot = self
            .definition
            .slot(node_id)
            .ok_or_else(|| ApplyError::UnknownNode(node_id.clone()))?;
        match self.nodes[slot.0].state {
            NodeState::Ready { .. } => {}
            _ => {
                return Err(ApplyError::Illegal(
                    "retarget_ready_deadline only on Ready".into(),
                ));
            }
        }
        self.set_state(slot, NodeState::Ready { runnable_at });
        match runnable_at {
            Some(at) => self.note_deadline(slot, at),
            None => self.clear_deadline_if(slot),
        }
        self.revision += 1;
        Ok(())
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

    pub(super) fn rebuild_deadline(&mut self) {
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

    /// Failed/TimedOut become Ready-now (dispatch increments attempt).
    /// Cancelled become Pending; Ready when every AllSucceeded pred
    /// Succeeded (or AllDone preds are terminal). Succeeded keep Bytes.
    /// Waiting stays Waiting.
    fn apply_retry_failed(&mut self, effect: &mut ApplyEffect) {
        let n = self.nodes.len();
        for i in 0..n {
            match self.nodes[i].state {
                NodeState::Failed | NodeState::TimedOut => {
                    self.nodes[i].state = NodeState::Ready { runnable_at: None };
                    self.mark_dirty(NodeSlot(i));
                    effect.newly_runnable.push(NodeSlot(i));
                }
                NodeState::Cancelled => {
                    let n = &mut self.nodes[i];
                    n.state = NodeState::Pending;
                    n.attempt = 0;
                    n.resume_token = None;
                    n.output = None;
                    n.reinvoke = false;
                    self.mark_dirty(NodeSlot(i));
                }
                _ => {}
            }
        }
        self.cancelled = false;
        self.fail_execution = false;
        self.recount_counts();
        for i in 0..n {
            self.remain[i] = super::restore::remain_for(&self.definition, &self.nodes, NodeSlot(i));
        }
        for i in 0..n {
            if self.remain[i] == 0 && matches!(self.nodes[i].state, NodeState::Pending) {
                self.mark_ready(NodeSlot(i), effect);
            }
        }
        self.rebuild_deadline();
    }

    fn recount_counts(&mut self) {
        self.n_pending = 0;
        self.n_ready = 0;
        self.n_running = 0;
        self.n_waiting = 0;
        self.n_succeeded = 0;
        self.n_failed = 0;
        self.n_cancelled = 0;
        for i in 0..self.nodes.len() {
            self.inc_kind(super::count_kind(&self.nodes[i].state));
        }
    }

    pub(super) fn derive_state(&self) -> ExecutionState {
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
        } else {
            // Waiting, including Waiting + leftover Pending joins.
            // Pending-only (no Ready/Running/Waiting) is not produced: AllDone
            // remain hits 0 and mark_ready runs when every predecessor reports.
            ExecutionState::Waiting
        }
    }

    fn recompute_execution_state(
        &mut self,
        prev: ExecutionState,
        effect: &mut ApplyEffect,
        now: Timestamp,
    ) {
        let next = self.derive_state();
        if next != prev {
            match next {
                ExecutionState::Failed => self.emit_exec(effect, now, ExecKind::Failed),
                ExecutionState::Cancelled => self.emit_exec(effect, now, ExecKind::Cancelled),
                ExecutionState::Succeeded => self.emit_exec(effect, now, ExecKind::Succeeded),
                ExecutionState::Completed => self.emit_exec(effect, now, ExecKind::Completed),
                ExecutionState::Waiting | ExecutionState::Running | ExecutionState::Created => {}
            }
        }
        self.state = next;
    }
}
