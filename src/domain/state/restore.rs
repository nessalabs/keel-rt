use super::*;
use crate::domain::definition::{Join, WorkflowDefinition};
use crate::domain::snapshot::{ExecutionSnapshot, SCHEMA_VERSION, SnapshotError};

impl Execution {
    /// Rebuild slot state from a snapshot. Definition stays a separate value.
    ///
    /// A node that was [`NodeState::Running`] is restored as Ready and will be
    /// re-invoked (attempt + 1 at dispatch). Succeeded / Failed / TimedOut /
    /// Cancelled are never re-run. Waiting keeps the same token.
    pub fn from_snapshot(
        definition: WorkflowDefinition,
        snap: ExecutionSnapshot,
    ) -> Result<Self, SnapshotError> {
        if snap.schema_version != SCHEMA_VERSION {
            return Err(SnapshotError::SchemaMismatch {
                found: snap.schema_version,
                expected: SCHEMA_VERSION,
            });
        }
        if snap.workflow_id != *definition.id() {
            return Err(SnapshotError::WorkflowIdMismatch);
        }
        if snap.definition_hash != definition.content_hash() {
            return Err(SnapshotError::DefinitionHashMismatch);
        }

        let n = definition.len();
        let mut nodes: Vec<NodeRuntime> = (0..n).map(|_| NodeRuntime::default()).collect();
        let mut converted_running = false;

        for (i, def_node) in definition.nodes().iter().enumerate() {
            let Some(ns) = snap.nodes.get(&def_node.id) else {
                return Err(SnapshotError::MissingNode(def_node.id.clone()));
            };
            let mut rt = NodeRuntime {
                state: ns.state.clone(),
                output: ns.output.clone(),
                attempt: ns.attempt,
                last_error: ns.last_error.clone(),
                resume_token: ns.resume_token.clone(),
                reinvoke: false,
                last_outcome: ns.output.clone().map(NodeOutcome::Succeeded),
            };
            if let NodeState::Running { attempt } = rt.state {
                rt.attempt = attempt;
                rt.state = NodeState::Ready { runnable_at: None };
                converted_running = true;
            }
            if let NodeState::Waiting { token, attempt } = &rt.state {
                rt.resume_token = Some(token.clone());
                rt.attempt = *attempt;
            }
            nodes[i] = rt;
        }
        for id in snap.nodes.keys() {
            if definition.slot(id).is_none() {
                return Err(SnapshotError::UnknownNode(id.clone()));
            }
        }

        let remain: Vec<u32> = (0..n)
            .map(|i| remain_for(&definition, &nodes, NodeSlot(i)))
            .collect();

        let mut exec = Self {
            id: snap.execution_id,
            workflow_id: definition.id().clone(),
            definition,
            state: snap.state,
            nodes,
            revision: snap.revision,
            cancelled: snap.state == ExecutionState::Cancelled,
            fail_execution: snap.state == ExecutionState::Failed,
            next_deadline: None,
            dirty: vec![0u8; n],
            dirty_list: Vec::new(),
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
        exec.rebuild_deadline();
        if converted_running {
            exec.revision = exec.revision.saturating_add(1);
            for i in 0..n {
                exec.mark_dirty(NodeSlot(i));
            }
            exec.state = exec.derive_state();
        }
        Ok(exec)
    }
}

fn remain_for(def: &WorkflowDefinition, nodes: &[NodeRuntime], slot: NodeSlot) -> u32 {
    let preds = def.pred_slots(slot);
    match def.join_at(slot) {
        Join::AllSucceeded => preds
            .iter()
            .filter(|p| !matches!(nodes[p.0].state, NodeState::Succeeded))
            .count() as u32,
        Join::AllDone => preds
            .iter()
            .filter(|p| !nodes[p.0].state.is_terminal())
            .count() as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::DefinitionHash;
    use crate::domain::policy::{AcceptPolicy, RetryPolicy};
    use crate::domain::time::Timestamp;
    use bytes::Bytes;

    fn linear() -> WorkflowDefinition {
        WorkflowDefinition::builder("wf")
            .node("a", "ea")
            .node("b", "eb")
            .edge("a", "b")
            .build()
            .unwrap()
    }

    #[test]
    fn from_snapshot_reinvokes_running_and_keeps_succeeded() {
        let def = linear();
        let mut ex = Execution::new(def.clone());
        let p = AcceptPolicy;
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Succeeded(Bytes::from_static(b"A"))),
            },
            &p,
            now,
        )
        .unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "b".into() }, &p, now)
            .unwrap();
        assert!(matches!(
            ex.node(&NodeId::new("b")).unwrap().state,
            NodeState::Running { attempt: 1 }
        ));
        let snap = ex.snapshot();
        let restored = Execution::from_snapshot(def, snap).unwrap();
        assert!(matches!(
            restored.node(&NodeId::new("a")).unwrap().state,
            NodeState::Succeeded
        ));
        assert!(matches!(
            restored.node(&NodeId::new("b")).unwrap().state,
            NodeState::Ready { runnable_at: None }
        ));
        assert_eq!(restored.node(&NodeId::new("b")).unwrap().attempt, 1);
        assert_eq!(restored.id(), ex.id());
    }

    #[test]
    fn from_snapshot_keeps_waiting_token() {
        let def = linear();
        let mut ex = Execution::new(def.clone());
        let p = AcceptPolicy;
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        let token = ex.resume_token(&NodeId::new("a")).unwrap();
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
        let snap = ex.snapshot();
        let restored = Execution::from_snapshot(def, snap).unwrap();
        match &restored.node(&NodeId::new("a")).unwrap().state {
            NodeState::Waiting { token: t, attempt: 1 } => assert_eq!(t, &token),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn from_snapshot_rejects_schema_and_hash_mismatch() {
        let def = linear();
        let ex = Execution::new(def.clone());
        let mut snap = ex.snapshot();
        snap.schema_version = 99;
        assert!(matches!(
            Execution::from_snapshot(def.clone(), snap).unwrap_err(),
            SnapshotError::SchemaMismatch { found: 99, expected: SCHEMA_VERSION }
        ));

        let mut snap = ex.snapshot();
        snap.definition_hash = DefinitionHash::parse("deadbeefdeadbeef").unwrap();
        assert_eq!(
            Execution::from_snapshot(def.clone(), snap).unwrap_err(),
            SnapshotError::DefinitionHashMismatch
        );

        let other = WorkflowDefinition::builder("other")
            .node("a", "ea")
            .node("b", "eb")
            .edge("a", "b")
            .build()
            .unwrap();
        assert_eq!(
            Execution::from_snapshot(other, ex.snapshot()).unwrap_err(),
            SnapshotError::WorkflowIdMismatch
        );
    }

    #[test]
    fn from_snapshot_rejects_missing_and_unknown_nodes() {
        let def = linear();
        let ex = Execution::new(def.clone());
        let mut snap = ex.snapshot();
        snap.nodes.remove(&NodeId::new("b"));
        assert!(matches!(
            Execution::from_snapshot(def.clone(), snap).unwrap_err(),
            SnapshotError::MissingNode(_)
        ));

        let mut snap = ex.snapshot();
        snap.nodes.insert(
            NodeId::new("ghost"),
            crate::domain::snapshot::NodeSnapshot {
                state: NodeState::Pending,
                output: None,
                attempt: 0,
                resume_token: None,
                last_error: None,
            },
        );
        assert!(matches!(
            Execution::from_snapshot(def, snap).unwrap_err(),
            SnapshotError::UnknownNode(_)
        ));
    }

    #[test]
    fn from_snapshot_keeps_retry_deadline() {
        use crate::domain::policy::RetryPolicy;
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        let mut ex = Execution::new(def.clone());
        let p = RetryPolicy::new(3, std::time::Duration::from_millis(50));
        let now = Timestamp(0);
        ex.apply(ApplyCmd::Start, &p, now).unwrap();
        ex.apply(ApplyCmd::StartNode { node_id: "a".into() }, &p, now)
            .unwrap();
        ex.apply(
            ApplyCmd::FinishNode {
                node_id: "a".into(),
                attempt: 1,
                outcome: Ok(NodeOutcome::Failed(NodeError::new("boom"))),
            },
            &p,
            now,
        )
        .unwrap();
        let due = match &ex.node(&NodeId::new("a")).unwrap().state {
            NodeState::Ready {
                runnable_at: Some(at),
            } => *at,
            other => panic!("{other:?}"),
        };
        let restored = Execution::from_snapshot(def, ex.snapshot()).unwrap();
        assert_eq!(
            restored.node(&NodeId::new("a")).unwrap().state,
            NodeState::Ready {
                runnable_at: Some(due)
            }
        );
    }
}
