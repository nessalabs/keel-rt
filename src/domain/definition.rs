use crate::domain::ids::{DefinitionHash, ExecutorId, NodeId, NodeSlot, WorkflowId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

/// What happens after policy Accepts Failed or TimedOut.
///
/// Retry is decided **before** this.
///
/// **Library default is [`OnFailure::FailExecution`]** (execution-wide fail-fast).
/// [`OnFailure::FailSubtree`] is opt-in on [`WorkflowDefinition::builder`] via
/// `.on_failure(...)`. It is not a `Runtime` default, feature flag, or process
/// static — existing workflows keep fail-fast unless they set it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnFailure {
    /// Library default. Cancel every non-terminal node; execution
    /// [`Failed`](crate::ExecutionState::Failed).
    #[default]
    FailExecution,
    /// Opt-in. Cancel only AllSucceeded descendants of the failed/timed-out
    /// node. Siblings keep running. Execution is not Failed.
    FailSubtree,
}

/// When a node becomes Ready relative to its predecessors.
///
/// **Library default is [`Join::AllSucceeded`]** (AND-join). [`Join::AllDone`]
/// is opt-in per node via [`WorkflowDefinitionBuilder::join`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Join {
    /// Library default. AND: Ready only when every predecessor is Succeeded.
    #[default]
    AllSucceeded,
    /// Opt-in. Ready when every predecessor is terminal. `inputs_for` is succeeded preds only.
    AllDone,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDef {
    pub id: NodeId,
    pub executor_id: ExecutorId,
    #[serde(default)]
    pub join: Join,
}

/// Hard predecessor edge. Phase 1 has no edge predicates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DefinitionError {
    #[error("workflow graph is empty")]
    Empty,
    #[error("workflow id must not be empty")]
    EmptyWorkflowId,
    #[error("node id must not be empty")]
    EmptyNodeId,
    #[error("executor id must not be empty")]
    EmptyExecutorId,
    #[error("duplicate node id: {0}")]
    DuplicateNode(NodeId),
    #[error("edge references unknown node: {0}")]
    DisconnectedNode(NodeId),
    #[error("workflow graph contains a cycle")]
    Cycle,
    #[error("stored definition bytes are not a valid workflow")]
    CorruptDurableBytes,
}

/// Validated DAG. Indices stay private; callers use [`NodeId`].
///
/// Failure scope and join predicates live here — the only config surface:
/// `.on_failure(OnFailure::FailSubtree)` and `.join(id, Join::AllDone)`.
/// Both default to fail-fast / AND (`FailExecution`, `AllSucceeded`).
#[derive(Clone, Debug)]
pub struct WorkflowDefinition {
    id: WorkflowId,
    on_failure: OnFailure,
    nodes: Vec<NodeDef>,
    edges: Vec<Edge>,
    /// `NodeId` → dense slot (same order as `nodes`).
    index: HashMap<NodeId, NodeSlot>,
    preds: Vec<Vec<NodeSlot>>,
    succs: Vec<Vec<NodeSlot>>,
    sources: Vec<NodeSlot>,
}

impl WorkflowDefinition {
    pub fn builder(id: impl Into<WorkflowId>) -> WorkflowDefinitionBuilder {
        WorkflowDefinitionBuilder {
            id: id.into(),
            nodes: Vec::new(),
            edges: Vec::new(),
            on_failure: OnFailure::FailExecution,
            node_joins: Vec::new(),
        }
    }

    pub fn id(&self) -> &WorkflowId {
        &self.id
    }

    pub fn on_failure(&self) -> OnFailure {
        self.on_failure
    }

    pub fn join_of(&self, id: &NodeId) -> Option<Join> {
        let slot = self.slot(id)?;
        Some(self.nodes[slot.0].join)
    }

    pub fn nodes(&self) -> &[NodeDef] {
        &self.nodes
    }

    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn node(&self, id: &NodeId) -> Option<&NodeDef> {
        let slot = self.slot(id)?;
        self.nodes.get(slot.0)
    }

    pub(crate) fn slot(&self, id: &NodeId) -> Option<NodeSlot> {
        self.index.get(id).copied()
    }

    pub(crate) fn id_at(&self, slot: NodeSlot) -> &NodeId {
        &self.nodes[slot.0].id
    }

    pub(crate) fn executor_at(&self, slot: NodeSlot) -> &ExecutorId {
        &self.nodes[slot.0].executor_id
    }

    pub(crate) fn join_at(&self, slot: NodeSlot) -> Join {
        self.nodes[slot.0].join
    }

    pub(crate) fn pred_slots(&self, slot: NodeSlot) -> &[NodeSlot] {
        &self.preds[slot.0]
    }

    pub(crate) fn succ_slots(&self, slot: NodeSlot) -> &[NodeSlot] {
        &self.succs[slot.0]
    }

    pub(crate) fn source_slots(&self) -> &[NodeSlot] {
        &self.sources
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn predecessors(&self, id: &NodeId) -> Vec<NodeId> {
        let Some(slot) = self.slot(id) else {
            return Vec::new();
        };
        self.pred_slots(slot)
            .iter()
            .map(|s| self.id_at(*s).clone())
            .collect()
    }

    pub fn successors(&self, id: &NodeId) -> Vec<NodeId> {
        let Some(slot) = self.slot(id) else {
            return Vec::new();
        };
        self.succ_slots(slot)
            .iter()
            .map(|s| self.id_at(*s).clone())
            .collect()
    }

    pub fn sources(&self) -> Vec<NodeId> {
        self.sources
            .iter()
            .map(|s| self.id_at(*s).clone())
            .collect()
    }

    /// Canonical body for durable store. Not a snapshot — definition stays data.
    pub fn durable_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&WorkflowDefinitionRecord {
            id: self.id.clone(),
            on_failure: self.on_failure,
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        })
        .expect("WorkflowDefinitionRecord is always serializable")
    }

    pub fn content_hash(&self) -> DefinitionHash {
        DefinitionHash::parse(fnv1a64_hex(&self.durable_bytes()))
            .expect("fnv hex is never empty")
    }

    /// Rebuild a validated DAG from stored bytes. Invalid graphs fail closed.
    pub fn from_durable_bytes(bytes: &[u8]) -> Result<Self, DefinitionError> {
        let rec: WorkflowDefinitionRecord = serde_json::from_slice(bytes)
            .map_err(|_| DefinitionError::CorruptDurableBytes)?;
        let mut b = WorkflowDefinition::builder(rec.id).on_failure(rec.on_failure);
        for n in rec.nodes {
            b = b.node(n.id.clone(), n.executor_id).join(n.id, n.join);
        }
        for e in rec.edges {
            b = b.edge(e.from, e.to);
        }
        b.build()
    }
}

#[derive(Serialize, Deserialize)]
struct WorkflowDefinitionRecord {
    id: WorkflowId,
    on_failure: OnFailure,
    nodes: Vec<NodeDef>,
    edges: Vec<Edge>,
}

fn fnv1a64_hex(data: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in data {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

#[derive(Debug)]
pub struct WorkflowDefinitionBuilder {
    id: WorkflowId,
    nodes: Vec<NodeDef>,
    edges: Vec<Edge>,
    on_failure: OnFailure,
    node_joins: Vec<(NodeId, Join)>,
}

impl WorkflowDefinitionBuilder {
    pub fn node(mut self, id: impl Into<NodeId>, executor: impl Into<ExecutorId>) -> Self {
        self.nodes.push(NodeDef {
            id: id.into(),
            executor_id: executor.into(),
            join: Join::AllSucceeded,
        });
        self
    }

    pub fn edge(mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    /// Opt-in failure scope. Omit this to keep the library default
    /// [`OnFailure::FailExecution`].
    pub fn on_failure(mut self, on_failure: OnFailure) -> Self {
        self.on_failure = on_failure;
        self
    }

    /// Opt-in per-node join. Omit this to keep [`Join::AllSucceeded`].
    /// Unknown ids fail at `build`.
    pub fn join(mut self, id: impl Into<NodeId>, join: Join) -> Self {
        self.node_joins.push((id.into(), join));
        self
    }

    pub fn build(mut self) -> Result<WorkflowDefinition, DefinitionError> {
        if self.nodes.is_empty() {
            return Err(DefinitionError::Empty);
        }
        if self.id.as_str().is_empty() {
            return Err(DefinitionError::EmptyWorkflowId);
        }

        let mut seen = HashSet::new();
        for n in &self.nodes {
            if n.id.as_str().is_empty() {
                return Err(DefinitionError::EmptyNodeId);
            }
            if n.executor_id.as_str().is_empty() {
                return Err(DefinitionError::EmptyExecutorId);
            }
            if !seen.insert(n.id.clone()) {
                return Err(DefinitionError::DuplicateNode(n.id.clone()));
            }
        }

        let mut index = HashMap::with_capacity(self.nodes.len());
        for (i, n) in self.nodes.iter().enumerate() {
            index.insert(n.id.clone(), NodeSlot(i));
        }

        let n = self.nodes.len();
        let mut preds = vec![Vec::new(); n];
        let mut succs = vec![Vec::new(); n];
        for (id, join) in &self.node_joins {
            let slot = *index
                .get(id)
                .ok_or_else(|| DefinitionError::DisconnectedNode(id.clone()))?;
            self.nodes[slot.0].join = *join;
        }
        for e in &self.edges {
            let from = *index
                .get(&e.from)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.from.clone()))?;
            let to = *index
                .get(&e.to)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.to.clone()))?;
            if !succs[from.0].contains(&to) {
                succs[from.0].push(to);
            }
            if !preds[to.0].contains(&from) {
                preds[to.0].push(from);
            }
        }

        // Iterative Kahn topological count — no recursive DFS (deep DAGs).
        if has_cycle(&preds, &succs) {
            return Err(DefinitionError::Cycle);
        }

        let sources = (0..n)
            .filter(|&i| preds[i].is_empty())
            .map(NodeSlot)
            .collect();

        Ok(WorkflowDefinition {
            id: self.id,
            on_failure: self.on_failure,
            nodes: self.nodes,
            edges: self.edges,
            index,
            preds,
            succs,
            sources,
        })
    }
}

fn has_cycle(preds: &[Vec<NodeSlot>], succs: &[Vec<NodeSlot>]) -> bool {
    let n = preds.len();
    let mut indeg: Vec<usize> = preds.iter().map(|p| p.len()).collect();
    let mut q: VecDeque<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();
    let mut seen = 0usize;
    while let Some(i) = q.pop_front() {
        seen += 1;
        for s in &succs[i] {
            indeg[s.0] -= 1;
            if indeg[s.0] == 0 {
                q.push_back(s.0);
            }
        }
    }
    seen != n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_graph_rejected() {
        let err = WorkflowDefinition::builder("wf").build().unwrap_err();
        assert_eq!(err, DefinitionError::Empty);
    }

    #[test]
    fn duplicate_node_rejected() {
        let err = WorkflowDefinition::builder("wf")
            .node("a", "exec")
            .node("a", "exec")
            .build()
            .unwrap_err();
        assert!(matches!(err, DefinitionError::DuplicateNode(_)));
    }

    #[test]
    fn cycle_rejected() {
        let err = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .edge("b", "a")
            .build()
            .unwrap_err();
        assert_eq!(err, DefinitionError::Cycle);
    }

    #[test]
    fn dangling_edge_rejected() {
        let err = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .edge("a", "ghost")
            .build()
            .unwrap_err();
        assert!(matches!(err, DefinitionError::DisconnectedNode(_)));
    }

    #[test]
    fn empty_workflow_id_rejected() {
        let err = WorkflowDefinition::builder("")
            .node("a", "e")
            .build()
            .unwrap_err();
        assert_eq!(err, DefinitionError::EmptyWorkflowId);
        assert!(err.to_string().contains("workflow id"));
    }

    #[test]
    fn empty_node_id_rejected() {
        let err = WorkflowDefinition::builder("wf")
            .node("", "e")
            .build()
            .unwrap_err();
        assert_eq!(err, DefinitionError::EmptyNodeId);
        assert!(err.to_string().contains("node id"));
    }

    #[test]
    fn empty_executor_id_rejected() {
        let err = WorkflowDefinition::builder("wf")
            .node("a", "")
            .build()
            .unwrap_err();
        assert_eq!(err, DefinitionError::EmptyExecutorId);
        assert!(err.to_string().contains("executor id"));
    }

    #[test]
    fn every_definition_error_variant_has_display() {
        let cases = [
            (DefinitionError::Empty, "empty"),
            (DefinitionError::EmptyWorkflowId, "workflow id"),
            (DefinitionError::EmptyNodeId, "node id"),
            (DefinitionError::EmptyExecutorId, "executor id"),
            (
                DefinitionError::DuplicateNode(NodeId::new("a")),
                "duplicate",
            ),
            (
                DefinitionError::DisconnectedNode(NodeId::new("ghost")),
                "unknown",
            ),
            (DefinitionError::Cycle, "cycle"),
            (DefinitionError::CorruptDurableBytes, "stored definition"),
        ];
        for (err, needle) in cases {
            let s = err.to_string();
            assert!(
                s.to_lowercase().contains(needle),
                "{s} should mention {needle}"
            );
        }
    }

    #[test]
    fn node_lookup_uses_index() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "ea")
            .node("b", "eb")
            .edge("a", "b")
            .build()
            .unwrap();
        assert_eq!(
            def.node(&NodeId::new("b")).unwrap().executor_id.as_str(),
            "eb"
        );
        assert_eq!(def.predecessors(&NodeId::new("b")).len(), 1);
        assert!(def.predecessors(&NodeId::new("a")).is_empty());
    }

    #[test]
    fn builder_accepts_format_string() {
        let def = WorkflowDefinition::builder(format!("burst-{}", 7))
            .node("a", "e")
            .build()
            .unwrap();
        assert_eq!(def.id().as_str(), "burst-7");
    }

    #[test]
    fn on_failure_library_default_is_fail_execution() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .build()
            .unwrap();
        assert_eq!(def.on_failure(), OnFailure::FailExecution);
        assert_eq!(def.join_of(&NodeId::new("a")), Some(Join::AllSucceeded));
    }

    #[test]
    fn on_failure_and_join_opt_in() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .join("b", Join::AllDone)
            .on_failure(OnFailure::FailSubtree)
            .build()
            .unwrap();
        assert_eq!(def.on_failure(), OnFailure::FailSubtree);
        assert_eq!(def.join_of(&NodeId::new("a")), Some(Join::AllSucceeded));
        assert_eq!(def.join_of(&NodeId::new("b")), Some(Join::AllDone));
    }

    #[test]
    fn durable_bytes_round_trip_preserves_join_and_failure_scope() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "e")
            .node("b", "e")
            .edge("a", "b")
            .join("b", Join::AllDone)
            .on_failure(OnFailure::FailSubtree)
            .build()
            .unwrap();
        let bytes = def.durable_bytes();
        let back = WorkflowDefinition::from_durable_bytes(&bytes).unwrap();
        assert_eq!(back.id(), def.id());
        assert_eq!(back.on_failure(), OnFailure::FailSubtree);
        assert_eq!(back.join_of(&NodeId::new("b")), Some(Join::AllDone));
        assert_eq!(back.content_hash(), def.content_hash());
        let again = WorkflowDefinition::from_durable_bytes(&back.durable_bytes()).unwrap();
        assert_eq!(again.content_hash(), def.content_hash());
    }

    #[test]
    fn from_durable_bytes_rejects_corrupt_json() {
        let err = WorkflowDefinition::from_durable_bytes(b"not-json").unwrap_err();
        assert_eq!(err, DefinitionError::CorruptDurableBytes);
    }
}
