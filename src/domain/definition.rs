use crate::domain::ids::{ExecutorId, NodeId, NodeSlot, WorkflowId};
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Phase 1 only: an edge is a hard AND-join predecessor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgePredicate {
    Always,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDef {
    pub id: NodeId,
    pub executor_id: ExecutorId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub predicate: EdgePredicate,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DefinitionError {
    #[error("workflow graph is empty")]
    Empty,
    #[error("duplicate node id: {0}")]
    DuplicateNode(NodeId),
    #[error("edge references unknown node: {0}")]
    DisconnectedNode(NodeId),
    #[error("workflow graph contains a cycle")]
    Cycle,
}

/// Validated DAG. Indices stay private; callers use [`NodeId`].
#[derive(Clone, Debug)]
pub struct WorkflowDefinition {
    pub id: WorkflowId,
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
        }
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
}

#[derive(Debug)]
pub struct WorkflowDefinitionBuilder {
    id: WorkflowId,
    nodes: Vec<NodeDef>,
    edges: Vec<Edge>,
}

impl WorkflowDefinitionBuilder {
    pub fn node(mut self, id: impl Into<NodeId>, executor: impl Into<ExecutorId>) -> Self {
        self.nodes.push(NodeDef {
            id: id.into(),
            executor_id: executor.into(),
        });
        self
    }

    pub fn edge(mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            predicate: EdgePredicate::Always,
        });
        self
    }

    pub fn build(self) -> Result<WorkflowDefinition, DefinitionError> {
        if self.nodes.is_empty() {
            return Err(DefinitionError::Empty);
        }

        let mut seen = HashSet::new();
        for n in &self.nodes {
            if !seen.insert(n.id.clone()) {
                return Err(DefinitionError::DuplicateNode(n.id.clone()));
            }
        }

        let mut graph = DiGraph::new();
        let mut index = HashMap::new();
        let mut pg_ix = HashMap::new();
        for (i, n) in self.nodes.iter().enumerate() {
            let slot = NodeSlot(i);
            index.insert(n.id.clone(), slot);
            let ix = graph.add_node(n.id.clone());
            pg_ix.insert(n.id.clone(), ix);
        }

        for e in &self.edges {
            let from = pg_ix
                .get(&e.from)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.from.clone()))?;
            let to = pg_ix
                .get(&e.to)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.to.clone()))?;
            graph.add_edge(*from, *to, e.predicate.clone());
        }

        if is_cyclic_directed(&graph) {
            return Err(DefinitionError::Cycle);
        }

        let n = self.nodes.len();
        let mut preds = vec![Vec::new(); n];
        let mut succs = vec![Vec::new(); n];
        for e in &self.edges {
            let from = index[&e.from];
            let to = index[&e.to];
            succs[from.0].push(to);
            preds[to.0].push(from);
        }

        let sources = (0..n)
            .filter(|&i| preds[i].is_empty())
            .map(NodeSlot)
            .collect();

        Ok(WorkflowDefinition {
            id: self.id,
            nodes: self.nodes,
            edges: self.edges,
            index,
            preds,
            succs,
            sources,
        })
    }
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
    fn node_lookup_uses_index() {
        let def = WorkflowDefinition::builder("wf")
            .node("a", "ea")
            .node("b", "eb")
            .edge("a", "b")
            .build()
            .unwrap();
        assert_eq!(def.node(&NodeId::new("b")).unwrap().executor_id.as_str(), "eb");
        assert_eq!(def.predecessors(&NodeId::new("b")).len(), 1);
        assert!(def.predecessors(&NodeId::new("a")).is_empty());
    }
}
