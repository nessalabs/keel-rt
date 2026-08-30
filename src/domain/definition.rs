use crate::domain::ids::{ExecutorId, NodeId, WorkflowId};
use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
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
    graph: DiGraph<NodeId, EdgePredicate>,
    index: HashMap<NodeId, NodeIndex>,
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
        self.nodes.iter().find(|n| n.id == *id)
    }

    pub fn predecessors(&self, id: &NodeId) -> Vec<NodeId> {
        let Some(&ix) = self.index.get(id) else {
            return Vec::new();
        };
        self.graph
            .edges_directed(ix, petgraph::Direction::Incoming)
            .map(|e| self.graph[e.source()].clone())
            .collect()
    }

    pub fn successors(&self, id: &NodeId) -> Vec<NodeId> {
        let Some(&ix) = self.index.get(id) else {
            return Vec::new();
        };
        self.graph
            .edges_directed(ix, petgraph::Direction::Outgoing)
            .map(|e| self.graph[e.target()].clone())
            .collect()
    }

    pub fn sources(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| self.predecessors(&n.id).is_empty())
            .map(|n| n.id.clone())
            .collect()
    }

    pub fn topo(&self) -> Vec<NodeId> {
        toposort(&self.graph, None)
            .unwrap_or_default()
            .into_iter()
            .map(|ix| self.graph[ix].clone())
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
        for n in &self.nodes {
            let ix = graph.add_node(n.id.clone());
            index.insert(n.id.clone(), ix);
        }

        for e in &self.edges {
            let from = index
                .get(&e.from)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.from.clone()))?;
            let to = index
                .get(&e.to)
                .ok_or_else(|| DefinitionError::DisconnectedNode(e.to.clone()))?;
            graph.add_edge(*from, *to, e.predicate.clone());
        }

        if is_cyclic_directed(&graph) {
            return Err(DefinitionError::Cycle);
        }

        // Isolated nodes are allowed (fan-out of independent sources).
        // "Disconnected" means an edge to an undeclared NodeId (above).

        Ok(WorkflowDefinition {
            id: self.id,
            nodes: self.nodes,
            edges: self.edges,
            graph,
            index,
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
}
