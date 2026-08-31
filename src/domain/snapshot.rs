use crate::domain::ids::{ExecutionId, NodeId, ResumeToken, WorkflowId};
use crate::domain::outcome::NodeError;
use crate::domain::state::{ExecutionState, NodeState};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub state: NodeState,
    pub output: Option<Bytes>,
    pub attempt: u32,
    pub resume_token: Option<ResumeToken>,
    pub last_error: Option<NodeError>,
}

/// Persisted aggregate state. Keys are [`NodeId`], never petgraph indices.
///
/// Lookup stays a [`HashMap`]. Walk nodes in **definition order** with
/// [`ExecutionSnapshot::iter_nodes`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub schema_version: u32,
    pub revision: u64,
    pub execution_id: ExecutionId,
    pub workflow_id: WorkflowId,
    pub state: ExecutionState,
    pub nodes: HashMap<NodeId, NodeSnapshot>,
    /// Same order as [`crate::WorkflowDefinition::nodes`]. Empty on old snapshots.
    #[serde(default)]
    pub node_order: Vec<NodeId>,
}

impl ExecutionSnapshot {
    pub fn node(&self, id: &NodeId) -> Option<&NodeSnapshot> {
        self.nodes.get(id)
    }

    /// `(id, snapshot)` in **definition order**. Falls back to sorted HashMap
    /// keys if `node_order` is empty (legacy persisted snapshots).
    pub fn iter_nodes(&self) -> impl Iterator<Item = (&NodeId, &NodeSnapshot)> + '_ {
        self.iter_ordered()
    }
}

impl fmt::Display for ExecutionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} rev={} nodes={}",
            self.execution_id,
            format_state(self.state),
            self.revision,
            self.nodes.len()
        )?;
        for (id, n) in self.iter_nodes() {
            write!(f, "\n  {id} {}", format_node(&n.state))?;
        }
        Ok(())
    }
}

impl ExecutionSnapshot {
    /// Definition order when `node_order` is populated; otherwise HashMap keys
    /// sorted by id (stable, not definition order).
    pub fn iter_ordered(&self) -> impl Iterator<Item = (&NodeId, &NodeSnapshot)> + '_ {
        OrderedIter {
            snap: self,
            idx: 0,
            fallback: None,
        }
    }
}

struct OrderedIter<'a> {
    snap: &'a ExecutionSnapshot,
    idx: usize,
    fallback: Option<Vec<&'a NodeId>>,
}

impl<'a> Iterator for OrderedIter<'a> {
    type Item = (&'a NodeId, &'a NodeSnapshot);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.snap.node_order.is_empty() {
            loop {
                let id = self.snap.node_order.get(self.idx)?;
                self.idx += 1;
                if let Some(n) = self.snap.nodes.get(id) {
                    return Some((id, n));
                }
            }
        }
        if self.fallback.is_none() {
            let mut keys: Vec<&NodeId> = self.snap.nodes.keys().collect();
            keys.sort();
            self.fallback = Some(keys);
        }
        let keys = self.fallback.as_ref()?;
        let id = keys.get(self.idx)?;
        self.idx += 1;
        self.snap.nodes.get(id).map(|n| (*id, n))
    }
}

fn format_state(s: ExecutionState) -> &'static str {
    match s {
        ExecutionState::Created => "Created",
        ExecutionState::Running => "Running",
        ExecutionState::Waiting => "Waiting",
        ExecutionState::Succeeded => "Succeeded",
        ExecutionState::Failed => "Failed",
        ExecutionState::Cancelled => "Cancelled",
        ExecutionState::Completed => "Completed",
    }
}

fn format_node(s: &NodeState) -> String {
    match s {
        NodeState::Pending => "Pending".into(),
        NodeState::Ready { .. } => "Ready".into(),
        NodeState::Running { attempt } => format!("Running({attempt})"),
        NodeState::Waiting { .. } => "Waiting".into(),
        NodeState::Succeeded => "Succeeded".into(),
        NodeState::Failed => "Failed".into(),
        NodeState::Cancelled => "Cancelled".into(),
        NodeState::TimedOut => "TimedOut".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::NodeId;

    fn empty_node() -> NodeSnapshot {
        NodeSnapshot {
            state: NodeState::Pending,
            output: None,
            attempt: 0,
            resume_token: None,
            last_error: None,
        }
    }

    #[test]
    fn iter_nodes_follows_definition_order() {
        let order: Vec<NodeId> = (0..20).map(|i| NodeId::new(format!("page-{i:02}"))).collect();
        let mut nodes = HashMap::new();
        for id in &order {
            nodes.insert(id.clone(), empty_node());
        }
        let snap = ExecutionSnapshot {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            execution_id: ExecutionId::new(),
            workflow_id: WorkflowId::new("fan"),
            state: ExecutionState::Running,
            nodes,
            node_order: order.clone(),
        };
        let got: Vec<&str> = snap.iter_nodes().map(|(id, _)| id.as_str()).collect();
        let want: Vec<&str> = order.iter().map(|i| i.as_str()).collect();
        assert_eq!(got, want);
        assert!(snap.to_string().contains("page-00"));
        assert!(snap.to_string().contains("page-19"));
    }
}
