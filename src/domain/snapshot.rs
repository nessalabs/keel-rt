use crate::domain::ids::{DefinitionHash, ExecutionId, NodeId, ResumeToken, WorkflowId};
use crate::domain::outcome::NodeError;
use crate::domain::state::{ExecutionState, NodeState};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    #[error("schema version {found} does not match {expected}")]
    SchemaMismatch { found: u32, expected: u32 },
    #[error("snapshot workflow id does not match definition")]
    WorkflowIdMismatch,
    #[error("definition hash does not match snapshot")]
    DefinitionHashMismatch,
    #[error("snapshot missing node {0}")]
    MissingNode(NodeId),
    #[error("snapshot has unknown node {0}")]
    UnknownNode(NodeId),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub state: NodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Bytes>,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<ResumeToken>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Identity of the definition body stored beside this snapshot. Empty on
    /// pre-resume snapshots (fail closed on restore).
    #[serde(default)]
    pub definition_hash: DefinitionHash,
}

impl ExecutionSnapshot {
    pub fn node(&self, id: &NodeId) -> Option<&NodeSnapshot> {
        self.nodes.get(id)
    }

    /// `(id, snapshot)` in **definition order**. Falls back to sorted HashMap
    /// keys if `node_order` is empty (legacy persisted snapshots).
    pub fn iter_nodes(&self) -> impl Iterator<Item = (&NodeId, &NodeSnapshot)> + '_ {
        OrderedIter {
            snap: self,
            idx: 0,
            fallback: None,
        }
    }

    /// Nodes in [`NodeState::Running`]. Each holds a concurrency permit.
    /// Equals in-flight execute tasks; Waiting is not counted (permit released).
    pub fn running_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n.state, NodeState::Running { .. }))
            .count()
    }

    /// Nodes in [`NodeState::Waiting`]. Permit already returned to the cap.
    pub fn waiting_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n.state, NodeState::Waiting { .. }))
            .count()
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
    fn node_snapshot_omits_null_optionals_and_reads_legacy_nulls() {
        let compact = serde_json::to_string(&empty_node()).unwrap();
        assert!(
            !compact.contains("resume_token") && !compact.contains("last_error"),
            "{compact}"
        );
        let legacy = r#"{"state":"Pending","output":null,"attempt":0,"resume_token":null,"last_error":null}"#;
        let got: NodeSnapshot = serde_json::from_str(legacy).unwrap();
        assert_eq!(got, empty_node());
        let ready_now = NodeSnapshot {
            state: NodeState::Ready { runnable_at: None },
            ..empty_node()
        };
        let body = serde_json::to_string(&ready_now).unwrap();
        assert!(!body.contains("runnable_at"), "{body}");
    }

    #[test]
    fn iter_nodes_follows_definition_order() {
        let order: Vec<NodeId> = (0..20)
            .map(|i| NodeId::new(format!("page-{i:02}")))
            .collect();
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
            definition_hash: DefinitionHash::default(),
        };
        let got: Vec<&str> = snap.iter_nodes().map(|(id, _)| id.as_str()).collect();
        let want: Vec<&str> = order.iter().map(|i| i.as_str()).collect();
        assert_eq!(got, want);
        assert!(snap.to_string().contains("page-00"));
        assert!(snap.to_string().contains("page-19"));
    }

    #[test]
    fn iter_nodes_legacy_empty_order_sorts_keys() {
        let a = NodeId::new("z-last");
        let b = NodeId::new("a-first");
        let mut nodes = HashMap::new();
        nodes.insert(a.clone(), empty_node());
        nodes.insert(b.clone(), empty_node());
        let snap = ExecutionSnapshot {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            execution_id: ExecutionId::new(),
            workflow_id: WorkflowId::new("legacy"),
            state: ExecutionState::Failed,
            nodes,
            node_order: Vec::new(),
            definition_hash: DefinitionHash::default(),
        };
        let got: Vec<&str> = snap.iter_nodes().map(|(id, _)| id.as_str()).collect();
        assert_eq!(got, vec!["a-first", "z-last"]);
        assert!(snap.to_string().contains("Failed"));
    }

    #[test]
    fn iter_nodes_skips_order_ids_missing_from_the_map() {
        let a = NodeId::new("keep");
        let mut nodes = HashMap::new();
        nodes.insert(a.clone(), empty_node());
        let snap = ExecutionSnapshot {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            execution_id: ExecutionId::new(),
            workflow_id: WorkflowId::new("gap"),
            state: ExecutionState::Running,
            nodes,
            node_order: vec![NodeId::new("ghost"), a.clone(), NodeId::new("also-missing")],
            definition_hash: DefinitionHash::default(),
        };
        let got: Vec<&str> = snap.iter_nodes().map(|(id, _)| id.as_str()).collect();
        assert_eq!(got, vec!["keep"]);
    }

    #[test]
    fn snapshot_display_names_every_execution_and_node_state() {
        fn snap(state: ExecutionState, node: NodeState) -> String {
            let id = NodeId::new("n");
            let mut nodes = HashMap::new();
            nodes.insert(
                id.clone(),
                NodeSnapshot {
                    state: node,
                    output: None,
                    attempt: 2,
                    resume_token: None,
                    last_error: None,
                },
            );
            ExecutionSnapshot {
                schema_version: SCHEMA_VERSION,
                revision: 1,
                execution_id: ExecutionId::new(),
                workflow_id: WorkflowId::new("d"),
                state,
                nodes,
                node_order: vec![id],
                definition_hash: DefinitionHash::default(),
            }
            .to_string()
        }
        assert!(snap(ExecutionState::Created, NodeState::Pending).contains("Created"));
        assert!(snap(
            ExecutionState::Running,
            NodeState::Ready { runnable_at: None }
        )
        .contains("Ready"));
        assert!(
            snap(ExecutionState::Waiting, NodeState::Running { attempt: 1 }).contains("Waiting")
        );
        assert!(
            snap(ExecutionState::Waiting, NodeState::Running { attempt: 1 }).contains("Running(1)")
        );
        let token = ResumeToken::issue(ExecutionId::new(), NodeId::new("n"), 1);
        assert!(snap(
            ExecutionState::Cancelled,
            NodeState::Waiting { token, attempt: 1 }
        )
        .contains("Cancelled"));
        assert!(snap(ExecutionState::Completed, NodeState::Failed).contains("Completed"));
        assert!(snap(ExecutionState::Completed, NodeState::Failed).contains("Failed"));
        assert!(snap(ExecutionState::Succeeded, NodeState::Cancelled).contains("Cancelled"));
        assert!(snap(ExecutionState::Succeeded, NodeState::TimedOut).contains("TimedOut"));
        assert!(snap(ExecutionState::Succeeded, NodeState::Succeeded).contains("Succeeded"));
    }

    #[test]
    fn running_and_waiting_counts_match_node_states() {
        let mut nodes = HashMap::new();
        nodes.insert(
            NodeId::new("r"),
            NodeSnapshot {
                state: NodeState::Running { attempt: 1 },
                output: None,
                attempt: 1,
                resume_token: None,
                last_error: None,
            },
        );
        nodes.insert(
            NodeId::new("w"),
            NodeSnapshot {
                state: NodeState::Waiting {
                    token: ResumeToken::issue(ExecutionId::new(), NodeId::new("w"), 1),
                    attempt: 1,
                },
                output: None,
                attempt: 1,
                resume_token: None,
                last_error: None,
            },
        );
        nodes.insert(NodeId::new("s"), empty_node());
        let snap = ExecutionSnapshot {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            execution_id: ExecutionId::new(),
            workflow_id: WorkflowId::new("c"),
            state: ExecutionState::Running,
            nodes,
            node_order: vec![NodeId::new("r"), NodeId::new("w"), NodeId::new("s")],
            definition_hash: DefinitionHash::default(),
        };
        assert_eq!(snap.running_count(), 1);
        assert_eq!(snap.waiting_count(), 1);
    }
}
