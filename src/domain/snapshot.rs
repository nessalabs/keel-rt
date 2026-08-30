use crate::domain::ids::{ExecutionId, NodeId, ResumeToken, WorkflowId};
use crate::domain::outcome::NodeError;
use crate::domain::state::{ExecutionState, NodeState};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub schema_version: u32,
    pub revision: u64,
    pub execution_id: ExecutionId,
    pub workflow_id: WorkflowId,
    pub state: ExecutionState,
    pub nodes: HashMap<NodeId, NodeSnapshot>,
}

impl ExecutionSnapshot {
    pub fn node(&self, id: &NodeId) -> Option<&NodeSnapshot> {
        self.nodes.get(id)
    }
}
