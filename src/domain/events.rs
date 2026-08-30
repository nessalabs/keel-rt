use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
use crate::domain::outcome::NodeError;
use crate::domain::time::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Domain events as data. The kernel does not interpret payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainEvent {
    ExecutionStarted { execution_id: ExecutionId },
    ExecutionSucceeded { execution_id: ExecutionId },
    ExecutionFailed { execution_id: ExecutionId },
    ExecutionCancelled { execution_id: ExecutionId },
    ExecutionWaiting { execution_id: ExecutionId },
    NodeReady {
        node_id: NodeId,
        runnable_at: Option<Timestamp>,
    },
    NodeStarted { node_id: NodeId, attempt: u32 },
    NodeSucceeded { node_id: NodeId },
    NodeFailed { node_id: NodeId, error: NodeError },
    NodeCancelled { node_id: NodeId },
    NodeWaiting { node_id: NodeId, token: ResumeToken },
    NodeTimedOut { node_id: NodeId },
}

impl fmt::Display for DomainEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionStarted { execution_id } => {
                write!(f, "execution started {execution_id}")
            }
            Self::ExecutionSucceeded { execution_id } => {
                write!(f, "execution succeeded {execution_id}")
            }
            Self::ExecutionFailed { execution_id } => {
                write!(f, "execution failed {execution_id}")
            }
            Self::ExecutionCancelled { execution_id } => {
                write!(f, "execution cancelled {execution_id}")
            }
            Self::ExecutionWaiting { execution_id } => {
                write!(f, "execution waiting {execution_id}")
            }
            Self::NodeReady {
                node_id,
                runnable_at,
            } => match runnable_at {
                Some(at) => write!(f, "node {node_id} ready at {at}"),
                None => write!(f, "node {node_id} ready"),
            },
            Self::NodeStarted { node_id, attempt } => {
                write!(f, "node {node_id} started attempt={attempt}")
            }
            Self::NodeSucceeded { node_id } => write!(f, "node {node_id} succeeded"),
            Self::NodeFailed { node_id, error } => {
                write!(f, "node {node_id} failed: {error}")
            }
            Self::NodeCancelled { node_id } => write!(f, "node {node_id} cancelled"),
            Self::NodeWaiting { node_id, .. } => write!(f, "node {node_id} waiting"),
            Self::NodeTimedOut { node_id } => write!(f, "node {node_id} timed out"),
        }
    }
}
