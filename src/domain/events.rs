use crate::domain::ids::{ExecutionId, NodeId, ResumeToken};
use crate::domain::outcome::NodeError;
use crate::runtime::time::Timestamp;
use serde::{Deserialize, Serialize};

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
