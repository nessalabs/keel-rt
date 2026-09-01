use crate::domain::ids::{ExecutionId, NodeId, ResumeToken, WorkflowId};
use crate::domain::outcome::NodeError;
use crate::domain::snapshot::SCHEMA_VERSION;
use crate::domain::time::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Kernel event. The public log surface is this type plus [`crate::EventSink`].
/// There is no `EventLog` port. Resume does not fold these.
///
/// At-least-once resume may emit the same node event twice (re-invoke).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    ExecutionStarted {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
        schema_version: u32,
    },
    ExecutionSucceeded {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
        schema_version: u32,
    },
    ExecutionFailed {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
        schema_version: u32,
    },
    ExecutionCompleted {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
        schema_version: u32,
    },
    ExecutionCancelled {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
        schema_version: u32,
    },
    NodeStarted {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
    },
    NodeSucceeded {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
    },
    NodeFailed {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
        error: NodeError,
    },
    NodeTimedOut {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
    },
    NodeCancelled {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
    },
    NodeWaiting {
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
        schema_version: u32,
        token: ResumeToken,
    },
}

impl Event {
    pub fn execution_id(&self) -> &ExecutionId {
        match self {
            Self::ExecutionStarted { execution_id, .. }
            | Self::ExecutionSucceeded { execution_id, .. }
            | Self::ExecutionFailed { execution_id, .. }
            | Self::ExecutionCompleted { execution_id, .. }
            | Self::ExecutionCancelled { execution_id, .. }
            | Self::NodeStarted { execution_id, .. }
            | Self::NodeSucceeded { execution_id, .. }
            | Self::NodeFailed { execution_id, .. }
            | Self::NodeTimedOut { execution_id, .. }
            | Self::NodeCancelled { execution_id, .. }
            | Self::NodeWaiting { execution_id, .. } => execution_id,
        }
    }

    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            Self::ExecutionStarted { workflow_id, .. }
            | Self::ExecutionSucceeded { workflow_id, .. }
            | Self::ExecutionFailed { workflow_id, .. }
            | Self::ExecutionCompleted { workflow_id, .. }
            | Self::ExecutionCancelled { workflow_id, .. }
            | Self::NodeStarted { workflow_id, .. }
            | Self::NodeSucceeded { workflow_id, .. }
            | Self::NodeFailed { workflow_id, .. }
            | Self::NodeTimedOut { workflow_id, .. }
            | Self::NodeCancelled { workflow_id, .. }
            | Self::NodeWaiting { workflow_id, .. } => workflow_id,
        }
    }

    pub fn at(&self) -> Timestamp {
        match self {
            Self::ExecutionStarted { at, .. }
            | Self::ExecutionSucceeded { at, .. }
            | Self::ExecutionFailed { at, .. }
            | Self::ExecutionCompleted { at, .. }
            | Self::ExecutionCancelled { at, .. }
            | Self::NodeStarted { at, .. }
            | Self::NodeSucceeded { at, .. }
            | Self::NodeFailed { at, .. }
            | Self::NodeTimedOut { at, .. }
            | Self::NodeCancelled { at, .. }
            | Self::NodeWaiting { at, .. } => *at,
        }
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        match self {
            Self::NodeStarted { node_id, .. }
            | Self::NodeSucceeded { node_id, .. }
            | Self::NodeFailed { node_id, .. }
            | Self::NodeTimedOut { node_id, .. }
            | Self::NodeCancelled { node_id, .. }
            | Self::NodeWaiting { node_id, .. } => Some(node_id),
            Self::ExecutionStarted { .. }
            | Self::ExecutionSucceeded { .. }
            | Self::ExecutionFailed { .. }
            | Self::ExecutionCompleted { .. }
            | Self::ExecutionCancelled { .. } => None,
        }
    }

    pub fn attempt(&self) -> Option<u32> {
        match self {
            Self::NodeStarted { attempt, .. }
            | Self::NodeSucceeded { attempt, .. }
            | Self::NodeFailed { attempt, .. }
            | Self::NodeTimedOut { attempt, .. }
            | Self::NodeCancelled { attempt, .. }
            | Self::NodeWaiting { attempt, .. } => Some(*attempt),
            Self::ExecutionStarted { .. }
            | Self::ExecutionSucceeded { .. }
            | Self::ExecutionFailed { .. }
            | Self::ExecutionCompleted { .. }
            | Self::ExecutionCancelled { .. } => None,
        }
    }

    pub fn schema_version(&self) -> u32 {
        match self {
            Self::ExecutionStarted { schema_version, .. }
            | Self::ExecutionSucceeded { schema_version, .. }
            | Self::ExecutionFailed { schema_version, .. }
            | Self::ExecutionCompleted { schema_version, .. }
            | Self::ExecutionCancelled { schema_version, .. }
            | Self::NodeStarted { schema_version, .. }
            | Self::NodeSucceeded { schema_version, .. }
            | Self::NodeFailed { schema_version, .. }
            | Self::NodeTimedOut { schema_version, .. }
            | Self::NodeCancelled { schema_version, .. }
            | Self::NodeWaiting { schema_version, .. } => *schema_version,
        }
    }

    pub(crate) fn exec(
        kind: ExecKind,
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        at: Timestamp,
    ) -> Self {
        match kind {
            ExecKind::Started => Self::ExecutionStarted {
                execution_id,
                workflow_id,
                at,
                schema_version: SCHEMA_VERSION,
            },
            ExecKind::Succeeded => Self::ExecutionSucceeded {
                execution_id,
                workflow_id,
                at,
                schema_version: SCHEMA_VERSION,
            },
            ExecKind::Failed => Self::ExecutionFailed {
                execution_id,
                workflow_id,
                at,
                schema_version: SCHEMA_VERSION,
            },
            ExecKind::Completed => Self::ExecutionCompleted {
                execution_id,
                workflow_id,
                at,
                schema_version: SCHEMA_VERSION,
            },
            ExecKind::Cancelled => Self::ExecutionCancelled {
                execution_id,
                workflow_id,
                at,
                schema_version: SCHEMA_VERSION,
            },
        }
    }

    pub(crate) fn node(
        kind: NodeKind,
        execution_id: ExecutionId,
        workflow_id: WorkflowId,
        node_id: NodeId,
        attempt: u32,
        at: Timestamp,
    ) -> Self {
        match kind {
            NodeKind::Started => Self::NodeStarted {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
            },
            NodeKind::Succeeded => Self::NodeSucceeded {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
            },
            NodeKind::Failed(error) => Self::NodeFailed {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
                error,
            },
            NodeKind::TimedOut => Self::NodeTimedOut {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
            },
            NodeKind::Cancelled => Self::NodeCancelled {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
            },
            NodeKind::Waiting(token) => Self::NodeWaiting {
                execution_id,
                workflow_id,
                node_id,
                attempt,
                at,
                schema_version: SCHEMA_VERSION,
                token,
            },
        }
    }
}

pub(crate) enum ExecKind {
    Started,
    Succeeded,
    Failed,
    Completed,
    Cancelled,
}

pub(crate) enum NodeKind {
    Started,
    Succeeded,
    Failed(NodeError),
    TimedOut,
    Cancelled,
    Waiting(ResumeToken),
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionStarted { execution_id, .. } => {
                write!(f, "execution started {execution_id}")
            }
            Self::ExecutionSucceeded { execution_id, .. } => {
                write!(f, "execution succeeded {execution_id}")
            }
            Self::ExecutionFailed { execution_id, .. } => {
                write!(f, "execution failed {execution_id}")
            }
            Self::ExecutionCancelled { execution_id, .. } => {
                write!(f, "execution cancelled {execution_id}")
            }
            Self::ExecutionCompleted { execution_id, .. } => {
                write!(f, "execution completed {execution_id}")
            }
            Self::NodeStarted {
                node_id, attempt, ..
            } => write!(f, "node {node_id} started attempt={attempt}"),
            Self::NodeSucceeded { node_id, .. } => write!(f, "node {node_id} succeeded"),
            Self::NodeFailed {
                node_id, error, ..
            } => write!(f, "node {node_id} failed: {error}"),
            Self::NodeCancelled { node_id, .. } => write!(f, "node {node_id} cancelled"),
            Self::NodeWaiting { node_id, .. } => write!(f, "node {node_id} waiting"),
            Self::NodeTimedOut { node_id, .. } => write!(f, "node {node_id} timed out"),
        }
    }
}
