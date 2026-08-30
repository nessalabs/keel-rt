use crate::domain::ids::ResumeToken;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct NodeError {
    pub message: String,
}

impl NodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeOutcome {
    Succeeded(Bytes),
    Failed(NodeError),
    Waiting { token: ResumeToken },
    TimedOut,
}

impl NodeOutcome {
    pub fn succeeded(bytes: impl Into<Bytes>) -> Self {
        Self::Succeeded(bytes.into())
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(NodeError::new(message))
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    /// Two completes are equivalent when both succeeded with the same bytes
    /// (or both failed with the same message, both timed out).
    pub fn equivalent(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Succeeded(a), Self::Succeeded(b)) => a == b,
            (Self::Failed(a), Self::Failed(b)) => a.message == b.message,
            (Self::TimedOut, Self::TimedOut) => true,
            (Self::Waiting { token: a }, Self::Waiting { token: b }) => a == b,
            _ => false,
        }
    }
}

/// Handle resume command. Kernel issues the token; caller supplies the action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resume {
    Complete(NodeOutcome),
    Reinvoke,
}

impl fmt::Display for NodeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded(b) => write!(f, "Succeeded({} bytes)", b.len()),
            Self::Failed(e) => write!(f, "Failed({e})"),
            Self::Waiting { token } => write!(f, "Waiting({})", token.node_id),
            Self::TimedOut => write!(f, "TimedOut"),
        }
    }
}
