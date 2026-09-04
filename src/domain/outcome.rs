use crate::domain::ids::ResumeToken;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Max bytes stored on a Failed/TimedOut [`NodeError`] (`last_error` on the
/// snapshot, `NodeFailed` events). **Succeeded `Bytes` are not capped** —
/// fat payloads stay refcounted (`fat_bytes_join_input_is_refcount_not_copy`).
pub const MAX_LAST_ERROR: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct NodeError {
    pub message: String,
}

impl NodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: cap_last_error(message.into()),
        }
    }

    /// Bound a value that bypassed [`Self::new`] (struct literal, serde).
    pub fn capped(self) -> Self {
        Self {
            message: cap_last_error(self.message),
        }
    }
}

fn cap_last_error(s: String) -> String {
    if s.len() <= MAX_LAST_ERROR {
        return s;
    }
    let mut end = MAX_LAST_ERROR;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut s = s;
    s.truncate(end);
    s
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

/// Handle / [`crate::Runtime::complete`] command. Kernel issues the token;
/// caller supplies the action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resume {
    Complete(NodeOutcome),
    Reinvoke,
}

/// How [`crate::Runtime::resume_with`] continues a stored execution.
///
/// [`Self::Continue`] is Phase 2 default (`Runtime::resume`): Failed stay
/// Failed. [`Self::RetryFailed`] re-invokes Failed/TimedOut nodes.
/// [`Resume`] on the handle is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recover {
    Continue,
    RetryFailed,
}

impl fmt::Display for NodeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded(b) => write!(f, "Succeeded({} bytes)", b.len()),
            Self::Failed(e) => write!(f, "Failed({e})"),
            Self::Waiting { token } => write!(f, "Waiting({})", token.node_id()),
            Self::TimedOut => write!(f, "TimedOut"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_caps_message_over_max() {
        let err = NodeError::new("x".repeat(MAX_LAST_ERROR + 8));
        assert_eq!(err.message.len(), MAX_LAST_ERROR);
    }

    #[test]
    fn exact_max_is_kept() {
        let err = NodeError::new("y".repeat(MAX_LAST_ERROR));
        assert_eq!(err.message.len(), MAX_LAST_ERROR);
    }

    #[test]
    fn cap_does_not_split_utf8_char() {
        let mut s = "x".repeat(MAX_LAST_ERROR - 1);
        s.push('\u{1F600}');
        assert!(s.len() > MAX_LAST_ERROR);
        let err = NodeError { message: s.clone() }.capped();
        assert!(err.message.len() <= MAX_LAST_ERROR);
        assert!(err.message.is_char_boundary(err.message.len()));
        assert!(s.starts_with(&err.message));
        assert!(!err.message.contains('\u{1F600}'));
    }
}
