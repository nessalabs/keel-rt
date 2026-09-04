use crate::domain::ids::ResumeToken;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Marker appended when a Failed message is shortened for snapshot state.
pub const SNAPSHOT_ERROR_MARK: &str = "\u{2026}";

/// Max bytes of `last_error` on a live/durable snapshot (`NodeSnapshot`).
/// Truncated form is a UTF-8 prefix plus [`SNAPSHOT_ERROR_MARK`].
/// Full Failed detail is [`crate::Event::NodeFailed`] / [`crate::Event::NodeAttemptFailed`]
/// (see [`MAX_SINK_ERROR`]).
/// **Succeeded `Bytes` are not capped** —
/// fat payloads stay refcounted (`fat_bytes_join_input_is_refcount_not_copy`).
pub const MAX_SNAPSHOT_ERROR: usize = 512;

/// Max bytes of [`NodeError`] on [`crate::Event::NodeFailed`] and
/// [`crate::Event::NodeAttemptFailed`] (EventSink and `persist_with_events`
/// rows). Snapshot `last_error` uses [`MAX_SNAPSHOT_ERROR`]. Succeeded
/// `Bytes` are not capped.
pub const MAX_SINK_ERROR: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct NodeError {
    pub message: String,
}

impl NodeError {
    /// Construct a sink-capped error. Snapshot storage still shortens at apply.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: truncate_utf8(message.into(), MAX_SINK_ERROR),
        }
    }

    /// Short form stored on `last_error` / inspect / store snapshots.
    pub fn snapshot_short(self) -> Self {
        Self {
            message: truncate_with_mark(self.message, MAX_SNAPSHOT_ERROR),
        }
    }

    /// Bound a value that bypassed [`Self::new`] (struct literal, serde)
    /// before it is announced on [`crate::Event::NodeFailed`] /
    /// [`crate::Event::NodeAttemptFailed`].
    pub fn sink_capped(self) -> Self {
        Self {
            message: truncate_utf8(self.message, MAX_SINK_ERROR),
        }
    }
}

fn truncate_utf8(s: String, max: usize) -> String {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut s = s;
    s.truncate(end);
    s
}

fn truncate_with_mark(s: String, max: usize) -> String {
    if s.len() <= max {
        return s;
    }
    let mark = SNAPSHOT_ERROR_MARK;
    debug_assert!(max > mark.len());
    let mut end = max - mark.len();
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + mark.len());
    out.push_str(&s[..end]);
    out.push_str(mark);
    out
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
    fn new_caps_message_at_sink_bound() {
        let err = NodeError::new("x".repeat(MAX_SINK_ERROR + 8));
        assert_eq!(err.message.len(), MAX_SINK_ERROR);
        assert!(!err.message.contains('\u{2026}'));
    }

    #[test]
    fn exact_sink_max_is_kept() {
        let err = NodeError::new("y".repeat(MAX_SINK_ERROR));
        assert_eq!(err.message.len(), MAX_SINK_ERROR);
    }

    #[test]
    fn snapshot_short_is_max_snapshot_error() {
        let err = NodeError {
            message: "z".repeat(MAX_SNAPSHOT_ERROR + 64),
        }
        .snapshot_short();
        assert!(err.message.len() <= MAX_SNAPSHOT_ERROR);
        assert!(err.message.ends_with('\u{2026}'));
        assert!(err.message.len() < MAX_SNAPSHOT_ERROR + 64);
    }

    #[test]
    fn exact_snapshot_max_is_kept() {
        let err = NodeError {
            message: "y".repeat(MAX_SNAPSHOT_ERROR),
        }
        .snapshot_short();
        assert_eq!(err.message.len(), MAX_SNAPSHOT_ERROR);
        assert!(!err.message.ends_with('\u{2026}'));
    }

    #[test]
    fn snapshot_short_does_not_split_utf8_char() {
        let mut s = "x".repeat(MAX_SNAPSHOT_ERROR - 1);
        s.push('\u{1F600}');
        assert!(s.len() > MAX_SNAPSHOT_ERROR);
        let err = NodeError { message: s.clone() }.snapshot_short();
        assert!(err.message.len() <= MAX_SNAPSHOT_ERROR);
        assert!(err.message.is_char_boundary(err.message.len()));
        assert!(err.message.ends_with('\u{2026}'));
        let prefix = err.message.trim_end_matches('\u{2026}');
        assert!(s.starts_with(prefix));
        assert!(!prefix.contains('\u{1F600}'));
    }

    #[test]
    fn sink_capped_does_not_split_utf8_char() {
        let mut s = "x".repeat(MAX_SINK_ERROR - 1);
        s.push('\u{1F600}');
        assert!(s.len() > MAX_SINK_ERROR);
        let err = NodeError { message: s.clone() }.sink_capped();
        assert!(err.message.len() <= MAX_SINK_ERROR);
        assert!(err.message.is_char_boundary(err.message.len()));
        assert!(s.starts_with(&err.message));
        assert!(!err.message.contains('\u{1F600}'));
        assert!(!err.message.contains('\u{2026}'));
    }
}
