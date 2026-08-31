use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// Stable node identity. Clones are refcount bumps (`Arc<str>`).
/// Public API never exposes petgraph indices or aggregate slots.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<str>);

impl Serialize for NodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(s))
    }
}

impl NodeId {
    pub fn new(id: impl AsRef<str>) -> Self {
        Self(Arc::from(id.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Dense slot inside one validated definition / execution aggregate.
/// Never part of the public API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NodeSlot(pub usize);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(String);

impl WorkflowId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for WorkflowId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for WorkflowId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(String);

static EXEC_SEQ: AtomicU64 = AtomicU64::new(1);

impl ExecutionId {
    pub fn new() -> Self {
        let n = EXEC_SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self(format!("exec-{n}-{nanos}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Restore an id from a snapshot. Empty is not an identity.
    pub fn parse(s: impl AsRef<str>) -> Result<Self, InvalidId> {
        let s = s.as_ref();
        if s.is_empty() {
            return Err(InvalidId::EmptyExecutionId);
        }
        Ok(Self(s.to_string()))
    }
}

impl Default for ExecutionId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InvalidId {
    #[error("execution id must not be empty")]
    EmptyExecutionId,
    #[error("definition hash must not be empty")]
    EmptyDefinitionHash,
}

/// Identity of a [`crate::WorkflowDefinition`] body. Snapshot holds this, not the DAG.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DefinitionHash(String);

impl DefinitionHash {
    pub fn parse(s: impl AsRef<str>) -> Result<Self, InvalidId> {
        let s = s.as_ref();
        if s.is_empty() {
            return Err(InvalidId::EmptyDefinitionHash);
        }
        Ok(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for DefinitionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutorId(String);

impl ExecutorId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ExecutorId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for ExecutorId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl fmt::Display for ExecutorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Bound to `(execution, node, attempt)` plus a nonce so tokens are not interchangeable.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResumeToken {
    execution_id: ExecutionId,
    node_id: NodeId,
    attempt: u32,
    nonce: u64,
}

static TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

impl ResumeToken {
    pub fn issue(execution_id: ExecutionId, node_id: NodeId, attempt: u32) -> Self {
        Self {
            execution_id,
            node_id,
            attempt,
            nonce: TOKEN_SEQ.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn nonce(&self) -> u64 {
        self.nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_id_parse_rejects_empty() {
        assert_eq!(
            ExecutionId::parse("").unwrap_err(),
            InvalidId::EmptyExecutionId
        );
        assert_eq!(ExecutionId::parse("exec-1").unwrap().as_str(), "exec-1");
    }

    #[test]
    fn definition_hash_parse_rejects_empty() {
        assert_eq!(
            DefinitionHash::parse("").unwrap_err(),
            InvalidId::EmptyDefinitionHash
        );
        assert_eq!(DefinitionHash::parse("abc").unwrap().as_str(), "abc");
        assert!(DefinitionHash::default().is_empty());
        assert_eq!(DefinitionHash::parse("abc").unwrap().to_string(), "abc");
    }
}
