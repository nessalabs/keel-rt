use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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

/// Bound to `(execution, node, attempt)` plus a 128-bit nonce so tokens
/// are not interchangeable or guessable from the id alone.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResumeToken {
    execution_id: ExecutionId,
    node_id: NodeId,
    attempt: u32,
    #[serde(serialize_with = "ser_nonce", deserialize_with = "de_nonce")]
    nonce: u128,
}

static TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);
static PROCESS_KEY: OnceLock<u128> = OnceLock::new();

fn process_key() -> u128 {
    *PROCESS_KEY.get_or_init(|| {
        let mut b = [0u8; 16];
        let _ = getrandom::getrandom(&mut b);
        u128::from_le_bytes(b) | 1
    })
}

/// SplitMix-style mix: one atomic + arithmetic, no per-token syscall.
/// Consecutive `issue` values are not sequential integers.
fn mix_nonce(seq: u64) -> u128 {
    let key = process_key();
    let mut z = key ^ ((seq as u128) << 64 | seq as u128);
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835);
    z = (z ^ (z >> 64)).wrapping_mul(0xBF58_476D_1CE4_E5B9_2917_F0D7_C8C5_D3A5);
    z ^ (z >> 64)
}

fn ser_nonce<S: Serializer>(n: &u128, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format!("{n:032x}"))
}

fn de_nonce<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
    let raw = String::deserialize(d)?;
    u128::from_str_radix(&raw, 16).map_err(serde::de::Error::custom)
}

impl ResumeToken {
    pub fn issue(execution_id: ExecutionId, node_id: NodeId, attempt: u32) -> Self {
        let seq = TOKEN_SEQ.fetch_add(1, Ordering::Relaxed);
        Self {
            execution_id,
            node_id,
            attempt,
            nonce: mix_nonce(seq),
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

    pub fn nonce(&self) -> u128 {
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

    #[test]
    fn resume_tokens_are_not_sequential_ints() {
        let e = ExecutionId::parse("exec-1").unwrap();
        let n = NodeId::new("hold");
        let a = ResumeToken::issue(e.clone(), n.clone(), 1);
        let b = ResumeToken::issue(e, n, 1);
        assert_ne!(a.nonce(), b.nonce());
        assert_ne!(
            a.nonce().abs_diff(b.nonce()),
            1,
            "consecutive issue() must not be sequential integers"
        );
        assert_ne!(a.nonce(), 1);
        assert_ne!(b.nonce(), 2);
    }

    #[test]
    fn resume_token_nonce_round_trips_as_hex_not_guessable_from_id() {
        let e = ExecutionId::parse("exec-1").unwrap();
        let n = NodeId::new("hold");
        let t = ResumeToken::issue(e.clone(), n.clone(), 1);
        let v = serde_json::to_value(&t).unwrap();
        let hex = v["nonce"].as_str().expect("nonce is hex, not a JSON int");
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        let back: ResumeToken = serde_json::from_value(v).unwrap();
        assert_eq!(back.nonce(), t.nonce());
        let guess: ResumeToken = serde_json::from_value(serde_json::json!({
            "execution_id": e.as_str(),
            "node_id": "hold",
            "attempt": 1,
            "nonce": "00000000000000000000000000000001"
        }))
        .unwrap();
        assert_ne!(guess.nonce(), t.nonce());
    }
}
