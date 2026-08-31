//! Shared helpers for the adversarial pack. Every async test is timeout-wrapped
//! via `within` so a hang is a failure, not a stall.

use bytes::Bytes;
use keel_rt::testing::ScriptedExecutor;
use keel_rt::{DomainEvent, ExecutionSnapshot, NodeState};
use std::future::Future;
use std::time::Duration;

pub const BOUND: Duration = Duration::from_secs(5);

pub async fn within<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(BOUND, f)
        .await
        .expect("adversarial test timed out")
}

pub fn hang(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).hang(false)
}

pub fn ok(id: &str) -> ScriptedExecutor {
    ScriptedExecutor::new(id).succeed(Bytes::from(format!("{id}-out")))
}

pub fn count_failed(events: &[DomainEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, DomainEvent::ExecutionFailed { .. }))
        .count()
}

pub fn running_count(snap: &ExecutionSnapshot) -> usize {
    snap.nodes
        .values()
        .filter(|n| matches!(n.state, NodeState::Running { .. }))
        .count()
}
