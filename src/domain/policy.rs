use crate::domain::outcome::NodeOutcome;
use std::time::Duration;

/// Policy result consulted inside `apply_outcome` (sync, never in the scheduler).
///
/// - [`Accept`]: take the executor/resume outcome as written (including Waiting).
/// - [`Retry`]: Failed/TimedOut only — node becomes `Ready { runnable_at }`, never Waiting.
/// - [`Reject`]: fail the node (and fail-fast). Refuse Waiting without encoding "no" as Retry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    Accept,
    Retry { delay: Duration },
    Reject,
}

/// Consulted after an executor outcome or a resume Complete. Must stay sync:
/// the scheduler apply loop never awaits user code.
pub trait Policy: Send + Sync {
    fn decide(&self, outcome: &NodeOutcome, attempt: u32) -> PolicyDecision;
}

/// Default policy: accept every outcome, never retry.
#[derive(Clone, Debug, Default)]
pub struct AcceptPolicy;

impl Policy for AcceptPolicy {
    fn decide(&self, _outcome: &NodeOutcome, _attempt: u32) -> PolicyDecision {
        PolicyDecision::Accept
    }
}

/// Retry Failed / TimedOut while `attempt < max_attempts`.
/// `max_attempts` is the total number of execute invocations, not extra retries.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay: Duration,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            delay,
        }
    }
}

impl Policy for RetryPolicy {
    fn decide(&self, outcome: &NodeOutcome, attempt: u32) -> PolicyDecision {
        match outcome {
            NodeOutcome::Failed(_) | NodeOutcome::TimedOut => {
                if attempt < self.max_attempts {
                    PolicyDecision::Retry { delay: self.delay }
                } else {
                    PolicyDecision::Accept
                }
            }
            NodeOutcome::Succeeded(_) | NodeOutcome::Waiting { .. } => PolicyDecision::Accept,
        }
    }
}

/// Accepts Succeeded / Failed / TimedOut. [`Reject`]s Waiting (node Failed + fail-fast).
#[derive(Clone, Debug, Default)]
pub struct NeverWaitPolicy;

impl Policy for NeverWaitPolicy {
    fn decide(&self, outcome: &NodeOutcome, _attempt: u32) -> PolicyDecision {
        match outcome {
            NodeOutcome::Waiting { .. } => PolicyDecision::Reject,
            _ => PolicyDecision::Accept,
        }
    }
}

impl Policy for Box<dyn Policy> {
    fn decide(&self, outcome: &NodeOutcome, attempt: u32) -> PolicyDecision {
        (**self).decide(outcome, attempt)
    }
}

impl Policy for std::sync::Arc<dyn Policy> {
    fn decide(&self, outcome: &NodeOutcome, attempt: u32) -> PolicyDecision {
        (**self).decide(outcome, attempt)
    }
}
