//! Failure-injection test harness. Always available under
//! `#[cfg(any(test, feature = "test-util"))]`.
//!
//! Inspired by Tokio's `test-util` (paused clock, mock park) and scripted
//! failures — not loom.

pub mod clock;
pub mod failpoint;
pub mod faults;
pub mod harness;
pub mod scripted;
pub mod store;

pub use clock::FakeClock;
pub use failpoint::{disable, enable, remaining, reset, take};
pub use faults::{FaultySink, FlakyThen, NetFault};
pub use harness::{TestRun, WorkflowTest};
pub use scripted::{ScriptedAction, ScriptedExecutor};
pub use store::{FailingStore, SequenceStore};
