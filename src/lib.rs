//! **Keel** runtime (`keel-rt`): small DAG workflow execution kernel.
//!
//! The runtime is a **bundle** (scheduler + optional store/sink + handle).
//! The scheduler does not know resource types. Drivers only wake. This is a
//! current-thread analog, not work-stealing.
//!
//! Execution is the aggregate. Node is an entity inside it. Ids / outcomes /
//! snapshots are value objects. Policy and Executor are domain ports.
//! StateStore is the repository port.
//!
//! [`OnFailure::FailExecution`] is the library default (fail-fast). FailSubtree
//! and AllDone are opt-in on [`WorkflowDefinition`] only — not a Runtime or
//! process-wide switch.
//!
//! Consumer path: [`WorkflowDefinition::builder`] →
//! [`RuntimeBuilder::register_fn`] → [`Runtime::run`] / [`Runtime::start`].
//! After process death, [`Runtime::resume`] loads the snapshot (at-least-once).
//! [`Runtime::resume_with`] + [`Recover::RetryFailed`] re-invokes Failed /
//! TimedOut nodes; default [`Runtime::resume`] still leaves them Failed.
//! Builtin [`Wait`] (`"wait"`) parks; [`Runtime::complete`] injects into a
//! live drive or applies on the store then drives.
//! Announce via [`Event`] + [`EventSink`] (no EventLog). Persist then emit.
//! Snapshot deadline T is `Ready { runnable_at: Some(Timestamp) }`. The Runtime
//! drive waits via [`Clock::wait_until`]; `FakeClock` is test harness only.
//! `timeout_after` keeps the node Running (executor Delay), not snapshot T.
//! Waiting is an executor yield, not a timer. Use [`ExecutionState::is_successful_finish`]
//! (not `== Succeeded`) so
//! FailSubtree [`Completed`](ExecutionState::Completed) counts as ok.
//!
//! This crate does not pick an allocator and has no `jemalloc` feature.
//! See `benches/JEMALLOC.md` (jemalloc was not a stable win on current_thread).

pub(crate) mod domain;
pub(crate) mod runtime;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

pub use domain::definition::{DefinitionError, Join, OnFailure, WorkflowDefinition};
pub use domain::events::Event;
pub use domain::ids::{
    DefinitionHash, ExecutionId, ExecutorId, InvalidId, NodeId, ResumeToken, WorkflowId,
};
pub use domain::outcome::{NodeError, NodeOutcome, Recover, Resume};
pub use domain::policy::{AcceptPolicy, NeverWaitPolicy, Policy, PolicyDecision, RetryPolicy};
pub use domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SnapshotError, SCHEMA_VERSION};
pub use domain::state::{ApplyCmd, ApplyEffect, ApplyError, Execution, ExecutionState, NodeState};
pub use domain::time::Timestamp;
pub use runtime::executor::{ExecutionContext, Executor, FunctionExecutor};
pub use runtime::handle::ExecutionHandle;
pub use runtime::runtime::{
    CompleteError, ResumeError, Runtime, RuntimeBuilder, StartError, UnregisteredExecutors,
    DEFAULT_CANCEL_BOUND,
};
pub use runtime::sink::{EventSink, FnSink, SinkError};
pub use runtime::store::{
    ClaimError, LeaseEpoch, MemoryStore, NoopStore, OwnerId, StateStore, StoreError,
    DEFAULT_LEASE_TTL,
};
pub use runtime::time::Clock;
pub use runtime::wait::{Wait, WAIT_ID};

#[cfg(any(test, feature = "test-util"))]
pub use testing::{
    FailingStore, FakeClock, FaultySink, FlakyThen, NetFault, ScriptedAction, ScriptedExecutor,
    SequenceStore, TestRun, WorkflowTest,
};
