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
//! Announce via [`Event`] + [`EventSink`] (no EventLog). Persist then emit.
//! Snapshot deadline T is `Ready { runnable_at: Some(T) }` (FakeClock in tests).
//! Waiting is HITL, not a timer. Use [`ExecutionState::is_successful_finish`]
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
pub use domain::outcome::{NodeError, NodeOutcome, Resume};
pub use domain::policy::{AcceptPolicy, NeverWaitPolicy, Policy, PolicyDecision, RetryPolicy};
pub use domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SnapshotError, SCHEMA_VERSION};
pub use domain::state::{ApplyCmd, ApplyEffect, ApplyError, Execution, ExecutionState, NodeState};
pub use domain::time::Timestamp;
pub use runtime::executor::{ExecutionContext, Executor, FunctionExecutor};
pub use runtime::handle::ExecutionHandle;
pub use runtime::runtime::{
    ResumeError, Runtime, RuntimeBuilder, StartError, UnregisteredExecutors, DEFAULT_CANCEL_BOUND,
};
pub use runtime::sink::{EventSink, FnSink, SinkError};
pub use runtime::store::{MemoryStore, NoopStore, StateStore, StoreError};
pub use runtime::time::Clock;

#[cfg(any(test, feature = "test-util"))]
pub use testing::{
    FailingStore, FakeClock, FaultySink, FlakyThen, NetFault, ScriptedAction, ScriptedExecutor,
    SequenceStore, TestRun, WorkflowTest,
};
