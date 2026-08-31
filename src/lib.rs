//! **Keel** runtime (`keel-rt`): small DAG workflow execution kernel (Phase 1).
//!
//! The runtime is a **bundle** (scheduler + optional store/sink + handle).
//! The scheduler does not know resource types. Drivers only wake. This is a
//! current-thread analog, not work-stealing.
//!
//! Execution is the aggregate. Node is an entity inside it. Ids / outcomes /
//! snapshots are value objects. Policy and Executor are domain ports.
//! StateStore is the repository port.

pub mod domain;
pub mod runtime;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

pub use domain::definition::{DefinitionError, Join, OnFailure, WorkflowDefinition};
pub use domain::events::DomainEvent;
pub use domain::ids::{ExecutionId, ExecutorId, NodeId, ResumeToken, WorkflowId};
pub use domain::outcome::{NodeError, NodeOutcome, Resume};
pub use domain::policy::{AcceptPolicy, NeverWaitPolicy, Policy, PolicyDecision, RetryPolicy};
pub use domain::snapshot::{ExecutionSnapshot, NodeSnapshot, SCHEMA_VERSION};
pub use domain::state::{ApplyError, ExecutionState, NodeState};
pub use domain::time::Timestamp;
pub use runtime::executor::{ExecutionContext, Executor, FunctionExecutor};
pub use runtime::handle::ExecutionHandle;
pub use runtime::sink::{EventSink, FnSink};
pub use runtime::runtime::{Runtime, RuntimeBuilder, DEFAULT_CANCEL_BOUND};
pub use runtime::store::{MemoryStore, NoopStore, StateStore, StoreError};

#[cfg(any(test, feature = "test-util"))]
pub use testing::{
    FakeClock, FailingStore, FaultySink, FlakyThen, NetFault, ScriptedAction, ScriptedExecutor,
    SequenceStore, TestRun, WorkflowTest,
};
