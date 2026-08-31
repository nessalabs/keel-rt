//! Scenario suite: happy path, fail-fast, cancel, policy, waiting, store, isolation.
//!
//! `cargo test --test scenarios -- --test-threads=1`

#[path = "scenarios/happy_path.rs"]
mod happy_path;
#[path = "scenarios/failure.rs"]
mod failure;
#[path = "scenarios/failure_scope.rs"]
mod failure_scope;
#[path = "scenarios/cancel.rs"]
mod cancel;
#[path = "scenarios/policy.rs"]
mod policy;
#[path = "scenarios/waiting.rs"]
mod waiting;
#[path = "scenarios/store.rs"]
mod store;
#[path = "scenarios/isolation.rs"]
mod isolation;
