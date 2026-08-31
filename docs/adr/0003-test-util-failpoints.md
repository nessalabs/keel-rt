# 0003. Failpoints stay a test-util process map

- **Date:** 2026-08-31
- **Status:** accepted

## Context

`testing::failpoint` is a named hit counter (`OnceLock<Mutex<HashMap>>`) used
by `FailingStore` and `ScriptedExecutor`. It is not consulted by the
production scheduler. Structure forbids ambient singletons in the kernel;
this lives behind `test-util`.

## Decision

Keep the process-wide map in `src/testing/failpoint.rs`. Do not scatter
`failpoint::take` through `scheduler.rs` or `apply`. Tests that share a
process must `reset` if they enable a name.

## Alternatives considered

- **Thread-local map.** Does not match how `WorkflowTest` shares doubles
  across spawn.
- **Pass a Faults handle through Runtime.** New configurability on the
  production builder. Not asked for.
- **Delete failpoints.** `FailingStore::fail_on_nth_put` already covers
  store faults; executor panic still wants a named trip.

## Consequences

Parallel `cargo test` with shared failpoint names can flake. The suite runs
`--test-threads=1`. Production `src/domain` and `src/runtime` stay free of
the map.
