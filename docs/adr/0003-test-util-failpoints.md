# 0003. Failpoints are per-double registries

- **Date:** 2026-08-31
- **Status:** accepted
- **Revised:** 2026-10-06 — replaced the process-wide `OnceLock` map

## Context

`testing::failpoint` is a named hit counter used by `FailingStore` and
`ScriptedExecutor`. It is not consulted by the production scheduler.
A process-wide `OnceLock<Mutex<HashMap>>` let parallel tests that
`enable("store.put", …)` steal each other's hits.

## Decision

Each `FailingStore` and `ScriptedExecutor` owns an `Arc<Failpoints>`.
`take` / `enable` / `disable` / `reset` / `remaining` are methods on that
registry. Constructors create a fresh `Arc`. Do not scatter
`Failpoints::take` through `scheduler.rs` or `apply`.

## Alternatives considered

- **Process-wide map.** Flakes when two tests arm the same name.
- **Thread-local map.** Does not match how `WorkflowTest` shares doubles
  across spawn.
- **Pass a Faults handle through Runtime.** New configurability on the
  production builder. Not asked for.
- **Delete failpoints.** `FailingStore::fail_on_nth_put` already covers
  store faults; executor panic still wants a named trip.

## Consequences

Two stores or executors with the same failpoint name do not share hits.
Clones of one `ScriptedExecutor` share its registry. Production
`src/domain` and `src/runtime` stay free of the map.
