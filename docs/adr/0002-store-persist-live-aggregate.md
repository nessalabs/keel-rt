# 0002. StateStore::persist may see the live Execution

- **Date:** 2026-08-31
- **Status:** accepted

## Context

`MemoryStore` incremental persist (dirty slots only) is a measured win on
wide/deep DAGs (`benches/BASELINE.md`). The default `put(&snapshot)` rebuilds
a full `HashMap`. A dirty-delta DTO would be a new type nobody asked for.

`Execution` is already a public apply API (stale/timer packs, apply-only bench).
The store port is not a product adapter; it is the repository port.

## Decision

Leave `StateStore::persist(&Execution)` as a defaulted method. Custom stores
keep implementing `put`/`get` on snapshots. `MemoryStore` overrides `persist`.
`is_noop` stays so the scheduler can skip work for `NoopStore`.

## Alternatives considered

- **Dirty-patch value object.** Extra type, same information `Execution`
  already has. Not extracted: [`Execution::dirty_nodes`] is the list the
  second store (`keel-rt-sqlite`) writes.
- **Downcast MemoryStore in the scheduler.** Worse: scheduler would name an
  adapter.
- **Revert incremental persist.** Costs the hot path we already paid to fix.

## Consequences

Implementors of `StateStore` see `Execution` in the trait. They can ignore
`persist`. `MemoryStore` still walks `dirty_slots` (no extra public clone).
`keel-rt-sqlite` uses `dirty_nodes()` after the first full snapshot write.
Do not add more live-aggregate methods to the port.
