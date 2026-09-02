# 0004. Resume is at-least-once snapshot restore, not event replay

- **Date:** 2026-08-31
- **Status:** accepted

## Context

Phase 2: if the process dies, resume from that point. Speed is load snapshot
+ rebuild slot state. The apply inbox stays unbounded (ADR 0001). Failpoints
stay out of the scheduler (ADR 0003). Definition and execution stay separate
values (system-architect: definition vs execution). The store is a port;
sqlite is a sibling adapter.

A node that was `Running` when the process died has already begun `execute`.
Side effects may have happened. The snapshot does not record "how far"
inside user work.

## Decision

Restore from the last successful snapshot CAS. A `Running` node becomes
`Ready { runnable_at: None }` and is re-invoked. Dispatch increments
`attempt` (keyed). Succeeded / Failed / TimedOut / Cancelled nodes never
re-run. Waiting keeps the same `ResumeToken`. Execution-level terminals
stay terminal.

`Runtime::resume` loads snapshot + definition, rebuilds, and spawns. It
does not replay `Event` history. Persist succeeds, then the sink is
told. No persist queue.

`keel-rt-sqlite` implements `StateStore` in a sibling crate. Deleting it
does not edit `scheduler.rs`. The kernel does not learn "sqlite".

## Alternatives considered

- **Park the interrupted node (do not re-invoke).** Loses the only chance
  to finish work that crashed mid-`execute`. Waiting already parks; Running
  is in-flight user work, not a yield.
- **Exactly-once.** Requires a user-side idempotency key the kernel does
  not own. The executor is the information owner for side effects.
- **Event replay (Phase 4).** Rebuild by folding history. Slower, and it
  still cannot make a non-idempotent `execute` exactly-once. Snapshot
  resume is the Phase 2 contract.
- **Fuse definition into the snapshot object the caller mutates.** Fights
  definition-vs-execution. Snapshot holds a hash; the store keeps the
  body beside it.
- **Put sqlite in the kernel / name it in `scheduler.rs`.** Fights
  mechanism-vs-policy and cheap-to-delete. Rejected.

## Consequences

A crash during `execute` may run that node's side effects twice. Callers
that need exactly-once make the executor idempotent (same as retry).
Tests prove terminals are not lost if persist succeeded and emit did not
(persist-then-panic). ADR 0001 still applies: do not add a persist queue
to hide sqlite latency.

`keel-rt-sqlite` is that second store: WAL, one transaction per persist,
dirty node rows after the first write (ADR 0002). It does not serialize the
whole graph on every event.

**Durability.** Default `SqliteStore::open` is `synchronous=FULL` (last COMMIT
survives process kill and machine power loss). `SqliteStore::open_fast` is
`NORMAL`: process kill after COMMIT still recovers (crash-after-CAS tests);
power loss may drop the last WAL frames. FULL 256-wide resume still meets
the ≥50% cut vs the pre-opt baseline, so the default did not stay on NORMAL
for the headline number. Crate README: `crates/keel-rt-sqlite/README.md`.

**Store lease + epoch.** `AlreadyActive` is per `Runtime`. Two Runtimes
on one sqlite file (or one shared `MemoryStore`) take store-level
ownership of one execution: `claim` / `heartbeat` / `release` on
`StateStore`. A live lease for another owner is `ClaimedElsewhere`
(`two_runtimes_same_file_are_not_fenced`). Persist and complete carry
the epoch; a stale epoch is rejected. Default TTL is 30s (`Clock` `now`,
not a wall sleep). This is not distributed workers / placement. CAS
still rejects a stale `put`.
