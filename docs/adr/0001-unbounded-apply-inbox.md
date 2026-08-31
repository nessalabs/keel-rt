# 0001. The apply inbox stays unbounded

- **Date:** 2026-08-31
- **Status:** accepted

## Context

Every execute completion, resume, inspect, cancel, timer, and shutdown is an
`Event` on one mpsc into the apply loop. A bounded channel would make
`ExecutionHandle::resume` / `inspect` wait for apply capacity. If apply is
itself waiting on a store persist or a full outbound path, that is a deadlock.

In-flight execute tasks are already bounded by `RuntimeBuilder::concurrency`.
Handle messages are caller-driven.

## Decision

Keep `tokio::sync::mpsc::unbounded_channel` for the apply inbox. Document the
implicit bound (concurrency + handle ops) in `docs/ARCHITECTURE.md` and on
the channel type.

## Ownership (RAII)

| Holder | What it owns | Drop |
|---|---|---|
| `ExecutionHandle` | sender clone | If not consumed by `wait`: send `Cancel`. Always send `Shutdown`. |
| Scheduler (`ChannelPark`) | **receiver** | End of `run` / panic: `SpawnSet` Drop aborts execute tasks; cancel-bound sleep is aborted. `recv` on a closed channel is `Shutdown`. |
| Scheduler | sender clone | Cancel-bound timer. Aborted in `Scheduler::Drop`. |
| Execute task | sender clone | Sends `NodeFinished`, then the clone drops. |
| `Runtime` | none of the inbox | Drop does **not** cancel in-flight executions. The **handle** owns cancel/JoinSet. |

Last sender drop closes the channel. A live handle keeps a sender, so the
apply loop stays up until the handle is dropped (or `wait` consumes it and
then Drop still sends `Shutdown`).

## Alternatives considered

- **Bounded mpsc.** Loses to deadlock on `resume`/`inspect` when apply is
  busy. Not acceptable for a Drop-cancels handle.
- **`try_send` + drop.** Loses events; apply would miss completions.
- **Per-source queues.** Speculative. No second consumer asked for it.

## Consequences

A stuck apply loop can grow memory with completions. That is the same class
of failure as a stuck current-thread scheduler. Persist errors are logged and
skipped; they must not stall apply. Do not add a bound without a deadlock test.

`inspect` / `resume` during a blocking `persist` wait for apply to drain —
backpressure, not deadlock. Pinned by
`inspect_during_blocking_persist_completes_after_persist` in `tests/catalog.rs`.
