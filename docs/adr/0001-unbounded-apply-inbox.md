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

## Alternatives considered

- **Bounded mpsc.** Loses to deadlock on `resume`/`inspect` when apply is
  busy. Not acceptable for a Drop-cancels handle.
- **`try_send` + drop.** Loses events; apply would miss completions.
- **Per-source queues.** Speculative. No second consumer asked for it.

## Consequences

A stuck apply loop can grow memory with completions. That is the same class
of failure as a stuck current-thread scheduler. Persist errors are logged and
skipped; they must not stall apply. Do not add a bound without a deadlock test.
