# 0005. Durable startup acceptance is an explicit store capability

- **Date:** 2026-09-07
- **Status:** accepted

## Context

The ordinary startup path claims a lease, applies Start, dispatches, and then
persists. Its returned handle does not acknowledge durable acceptance. Continuing
in memory despite persistence errors is a tested contract and remains unchanged.

An interrupted SQLite claim can leave a revision-zero reservation with no
definition or nodes. Previously, reads parsed its empty definition hash as a
snapshot error, and equal-revision persistence could acknowledge initialization
without replacing the reservation.

## Decision

SQLite recognizes only its complete reservation shape as absence. Snapshot and
definition reads return None without changing ownership. Partial/corrupt records
remain errors. Reads hold a consistent transaction across metadata and child
queries, including while another connection initializes the reservation.
`persist` initializes a reservation with the full snapshot and
definition even at revision zero; subsequent equal-revision writes keep their
existing idempotency. Snapshot-only `put` requires prior initialization.

Add `Runtime::start_durable`, an asynchronous opt-in API. It validates executor
registration before calling the new `StateStore::initialize` capability. The
default implementation returns `InitializeError::Unsupported`; default `persist`
cannot guarantee definition storage or process-crash recovery. NoopStore and
MemoryStore do not implement this capability.

SQLite's initializer uses one immediate transaction to reserve a fresh execution
ID, store the definition and full Created revision-zero snapshot, and set its
lease. Existing IDs, including reservations, are rejected. Temporary and in-memory
databases are unsupported. Transaction Drop rolls back failed writes, failed
COMMIT, and unwinding. FULL and NORMAL retain their documented durability modes.

The runtime reserves the ID locally before the initialization await so a
concurrent resume cannot launch a snapshot that just became visible. An
ActiveGuard removes that reservation on error, panic, or cancellation; no task
is spawned until initialization succeeds. It then uses the common execution
driver with a Start event, which refreshes ownership before dispatch. Initializer
errors and panics return `DurableStartError` without launching work. There is no
additional await between initialization success and returning the handle.

## Crash and cancellation boundaries

| Boundary | Recoverable state |
| --- | --- |
| Legacy claim committed, initial snapshot absent | Recognized reservation; reads return None. |
| Durable initialization interrupted before COMMIT | No new execution, definition, or node rows. |
| Initial COMMIT succeeded, reply lost or startup future cancelled | Created snapshot remains; lease can expire normally. |
| Durable handle returned, process dies before dispatch | The acknowledged ID has its snapshot and definition. |
| Another owner takes over before delayed startup dispatch | Start's claim refuses to launch under the other owner's live lease. |

Cancelling a caller does not undo a completed commit. Store implementations must
not continue initialization in detached background work after cancellation.
An acknowledged snapshot does not allow resume to bypass a live lease. Handle
Drop still cancels after a handle is returned.

## Ownership and limits

Keel guarantees correct interpretation of its persisted records and recoverable
initial state for durable acknowledgements. This does not make subsequent
execution exactly-once or change how later persistence failures are handled.

Applications such as Nessa retain submission inputs and own idempotency of
external effects. Retrying `start_durable` allocates a new execution ID, so it is
not an idempotent submission API. Application operation IDs must survive both
attempt changes and replacement execution IDs. A model response can only be
reused after the application durably records it.

## Verification

`tests/durable_start.rs` covers unsupported stores, fail-fast validation, errors,
panics, pre/post-commit cancellation, acknowledgement ordering, and lease takeover.
The coverage gate includes this suite.

The SQLite startup integration suite covers reservation classification,
revision-zero initialization, stale fencing, corruption, ownership preservation,
competing initializers, and handle cancellation. Library tests use subprocesses
and test-only SQL barriers to kill actual first-snapshot transactions and restart
after committed initialization and acknowledgement. A deferred SQL constraint
also verifies rollback when COMMIT itself fails in claim, persist, and initialize.
Production scheduler failpoints
and new dependencies in the kernel are not required.
