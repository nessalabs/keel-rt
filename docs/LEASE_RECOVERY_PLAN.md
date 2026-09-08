# Lease recovery implementation and verification

Status: implemented and verified.

The original review was against commit
`2d2a68308176c80d9d4185d485e6be9a7ea4f6dc`. The implementation decision is recorded
in [ADR 0005](adr/0005-durable-start.md).

## Ownership

| Owner | Guarantee |
| --- | --- |
| Keel | Persisted state is interpreted correctly. Recovery recognizes Keel's lease placeholders without treating them as corrupted snapshots. |
| Nessa | Replay and retry cannot duplicate memory side effects; stable business operation IDs and transactional/idempotent effects remain application responsibilities. |
| Keel durable start | A successful `start_durable` acknowledgement has a recoverable initial snapshot and workflow definition under the store's durability mode. |

## Implemented

- Snapshot and definition reads classify the complete lease reservation shape
  as absence. They preserve owner, epoch, and expiry, and keep corruption errors
  visible. Each read uses a transaction so concurrent initialization cannot mix
  reservation metadata with newly committed node rows.
- `Runtime::resume` returns `UnknownExecution` for a reservation. It does not
  automatically recreate work or bypass a live lease.
- `persist` fully initializes a reservation even at revision zero. It retains
  fencing and subsequent revision/event idempotency. `put` requires prior
  initialization with the definition.
- Failed COMMIT rolls back legacy claim and persistence transactions too, so
  their connections cannot expose uncommitted state after returning an error.
- `StateStore::initialize` is an explicit capability with an unsupported default.
  SQLite reserves a fresh ID and commits the Created snapshot, all nodes,
  definition, and lease atomically. Existing IDs and temporary/in-memory stores
  cannot acknowledge a new durable execution.
- `Runtime::start_durable` validates executors first, holds an ActiveGuard while
  initializing, and starts no scheduler until initialization succeeds. The guard
  prevents concurrent local recovery from launching the same initial state and
  cleans up on error, panic, or cancelled futures. Start refreshes ownership at
  dispatch. Existing `start`, `run`, and handle cancellation behavior remain.

## Regression coverage

| Boundary | Checked behavior |
| --- | --- |
| Claim then process exit before first save | Reopened snapshot/definition absent; live lease still excludes competitors. |
| Expiry, takeover, and release | Reads remain absent; stale writer cannot initialize after takeover. |
| Revision-zero initialization | Full snapshot/definition round-trip and resume succeed; repeat persist does not duplicate events. |
| Corrupt or partial reservations | Errors are retained and writes do not overwrite them. |
| Real snapshot missing a definition | Recovery still fails closed. |
| Concurrent initialization during a read | Metadata and child rows come from one committed view. |
| Failed first write or COMMIT | No partial snapshot/definition; retry can initialize normally. |
| Process killed inside first snapshot transaction | Legacy reservation or no execution survives; partial rows roll back. |
| Commit then process exit before dispatch, with/without acknowledgement | Created snapshot and definition resume successfully in FULL and NORMAL modes. |
| Two initializers for the same ID | One commits; the other cannot overwrite its state or owner. |
| Unsupported store, missing executor, initialization error/panic | No executor launches. |
| Cancelled initialization before/after commit | No leaked local registration; committed state remains resumable. |
| Concurrent local resume during startup | AlreadyActive until the initializer resolves; no duplicate launch. |
| Ownership stolen before delayed startup dispatch | Original startup does not launch under the other owner's live lease. |
| Returned handle dropped while Waiting | Execution is cancelled as before. |

SQL barriers, process-kill controls, and the SQLite authorizer used to force a
read/write interleaving live only in tests. No production scheduler failpoints
were added. The incremental write path compares revisions without loading the
full node order.

## Verification

- PASS: `cargo test --locked --workspace -- --test-threads=1` — 868 tests.
- PASS: `just coverage` — 6,086/6,086 kernel lines, 100%; empty allowlist and
  patch coverage gate passed.
- PASS: `just stress-resume` — 11 tests.
- PASS: `just stress-100k` — all five scale tests.
- PASS: `just chaos-sqlite` — existing chaos/crash-inject packs plus startup
  library and integration regressions.
- PASS: `cargo clippy --locked --workspace --lib -- -D warnings`.
- PASS: rustfmt checks on changed Rust files and `git diff --check`.

The coverage gate now runs `tests/durable_start.rs`. `just chaos-sqlite` includes
SQLite library startup crash tests and the startup integration suite in addition
to the existing chaos and crash-inject packs. Existing behavior locks are intact.

## Application boundary

Nessa's code and memory database are outside this repository and were not
modified or validated here. Its integration suite must cover replay after a
memory transaction commits, crash before Keel saves node success, replacement
start with a new execution ID, and concurrent retries of one business operation.
A model response can only be reused after Nessa durably records it.

A lost startup acknowledgement does not make a second submission idempotent:
`start_durable` allocates a new ID. Durable acceptance guarantees initial recovery;
later execution and persistence-failure behavior remain unchanged.
