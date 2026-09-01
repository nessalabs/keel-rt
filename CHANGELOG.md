# Changelog

## Phase 5 snapshot deadlines (`phase-5/timers`)

A node is not runnable until Instant **T**. T is `NodeState::Ready { runnable_at: Some(T) }`
on the snapshot so crash-resume sees it. Policy (how long, retry/backoff counts)
stays on `RetryPolicy` / `timeout_after`. Waiting stays HITL. Kernel has no cron,
no wall timezone, no sqlite timer table. `Event::NodeTimedOut` already exists;
do not emit `NodeReady`. Drop handle still cancels park sleepers (RAII).
Fail-fast / AND-join defaults unchanged. `Recover::RetryFailed` is not a kernel
command. FakeClock drives timer tests. sqlite persists whatever the snapshot
already has (`synchronous=FULL` default). Park prefers the apply inbox when T
is already due (`try_recv`) so Cancel/Shutdown at the same instant as a due
deadline does not dispatch. `Timestamp::saturating_add` saturates `Duration`
millis that do not fit in `u64` (`1<<61` seconds used to wrap to T==now).
Node JSON omits null optionals; a retry park drops the stale attempt token
(Waiting still carries the token). MemoryStore no-timer medians stay within
10% of `main` (`da1e6fa`).

## Phase 3 events (`phase-3/events`)

Public surface is [`Event`] + [`EventSink`] only (no `EventLog`). Variants:
ExecutionStarted/Succeeded/Failed/Completed/Cancelled and
NodeStarted/Succeeded/Failed/TimedOut/Cancelled/Waiting. No `NodeReady`.
Each event carries execution id, workflow id, node id when it is a node
event, attempt, Clock time, and `schema_version`. persist_then_emit is
store Ok then sink; sink `Err` / panic does not un-persist or fail the
run. Resume is still the StateStore snapshot. sqlite may write event rows
in the same persist txn; those rows are never used to resume. At-least-once
re-invoke may emit the same node event twice. Absences are tests: no public
`EventLog`, frozen Event variants (no `NodeReady`), persist-before-announce,
`scripts/pr_body_gate.py` (mermaid + `When a caller` + base `main` unless
`[stack]`). persist `Err` no longer drops pending events: a later persist
`Ok` (including Shutdown retry) announces the transitions that became durable.
sqlite: COMMIT is persist Ok; equal-revision does not insert event rows;
checkpoint `Err` after COMMIT does not fail persist. `EventSink::try_emit` is
the required method; `emit` swallows `Err`. `Event::node_id` / `attempt` are
accessors. Resume seeds `last_persisted` from the snapshot so a no-op Waiting
restore does not open a new sqlite txn.

## Shutdown retries persist until the store recovers (`chaos/load-p2`)

A single extra persist on Shutdown left sqlite **Running** when the terminal
write failed twice (command + first Shutdown attempt). `wait()` had already
returned Succeeded. Resume re-invoked work the caller treated as done. Fix:
Shutdown retries persist up to eight times. Permanently failing persist is
still Phase 1 in-memory wins. Fail-fast / AND-join unchanged.

Seeded sqlite crash-inject (`crates/keel-rt-sqlite/tests/crash_inject.rs`)
runs 256 process-kill/resume seeds against small DAGs (FakeClock, no wall
sleep) plus constructed killers (clock jump, stale FinishNode, HITL
duplicate Complete, persisted cancel, AlreadyActive, uncommitted retry
delay, WAL after Succeeded COMMIT, definition mismatch, unicode ids, 64KiB
join inputs, FIFO-64, `open` vs `open_fast`). CI job `chaos-sqlite`.

## Shutdown flushes a transient last persist (`chaos/load-p2`)

`wait()` / Drop-cancel used to leave sqlite at **Running** when the terminal
or Cancel persist returned `Err` once (`last_persisted` advanced on failure;
Shutdown did not persist). Resume re-invoked work the caller already saw as
Succeeded or Cancelled. Fix: persist `Ok` only advances `last_persisted`;
Shutdown retries `persist_then_emit`. Fail-fast / AND-join unchanged.

## Standing sqlite chaos pack (`chaos/load-p2`)

`crates/keel-rt-sqlite/tests/chaos.rs` is the standing breaker: thousands of
short jobs, 256/2k-wide AND-join crash/resume, diamond farm + retry + HITL,
burst/idle/burst, fat vs 1-byte, start-crash-resume storms, two Runtimes on
one file, 1pm/4pm `FakeClock` AND-join, drop-handle mid-persist, policy/sink/
executor/persist panic. Nothing broke on the first pass. Numbers:
[`docs/CHAOS_LOG.md`](docs/CHAOS_LOG.md). CI job `chaos-sqlite` (not coverage).
256-wide resume under 32 concurrent starts is **sqlite-bound** (~474 ms idle
and under load) — the 50-diamond −32% loop is start/drop bound.

## Sqlite persist ≥50% (phase-2/resume)

`keel-rt-sqlite` no longer dumps the whole graph JSON on every persist.
WAL, one transaction per persist call, dirty node rows after the first write.
Default `SqliteStore::open` is `synchronous=FULL`. 256-wide resume used to
take **1.008 s** debug median; now **421 ms** (−58%) without donating
power-loss durability. `open_fast` (`NORMAL`) is **259 ms** (−74%) if the
caller accepts that power loss may drop the last WAL frames. Process kill
after COMMIT recovers on both. Crash-after-CAS, torn WAL, and `SQLITE_BUSY`
stay typed. MemoryStore benches unchanged (≤10% vs RAII). Kernel coverage
100%.

## Phase 2 snapshot resume

At-least-once resume from the last CAS snapshot. `Runtime::resume(&id)`.
Running-at-crash is re-invoked (attempt + 1). File adapter is
`keel-rt-sqlite` (sibling crate). ADR 0004. Kernel still has no Agent/HTTP
and does not pick an allocator.

## Phase 1 freeze

Local DAG kernel (`keel-rt`) is frozen on `main` at
`5ca1b971c0b21e9259881709b491c0bd7d58bebd`.

This changelog commit exists so Origin can open a review PR (`main` cannot
PR onto itself). **Kernel code stays on `main`**; this branch does not revert
or rewrite it.

- Fail-fast default: `OnFailure::FailExecution`. AND-join: `Join::AllSucceeded`.
- `FailSubtree` / `Join::AllDone` stay definition-only opt-in.
- No Agent, HTTP, or merge of `examples/studio`.
- Library does not set `#[global_allocator]` and has no `jemalloc` feature.
  Benches on `current_thread` were noise; jemalloc is **not recommended**.
- Review description (template filled): [`docs/PR_BASELINE.md`](docs/PR_BASELINE.md).
- Architecture baseline: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
