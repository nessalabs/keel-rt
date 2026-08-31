# Keel (`keel-rt`)

**Keel** is a small DAG workflow execution kernel. The crate is `keel-rt`
(Tokio-style `-rt` = the runtime). In Rust: `use keel_rt::...`.

Phase 1: AND-join, fail-fast, opaque byte dataflow, first-class
Waiting/resume, retry-as-Ready, and a Tokio-inspired failure-injection
test harness.

The runtime is a **bundle** (scheduler + optional store/sink + handle). The
scheduler does not know resource types. Drivers only wake. This is a
current-thread analog — FIFO ready queue, no work-stealing.

Execution is the aggregate. Node is an entity inside it. Ids, outcomes, and
snapshots are value objects. `Policy` and `Executor` are ports. `StateStore` is
the repository port. Do not look for an Agent, HTTP, or SQL type here; they do
not belong in the kernel.

## Build and test

```bash
cargo test -- --test-threads=1
cargo test --features test-util
cargo clippy --lib -- -D warnings
```

### Regression packs

`tests/adversarial/` is the attack / regression pack (concurrency, AND-join,
stale completions, timers, panic isolation, definition edges, inspect, diamond
repeat). `tests/stress.rs` is the size pack (wide 256, deep 128, cancel-under-load,
plus kernel microbenches). `tests/stress_100k.rs` is the 100k-node scale pack
(debug finishes; `wide_100k` is ~57s). `tests/stress_uneven.rs` is stragglers,
mixed fan-in, hourglass, fat payloads, FIFO vs timer, skewed retry.
`tests/workloads.rs` is production-shaped graphs (agent farm, crawl-as-N-executions,
map-reduce, HITL drain, flaky I/O, burst/idle/burst, many Runtime jobs).
`tests/resilience.rs` is the I/O-fault harness matrix (`NetFault` + FakeClock): Timeout
fail-fast, retry permit release, Delay AND-join, Reset+Retry, 100 sequential diamonds
isolating one Timeout, mixed Delay/Timeout/Reset under concurrency 8.
`tests/scenarios/failure_scope.rs` locks `OnFailure::FailSubtree` + `Join::AllDone`
without changing the default fail-fast tests.

```bash
cargo test --test adversarial -- --test-threads=1
cargo test --test stress -- --nocapture
cargo test --test stress_100k -- --nocapture --test-threads=1
cargo test --release --test stress_100k -- --nocapture --test-threads=1
cargo test --test stress_uneven -- --nocapture --test-threads=1
cargo test --test workloads -- --nocapture --test-threads=1
cargo test --test resilience -- --nocapture --test-threads=1
cargo test --test scenarios -- --test-threads=1
```

## Examples

Library-user surface (`Runtime`, `WorkflowDefinition`, `FunctionExecutor`, `FnSink`). No test harness.

```bash
cargo run --example research_diamond
cargo run --example fail_fast
cargo run --example waiting
```

Integration tests live in `tests/` and go through `WorkflowTest` (not ad-hoc
mocks). Domain apply transitions are table-tested in `src/domain/state.rs`.

## Architecture

```
definition ──► Runtime::start ──► ExecutionHandle
                    │
                    ▼
              scheduler apply loop     (sync; never awaits execute())
                    │
          ┌─────────┼──────────┐
          ▼         ▼          ▼
       StateStore  EventSink  spawn(execute)  → Event::NodeFinished
```

- **Join default is AND (`AllSucceeded`).** A node becomes Ready only when every
  predecessor is Succeeded. `Join::AllDone` is Ready when every predecessor is
  terminal; `inputs_for` still contains succeeded preds only.
- **Fail-fast (`OnFailure::FailExecution`, default).** After policy Accepts
  Failed/TimedOut, that node is Failed, all non-terminal nodes are Cancelled,
  execution is Failed. `OnFailure::FailSubtree` cancels only AllSucceeded
  descendants of the failed node; siblings keep running; execution becomes
  `Completed` (not Failed) when every node is terminal.
- **Dataflow.** Opaque `bytes::Bytes` keyed by `NodeId`. Dependents receive a
  `HashMap` of succeeded predecessors' outputs. The kernel does not interpret.
- **Waiting.** Executor may return `Waiting { token }`. The permit is released.
  Resume via `ExecutionHandle`: `Complete(outcome)` or `Reinvoke`. Tokens are
  bound to `(execution, node, attempt)`. Duplicate equivalent Complete is Ok
  noop; conflicting Complete is an error; resume after cancel is an error.
- **Retry delay is Ready { runnable_at }, not Waiting.** Waiting is only from
  Running after an executor yield.
- **Cancel.** `CancellationToken` to running executors. Pending/Ready/Waiting
  become Cancelled. Dropping `ExecutionHandle` **cancels** (JoinSet semantics,
  not detach). Hung executors that ignore cancel are aborted after
  `DEFAULT_CANCEL_BOUND` (50ms, configurable).
- **Persistence.** One `StateStore::put(snapshot)` per injected event, after
  dispatch drains launches — not after every internal `ApplyCmd`. `NoopStore`
  skips `snapshot()` entirely. Errors do **not** roll back in-memory apply.
  Phase 1 does not recover via `get()` on start. Snapshots use `NodeId` (never
  petgraph indices), serde, `schema_version` + `revision`. No-op apply does
  not bump `revision` or persist.

Node states: `Pending`, `Ready` (optional `runnable_at`), `Running`, `Waiting`,
`Succeeded`, `Failed`, `Cancelled`, `TimedOut`.

Execution states: `Created`, `Running`, `Waiting`, `Succeeded`, `Failed`,
`Cancelled`, `Completed`. `Completed` means mixed terminals after FailSubtree
— do not call that Succeeded. `wait()` / `wait_stable()` treat it as terminal.

## Write a test with ScriptedExecutor

Enable the `test-util` feature (already on for this crate's own tests):

```rust
use bytes::Bytes;
use keel_rt::testing::{ScriptedExecutor, WorkflowTest};
use keel_rt::ExecutionState;

#[tokio::test(flavor = "current_thread")]
async fn linear() {
    let run = WorkflowTest::new()
        .node("a", ScriptedExecutor::new("a").succeed(Bytes::from_static(b"A")))
        .node("b", ScriptedExecutor::new("b").succeed(Bytes::from_static(b"B")))
        .edge("a", "b")
        .concurrency(1)
        .run()
        .await;
    assert_eq!(run.execution_state().await, ExecutionState::Succeeded);
}
```

Scripted actions: `Succeed`, `Fail`, `Wait`, `Panic`, `Hang { ignore_cancel }`,
`Delay` then Succeed, `TimedOut`. Attempts are recorded. Hang is released with
`run.release_hang("a")` or `executor.release()`.

I/O faults (`NetFault`) compose onto `ScriptedExecutor` and sleep on
`FakeClock` — no sockets in the kernel. `Delay` is cancelled if
`CancellationToken` fires first.

```rust
use std::time::Duration;
use keel_rt::testing::{NetFault, ScriptedExecutor};

ScriptedExecutor::new("n").fault(NetFault::Delay(Duration::from_millis(50)));
ScriptedExecutor::new("n").fault(NetFault::Timeout);           // TimedOut
ScriptedExecutor::new("n").fault(NetFault::Reset);             // Failed("reset")
ScriptedExecutor::new("n").fault(NetFault::TimeoutThenSucceed);
ScriptedExecutor::new("n").timeout_after(Duration::from_millis(50)); // Running until clock
```

`FaultySink::panic_on_nth(n)` panics on emit (scheduler `catch_unwind`s).
`FailingStore` still fails `put` without rolling back in-memory apply.

`.run()` waits until the execution is terminal **or** `Waiting` (so you can
resume). `.start()` returns immediately for mid-run inspect.

## Inject faults

```rust
use keel_rt::testing::{enable, FailingStore, ScriptedExecutor};

// Named failpoints (remaining-hit counter). Checked by test doubles.
enable("store.put", 1);
enable("executor.panic", 1);

// Store that fails on the Nth put. In-memory apply still progresses.
let store = FailingStore::fail_on_nth_put(2);

// Executor that panics or hangs.
ScriptedExecutor::new("x").panic();
ScriptedExecutor::new("h").hang(true); // ignore CancellationToken

// I/O faults (FakeClock; CancellationToken aborts Delay).
use keel_rt::testing::NetFault;
use std::time::Duration;
ScriptedExecutor::new("n").fault(NetFault::Timeout);
ScriptedExecutor::new("n").timeout_after(Duration::from_millis(50));
```

`FaultySink::panic_on_nth(1)` panics on emit; the scheduler stays alive.

`FakeClock::advance(Duration)` fires retry delays. Waiting tests do not need a
timer. Park is channel + clock; tests do not need epoll.

## Swap StateStore

```rust
use keel_rt::{MemoryStore, NoopStore, Runtime};

let rt = Runtime::builder()
    .store(MemoryStore::new())   // default
    // .store(NoopStore)
    .concurrency(4)
    .build();
```

`StateStore` is `put` + `get` on `ExecutionSnapshot`. Implement the trait for a
durable adapter; the kernel stays database-free. Persistence errors are logged
and ignored — the in-memory aggregate is the source of truth for the live run.

## Public API

`Runtime`, `RuntimeBuilder`, `ExecutionHandle`, `WorkflowDefinition`,
`OnFailure`, `Join`, `Executor`, `Policy`, `StateStore`, `ExecutionSnapshot`,
`NodeOutcome`, `Resume`. Scheduler, park, and inject are crate-private.
