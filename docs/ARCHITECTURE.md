# Keel-rt architecture

Current-thread DAG kernel. One execution = one apply loop + N execute tasks.
No work-stealing, no product resource types, no YAML/HTTP/CLI.

## Modules

```
src/domain/          rules. No tokio, no runtime, no std::net.
  definition.rs      validated DAG; OnFailure + Join live here only
  ids.rs             NodeId / WorkflowId / ExecutionId / ExecutorId / ResumeToken
  outcome.rs         NodeOutcome, Resume, NodeError
  policy.rs          Policy port + Accept / Retry / NeverWait
  snapshot.rs        persistable ExecutionSnapshot (HashMap + definition order)
  state.rs           Execution aggregate + NodeState / ExecutionState
  apply.rs           apply / join / fail-fast / FailSubtree (impl Execution)
  events.rs          DomainEvent as data
  time.rs            Timestamp value object

src/runtime/         bundle. May import domain. Never imported by domain.
  runtime.rs         Runtime / RuntimeBuilder / StartError
  scheduler.rs       event loop: apply → dispatch → persist. No policy rules.
  spawn.rs           one tokio::spawn per execute; completions are Events
  park.rs            wait for Event or retry deadline (Clock)
  inject.rs          Event + unbounded mpsc (see docs/adr/0001)
  handle.rs          ExecutionHandle; Drop cancels; not Clone
  executor.rs        Executor port, FunctionExecutor, ExecutionContext
  store.rs           StateStore port; MemoryStore / NoopStore
  sink.rs            EventSink port; FnSink / NoopSink
  time.rs            Clock port; SystemClock

src/testing/         feature test-util. Harness + doubles. Not the kernel.
```

**Dependency arrow:** `testing → runtime → domain`. Never reverse. No cycles
between crate modules.

**Public surface** is the crate root (`lib.rs` re-exports). `domain` and
`runtime` are `pub(crate)`. `Execution::apply` is the documented domain API
(used by apply-only benches and stale/timer packs). Inspect live runs through
`ExecutionHandle` / `ExecutionSnapshot`.

## Absences (structure tests hold these)

- No Agent, HTTP, SQL, crawl, or HITL **types** in `src/`.
- No `utils` / `common` / `helpers` / `shared`.
- No Runtime-wide FailSubtree or AllDone switch.
- No test-only constructor that builds an illegal `WorkflowDefinition`.
- Waiting is a node state. Retry delay is `Ready { runnable_at }`.

## Where to change

| If you are changing…                         | Touch                                      | Do not touch              |
|----------------------------------------------|--------------------------------------------|---------------------------|
| AND-join / AllDone readiness                 | `definition` + `apply` remain-pred         | scheduler                 |
| Fail-fast / FailSubtree                      | `definition` (opt-in) + `apply`            | `RuntimeBuilder`          |
| Retry / reject Waiting                       | a `Policy` impl                            | readiness / park          |
| User work / sleep                            | `Executor` / `ExecutionContext`            | `apply`                   |
| Persist / dirty slots                        | `StateStore` / `MemoryStore`               | scheduler policy          |
| Cancel, wait, resume, inspect                | `handle` + `inject::Event`                 | domain types              |
| Ready-queue / permits / spawn                | `scheduler` + `spawn`                      | `Policy`                  |
| Test graph construction                      | `WorkflowTest`                             | private scheduler fields  |
| Snapshot walk order                          | `ExecutionSnapshot::iter_nodes`            | HashMap `.node(id)`       |

## Apply vs run

- **`Execution::apply`** — synchronous, no I/O. Policy is consulted here.
- **`Runtime::start` / `run`** — fail-fast if an executor id is missing, then
  spawn the apply loop. Drop `ExecutionHandle` cancels (`#[must_use]`).
- Default `OnFailure` = `FailExecution`. Default `Join` = `AllSucceeded`.

## Bounds

- **Concurrency:** `RuntimeBuilder::concurrency` (permits). In-flight execute
  tasks ≤ that number.
- **Apply inbox:** unbounded mpsc (ADR 0001). Producers are execute tasks +
  handle ops; they must not block on apply.
- **Retry:** `RetryPolicy::max_attempts` is the only retry bound. Delay is a
  deadline on `Ready`, not a wait state.
- **Cancel hang:** `cancel_bound` is **wall** time (`tokio::time::sleep`), not
  `Clock`. FakeClock does not stretch it.

## Performance gate

No Criterion crate. The gate is the in-tree harness:

```bash
cargo test --test stress -- --nocapture --test-threads=1
cargo test --test stress_100k -- --nocapture --test-threads=1
cargo test --test stress_uneven -- --nocapture --test-threads=1
```

Numbers live in `benches/BASELINE.md`. A hot-path change that regresses those
medians is a bug: fix or revert and ADR.
