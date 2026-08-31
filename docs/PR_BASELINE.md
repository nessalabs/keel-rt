# Filled PR body (this `main` is the baseline)

Architecture (before / after): **after = before**. No module-graph or
public run-loop type change. Diagrams: [`docs/ARCHITECTURE.md`](ARCHITECTURE.md).

DX that already landed on this branch (keep; do not revert):

- When a caller runs **start** with an executor id that is not registered, it
  used to spawn the apply loop and fail that node at runtime (or hang). Now
  `Runtime::start` / `run` returns `StartError::UnregisteredExecutors` and
  nothing runs.
- When a caller runs **start** and only needs the terminal state, it used to
  hold `ExecutionHandle` and risk Drop-cancel. Now they can call
  `Runtime::run` (start + wait; no handle).
- When a caller registers a function executor, it used to name
  `FunctionExecutor`. Now `RuntimeBuilder::register_fn` is the usual path;
  `register(impl Executor)` remains.
- When a caller sleeps in an executor, it used to reach into runtime time.
  Now `ExecutionContext::sleep` and the crate-root `Clock` re-export are the
  public path.
- When a caller builds a definition from `format!(...)`, it used to wrap
  `WorkflowId` by hand. Now `WorkflowDefinition::builder(String)` works.
- When a caller walks a snapshot in definition order, it used to iterate a
  HashMap. Now `ExecutionSnapshot::iter_nodes()` is definition order;
  `.node(id)` is unchanged.
- When a caller checks “did this job finish ok?”, it used to compare
  `== Succeeded` and miss FailSubtree `Completed`. Now
  `ExecutionState::is_successful_finish()` is true for Succeeded and
  Completed.

Unchanged (say so on PRs that only refactor):

- When a caller runs **wait**, it used to return only on terminal
  (`Succeeded` / `Failed` / `Cancelled` / `Completed`). Now it still does.
  Waiting is not done; use `wait_stable` then `resume`. If the scheduler
  task dies without a terminal publish, wait / wait_stable used to return
  the last watch value (`Created` / `Running` / `Waiting`). Now they return
  `Cancelled`, matching `inspect`'s stopped snapshot.
- When a caller runs **cancel** or drops `ExecutionHandle`, it used to cancel
  the execution (not detach). Now it still does. The handle is not `Clone`.
- When a caller runs **resume**, it used to `Complete` or `Reinvoke` a waiting
  node by token. Now it still does. Duplicate equivalent Complete is a no-op;
  conflicting Complete errors; resume after cancel errors.
- When a caller runs **fail** (policy Accepts Failed/TimedOut), it used to
  fail-fast the whole execution (`OnFailure::FailExecution` default). Now it
  still does. `FailSubtree` remains definition-only opt-in.
- When a caller runs **retry**, it used to become `Ready { runnable_at }`,
  never Waiting. Now it still does. Waiting still releases the permit.
- When a caller runs **inspect**, it used to return an `ExecutionSnapshot`
  keyed by `NodeId`. Now it still does. After scheduler death it used to
  depend on whether the inbox send or the oneshot failed first; both paths
  are the stopped snapshot (`workflow_id = "stopped"`, `Cancelled`).
- When `StateStore::persist` panics, it used to tear down the apply loop
  (unlike `persist` returning `Err`, which kept in-memory progress). Now a
  persist panic is caught like `EventSink::emit` panic: in-memory apply
  continues.
- When CI runs **coverage**, it used to pass at 93.2% with allowlisted kernel
  lines. Now it fails unless kernel `src/` (not `src/testing/`) is 100%
  executable lines. Wait / cancel / resume / fail / retry / inspect are
  unchanged.
- When a caller builds a definition with an empty node id, empty workflow id,
  or empty executor id, it used to succeed. Now `build` returns
  `DefinitionError::EmptyNodeId` / `EmptyWorkflowId` / `EmptyExecutorId`.
- When `MemoryStore`'s mutex is poisoned (a panic while a put/get/persist held
  the lock), the next persist used to `expect` and tear down the scheduler.
  Now it recovers with `Mutex::into_inner` and continues.
- When a caller displays `NodeState` or `ExecutionState`, it used to only work
  through snapshot formatting. Now both enums implement `Display` (every
  variant).
- When a caller inspects during a blocking `StateStore::persist`, it used to
  wait behind persist (unbounded inbox, ADR 0001). Now it still does: that is
  backpressure, not deadlock (`inspect_during_blocking_persist_completes_after_persist`).
- When a caller drops `Runtime` after `start`, in-flight executions used to
  keep running. Now they still do (`start_after_runtime_dropped_execution_still_runs`).
- When a leaf node fails under opt-in `FailSubtree` (no children), the
  execution used to become `Completed`. Now it still does — FailSubtree does
  not fail-fast the execution.
- When a caller cancels (or drops a non-consumed handle) after the execution
  is already terminal, it used to apply `Cancel` and rewrite `Succeeded` /
  `Failed` to `Cancelled`. Now cancel is a no-op on a terminal execution.
- When `Policy::decide` panics under opt-in `FailSubtree`, it used to fail-fast
  the execution (`Failed`, not `Completed`). Now it still does — process
  resilience is not graph resilience. Executor panic under `FailSubtree` still
  honours FailSubtree (`Completed`, sibling runs).
- When `EventSink::emit` blocks, inspect used to wait behind apply (unbounded
  inbox, ADR 0001). Now it still does: stall, not a lock-cycle
  (`eventsink_blocking_does_not_deadlock_inspect`).
- When a caller inspects in-flight work, they used to scan `snapshot.nodes`.
  Now `ExecutionSnapshot::running_count()` / `waiting_count()` are the
  in-flight observability (Running holds a permit; Waiting does not).
- When `ExecutionHandle` is dropped, execute tasks used to rely on `Shutdown`
  aborting the JoinSet; a panicking apply loop could detach them. Now
  `SpawnSet::Drop` aborts inflight execute, and the cancel-bound sleeper is
  aborted in `Scheduler::Drop`.
- When an executor calls `ExecutionContext::sleep`, it used to wait the full
  duration even after cancel (until task abort). Now cancel parks the sleep
  until abort so a loop cannot busy-spin on current_thread.
- Allocator is the **consumer binary’s** choice, not `keel-rt`’s.
  No `jemalloc` crate feature. Default `keel-rt` never sets
  `#[global_allocator]`. Jemalloc comparison is a separate unpublished
  binary under `benches/jemalloc_compare/`. `Runtime::start` is unchanged.
- Phase 1 failure catalog: [`docs/FAILURE_CATALOG.md`](FAILURE_CATALOG.md)
  (zero MISSING rows).

Architecture (this change): **after = before**. No module split. New public
items: `ExecutionSnapshot::running_count` / `waiting_count`. No `jemalloc`
crate feature. Consumer binaries may set an allocator themselves.
