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
  Waiting is not done; use `wait_stable` then `resume`.
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
  keyed by `NodeId`. Now it still does.
- When CI runs **coverage**, it used to pass at 93.2% with allowlisted kernel
  lines. Now it fails unless kernel `src/` (not `src/testing/`) is 100%
  executable lines. Wait / cancel / resume / fail / retry / inspect are
  unchanged.
