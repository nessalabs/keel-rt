## Architecture (before)

<!--
If this PR does not change the module graph or lib.rs re-exports vs `main`,
write: "Unchanged vs main. Baseline: docs/ARCHITECTURE.md."
If it does change them, paste mermaid (a) repo/module map and (b) public
run-loop class diagram generated from `main`'s src/ + lib.rs.
Greenfield / first baseline: say these are the baseline and omit a separate
before pair.
-->

Unchanged vs `main` / first baseline (delete whichever is wrong).

```mermaid
flowchart TB
  %% (a) repo/module map from main — replace if structure changed
```

```mermaid
classDiagram
  %% (b) public run loop from main — replace if public API changed
```

## Architecture (after)

<!--
Always: two mermaid diagrams of THIS BRANCH.
(a) flowchart of real src/ modules; arrow testing → runtime → domain.
(b) classDiagram: RuntimeBuilder → Runtime → ExecutionHandle → Execution,
    WorkflowDefinition, Executor, Policy, StateStore, EventSink, NodeOutcome,
    ExecutionState.
Read src/lib.rs and src/*/mod.rs. Do not invent types.
Copy from docs/ARCHITECTURE.md only if that file still matches this branch.
-->

```mermaid
flowchart TB
```

```mermaid
classDiagram
```

## User behavior (when X, used to Y, now Z)

<!--
Required even when the refactor has no observable change.
Cover: start, wait, cancel, resume, fail, retry, inspect.
-->

- When a caller runs **start**, it used to Y. Now it Z.
- When a caller runs **wait**, it used to Y. Now it Z.
- When a caller runs **cancel** (or drops `ExecutionHandle`), it used to Y. Now it Z.
- When a caller runs **resume**, it used to Y. Now it Z.
- When a caller runs **fail** (executor Failed / TimedOut, policy Accept), it used to Y. Now it Z.
- When a caller runs **retry**, it used to Y. Now it Z.
- When a caller runs **inspect**, it used to Y. Now it Z.
