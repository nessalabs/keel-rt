# Agent rules (keel-rt)

This crate is a **library kernel**. Do not add HTTP, YAML, Agent types, CLI, or
merge `examples/studio`. Keep DX APIs (`register_fn`, `Clock`, `Runtime::run`,
start fail-fast, `iter_nodes`, `is_successful_finish`). Coverage floor and
stress/adversarial tests are behavior locks.

## Blocking: every PR description

A PR is **incomplete** without all three sections. This is not a nit. Do not
mark the PR ready, and do not treat the change as reviewable, until they are
filled from **this branch's real `src/`**.

Copy the skeleton in [`.github/pull_request_template.md`](.github/pull_request_template.md).
Baseline mermaid (greenfield / first diagrams): [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### 1. Two mermaid diagrams of **this branch**

**(a) Repo / module map** — flowchart. Nodes are real directories/files under
`src/` (`domain/*`, `runtime/*`, `testing/*`, `lib.rs`). Show the dependency
arrow: `testing → runtime → domain`. Never reverse. Do not invent `utils`,
`common`, adapters, or product types.

**(b) Public run-loop class diagram** — UML `classDiagram`. Must include, and
only real types from `src/lib.rs` re-exports plus the ports they use:

`RuntimeBuilder` → `Runtime` → `ExecutionHandle` → `Execution`,
`WorkflowDefinition`, ports `Executor` / `Policy` / `StateStore` / `EventSink`
(and `Clock` if the builder still takes it), `NodeOutcome`, `ExecutionState`.

No fake types. Crate-private scheduler/park/inject stay off (b) unless they
became public.

Regenerate by reading `src/lib.rs`, `src/domain/mod.rs`, `src/runtime/mod.rs`,
`src/testing/mod.rs`. If a diagram would mention a name that is not in those
files, delete that name.

### 2. Before vs after when structure or public API changed

If `src/` module graph or `lib.rs` re-exports changed vs `main`, the PR **must**
include the **same two diagrams for `main` (before)** and **this branch (after)**.

Greenfield / first baseline: ship current diagrams only and state they are the
baseline (already true in `docs/ARCHITECTURE.md`).

### 3. User-behavior diffs — always, even when unchanged

Every PR uses this sentence shape, covering **start / wait / cancel / resume /
fail / retry / inspect**:

> When a caller runs X, it used to Y. Now it Z.

If a refactor has no observable change, say that explicitly (`Y` and `Z` are
the same). Do not omit a verb because “it was only a rename.”

## Also required (existing)

- `cargo test -- --test-threads=1` green. Do not skip or weaken adversarial,
  scenario, stress, or resilience tests.
- Coverage: `just coverage` / `./scripts/coverage.sh`. Kernel `src/` lines
  100%; `coverage/BASELINE` allowlist stays empty.
- One reason per commit. Imperative, module prefix.
- `FailSubtree` / `Join::AllDone` stay definition-only opt-in.
