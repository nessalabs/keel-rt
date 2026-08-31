# Changelog

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
