# Changelog

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
