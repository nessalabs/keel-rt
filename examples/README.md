# Examples (not the kernel)

These binaries are **not** the `keel-rt` library. `keel-rt` has no `jemalloc`
feature and never sets `#[global_allocator]`.

```bash
# System allocator, current_thread (Phase 1 default)
cargo run --release --example kernel_benches

# Jemalloc comparison (separate unpublished package, not a crate feature)
cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml

# Multi-thread Tokio experiment (does not change Runtime::start)
cargo run --release --example kernel_benches -- --multi-thread
cargo run --release --manifest-path benches/jemalloc_compare/Cargo.toml -- --multi-thread

# Allocation count on 256-wide join (system allocator)
cargo run --release --example alloc_count
```

Or: `./scripts/jemalloc-benches.sh` (and pass `--multi-thread` for the experiment).

Results: [`benches/JEMALLOC.md`](../benches/JEMALLOC.md), [`benches/BASELINE.md`](../benches/BASELINE.md).
