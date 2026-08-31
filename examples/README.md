# Examples (not the kernel)

These binaries are **not** the `keel-rt` library. Allocator choice belongs to
the **consumer binary**. Default `keel-rt` never sets `#[global_allocator]`.
The optional `jemalloc` feature exists only so *this* example can install
jemalloc when you pass `--features jemalloc` (or enable it in *your*
`Cargo.toml` for this target). Production binaries should depend on
`tikv-jemallocator` themselves — see [`benches/JEMALLOC.md`](../benches/JEMALLOC.md).

```bash
# System allocator, current_thread (Phase 1 default)
cargo run --release --example kernel_benches

# Jemalloc (opt-in feature)
cargo run --release --example kernel_benches --features jemalloc

# Multi-thread Tokio experiment (does not change Runtime::start)
cargo run --release --example kernel_benches -- --multi-thread
cargo run --release --example kernel_benches --features jemalloc -- --multi-thread

# Allocation count on 256-wide join (system allocator; do not combine with jemalloc)
cargo run --release --example alloc_count
```

Results: [`benches/JEMALLOC.md`](../benches/JEMALLOC.md), [`benches/BASELINE.md`](../benches/BASELINE.md).
