# Examples (not the kernel)

These binaries are **not** part of the `keel-rt` library. They exist so a *consumer* can set `#[global_allocator]` and measure — the library itself never does.

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
